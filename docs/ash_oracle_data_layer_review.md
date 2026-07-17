# Building an Oracle Data Layer for Ash Framework: In-Depth Review

**Date:** 2026-07-17
**Scope:** Source-level review of `ash_postgres`, `ash_sql`, `ash_sqlite` (as the template for a second SQL dialect), and `jamdb_oracle`, with a feasibility assessment and roadmap for an `AshOracle` data layer.

---

## 1. Verdict (TL;DR)

**Yes — an AshOracle data layer is feasible, and `jamdb_oracle` is the right foundation for it.**

- The Ash team already extracted everything dialect-agnostic into a shared library, **`ash_sql`** (~11k lines: expression compiler, joins, aggregates, sorting, atomics). A new SQL data layer does *not* reimplement query building — it implements a comparatively small dialect contract (`AshSql.Implementation`, ~20 callbacks) plus the `Ash.DataLayer` behaviour glue.
- There is direct precedent: **`ash_sqlite`** (~2.8k lines of data-layer + dialect code) and **`ash_mysql`** (officially described as "derived from AshSqlite") were both built this way. AshOracle would be the third such derivation.
- **`jamdb_oracle`** is a pure-Erlang TNS wire-protocol driver (no Oracle Instant Client/OCI needed) with a real `Ecto.Adapters.SQL` adapter: CTEs, window functions, `OFFSET/FETCH`, locks, savepoints (→ SQL Sandbox works), DDL for migrations, `RETURN ... INTO` for returning clauses. It is actively maintained (v0.5.12, `ecto_sql ~> 3.12`, commits through April 2026).
- The two hard gaps are **upserts** (jamdb silently ignores Ecto's `on_conflict`; Oracle needs `MERGE`) and **no lateral joins** in the adapter's query builder. Both have well-understood workarounds: declare the capabilities off in v1 (exactly what ash_sqlite does) and add `MERGE`-based upserts in v2.
- Realistic effort: a useful v1 (CRUD, filters, sorts, calculations, migrations) is **6–10 weeks** of focused work by someone fluent in Ash internals; full ash_postgres parity (aggregates, atomics, bulk actions, multitenancy) is a multi-month project. Judge by ash_mysql: even with the sqlite skeleton it self-describes as alpha.

---

## 2. How AshPostgres Is Actually Architected

This is the part worth studying closely, because the architecture is what makes an Oracle port tractable.

### 2.1 Three-layer design

```
┌────────────────────────────────────────────────────────────┐
│ ash (core)                                                 │
│   Ash.DataLayer behaviour + capability negotiation (can?/2)│
├────────────────────────────────────────────────────────────┤
│ ash_sql (shared SQL engine, ~11k lines, dialect-agnostic)  │
│   AshSql.Expr (4,039 loc)  – Ash expr → Ecto dynamics      │
│   AshSql.Aggregate (2,706) – relationship aggregates       │
│   AshSql.Join (1,483)      – relationship joins            │
│   AshSql.Query/Sort/Distinct/Atomics/Calculation/Bindings  │
│   AshSql.Implementation    – the dialect behaviour         │
├────────────────────────────────────────────────────────────┤
│ ash_postgres / ash_sqlite / ash_mysql (dialect packages)   │
│   <Dialect>.DataLayer      – Ash.DataLayer impl + Spark DSL│
│   <Dialect>.SqlImplementation – AshSql.Implementation impl │
│   <Dialect>.Repo           – Ecto repo wrapper             │
│   Migration generator + mix tasks                          │
└────────────────────────────────────────────────────────────┘
```

Concrete sizes from the current repos:

| Component | ash_postgres | ash_sqlite |
|---|---:|---:|
| `data_layer.ex` (Ash.DataLayer impl + DSL) | 4,590 loc | 2,150 loc |
| `sql_implementation.ex` (dialect contract) | 517 loc | 632 loc |
| migration generator | ~6,959 loc | ~3,639 loc |
| ecto deps | `ecto_sql ~> 3.13`, `ash_sql ~> 0.6` | `ecto_sql ~> 3.13`, `ash_sql ~> 0.2` |

The lesson: **the sqlite dialect package is roughly half the size of postgres** because it turns off what its engine can't do — and Ash core degrades gracefully (runs those features in Elixir instead of SQL) whenever `can?/2` returns `false`.

### 2.2 Capability negotiation is the escape hatch

`Ash.DataLayer.can?/2` is queried per feature. ash_postgres answers `true` to essentially everything: lateral joins, `{:aggregate, :count | :sum | :first | :list | ...}`, `:aggregate_relationship`, `:distinct`, `:distinct_sort`, `{:atomic, :update|:upsert|:create}`, `:upsert`, `:bulk_create`, `:update_many`, `{:lock, :for_update}`, `:transact`, `:multitenancy`, `:combine` (union/intersect), `:expr_error`, etc.

ash_sqlite ships and is useful while answering `false` to: `:transact`(!), `{:lock, _}`, `{:aggregate, _}`, `:aggregate_filter`, `:aggregate_sort`, `{:aggregate_relationship, _}`, `{:lateral_join, _}`, `:distinct`, `:distinct_sort`, `:multitenancy`, `:async_engine`.

**This is the design that makes AshOracle shippable incrementally.** You do not need Oracle `MERGE`, lateral joins, or JSON aggregation on day one — you need an honest `can?/2`.

### 2.3 The dialect contract: `AshSql.Implementation`

The entire dialect surface that `ash_sql` needs (from `ash_sql/lib/implementation.ex`):

```elixir
@callback table(resource) :: String.t()
@callback schema(resource) :: String.t() | nil
@callback repo(resource, :mutate | :read) :: module
@callback expr(query, ash_expr, bindings, embedded?, acc, type) ::
            {:ok, dynamic, acc} | {:error, term} | :error   # dialect-specific rewrites
@callback parameterized_type(type, constraints) :: term
@callback storage_type(resource, field) :: term | nil
@callback determine_types(mod, args, returns) :: ...
@callback type_expr(expr, type) :: term                     # how to CAST
@callback ilike?() :: boolean                               # PG-only ILIKE
@callback equals_any?() :: boolean                          # PG `= ANY(?)`
@callback array_overlap_operator?() :: boolean              # PG `&&`
@callback multicolumn_distinct?() :: boolean
@callback list_aggregate(resource) :: String.t() | nil      # "array_agg" on PG
@callback strpos_function() :: String.t()                   # "strpos" / "instr"
@callback simple_join_first_aggregates(resource) :: [atom]
@callback manual_relationship_function() :: atom
@callback manual_relationship_subquery_function() :: atom
@callback require_ash_functions_for_or_and_and?() :: boolean
@callback require_extension_for_citext() :: {true, String.t()} | false
```

`use AshSql.Implementation` provides PG-ish defaults; a dialect overrides what differs and pattern-matches `expr/6` to rewrite specific Ash expression nodes into dialect SQL fragments (ash_sqlite does this for `ILike` → `LOWER(...) LIKE`, `StringLength` → `LENGTH(...)`, `GetPath` → `json_extract(...)`, map comparisons via JSON serialization, and `IN` handling). This is exactly the pattern AshOracle would follow.

Notably, `ash_sql` itself contains very few hardcoded PG-isms in raw fragments — a handful of `::jsonb` casts, `array_agg(...)`/`any_value(...)` in the aggregate builder (only reached when aggregates are enabled), and `&&` for array overlap (gated behind `array_overlap_operator?/0`). The dialect knobs cover almost everything else.

### 2.4 What else ash_postgres ships (the "everything else" you'd port over time)

- **Spark DSL extension** — the `postgres do ... end` block: `table`, `schema`, `repo` (incl. read replicas via `repo/2`), `references` (FK behavior/`match_with`), `check_constraints`, `custom_indexes`, `custom_statements`, `identity_index_names`, `polymorphic?`, `storage_types`, `migration_types`, `migration_defaults`, `skip_unique_indexes`.
- **Migration generator (~7k loc)** — snapshot-based: serializes each resource to JSON snapshots in `priv/resource_snapshots`, diffs snapshots between runs, and emits Ecto migrations (with `--check`, `--squash`, up/down phases, index/constraint ordering). This is the single most-loved AshPostgres feature and the largest porting line-item.
- **Repo behaviour** (`AshPostgres.Repo`) — `installed_extensions/0`, `min_pg_version/0` (feature-gates SQL generation at runtime — the same mechanism AshOracle should use for 19c vs 21c vs 23ai), `create?`/`drop?` overrides, `on_transaction_begin`, error → `Ash.Error` translation (constraint violations → `InvalidAttribute`).
- **Multitenancy** — `:attribute` strategy (tenant column) and `:context` strategy (PG schema per tenant, with `manage_tenant` templates creating/renaming schemas).
- **Upserts / bulk** — built on Ecto `insert_all` + `on_conflict`, `RETURNING`, and lateral joins for `update_many`.
- **Types & extensions** — `citext`, `ltree`, `tsvector`/`tsquery`, `vector` (pgvector), custom aggregates, custom extensions with install hooks.
- **Igniter installer** (`mix igniter.install ash_postgres`) and mix tasks (`ash_postgres.create/migrate/rollback/generate_migrations/squash_snapshots/gen.resources` — the last one reverse-engineers resources *from* an existing database, which is very relevant for Oracle brownfield adoption).

---

## 3. jamdb_oracle Assessment

**Repo:** [erlangbureau/jamdb_oracle](https://github.com/erlangbureau/jamdb_oracle) · MIT · v0.5.12 · `elixir ~> 1.11`, `ecto_sql ~> 3.12` · last commit 2026-04-09 · ~784 commits.

### 3.1 What it is

A **pure Erlang/Elixir implementation of the Oracle TNS wire protocol** (`jamdb_oracle_tns_encoder/decoder.erl`, `jamdb_oracle_conn.erl`, incl. the O5LOGON crypto handshake) plus a DBConnection driver (`Jamdb.Oracle`) and an Ecto adapter (`Ecto.Adapters.Jamdb.Oracle`). No Oracle Instant Client, no OCI, no NIFs, no ODBC — which means trivially deployable in releases/containers and no NIF-crash risk to the BEAM. This is the same architectural choice as `postgrex`/`myxql`, and it is the property that makes it attractive for Ash.

### 3.2 What the Ecto adapter genuinely supports (verified in source)

- `use Ecto.Adapters.SQL, driver: Jamdb.Oracle` — full `Ecto.Adapters.SQL.Connection` implementation: `all/1`, `update_all/1`, `delete_all/1`, `insert/7`, `update/6`, `delete/4`, `explain_query` (via `DBMS_XPLAN.DISPLAY`).
- **Query builder covers:** CTEs (`WITH`), window functions, `DISTINCT`, combinations (`UNION`/`INTERSECT`/etc.), joins, `ORDER BY`, locks (`FOR UPDATE`), and pagination via ANSI **`OFFSET n ROWS` / `FETCH NEXT n ROWS ONLY`** (⇒ effectively requires **Oracle 12c+**).
- **Returning:** `RETURN col INTO :out` bind-based returning for insert/update/delete — enough for Ash's `returning` needs on single-row mutations.
- **Transactions & sandbox:** `handle_begin/handle_commit/handle_rollback` implement both `:transaction` and `:savepoint` modes ⇒ `Ecto.Adapters.SQL.Sandbox` (concurrent tests) works.
- **Migrations:** `execute_ddl` for create/alter/drop table/index/constraint/rename ⇒ standard Ecto migrations run, which is what the ported Ash migration generator would emit.
- **Driver extras** useful for escape hatches: stored procedures/functions, ref cursors, batch DML, row prefetch, named binds.
- Type loaders/dumpers: `:binary_id ↔ Ecto.UUID` (RAW(16)), booleans as 0/1, maps/embeds as JSON text, `:float` from `Decimal`.

### 3.3 Gaps and risks (the honest list)

| Gap | Impact on Ash | Mitigation |
|---|---|---|
| `insert/7` **ignores `on_conflict`** (no `MERGE` generation) | Native upserts (`can? :upsert`, `{:atomic, :upsert}`) unavailable via Ecto path | v1: `can?(_, :upsert) → false`. v2: generate `MERGE INTO ... USING (SELECT :binds FROM dual)` in AshOracle (own `insert/7` override) or upstream a PR to jamdb |
| No lateral join / `CROSS APPLY` in query builder | No `can?({:lateral_join, _})` — Ash falls back to separate queries for relationship loading with limits | Same posture as ash_sqlite (ships that way). Oracle 12c+ does support `LATERAL`/`CROSS APPLY`, so this is upstreamable later |
| `supports_ddl_transaction?` = `false`, `lock_for_migrations` is a no-op | Migrations aren't transactional (an Oracle reality — DDL implicitly commits) and there's no migration lock | Document: run migrations from one node; consider `DBMS_LOCK`-based advisory lock later |
| No native arrays in Oracle; adapter's `{:array,_}` loader is string-decoding | Ash uses `{:array, _}` heavily (tags, union storage) | Map arrays to JSON (`JSON` type on 21c+, `CLOB ... IS JSON` on 19c) in `storage_type/2` + JSON-based expr rewrites — analogous to how ash_sqlite JSON-encodes maps |
| Booleans: Oracle < 23ai has no `BOOLEAN` in SQL | Adapter maps to `NUMBER(1)`/`CHAR` 0/1; comparisons must cast | Handle in `type_expr/2` + `expr/6`; native `BOOLEAN` when `min_oracle_version >= 23` |
| `pool_size` defaults to 1; driver less battle-tested than postgrex; effectively one maintainer (erlangbureau) | Operational risk under heavy concurrency; CLOB/charset edge cases documented in README | Load-test early (Phase 0); keep the driver boundary thin so a future driver (e.g., ODPI-C-based `oranif`) could be swapped |
| `query_many` unsupported | Minor — Ash doesn't require it | — |

### 3.4 Alternatives considered

- **`oranif`** (Erlang NIF over Oracle ODPI-C): closer to C-driver performance, but requires Instant Client, has NIF-crash blast radius, and has **no Ecto adapter** — you'd write both an Ecto adapter *and* the Ash layer. Not worth it as a starting point.
- **ODBC/`ecto_odbc`-style bridges:** effectively unmaintained for Ecto 3.x.

**Conclusion: jamdb_oracle is the only maintained, pure-BEAM, Ecto-3.x Oracle path. Use it.** The `ecto_sql` constraints are compatible (`~> 3.12` intersects `~> 3.13`).

---

## 4. Blueprint for `ash_oracle`

**Strategy: fork the `ash_sqlite` skeleton (not ash_postgres), swap the adapter to `Ecto.Adapters.Jamdb.Oracle`, and write an Oracle `SqlImplementation`.** This is precisely the ash_mysql playbook.

### 4.1 Package layout

```
ash_oracle/
  lib/
    ash_oracle.ex                 # public API
    data_layer.ex                 # @behaviour Ash.DataLayer + Spark DSL: `oracle do ... end`
    data_layer/info.ex            # DSL introspection
    sql_implementation.ex         # use AshSql.Implementation (the real dialect work)
    repo.ex                       # use Ecto.Repo, adapter: Ecto.Adapters.Jamdb.Oracle
                                  #   + min_oracle_version/0 feature gate (19c/21c/23ai)
    types/                        # RAW(16) uuid, JSON-backed arrays/maps
    migration_generator/          # ported from ash_sqlite (~3.6k loc to adapt)
    mix/tasks/                    # ash_oracle.create/migrate/rollback/generate_migrations
  deps: {:ash, "~> 3.x"}, {:ash_sql, "~> 0.6"}, {:ecto_sql, "~> 3.13"},
        {:jamdb_oracle, "~> 0.5"}
```

### 4.2 Dialect callback mapping (the core of the work)

| `AshSql.Implementation` callback | Oracle answer |
|---|---|
| `ilike?/0` | `false` → rewrite `ILike` as `LOWER(x) LIKE LOWER(y)` in `expr/6` (or rely on `NLS_COMP=LINGUISTIC` docs) |
| `strpos_function/0` | `"INSTR"` |
| `equals_any?/0` | `false` (no `= ANY(array)`) → ash_sql emits `IN` |
| `array_overlap_operator?/0` | `false` → JSON-based rewrite when arrays land |
| `multicolumn_distinct?/0` | `true` (`DISTINCT` over full row is fine; `DISTINCT ON` doesn't exist — keep `distinct_sort` off) |
| `list_aggregate/1` | `"JSON_ARRAYAGG"` (21c+; `CAST(COLLECT(...))` fallback) — or return `nil` in v1 to disable list aggregates |
| `type_expr/2` | `CAST(? AS <oracle type>)`; special-case boolean (`CASE WHEN ? THEN 1 ELSE 0 END` pre-23ai) and ci-strings (`NLSSORT`/`LOWER`) |
| `parameterized_type/2` / `storage_type/2` | see type table below |
| `expr/6` overrides | `ILike`, `StringLength`→`LENGTH`, `StringTrim`→`TRIM`, `GetPath`→`JSON_VALUE/JSON_QUERY`, `Fragment` passthrough, boolean literal handling, `IN` handling (Oracle's 1,000-item `IN` limit → chunk into OR groups), empty-string-vs-NULL (`'' IS NULL` in Oracle! see §4.5) |
| `manual_relationship_function/0` | `:ash_oracle_join` |
| `require_extension_for_citext/0` | `false` (document collation instead) |

### 4.3 Type mapping

| Ash/Ecto type | Oracle 19c | Oracle 21c/23ai |
|---|---|---|
| `:uuid` / `binary_id` | `RAW(16)` (adapter already round-trips via `Ecto.UUID`) | same |
| `:string` | `VARCHAR2(4000)` / `CLOB` beyond | same |
| `:integer` | `NUMBER(19,0)` / `INTEGER` | same |
| `:decimal`, `:float` | `NUMBER`, `BINARY_DOUBLE` | same |
| `:boolean` | `NUMBER(1)` + check constraint | native `BOOLEAN` (23ai) |
| `:utc_datetime[_usec]` | `TIMESTAMP WITH TIME ZONE` | same |
| `:naive_datetime` | `TIMESTAMP` / `DATE` | same |
| `:map`, embeds | `CLOB CHECK (col IS JSON)` | native `JSON` type |
| `{:array, t}` | JSON array in `CLOB IS JSON` | `JSON` |
| ci_string | `VARCHAR2` + `LOWER()` functional index | same (or 23ai `COLLATE`) |

### 4.4 v1 capability matrix (mirror ash_sqlite, plus what Oracle gives for free)

`true`: create/read/update/destroy, filter, sort, limit/offset (12c+ `FETCH`), joins, expression calculations, query aggregates (count/sum/min/max/avg/exists at query level), bulk_create, upsert **off**, `:transact` **true** (unlike sqlite — Oracle + DBConnection handle this fine), `{:lock, :for_update}` **true**, composite PKs, `:timeout`.

`false` initially: `{:lateral_join,_}`, `{:aggregate_relationship,_}`/`:aggregate_filter`/`:aggregate_sort` (until JSON_ARRAYAGG work lands), `:distinct_sort`, `:upsert`/`{:atomic, :upsert}` (until MERGE), `:multitenancy` (then `:attribute` strategy first; `:context` → `ALTER SESSION SET CURRENT_SCHEMA` later), `:async_engine` initially (validate pool behavior first).

### 4.5 Oracle-specific landmines to design for up front

1. **`'' IS NULL`** — Oracle treats empty string as NULL in `VARCHAR2`. Ash's `nil`-vs-`""` semantics must be papered over in `expr/6` and documented. This is the single most insidious behavioral difference.
2. **Identifier length** — 128 bytes on 12.2+, but only 30 on older versions; Ash generates long index/constraint names (`_unique_index` suffixes) → the migration generator needs a deterministic truncation/hashing scheme.
3. **Upper-cased unquoted identifiers** — decide once: quote everything (preserve case, what jamdb's `quote_name` does) and be consistent between the query layer and migration generator.
4. **No `LIMIT` in subqueries pre-12c** — non-issue if 12c+ is the floor. **Recommend: minimum supported = 19c** (the oldest version under Premier Support), feature-gate 21c/23ai via `min_oracle_version/0`.
5. **Sequences vs identity** — use `GENERATED BY DEFAULT AS IDENTITY` (12c+) for integer PKs in the migration generator; `RETURN id INTO` already works in jamdb.
6. **DDL auto-commits** — no transactional migrations, ever. Snapshot-diff correctness matters more (partial-failure recovery docs needed).
7. **CI** — `gvenzl/oracle-free` (23ai) and `gvenzl/oracle-xe` (21c) Docker images make GitHub Actions testing practical; run the matrix against both.

### 4.6 Phased roadmap

| Phase | Deliverable | Est. |
|---|---|---|
| **0 — Spike** | Repo skeleton (forked ash_sqlite), jamdb wired, CI with oracle-free container; prove: connect, basic resource CRUD, filter/sort/limit, sandbox tests | 1–2 wks |
| **1 — Core read/write** | Full `expr/6` coverage for Ash builtin functions/operators; type mapping incl. JSON maps; error → `Ash.Error` translation (ORA-00001 → InvalidAttribute etc.); `:transact`, locks | 3–4 wks |
| **2 — Migrations** | Port migration generator + mix tasks (`create/migrate/rollback/generate_migrations`); identity PKs, FK references, unique indexes with name truncation | 3–4 wks |
| **3 — Power features** | `MERGE`-based upserts + atomics; relationship aggregates via `JSON_ARRAYAGG`; attribute multitenancy; bulk update/destroy | 4–6 wks |
| **4 — Parity extras** | Context multitenancy (`CURRENT_SCHEMA`), lateral joins (`CROSS APPLY`) upstreamed to jamdb, 23ai vector type (`VECTOR`) for AshAi, `gen.resources` from Oracle data dictionary (`ALL_TAB_COLUMNS`) — brownfield gold | ongoing |

Where work should land: dialect behavior in `ash_oracle`; anything about SQL *generation from Ecto queries* (MERGE for `on_conflict`, `CROSS APPLY` joins) ideally upstreamed to `jamdb_oracle` so plain-Ecto users benefit too and the fork surface stays small.

---

## 5. Sources reviewed

- [ash-project/ash_postgres](https://github.com/ash-project/ash_postgres) — `data_layer.ex`, `sql_implementation.ex`, `migration_generator/`, `mix.exs`
- [ash-project/ash_sql](https://github.com/ash-project/ash_sql) — `implementation.ex`, `expr.ex`, `aggregate.ex`, `join.ex`
- [ash-project/ash_sqlite](https://github.com/ash-project/ash_sqlite) — the second-dialect template
- [ash-project/ash_mysql](https://github.com/ash-project/ash_mysql) — precedent: "derived from AshSqlite"
- [erlangbureau/jamdb_oracle](https://github.com/erlangbureau/jamdb_oracle) — `jamdb_oracle_ecto.ex`, `jamdb_oracle_query.ex`, `jamdb_oracle_sql.ex`, `src/*.erl`, README

*Note: this review is independent of the Lotus codebase in this repository; it was produced here to preserve the research on the task branch.*
