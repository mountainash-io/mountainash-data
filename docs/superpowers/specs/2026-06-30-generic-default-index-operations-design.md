# Generic-Default Index Operations — Design

**Date:** 2026-06-30
**Status:** Design — not yet implemented
**Branch:** `feature/generic-default-index-operations` (off `develop`)
**Predecessor:** [Generic-Default Dialect Operations](2026-06-29-generic-default-dialect-operations-design.md) (PR #91, merged) — reuses its `_render.py` primitives.

## 1. Goal & frame

Do for `create_index` / `drop_index` / `index_exists` exactly what PR #91 did for
`upsert` / `rename_table`: replace the duckdb-family-only capability **hooks** with
**generic-default rendering across the registry**, driven by a structured per-dialect
capability descriptor, retiring `duckdb_family_create_index` /
`duckdb_family_drop_index`.

This is a package-level capability — the complete, correct shape of index DDL for any
conventional-RDBMS consumer of `mountainash-data` — not a helper for one caller.

### Current state (the problem)

- `IbisBackend.create_index` / `create_unique_index` / `drop_index` / `index_exists`
  exist but are **hook-required**: they raise `NotImplementedError` unless the
  `DialectSpec` carries the relevant hook.
- Only **sqlite / duckdb / motherduck** are wired, via `duckdb_family_create_index` /
  `duckdb_family_drop_index` (raw `cur.execute`, string-built SQL). The other ~17
  dialects have no index support.
- `drop_index(table_name=…)` types `table_name` as **optional**, which is wrong for the
  table-scoped dialects (MySQL/SingleStore/T-SQL) where it is mandatory.
- `duckdb_family_create_index` **warns and downgrades** an unsupported `index_type` to
  BTREE, and passes `where_condition` through to DuckDB — which **does not support
  partial indexes at all** (latent invalid-SQL bug).

## 2. Scope

**In scope:** conventional secondary B-tree index DDL — `CREATE [UNIQUE] INDEX`,
`DROP INDEX`, and existence introspection (`index_exists`) — across every dialect that
has that concept.

**Out of scope (`index_caps=None` → `NotImplementedError`, documented in the support
matrix):**

- Stores with no user-facing secondary index: **snowflake, bigquery, redshift, trino,
  exasol, impala, druid, pyspark, databricks**.
- **clickhouse** data-skipping indexes (`ALTER TABLE … ADD INDEX … TYPE … GRANULARITY`)
  — a wholly different shape.
- **risingwave, materialize** streaming-arrangement indexes — verified as
  materialized-view-backed / in-memory arrangements (`USING arrangement`), not secondary
  B-trees. May be modelled as their own family in a later spec.

`create_index_hook` / `drop_index_hook` fields **remain** on `DialectSpec` as an
override-first escape hatch (mirrors how `upsert_hook` was kept), but the
`duckdb_family_*` registrations and the functions themselves are deleted at cutover.

## 3. Capability model (`backends/ibis/dialects/_registry.py`)

```python
class DropScope(str, enum.Enum):
    SCHEMA_GLOBAL = "schema_global"   # DROP INDEX name
    TABLE_SCOPED  = "table_scoped"    # DROP INDEX name ON tbl


@dataclass(frozen=True)
class IndexCapability:
    drop_scope: DropScope
    partial: bool                     # supports a WHERE filter (partial / filtered index)
    native_if_not_exists: bool        # engine has CREATE INDEX IF NOT EXISTS
    native_if_exists: bool            # engine has DROP INDEX IF EXISTS
    index_types: frozenset[str]       # valid USING <type> values; empty = no USING clause


# DialectSpec gains:
index_caps: t.Optional[IndexCapability] = None   # None = unsupported -> NotImplementedError
```

`native_if_not_exists` and `native_if_exists` are **two separate booleans** because they
genuinely diverge: SQL Server has `DROP INDEX IF EXISTS` but no
`CREATE INDEX IF NOT EXISTS`.

### Dispatch order (on the backend)

```
create_index_hook present?  -> call hook (override)
elif index_caps is not None  -> generic renderer
else                         -> NotImplementedError(dialect)
```
Same three-way shape as upsert.

### Invariant

A dialect with `index_caps is not None` **MUST** also set `get_index_exists_sql`.
Emulated idempotency (§6) depends on existence introspection, so a dialect that supports
indexes must be able to introspect them. Enforced by a registry-consistency unit test.

## 4. Support matrix (verified against official vendor docs, 2026-06-30)

| Dialect | `drop_scope` | `partial` | `native_if_not_exists` / `native_if_exists` | `index_types` | Test tier |
|---|---|---|---|---|---|
| sqlite | SCHEMA_GLOBAL | True | True / True | ∅ | live |
| duckdb | SCHEMA_GLOBAL | **False** | True / True | ∅ | live |
| motherduck | SCHEMA_GLOBAL | False | True / True | ∅ | render-only |
| postgres | SCHEMA_GLOBAL | True | True / True | btree,hash,gist,gin,brin,spgist | live |
| mysql¹ | TABLE_SCOPED | False | **False / False** (emulate) | btree,hash | live (MariaDB) |
| singlestoredb | TABLE_SCOPED | False | False / False (emulate) | btree,hash | render-only |
| mssql | TABLE_SCOPED | **True** (filtered) | **False / True** | ∅ | render-only |
| oracle² | SCHEMA_GLOBAL | False | False / False (emulate) | ∅ | render-only |
| snowflake, bigquery, redshift, trino, clickhouse, databricks, exasol, impala, materialize, risingwave, druid, pyspark | — | — | — | — | `None` (NotImplementedError) |

¹ The single `"mysql"` dialect serves **both** MySQL and MariaDB servers. MariaDB has
native `IF [NOT] EXISTS`; **MySQL 8.0/8.4 does not**. Because one dialect must be correct
against both engines, the dialect emulates (treats native as `False`/`False`). The live
test env is MariaDB, so the emulation code path is genuinely exercised (we force the
dialect path, not the server's latent capability).

² Oracle gained `IF [NOT] EXISTS` only in Release 19.28+/23ai. To avoid depending on the
server patch level, the dialect emulates. Oracle's "partial index" is partition-level
`INDEXING PARTIAL`, **not** a `WHERE` filter → `partial=False`.

**Sources:** sqlite.org/lang_createindex + partialindex; duckdb.org CREATE INDEX;
dev.mysql.com 8.4 + mariadb.com DROP/CREATE INDEX; docs.singlestore.com;
learn.microsoft.com DROP INDEX + filtered-indexes; docs.oracle.com 19/23/26 CREATE/DROP
INDEX; risingwave.com + materialize.com CREATE INDEX.

> Note: `partial` for sqlite/postgres is taken from docs; `index_types` for postgres is
> the documented method set. The live-tier dialects (sqlite, duckdb, postgres, MariaDB)
> have their flags re-confirmed by a probe step in the plan before the value is committed
> — the same empirical discipline PR #91 used (which caught duckdb-no-MERGE).

## 5. Render path

### 5.1 New module `backends/ibis/_index.py`

Index builders + generic dispatchers live in their own module rather than growing the
already-large `operations.py` (files that change together live together). Pure builders
take pre-computed, already-validated parts so registry golden tests render without a live
connection:

```python
def build_create_index_sql(
    *, dialect, target, index_name, cols, unique,
    index_type, guard, where_sql,
) -> str: ...

def build_drop_index_sql(
    *, dialect, drop_scope, index_name, target, guard,
) -> str: ...
```

- `target` is the already-qualified, already-quoted table reference (via
  `_render.qualified_name` + `quote_identifier`).
- `guard` is the pre-resolved native clause: `"IF NOT EXISTS "` / `"IF EXISTS "` / `""`
  (empty when emulating — the precheck in §6 supplies idempotency instead).
- `index_type` is rendered as the dialect's `USING <type>` only when non-empty.

Generic dispatchers orchestrate validation, emulation precheck, predicate compilation,
and execution:

```python
def _generic_create_index(con, table_name, columns, *, index_name, unique,
                          index_type, where, database, if_not_exists) -> None: ...
def _generic_drop_index(con, index_name, *, table_name, database, if_exists) -> None: ...
def _generic_index_exists(con, index_name, *, table_name, database) -> bool: ...
```

Identifiers (index name, columns, table) are validated with the existing
`_validate_simple_identifier` allowlist (`_SIMPLE_IDENTIFIER_RE`) and quoted via
`_render.quote_identifier`. `_IBIS_TO_SQLGLOT` maps the ibis backend name to the sqlglot
dialect (mssql→tsql, singlestoredb→singlestore, motherduck→duckdb).

### 5.2 Partial-index `WHERE` predicate (new single-relation path in `_render.py`)

The partial `WHERE` is expressed as an **ibis-expression predicate** — consistent with
the `update_condition` redesign in PR #91 — not a raw SQL string. A partial-index filter
is a single-table predicate over the indexed table's own columns (no join), so it needs a
simpler compile path than upsert's two-sentinel join extraction:

```python
IndexPredicate = t.Callable[["ibis.Table"], "ibis.BooleanColumn"]

def compile_index_predicate(con, schema, table_name, predicate) -> str:
    """Bind one ibis table at `schema`, apply `predicate`, validate, and render
    the boolean expression UNQUALIFIED (partial-index WHERE references bare
    columns), for the connection's dialect."""
```

- Binds one ibis table at the table's schema (introspected from the live connection, or
  passed explicitly in render-only tests).
- Reuses `_render.validate_predicate` (rejects aggregate / window / subquery — which also
  matches SQLite's documented partial-index restriction: no subqueries, no other tables,
  no non-deterministic functions).
- Renders the boolean **unqualified** (strips the table alias) — a partial-index `WHERE`
  references bare column names.

## 6. Idempotency & emulation

The convenient defaults stay: `create_index(if_not_exists=True)`,
`drop_index(if_exists=True)`.

- **Native** (sqlite, duckdb, motherduck, postgres; plus mssql for `DROP`): render the
  engine's native `IF [NOT] EXISTS` clause.
- **Emulated** (mysql, singlestoredb, oracle; plus mssql for `CREATE`): when the dialect
  lacks the native clause and the caller asked for the guard, run `index_exists` first and
  **skip** the CREATE/DROP if the desired state already holds. This is the same
  prove-state-then-act shape as the MySQL upsert preflight.

**TOCTOU:** emulation is two statements (check, then act), so a concurrent session can
change the index between them, producing a duplicate-index (CREATE) or no-such-index
(DROP) error. This window is **accepted and documented** — DDL is rare and typically
single-writer, the failure is a clear, bounded, re-runnable error (not data corruption),
and transactional DDL is unavailable on several of these engines (MySQL/Oracle auto-commit
DDL). No catch-and-swallow, no lock wrapping.

`index_exists` introspection SQL interpolates the index/table/schema names as **escaped
string literals** (`sqlglot exp.Literal.string`) and validates them through the identifier
allowlist first — identical hardening to the MySQL upsert preflight (the PR #91
final-review injection fix).

## 7. Public API (clean break — pre-release, no downstream consumers)

```python
IndexPredicate = Callable[[ibis.Table], ibis.BooleanColumn]

def create_index(
    self, table_name, columns, *, index_name=None, unique=False,
    index_type=None, where=None, database=None, if_not_exists=True,
) -> IbisBackend: ...

def create_unique_index(  # unchanged — delegates with unique=True
    self, table_name, columns, *, index_name=None, where=None, database=None,
) -> IbisBackend: ...

def drop_index(
    self, index_name, *, table_name=None, database=None, if_exists=True,
) -> IbisBackend: ...
```

Changes from today:
- `where_condition: str | None` → **`where: IndexPredicate | None`** (raw SQL string →
  injection-safe, dialect-portable ibis predicate).
- `index_type` is **validated** against `caps.index_types` (was: warn-and-downgrade).
- `drop_index` requires `table_name` **when the resolved dialect is `TABLE_SCOPED`**.

## 8. Error handling — no silent degradation

| Condition | Behaviour |
|---|---|
| `index_caps is None` | `NotImplementedError(dialect)` |
| `index_type` not in `caps.index_types` | **`ValueError`** (retires the warn-and-downgrade) |
| `where=` predicate but `caps.partial is False` | `ValueError` |
| `drop_scope is TABLE_SCOPED` and `table_name is None` | `ValueError` (fixes today's wrongly-optional param) |
| `if_not_exists` / `if_exists` requested, no native support | **emulate** via `index_exists` precheck (§6) |
| predicate references aggregate / window / subquery | `ValueError` (via `validate_predicate`) |
| identifier fails the allowlist | `ValueError` |

## 9. Testing

- **Golden (render-only):** parametrize over
  `{n: s for n, s in DIALECTS.items() if s.index_caps}` and assert per-dialect statement
  shape — `CREATE [UNIQUE] INDEX` body, the drop-scope clause (`ON tbl` present iff
  `TABLE_SCOPED`), guard present/absent per `native_*`, `USING <type>` rendering,
  partial-`WHERE` only where `partial`, identifier quoting.
- **Live** (sqlite / duckdb / postgres / MariaDB, via the existing `compose.yaml`):
  - create → `index_exists` (True) → drop → `index_exists` (False) round-trip.
  - partial index on **postgres** and **sqlite** (`where=lambda t: t.active == True`).
  - table-scoped drop on **MariaDB** (`DROP INDEX … ON tbl`).
  - **emulated idempotency**: double `create_index(if_not_exists=True)` on MariaDB is a
    no-op (exercises the `mysql`-dialect emulation path); `drop_index(if_exists=True)` of
    an absent index is a no-op.
  - `index_type` validation raises; `where` on duckdb (`partial=False`) raises.
- **Registry-consistency:** the §3 invariant (every `index_caps` dialect has
  `get_index_exists_sql`); every `index_caps` dialect maps to a known sqlglot dialect.
- Fail-closed under `MOUNTAINASH_REQUIRE_LIVE_DB=1`, same as PR #91. TOCTOU is **not**
  tested (accepted window).

## 10. Cutover

- Delete `duckdb_family_create_index` / `duckdb_family_drop_index` and their 3
  registrations (sqlite, duckdb, motherduck); these dialects now flow through the generic
  renderer.
- Remove the `index_type` warn-and-downgrade test.
- Implement `get_index_exists_sql` for the dialects that newly need it for emulation and
  round-trip tests (postgres, mysql, mssql, oracle, singlestoredb) — genuinely per-dialect
  introspection SQL (`pg_indexes`, `information_schema.STATISTICS`, `sys.indexes`,
  `user_indexes`), each literal-escaped and identifier-validated.

## 11. File structure

| File | Change |
|---|---|
| `backends/ibis/dialects/_registry.py` | Add `DropScope`, `IndexCapability`, `index_caps` field + per-dialect assignment; delete `duckdb_family_*` registrations; add `get_index_exists_sql` for the new dialects |
| `backends/ibis/_index.py` | **New** — pure builders + generic dispatchers |
| `backends/ibis/_render.py` | Add `compile_index_predicate` (single-relation path) |
| `backends/ibis/operations.py` | Delete `duckdb_family_create_index` / `duckdb_family_drop_index` |
| `backends/ibis/backend.py` | `create_index` / `drop_index` / `index_exists` → hook→generic→NotImplementedError dispatch; table-scoped `table_name` validation; `where` predicate param |
| `tests/test_unit/backends/ibis/test_index_render.py` | **New** — golden + validation |
| `tests/test_unit/backends/ibis/test_index_capability_registry.py` | **New** — invariant + matrix consistency |
| `tests/test_integration/test_index_ops_live.py` | **New** — live round-trip + emulation |

## 12. Out of scope (tracked)

- ClickHouse data-skipping and RisingWave/Materialize streaming indexes — possible future
  per-family specs.
- `list_indexes` generalization (`get_list_indexes_sql` hook) — not required by this work;
  leave as-is.
