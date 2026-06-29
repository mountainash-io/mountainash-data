# Generic-Default Dialect Operations — Design Spec

> **Status:** DESIGN (2026-06-29). Approved scope: build the *complete,
> reusable* write-operation surface for `IbisBackend` — portable `upsert`
> across every upsert family the dialect registry can name, plus a portable
> `rename_table` — not a minimum that unblocks one consumer. Successor to
> `2026-06-27-dialect-aware-add-columns-design.md` (shipped, PR #90); reuses
> and consolidates its sqlglot rendering pattern.

## 1. Purpose

`IbisBackend` exposes write/DDL operations through a `DialectSpec` hook
registry. Two of them are under-supported for no good reason:

- **`upsert`** is registered for only `sqlite`/`duckdb`/`motherduck` (3 of 21
  dialects); every other dialect — including `postgres` — raises
  `NotImplementedError`.
- **`rename_table`** has **zero** registered hooks; it raises for *every*
  dialect.

This package is strategic, reusable infrastructure — physical access to
backend data services — not a wearables helper. The unit of design is the
package's domain responsibility (upsert/rename across the SQL backends it
claims to serve), not any one consumer's current backend. This spec makes
both operations *work everywhere the SQL is expressible*, via generic
defaults rendered through sqlglot — the same engine Ibis itself uses — with
an override hook retained for genuine exceptions.

## 2. The Discriminator Principle

`IbisBackend` capability operations fall into two classes; dispatch must
reflect the class:

- **Uniform / family-uniform SQL** — the statement is the same across a
  family of dialects; only type rendering, identifier quoting, and a bounded
  statement-family choice vary. These get a **generic default** (sqlglot
  rendered) + an **optional override hook**. Default: *works everywhere it
  can be expressed.*
- **Capability-divergent** — the operation does not exist on some engines,
  or reads dialect-specific system catalogs. These stay **hook-required**;
  absent a hook, `NotImplementedError`. Default: *honestly unsupported.*

`upsert` and `rename_table` are the former. `create_index` / `drop_index`
and the index-catalog queries are the latter and are **out of scope** (no
secondary indexes on BigQuery/Snowflake — a generic default would emit DDL
the engine rejects while presenting as supported).

## 3. Goals / Non-Goals

**Goals**
- `rename_table` works on every dialect whose rename is expressible, with no
  per-dialect code (sqlglot renders `sp_rename` for SQL Server, `RENAME` for
  MySQL, `ALTER TABLE … RENAME TO` elsewhere).
- `upsert` works across **three** upsert families — `ON CONFLICT`,
  `MERGE`, and MySQL's `ON DUPLICATE KEY UPDATE` — preserving the full
  existing semantics (composite keys, `conflict_action`, `update_columns`,
  `update_condition`). Public signature unchanged.
- A single shared **rendering-primitives helper** backs `upsert`,
  `rename_table`, and (consolidated) `add_columns`.
- Portable staging: the source frame is materialised as a compiled subquery
  (Ibis's own mechanism), not a backend-specific temp-table registration.
- Live integration tests on `sqlite`, `duckdb`, `postgres`, `mysql`
  (Docker); golden-SQL render assertions for the full registry; a documented
  support matrix.

**Non-Goals**
- `create_index` / `drop_index` / index-catalog queries — stay hook-required.
- A bespoke statement-renderer framework — sqlglot **is** the renderer; we
  build its AST, we do not wrap it in a parallel layer.
- A second hand-maintained type map — types render through the connection's
  own `compiler.type_mapper`, exactly as `add_columns` does.
- Live testing of warehouse engines (Snowflake/BigQuery/MSSQL/…) — covered by
  golden-SQL render assertions, not execution.

## 4. API Surface

### 4.1 `upsert` (signature unchanged)

```python
def upsert(
    self,
    name: str,
    obj: t.Any,                       # frame or ibis Table
    *,
    conflict_columns: list[str] | str,
    update_columns: list[str] | str | None = None,
    conflict_action: str = "UPDATE",  # "UPDATE" | "NOTHING"
    update_condition: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> IbisBackend: ...
```

Fluent. Composite `conflict_columns` is supported (unlike Ibis 12's native
`upsert`, whose `on` is a single column — a key reason we render our own).

### 4.2 `rename_table` (signature unchanged)

```python
def rename_table(self, old_name: str, new_name: str) -> IbisBackend: ...
```

### 4.3 Dispatch (both operations)

```python
# rename_table
hook = self._spec.rename_table_hook
if hook is not None:
    hook(conn._ibis_conn, old_name, new_name)
else:
    _generic_rename_table(conn._ibis_conn, old_name, new_name)
return self

# upsert
hook = self._spec.upsert_hook
if hook is not None:
    hook(conn._ibis_conn, name, obj, conflict_columns=..., ...)
else:
    _generic_upsert(conn._ibis_conn, name, obj, style=self._spec.upsert_style, ...)
return self
```

`_generic_upsert` raises `NotImplementedError` when `style is None`, naming
the dialect — the same honest-unsupported contract the hook path had.

## 5. Architecture

### 5.1 Registry: `upsert_style`

`DialectSpec` gains one field:

```python
class UpsertStyle(str, enum.Enum):
    ON_CONFLICT      = "on_conflict"        # INSERT … ON CONFLICT DO UPDATE/NOTHING
    MERGE            = "merge"              # MERGE INTO … WHEN MATCHED / NOT MATCHED
    ON_DUPLICATE_KEY = "on_duplicate_key"  # INSERT … ON DUPLICATE KEY UPDATE (MySQL family)

@dataclass(frozen=True)
class DialectSpec:
    ...
    upsert_hook: t.Optional[UpsertHook] = None      # override seam (unchanged)
    upsert_style: t.Optional[UpsertStyle] = None    # NEW — generic-default selector
    rename_table_hook: t.Optional[RenameTableHook] = None
    add_columns_hook: t.Optional[AddColumnsHook] = None
```

The existing `upsert_hook` registrations on `sqlite`/`duckdb`/`motherduck`
are **removed**; those dialects instead carry `upsert_style=ON_CONFLICT` and
flow through the generic renderer. `upsert_hook` remains as the override
seam (now used by zero dialects — like `add_columns_hook`), reserved for a
dialect that genuinely needs a quirk.

### 5.2 Rendering-primitives helper (consolidation)

`add_columns` rendered its own quoting inline. Extract the shared primitives
into one new sibling module, `backends/ibis/_render.py` (a flat module
alongside `operations.py`, **not** a restructure of `operations.py` into a
package), with a single responsibility — turn names/types into
dialect-correct SQL fragments off a live connection:

```python
def dialect_of(ibis_conn) -> str: ...                       # ibis_conn.compiler.dialect
def quote_identifier(name: str, dialect) -> str: ...        # exp.to_identifier(name, quoted=True).sql(dialect)
def qualified_name(parts: list[str], dialect) -> str: ...   # ".".join(quote_identifier(p, dialect) …)
def render_type(type_mapper, dtype) -> str: ...             # type_mapper.to_string(dtype) — create_table parity
def compiled_source(ibis_conn, obj) -> str: ...             # ibis.memtable(obj) → ibis_conn.compile(…) subquery SQL
```

`_generic_add_columns`, `_generic_rename_table`, and `_generic_upsert` all
consume these. `add_columns` is migrated to the helper with **no behaviour
change** (the inline `_quote` is replaced by `quote_identifier`); its tests
remain green unchanged.

### 5.3 `_generic_rename_table`

```python
def _generic_rename_table(ibis_conn, old_name: str, new_name: str) -> None:
    _validate_simple_identifier(old_name, kind="old_name")
    _validate_simple_identifier(new_name, kind="new_name")
    dialect = dialect_of(ibis_conn)
    stmt = exp.Alter(
        this=exp.to_table(quote_identifier(old_name, dialect)),
        kind="TABLE",
        actions=[exp.AlterRename(this=exp.to_identifier(new_name, quoted=True))],
    ).sql(dialect=dialect)
    ibis_conn.raw_sql(stmt)
```

sqlglot renders this as `ALTER TABLE … RENAME TO …` for most dialects,
`EXEC sp_rename …` for `tsql`, and `ALTER TABLE … RENAME …` for MySQL —
the *output* is verified by transpile probe (§9). The exact sqlglot
expression classes shown (`exp.Alter` / `exp.AlterRename`) are illustrative
and pinned during implementation against the installed sqlglot (30.x); the
equivalent transpile path (`sqlglot.transpile(<canonical ANSI>, write=…)`) is
an accepted fallback if AST construction is more brittle. No override hooks
required initially.

### 5.4 `_generic_upsert` and the three renderers

```python
def _generic_upsert(
    ibis_conn, name, obj, *, style, conflict_columns, update_columns,
    conflict_action, update_condition, database, schema,
) -> None:
    if style is None:
        raise NotImplementedError(f"Dialect does not support upsert")
    _validate_simple_identifier(name, kind="name")
    # resolve target/conflict/update column sets, validate existence,
    # validate conflict_action ∈ {UPDATE, NOTHING}
    source_sql = compiled_source(ibis_conn, obj)             # subquery, no temp table
    if style is UpsertStyle.ON_CONFLICT:
        stmt = _render_on_conflict(...)
    elif style is UpsertStyle.MERGE:
        stmt = _render_merge(...)
    elif style is UpsertStyle.ON_DUPLICATE_KEY:
        stmt = _render_on_duplicate_key(...)
    ibis_conn.raw_sql(stmt)
```

Each renderer builds a sqlglot AST and renders with the live dialect. The
`MERGE` renderer mirrors Ibis 12's `_build_upsert_from_table` (`sge.merge`)
**extended** to composite `on` and our `conflict_action`.

### 5.5 Staging — compiled subquery, not temp table

The existing `duckdb_family_upsert` registers a temp staging table via the
DuckDB-specific `ibis_conn.con.register(...)`. That is the portability
blocker, not the SQL. The generic path instead compiles the source frame as
a subquery — `compiled_source()` returns `(SELECT … )` — used as the
`INSERT … SELECT … FROM (<subquery>)` source or the MERGE `USING
(<subquery>) AS src`. This is exactly Ibis 12's mechanism. No staging table,
no cleanup, portable by construction.

## 6. Semantics Mapping

The public semantics map onto each family as follows. All three honour the
identical public contract.

| Public param | `ON CONFLICT` | `MERGE` | `ON DUPLICATE KEY` |
|---|---|---|---|
| `conflict_columns` (composite) | `ON CONFLICT (c1,c2)` | `ON tgt.c1=src.c1 AND tgt.c2=src.c2` | unique-key implied; key cols excluded from SET |
| `conflict_action="UPDATE"` | `DO UPDATE SET …` | `WHEN MATCHED THEN UPDATE SET …` + `WHEN NOT MATCHED THEN INSERT …` | `ON DUPLICATE KEY UPDATE …` |
| `conflict_action="NOTHING"` | `DO NOTHING` | omit `WHEN MATCHED` (insert-if-absent only) | `… UPDATE c1=c1` (no-op self-assign) |
| `update_columns` (subset) | restrict `SET` list | restrict `WHEN MATCHED … SET` list | restrict `UPDATE` list |
| `update_condition` | `DO UPDATE SET … WHERE <cond>` | `WHEN MATCHED AND <cond> THEN UPDATE` | unsupported → `ValueError` (MySQL has no per-row update predicate) |
| default `update_columns` | all non-key columns | all non-key columns | all non-key columns |

`update_condition` on `ON_DUPLICATE_KEY` raises `ValueError` (honest: the
family cannot express it) rather than silently dropping it.

## 7. Coverage Matrix (all 21 registry dialects)

Confidence: **live** = executed against a real engine in CI/local; **render**
= golden-SQL assertion only (no credentials/engine available); **n/a** =
`upsert_style=None`, honest `NotImplementedError`.

| Dialect | `upsert_style` | Confidence | Notes |
|---|---|---|---|
| sqlite | on_conflict | **live** | in-memory |
| duckdb | on_conflict | **live** | in-memory; also exercises MERGE renderer (duckdb supports MERGE) |
| motherduck | on_conflict | render | duckdb engine |
| postgres | on_conflict | **live** | Docker; also exercises MERGE renderer (PG15+) |
| mysql | on_duplicate_key | **live** | Docker (mariadb, per Ibis) |
| singlestoredb | on_duplicate_key | render | MySQL-compatible |
| snowflake | merge | render | |
| bigquery | merge | render | |
| mssql | merge | render | sp_rename for rename |
| oracle | merge | render | |
| databricks | merge | render | Delta MERGE |
| exasol | merge | render | |
| trino | merge | render | connector-dependent at runtime |
| redshift | merge | render | postgres protocol but no `ON CONFLICT`; MERGE (2023+) |
| risingwave | on_conflict | render | postgres-wire table upsert |
| clickhouse | None | n/a | ReplacingMergeTree / ALTER UPDATE — divergent model |
| impala | None | n/a | MERGE only for Iceberg targets |
| materialize | None | n/a | restricts INSERT to write-only txns |
| druid | None | n/a | append-only analytics store |
| pyspark | None | n/a | MERGE only for Delta/Iceberg; Ibis marks notyet |

`rename_table`: generic sqlglot default for **all** dialects (render-verified
across the registry; live on sqlite/duckdb/postgres/mysql). The override hook
stays available for any engine later found to diverge beyond sqlglot's
rendering.

The matrix is shipped as a documented table in the module and asserted by the
golden-SQL tests (per-dialect rendered statement), so "render" coverage is
real verification of the emitted SQL, not an assumption.

## 8. Testing

### 8.1 Live backends — Docker

A minimal `compose.yaml` at repo root, modeled on Ibis's `docker/` services
but using **stock images** (we need no PostGIS/pgvector/plpython):

- `postgres`: `postgres:18-alpine`, env `POSTGRES_USER/PASSWORD/DB`,
  healthcheck `pg_isready`, port 5432.
- `mysql`: `mariadb:12.1.2` (Ibis's choice; ON-DUPLICATE-KEY compatible),
  healthcheck `mariadb-admin ping`, port 3306.

Connection parameters read from environment with Ibis-compatible defaults
(`IBIS_TEST_POSTGRES_*` / `PG*`, `IBIS_TEST_MYSQL_*`) so the same env works
locally and in CI.

### 8.2 Skip-if-unreachable fixtures

`postgres` / `mysql` fixtures attempt a connection; on failure they
`pytest.skip(...)` (not fail), so a developer without Docker and a CI job
without the service still pass green. The live tests are marked
`@pytest.mark.integration` (existing marker).

### 8.3 CI

The existing `python-run-pytest.yml` gains GitHub Actions `services:`
containers for postgres and mariadb (native service-container support; no
compose needed in CI). A `make test-live` / hatch script brings the compose
services up locally.

### 8.4 Test layers

1. **Unit / golden-SQL** (`tests/test_unit/backends/ibis/test_upsert_render.py`,
   `test_rename_table_render.py`): for every registry dialect, assert the
   exact rendered statement per `upsert_style` and for rename. No live engine.
   This is where the full matrix is verified.
2. **Live integration** (`tests/test_integration/test_write_ops_live.py`):
   sqlite + duckdb (in-memory) + postgres + mysql — round-trip upsert
   (insert + update + NOTHING + composite key + update_condition where
   supported) and rename, asserting data outcomes.
3. **Consolidation regression**: existing `test_add_columns.py` stays green
   unchanged after the helper extraction.

## 9. Verification already performed (sqlglot transpile probe, ibis 12 env)

- `ALTER TABLE … ADD COLUMN`, `RENAME`, and `MERGE` render correctly
  per-dialect from sqlglot (`sp_rename` for tsql, `RENAME` for mysql).
- `INSERT … ON CONFLICT` renders natively for postgres/duckdb/sqlite and is
  **not** transpiled to MERGE (confirming the family split is structural, not
  a rendering gap).
- Ibis 12 `SQLBackend.upsert` exists (MERGE-based) but its `on` is a single
  column — insufficient for the composite natural keys real consumers use —
  which is why we render our own, using Ibis's `sge.merge` shape as template.

## 10. Error Handling & Edge Cases

- `conflict_action` ∉ {UPDATE, NOTHING} → `ValueError`.
- `conflict_action="UPDATE"` with no updatable columns (all columns are keys
  and no `update_columns`) → `ValueError` (preserves current behaviour).
- `conflict_action="NOTHING"` with `update_columns`/`update_condition` →
  warn-and-ignore (preserves current behaviour), except MERGE where NOTHING
  simply omits the matched clause.
- `update_condition` on `on_duplicate_key` → `ValueError` (family cannot
  express it).
- `upsert_style=None` → `NotImplementedError` naming the dialect.
- Target table absent → `ValueError` (preserves current behaviour).
- Identifiers: `name`/`database`/rename names validated simple (non-dotted)
  via the existing `_validate_simple_identifier`; quoted per dialect. Dotted
  qualified names out of scope (consistent with `add_columns`).

## 11. Known Limitations

- **Not concurrency-safe / not atomic** beyond the engine's own statement
  atomicity — single-statement upsert is atomic where the engine makes it so;
  no cross-statement transaction is added.
- **Subquery staging** inlines the source data as a compiled `VALUES`/`SELECT`
  subquery (Ibis's mechanism). Very large frames produce large SQL; callers
  with bulk loads should use a staging table + `upsert`-from-table directly.
  Acceptable and matches upstream Ibis behaviour.
- **Render-only dialects** (matrix §7) are verified at the SQL-emission layer,
  not by execution; first real use on such an engine may surface
  engine-specific quirks, handled then via the `upsert_hook` override seam.
- **`update_condition` semantics** differ subtly across families (pre- vs
  post-match predicate); documented per family in §6.

## 12. Dependency

- Bump the pin to **`ibis-framework>=12.0.0`** (the env reality; `add_columns`
  already shipped against it; lets us reference Ibis's `sge.merge` shape). The
  renderers use `sqlglot.expressions` directly, so 12 is not strictly required
  for rendering, but aligning removes version drift.
- Correct the stale `ibis-framework == 10.4.0` reference in `CLAUDE.md` to the
  actual `>=12.0.0`.

## 13. Files Changed

- `dialects/_registry.py` — `UpsertStyle` enum, `upsert_style` field; assign
  styles per the matrix; remove the three `upsert_hook=duckdb_family_upsert`
  registrations.
- `backends/ibis/_render.py` (new) — shared rendering primitives.
- `backends/ibis/operations.py` — `_generic_rename_table`, `_generic_upsert`,
  `_render_on_conflict`, `_render_merge`, `_render_on_duplicate_key`; migrate
  `_generic_add_columns` to the helper; **delete** `duckdb_family_upsert` —
  the duckdb family routes through `_render_on_conflict` via
  `upsert_style=ON_CONFLICT`.
- `backends/ibis/backend.py` — `upsert`/`rename_table` dispatch updated to the
  hook-or-generic shape.
- `compose.yaml` (new) — postgres + mariadb stock services.
- `tests/test_unit/backends/ibis/test_upsert_render.py`,
  `test_rename_table_render.py` (new) — golden-SQL per dialect.
- `tests/test_integration/test_write_ops_live.py` (new) — live round-trips.
- `tests/conftest.py` / fixtures — skip-if-unreachable postgres/mysql.
- `.github/workflows/python-run-pytest.yml` — service containers.
- `pyproject.toml` — ibis pin `>=12.0.0`.
- `CLAUDE.md` — correct ibis version note.

## 14. Out of Scope (tracked elsewhere)

- `create_index` / `drop_index` / `get_index_exists_sql` /
  `get_list_indexes_sql` — capability-divergent, stay hook-required.
- Consumer migration (mountainash-wearables backend swap to postgres) — in
  that repo after this ships. This spec's `upsert`-on-postgres is its enabler.
- Live warehouse testing — requires credentials/infra; deferred.
