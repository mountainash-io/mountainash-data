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

**Column ordering (mandatory).** Every family renders an **explicit target
column list** and projects the source subquery columns **in target-column
order** — never `INSERT … SELECT *` or positional value lists. A source whose
columns are ordered `[name, id]` against a target `[id, name]` must not swap
values. The renderer derives the column list from the source schema,
intersected with the target schema, and projects both `INSERT` and the MERGE
`WHEN NOT MATCHED … INSERT (cols) VALUES (src.cols)` in that exact order.

**Type alignment across the subquery boundary (mandatory).** The source
projection **casts each column to the target table's column type** via the
connection's `compiler.type_mapper` (the same type-parity mechanism
`add_columns` uses). This is required because an all-null source column
compiles as an untyped `NULL` literal that warehouse `MERGE` engines reject
or mis-coerce against a typed/non-nullable target, and because temporal /
decimal types are backend-sensitive. Columns present in the target but absent
from the source are omitted from the column list (engine default / NULL
applies); columns present in the source but absent from the target raise
`ValueError`.

## 6. Semantics Mapping

The public semantics map onto each family as below. The `ON CONFLICT` and
`MERGE` families honour an identical public contract; `ON DUPLICATE KEY`
honours it with two **documented divergences** (conflict targeting and the
`NOTHING` action), spelled out under the table — it is *not* claimed
byte-equivalent.

| Public param | `ON CONFLICT` | `MERGE` | `ON DUPLICATE KEY` |
|---|---|---|---|
| `conflict_columns` (composite) | `ON CONFLICT (c1,c2)` | `ON tgt.c1=src.c1 AND tgt.c2=src.c2` | **detection is by the table's unique indexes, not this list** — see §6.1 |
| `conflict_action="UPDATE"` | `DO UPDATE SET …` | `WHEN MATCHED THEN UPDATE SET …` + `WHEN NOT MATCHED THEN INSERT …` | `ON DUPLICATE KEY UPDATE …` |
| `conflict_action="NOTHING"` | `DO NOTHING` | omit `WHEN MATCHED` (insert-if-absent only) | `… UPDATE k0=k0` self-assign — **not a true no-op**, see §6.2 |
| `update_columns` (subset) | restrict `SET` list | restrict `WHEN MATCHED … SET` list | restrict `UPDATE` list |
| `update_condition` | `DO UPDATE SET … WHERE <cond>` | `WHEN MATCHED AND <cond> THEN UPDATE` | unsupported → `ValueError` |
| default `update_columns` | all non-key columns | all non-key columns | all non-key columns |

### 6.1 `update_condition` alias contract (cross-family)

`update_condition` is a raw SQL boolean expression. A bare string cannot be
both ON-CONFLICT-valid and MERGE-valid (`EXCLUDED.x` vs `src.x`), so the
package **fixes the alias contract**: the condition references the **incoming
row as `EXCLUDED.<col>`** and the **existing row by the bare target table
name** — the ON CONFLICT convention. The MERGE renderer makes the same string
valid by aliasing the source subquery **`AS EXCLUDED`** and the target **`AS
<table_name>`**, so `EXCLUDED.updated_at > orders.updated_at` renders
correctly in both families. The matched/not-matched semantics are equivalent:
a row matching the key but failing the condition stays matched and is neither
updated nor re-inserted (no duplicate). `on_duplicate_key` cannot express a
per-row update predicate → `update_condition` raises `ValueError` rather than
silently dropping it.

### 6.2 `ON DUPLICATE KEY` documented divergences

- **Conflict targeting:** MySQL/MariaDB `ON DUPLICATE KEY UPDATE` fires on a
  collision with **any** unique or primary key, not a named subset.
  `conflict_columns` therefore governs only which columns are *excluded from
  the UPDATE set* (and is validated non-empty); it does **not** select the
  detection key. Callers must ensure `conflict_columns` corresponds to the
  table's intended unique constraint — on a table with multiple unique
  constraints the engine may update on a different collision. Documented
  limitation (§11); the `upsert_hook` override is the escape hatch for
  stricter needs. We do **not** introspect constraints in this iteration.
- **`NOTHING` is not a true no-op:** rendered as `ON DUPLICATE KEY UPDATE
  k0=k0` (self-assigning the first key column). Depending on table definition
  this may advance `ON UPDATE CURRENT_TIMESTAMP` columns, fire UPDATE
  triggers, take update locks, and alter affected-row counts — unlike `ON
  CONFLICT DO NOTHING`. We deliberately do **not** use `INSERT IGNORE` (it
  also silently suppresses unrelated type / NOT NULL / FK errors). Documented
  (§11); strict-skip callers use the override hook.

## 7. Coverage Matrix (all 20 registry dialects)

The registry currently has **20** dialects; every one appears below. The
matrix is not hand-counted in tests — the golden-SQL suite **iterates the live
`DIALECTS` registry** (§8.4) and asserts each entry renders or is explicitly
`None`, so adding a 21st dialect *forces* a matrix decision (no hardcoded
count to drift).

Columns: **Style** = assigned `upsert_style`. **Exec** = engine-execution
confidence: **live** (round-tripped against a real engine) / **render**
(rendered SQL asserted; engine acceptance and semantic support **not**
verified) / **n/a** (`upsert_style=None` → honest `NotImplementedError`).
**Basis** = how the style assignment was established: **verified** (live or
vendor-doc confirmed) / **inferred** (protocol/family compatibility) /
**unverified** (hypothesis pending docs/tests).

| Dialect | Style | Exec | Basis | Notes |
|---|---|---|---|---|
| sqlite | on_conflict | **live** | verified | in-memory |
| duckdb | on_conflict | **live** | verified | in-memory; also exercises MERGE renderer (duckdb supports MERGE) |
| motherduck | on_conflict | render | inferred | duckdb engine |
| postgres | on_conflict | **live** | verified | Docker; also exercises MERGE renderer (PG15+) |
| mysql | on_duplicate_key | **live** | verified | Docker (mariadb, per Ibis) |
| singlestoredb | on_duplicate_key | render | inferred | MySQL-compatible wire/syntax |
| snowflake | merge | render | verified | vendor MERGE |
| bigquery | merge | render | verified | vendor MERGE |
| mssql | merge | render | verified | vendor MERGE; sp_rename for rename |
| oracle | merge | render | verified | vendor MERGE |
| databricks | merge | render | verified | Delta MERGE |
| exasol | merge | render | inferred | MERGE documented |
| trino | merge | render | inferred | MERGE is **connector-dependent** at runtime — may reject |
| redshift | merge | render | verified | postgres protocol but no `ON CONFLICT`; MERGE (AWS docs, 2023+) |
| risingwave | on_conflict | render | **unverified** | postgres-wire; ON CONFLICT on tables assumed, not confirmed |
| clickhouse | None | n/a | verified | ReplacingMergeTree / ALTER UPDATE — divergent model |
| impala | None | n/a | verified | MERGE only for Iceberg targets |
| materialize | None | n/a | verified | restricts INSERT to write-only txns |
| druid | None | n/a | verified | append-only analytics store |
| pyspark | None | n/a | verified | MERGE only for Delta/Iceberg; Ibis marks notyet |

`rename_table`: generic sqlglot default for **all** dialects (rendered SQL
asserted across the registry; engine-executed live on
sqlite/duckdb/postgres/mysql). The override hook stays available for any
engine later found to diverge beyond sqlglot's rendering.

**Render coverage is syntax-emission verification only** — it confirms the
exact SQL string sqlglot emits per dialect, *not* that the engine accepts or
semantically supports it. The public-facing support matrix carries the
**Exec** and **Basis** caveats above so consumers do not mistake a green
golden-SQL test for runtime support (e.g. Trino MERGE may render cleanly yet
be rejected by the connector). `unverified` rows are flagged as hypotheses
until a live test or vendor doc upgrades them.

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

### 8.2 Skip-if-unreachable fixtures — but fail-closed in CI

`postgres` / `mysql` fixtures attempt a connection; on failure behaviour is
**environment-gated** to avoid silently-green CI:

- **Locally** (default): unreachable service → `pytest.skip(...)`, so a
  developer without Docker still passes green.
- **In CI**: the workflow sets `MOUNTAINASH_REQUIRE_LIVE_DB=1`; under that flag
  an unreachable required service **fails** (not skips). A misconfigured
  service password/port then breaks the build instead of silently skipping the
  promised live matrix.

Live tests are marked `@pytest.mark.integration` (existing marker).

### 8.3 CI

The existing `python-run-pytest.yml` gains GitHub Actions `services:`
containers for postgres and mariadb (native service-container support; no
compose needed in CI) and sets `MOUNTAINASH_REQUIRE_LIVE_DB=1` for the live
job. A `hatch` script (`test-live`) brings the local compose services up.

### 8.4 Test layers

1. **Unit / golden-SQL** (`tests/test_unit/backends/ibis/test_upsert_render.py`,
   `test_rename_table_render.py`): the suite **iterates the live `DIALECTS`
   registry** (not a hand-listed set) and, for each dialect, asserts the exact
   rendered statement for its `upsert_style` — or, for `None`, asserts
   `NotImplementedError` — and the rendered `rename_table`. Because it iterates
   the registry, a newly added dialect with no decision fails the suite,
   keeping the §7 matrix complete by construction. This layer verifies
   **emitted SQL only**, not engine acceptance (§7).
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

- **Duplicate source rows are the caller's responsibility.** If the source
  frame contains two rows with the same conflict key, behaviour is
  engine-divergent: `MERGE` raises a cardinality-violation error on most
  engines (e.g. Redshift) when multiple source rows match one target row,
  whereas `ON CONFLICT` / `ON DUPLICATE KEY` apply conflicts row-by-row with
  order-dependent last-write-wins. The renderer does **not** deduplicate.
  Callers must deduplicate the source on the conflict key for deterministic,
  portable behaviour. Documented, not handled (an optional `deduplicate=` flag
  is a future extension, not in this iteration).
- **MySQL-family conflict targeting** detects on *any* unique index, not
  `conflict_columns` (§6.2) — unenforceable on tables with multiple unique
  constraints; use the override hook for stricter needs.
- **MySQL-family `conflict_action="NOTHING"` is not a true no-op** (§6.2) — may
  fire `ON UPDATE` timestamps / triggers / locks. Documented; not `INSERT
  IGNORE`.
- **Not concurrency-safe / not atomic** beyond the engine's own statement
  atomicity — single-statement upsert is atomic where the engine makes it so;
  no cross-statement transaction is added.
- **Subquery staging** inlines the source data as a compiled `VALUES`/`SELECT`
  subquery (Ibis's mechanism), with every column cast to the target type
  (§5.5). Very large frames produce large SQL; callers with bulk loads should
  use a staging table + `upsert`-from-table directly. Acceptable and matches
  upstream Ibis behaviour.
- **Render-only dialects** (matrix §7) are verified at the SQL-emission layer
  only, **not** by execution or semantic-support confirmation; first real use
  on such an engine may surface engine-specific quirks (or outright rejection,
  e.g. connector-dependent Trino MERGE), handled then via the `upsert_hook`
  override seam. `unverified`-basis rows (e.g. risingwave) are hypotheses.
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
