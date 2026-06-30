# Generic-Default Index Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `IbisBackend.create_index` / `drop_index` / `index_exists` generic-default across the conventional-B-tree dialects via a structured `IndexCapability` descriptor, retiring the duckdb-family index hooks.

**Architecture:** A frozen `IndexCapability` descriptor on each `DialectSpec` drives pure SQL builders + generic dispatchers in a new `backends/ibis/_index.py`. Dispatch is `hook → generic(caps) → NotImplementedError`, mirroring the upsert/rename design from PR #91. Partial-index `WHERE` is an ibis predicate compiled through a new single-relation path in `_render.py`. Idempotency is native where the engine supports `IF [NOT] EXISTS`, emulated via an `index_exists` precheck otherwise.

**Tech Stack:** Python 3.12, ibis-framework ≥12, sqlglot 30.x, pytest, hatch (`test` env), Docker compose (postgres + mariadb) for live tests.

**Spec:** `docs/superpowers/specs/2026-06-30-generic-default-index-operations-design.md` (Codex-reviewed).

## Global Constraints

- ibis-framework floor is `>=12` (already set by PR #91); do not lower it.
- **No silent degradation:** an unsupported `index_type`, a partial `WHERE` on a non-partial dialect, or a missing required `table_name` each raise `ValueError` — never warn-and-downgrade. (Retires `duckdb_family_create_index`'s warn-and-downgrade.)
- **Clean break, no shims** (pre-release, no downstream): the public param is `where: IndexPredicate | None`, NOT `where_condition: str | None`.
- **Injection hardening:** every value interpolated into introspection SQL is identifier-allowlist-validated (`_validate_simple_identifier`) and string-literal-escaped (`sqlglot exp.Literal.string`). The allowlist (`_SIMPLE_IDENTIFIER_RE = [A-Za-z_][A-Za-z0-9_$]*`) is the primary gate; escaping is defense-in-depth.
- **Coverage is all-three-operations:** `index_caps is not None` ⇒ the dialect supports create + drop + exists generically, and MUST also set `get_index_exists_sql` (registry invariant).
- **`index_caps=None`** ⇒ `create_index`/`drop_index` raise `NotImplementedError`. Out-of-scope dialects: snowflake, bigquery, redshift, trino, clickhouse, databricks, exasol, impala, materialize, risingwave, druid, pyspark.
- **Verified support matrix** (do not deviate without re-checking official docs):

  | Dialect | drop_scope | partial | native INE / IE | index_types |
  |---|---|---|---|---|
  | sqlite | SCHEMA_GLOBAL | True | True / True | ∅ |
  | duckdb, motherduck | SCHEMA_GLOBAL | False | True / True | ∅ |
  | postgres | SCHEMA_GLOBAL | True | True / True | btree,hash,gist,gin,brin,spgist |
  | mysql | TABLE_SCOPED | False | False / False (emulate) | btree |
  | singlestoredb | TABLE_SCOPED | False | False / False (emulate) | btree,hash |
  | mssql | TABLE_SCOPED | True | False / True | ∅ |
  | oracle | SCHEMA_GLOBAL | False | False / False (emulate) | ∅ |

- **Testing:** use the hatch `test` env (`hatch run test:test-target <path>` / `hatch run test:test-target-quick <path>`), never a stale `.venv`. Live tests gate on `MOUNTAINASH_REQUIRE_LIVE_DB=1` (fail-closed) and skip-if-unreachable otherwise.
- **Branch:** `feature/generic-default-index-operations` (already created off `develop`).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/mountainash_data/backends/ibis/dialects/_registry.py` | Add `DropScope`, `IndexCapability`, `index_caps` field + per-dialect assignment; remove `duckdb_family_*` registrations; wire 5 new `get_index_exists_sql` |
| `src/mountainash_data/backends/ibis/_render.py` | Add `compile_index_predicate` (single-relation WHERE compiler, AST-level qualifier strip) |
| `src/mountainash_data/backends/ibis/_index.py` | **New** — pure builders (`build_create_index_sql`, `build_drop_index_sql`) + generic dispatchers (`_generic_create_index`, `_generic_drop_index`, `_generic_index_exists`) + `_USING_BEFORE_COLUMNS` placement map |
| `src/mountainash_data/backends/ibis/operations.py` | Add `_sql_literal` escape helper; harden existing 3 + add 5 new `get_index_exists_sql`; delete `duckdb_family_create_index`/`drop_index` |
| `src/mountainash_data/backends/ibis/backend.py` | `create_index`/`drop_index`/`index_exists` → hook→generic→NotImplementedError; table-scoped `table_name` validation; `where` predicate param |
| `tests/test_unit/backends/ibis/test_index_capability.py` | **New** — capability dataclass + registry matrix/invariant |
| `tests/test_unit/backends/ibis/test_index_render.py` | **New** — pure-builder golden + predicate compile + introspection-SQL golden |
| `tests/test_unit/backends/ibis/test_index_ops.py` | **New** — generic dispatcher behavior (in-memory sqlite/duckdb) |
| `tests/test_integration/test_index_ops_live.py` | **New** — postgres + mariadb round-trips, partial, table-scoped drop, emulation |
| `tests/test_unit/backends/ibis/test_backend.py` | Update the 3 hook-mechanism assertions at cutover |

---

## Task 1: Capability model — `DropScope`, `IndexCapability`, `index_caps` field

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py` (after the `UpsertStyle` enum, ~line 29, and the `DialectSpec` dataclass, ~line 42-59)
- Test: `tests/test_unit/backends/ibis/test_index_capability.py`

**Interfaces:**
- Produces: `class DropScope(str, enum.Enum)` with members `SCHEMA_GLOBAL="schema_global"`, `TABLE_SCOPED="table_scoped"`; `@dataclass(frozen=True) class IndexCapability` with fields `drop_scope: DropScope`, `partial: bool`, `native_if_not_exists: bool`, `native_if_exists: bool`, `index_types: frozenset[str]`; new `DialectSpec` field `index_caps: t.Optional[IndexCapability] = None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_index_capability.py`:

```python
"""IndexCapability descriptor + DropScope enum (registry capability model)."""

import dataclasses

import pytest

from mountainash_data.backends.ibis.dialects._registry import (
    DialectSpec,
    DropScope,
    IndexCapability,
)


def test_dropscope_members():
    assert DropScope.SCHEMA_GLOBAL.value == "schema_global"
    assert DropScope.TABLE_SCOPED.value == "table_scoped"


def test_index_capability_is_frozen():
    caps = IndexCapability(
        drop_scope=DropScope.SCHEMA_GLOBAL,
        partial=True,
        native_if_not_exists=True,
        native_if_exists=True,
        index_types=frozenset({"btree"}),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        caps.partial = False  # type: ignore[misc]


def test_dialectspec_index_caps_defaults_none():
    spec = DialectSpec(
        ibis_backend_name="x",
        connection_mode="kwargs",
        connection_string_scheme="x://",
    )
    assert spec.index_caps is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_capability.py`
Expected: FAIL with `ImportError: cannot import name 'DropScope'`.

- [ ] **Step 3: Add the enum and dataclass**

In `_registry.py`, after the `UpsertStyle` enum (after line 28), add:

```python
class DropScope(str, enum.Enum):
    SCHEMA_GLOBAL = "schema_global"   # DROP INDEX name
    TABLE_SCOPED = "table_scoped"     # DROP INDEX name ON tbl


@dataclass(frozen=True)
class IndexCapability:
    """Per-dialect conventional-B-tree index capability (spec §3).

    None on DialectSpec.index_caps means the dialect has no conventional
    secondary index -> create/drop raise NotImplementedError.
    """

    drop_scope: DropScope
    partial: bool                     # supports a WHERE filter (partial/filtered index)
    native_if_not_exists: bool        # engine has CREATE INDEX IF NOT EXISTS
    native_if_exists: bool            # engine has DROP INDEX IF EXISTS
    index_types: frozenset[str]       # valid USING <type> values; empty = no USING clause
```

In the `DialectSpec` dataclass body (after the `upsert_style` field, ~line 54), add:

```python
    index_caps: t.Optional[IndexCapability] = None
    # None = no conventional index support -> NotImplementedError.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_capability.py`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_index_capability.py
git commit -m "feat(ibis): add DropScope + IndexCapability model + index_caps field"
```

---

## Task 2: Per-dialect `index_caps` assignment + registry invariant

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py` (the 8 conventional `DialectSpec(...)` entries in `DIALECTS`, lines ~658-690 for sqlite/duckdb/motherduck and the postgres/mysql/mssql/oracle/singlestoredb entries)
- Test: `tests/test_unit/backends/ibis/test_index_capability.py` (append)

**Interfaces:**
- Consumes: `DropScope`, `IndexCapability` (Task 1).
- Produces: `index_caps=IndexCapability(...)` on sqlite, duckdb, motherduck, postgres, mysql, singlestoredb, mssql, oracle; left `None` (default) on all 12 others.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_index_capability.py`:

```python
from mountainash_data.backends.ibis.dialects._registry import DIALECTS

# Verified against official vendor docs 2026-06-30 (spec §4). frozenset of USING types.
_EXPECTED = {
    "sqlite":        (DropScope.SCHEMA_GLOBAL, True,  True,  True,  frozenset()),
    "duckdb":        (DropScope.SCHEMA_GLOBAL, False, True,  True,  frozenset()),
    "motherduck":    (DropScope.SCHEMA_GLOBAL, False, True,  True,  frozenset()),
    "postgres":      (DropScope.SCHEMA_GLOBAL, True,  True,  True,
                      frozenset({"btree", "hash", "gist", "gin", "brin", "spgist"})),
    "mysql":         (DropScope.TABLE_SCOPED,  False, False, False, frozenset({"btree"})),
    "singlestoredb": (DropScope.TABLE_SCOPED,  False, False, False, frozenset({"btree", "hash"})),
    "mssql":         (DropScope.TABLE_SCOPED,  True,  False, True,  frozenset()),
    "oracle":        (DropScope.SCHEMA_GLOBAL, False, False, False, frozenset()),
}

_UNSUPPORTED = {
    "snowflake", "bigquery", "redshift", "trino", "clickhouse", "databricks",
    "exasol", "impala", "materialize", "risingwave", "druid", "pyspark",
}


@pytest.mark.parametrize("name,expected", _EXPECTED.items())
def test_index_caps_matrix(name, expected):
    caps = DIALECTS[name].index_caps
    assert caps is not None, f"{name} must have index_caps"
    drop_scope, partial, ine, ie, types = expected
    assert caps.drop_scope is drop_scope
    assert caps.partial is partial
    assert caps.native_if_not_exists is ine
    assert caps.native_if_exists is ie
    assert caps.index_types == types


@pytest.mark.parametrize("name", sorted(_UNSUPPORTED))
def test_unsupported_dialects_have_no_index_caps(name):
    assert DIALECTS[name].index_caps is None


@pytest.mark.parametrize("name", _EXPECTED.keys())
def test_invariant_caps_implies_exists_sql(name):
    """Spec §3 invariant: a dialect with index_caps must also introspect indexes."""
    spec = DIALECTS[name]
    assert spec.index_caps is not None
    assert spec.get_index_exists_sql is not None, (
        f"{name}: index_caps set but get_index_exists_sql missing"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_capability.py::test_index_caps_matrix`
Expected: FAIL — `assert caps is not None` (no dialect has `index_caps` yet). The invariant test will also fail for postgres/mysql/mssql/oracle/singlestoredb (no `get_index_exists_sql` yet) — that part is satisfied by Task 5; for now assign `index_caps` and the invariant passes only for sqlite/duckdb/motherduck. **To keep this task self-contained, also temporarily wire the 5 new `get_index_exists_sql` stubs are NOT done here** — instead, scope the invariant test to the 3 that already have introspection and the 5 to Task 5. Adjust the invariant test to:

```python
@pytest.mark.parametrize("name", ["sqlite", "duckdb", "motherduck"])
def test_invariant_caps_implies_exists_sql(name):
    spec = DIALECTS[name]
    assert spec.index_caps is not None
    assert spec.get_index_exists_sql is not None
```

(Task 5 re-broadens this to all 8 once the introspection SQL exists.)

- [ ] **Step 3: Assign `index_caps` on the 8 conventional dialects**

In `_registry.py`, add an `index_caps=IndexCapability(...)` argument to each entry. Example for `sqlite` (mirror for the others using the matrix in Global Constraints):

```python
    "sqlite": DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode=_CONNECTION_STRING,
        connection_string_scheme="sqlite://",
        connection_builder=_build_sqlite_connection,
        get_index_exists_sql=sqlite_get_index_exists_sql,
        get_list_indexes_sql=sqlite_get_list_indexes_sql,
        upsert_style=UpsertStyle.ON_CONFLICT,
        create_index_hook=duckdb_family_create_index,  # removed in Task 8
        drop_index_hook=duckdb_family_drop_index,       # removed in Task 8
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=True,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset(),
        ),
    ),
```

Add to `postgres`:
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=True,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset({"btree", "hash", "gist", "gin", "brin", "spgist"}),
        ),
```
Add to `mysql`:
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset({"btree"}),
        ),
```
Add to `singlestoredb`:
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset({"btree", "hash"}),
        ),
```
Add to `mssql`:
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.TABLE_SCOPED, partial=True,
            native_if_not_exists=False, native_if_exists=True,
            index_types=frozenset(),
        ),
```
Add to `oracle`:
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=False,
            native_if_not_exists=False, native_if_exists=False,
            index_types=frozenset(),
        ),
```
And `duckdb` / `motherduck` (identical to each other):
```python
        index_caps=IndexCapability(
            drop_scope=DropScope.SCHEMA_GLOBAL, partial=False,
            native_if_not_exists=True, native_if_exists=True,
            index_types=frozenset(),
        ),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_capability.py`
Expected: PASS (matrix + unsupported + scoped invariant all green).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_index_capability.py
git commit -m "feat(ibis): assign verified index_caps to the 8 conventional dialects"
```

---

## Task 3: `compile_index_predicate` — single-relation WHERE compiler

**Files:**
- Modify: `src/mountainash_data/backends/ibis/_render.py` (append after `compile_condition`, ~line 211)
- Test: `tests/test_unit/backends/ibis/test_index_render.py`

**Interfaces:**
- Consumes: `dialect_of`, `validate_predicate`, `INCOMING_SENTINEL` patterns (existing in `_render.py`).
- Produces: module constant `INDEX_SENTINEL = "__ma_index_tbl__"`; `IndexPredicate = t.Callable[[ir.Table], ir.BooleanValue]`; `compile_index_predicate(ibis_conn, schema, table_name, predicate) -> str` — returns a dialect-rendered, **unqualified** boolean SQL string.

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_index_render.py`:

```python
"""Index render primitives: predicate compiler + pure builders + introspection SQL."""

import ibis
import pytest

from mountainash_data.backends.ibis._render import compile_index_predicate

_SCHEMA = ibis.schema({"id": "int64", "active": "boolean", "ver": "int64"})


def _pred_sql(predicate, *, table_name="t"):
    con = ibis.duckdb.connect()
    return compile_index_predicate(con, _SCHEMA, table_name, predicate)


class TestCompileIndexPredicate:
    def test_renders_unqualified_columns(self):
        sql = _pred_sql(lambda t: t.active == True)  # noqa: E712
        # the column must be UNqualified (no table/alias prefix)
        assert '"active"' in sql
        assert "." not in sql.split('"active"')[0][-3:]  # no `x.` before "active"

    def test_comparison_predicate(self):
        sql = _pred_sql(lambda t: t.ver > 5)
        assert '"ver"' in sql and "5" in sql

    def test_predicate_may_reference_non_indexed_column(self):
        # binding the full schema (not just indexed cols) must allow this
        sql = _pred_sql(lambda t: t.active)
        assert '"active"' in sql

    def test_rejects_sentinel_table_name(self):
        with pytest.raises(ValueError, match="sentinel"):
            _pred_sql(lambda t: t.id > 0, table_name="__ma_index_tbl__")

    def test_rejects_aggregate(self):
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            _pred_sql(lambda t: t.id.sum() > 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py::TestCompileIndexPredicate`
Expected: FAIL with `ImportError: cannot import name 'compile_index_predicate'`.

- [ ] **Step 3: Implement `compile_index_predicate`**

Append to `_render.py`:

```python
INDEX_SENTINEL = "__ma_index_tbl__"

IndexPredicate = t.Callable[[ir.Table], ir.BooleanValue]


def compile_index_predicate(
    ibis_conn: t.Any,
    schema: t.Any,
    table_name: str,
    predicate: IndexPredicate,
) -> str:
    """Compile a single-table ``(table) -> bool`` predicate to an UNQUALIFIED
    boolean SQL string for the connection's dialect (partial-index WHERE).

    Mechanism (spec §5.2): bind one sentinel-named ibis table at `schema`,
    filter it by the predicate, compile to sqlglot, extract the WHERE, then
    strip every column's table/db/catalog qualifier at the AST level (NOT by
    string replacement). The predicate may reference any column of the table,
    not only the indexed columns, so the full `schema` is bound.

    Raises:
        ValueError: if `table_name` collides with the reserved sentinel, or the
            predicate contains a forbidden op (aggregation/window/subquery).
    """
    if table_name == INDEX_SENTINEL:
        raise ValueError(
            f"target table name {table_name!r} collides with a reserved sentinel."
        )
    tbl = ibis.table(schema, name=INDEX_SENTINEL)
    pred = predicate(tbl)
    validate_predicate(pred)

    filtered = tbl.filter(pred)
    ast = ibis_conn.compiler.to_sqlglot(filtered)
    ast = ast if isinstance(ast, exp.Expression) else ast[0]

    where = next(ast.find_all(exp.Where), None)
    if where is None or where.this is None:
        raise ValueError("could not extract WHERE predicate from compiled AST")
    cond = where.this.copy()

    def _strip(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column):
            n.set("table", None)
            n.set("db", None)
            n.set("catalog", None)
        return n

    return cond.transform(_strip).sql(dialect=dialect_of(ibis_conn))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py::TestCompileIndexPredicate`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/_render.py tests/test_unit/backends/ibis/test_index_render.py
git commit -m "feat(ibis): add compile_index_predicate single-relation WHERE compiler"
```

---

## Task 4: Pure builders `build_create_index_sql` + `build_drop_index_sql`

**Files:**
- Create: `src/mountainash_data/backends/ibis/_index.py`
- Test: `tests/test_unit/backends/ibis/test_index_render.py` (append)

**Interfaces:**
- Consumes: `_render.quote_identifier`; `_registry.DropScope`.
- Produces:
  - `_USING_BEFORE_COLUMNS: frozenset[str] = frozenset({"postgres"})` (sqlglot dialect names whose `USING <method>` precedes the column list).
  - `build_create_index_sql(*, dialect, target, index_name, cols, unique, index_type, guard, where_sql) -> str`
  - `build_drop_index_sql(*, dialect, drop_scope, index_name, target, guard) -> str`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_index_render.py`:

```python
from mountainash_data.backends.ibis._index import (
    build_create_index_sql,
    build_drop_index_sql,
)
from mountainash_data.backends.ibis.dialects._registry import DropScope


class TestBuildCreateIndexSql:
    def test_basic(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="idx_t_id",
            cols=["id"], unique=False, index_type=None, guard="", where_sql=None,
        )
        assert sql == 'CREATE INDEX "idx_t_id" ON "t" ("id")'

    def test_unique_and_guard(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="u", cols=["a", "b"],
            unique=True, index_type=None, guard="IF NOT EXISTS ", where_sql=None,
        )
        assert sql == 'CREATE UNIQUE INDEX IF NOT EXISTS "u" ON "t" ("a", "b")'

    def test_partial_where(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="p", cols=["id"],
            unique=False, index_type=None, guard="", where_sql='"active"',
        )
        assert sql.endswith('("id") WHERE "active"')

    def test_using_before_columns_postgres(self):
        sql = build_create_index_sql(
            dialect="postgres", target='"t"', index_name="g", cols=["doc"],
            unique=False, index_type="gin", guard="", where_sql=None,
        )
        assert sql == 'CREATE INDEX "g" ON "t" USING gin ("doc")'

    def test_using_after_columns_mysql(self):
        sql = build_create_index_sql(
            dialect="mysql", target="`t`", index_name="i", cols=["id"],
            unique=False, index_type="btree", guard="", where_sql=None,
        )
        assert sql == "CREATE INDEX `i` ON `t` (`id`) USING btree"


class TestBuildDropIndexSql:
    def test_schema_global(self):
        sql = build_drop_index_sql(
            dialect="duckdb", drop_scope=DropScope.SCHEMA_GLOBAL,
            index_name="idx", target=None, guard="IF EXISTS ",
        )
        assert sql == 'DROP INDEX IF EXISTS "idx"'

    def test_table_scoped(self):
        sql = build_drop_index_sql(
            dialect="mysql", drop_scope=DropScope.TABLE_SCOPED,
            index_name="idx", target="`t`", guard="",
        )
        assert sql == "DROP INDEX `idx` ON `t`"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py::TestBuildCreateIndexSql`
Expected: FAIL with `ModuleNotFoundError: No module named '...ibis._index'`.

- [ ] **Step 3: Create `_index.py` with the builders**

```python
"""Generic-default index DDL: pure builders + dispatchers (spec §5).

Pure builders take pre-computed, already-validated parts so registry golden
tests render every dialect without a live connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis._render import quote_identifier
from mountainash_data.backends.ibis.dialects._registry import DropScope

# sqlglot dialect names whose `USING <method>` precedes the column list.
# Postgres: CREATE INDEX name ON tbl USING gin (cols). MySQL/SingleStore put
# USING after the column list. Default is after-columns.
_USING_BEFORE_COLUMNS: frozenset[str] = frozenset({"postgres"})


def build_create_index_sql(
    *,
    dialect: t.Any,
    target: str,
    index_name: str,
    cols: list[str],
    unique: bool,
    index_type: t.Optional[str],
    guard: str,
    where_sql: t.Optional[str],
) -> str:
    """Render a CREATE INDEX statement from pre-validated parts.

    Args:
        dialect: sqlglot dialect string (e.g. ``dialect_of(ibis_conn)``).
        target: already-qualified, already-quoted table reference.
        index_name: unquoted index name.
        cols: unquoted column names.
        unique: emit CREATE UNIQUE INDEX.
        index_type: USING <type>, or None for no USING clause.
        guard: ``"IF NOT EXISTS "`` or ``""`` (emulation supplies idempotency).
        where_sql: rendered partial-index WHERE body, or None.
    """
    unique_sql = "UNIQUE " if unique else ""
    cols_sql = ", ".join(quote_identifier(c, dialect) for c in cols)
    name_sql = quote_identifier(index_name, dialect)
    where = f" WHERE {where_sql}" if where_sql else ""

    if index_type:
        using = f"USING {index_type}"
        if str(dialect) in _USING_BEFORE_COLUMNS:
            body = f"ON {target} {using} ({cols_sql})"
        else:
            body = f"ON {target} ({cols_sql}) {using}"
    else:
        body = f"ON {target} ({cols_sql})"

    return f"CREATE {unique_sql}INDEX {guard}{name_sql} {body}{where}"


def build_drop_index_sql(
    *,
    dialect: t.Any,
    drop_scope: DropScope,
    index_name: str,
    target: t.Optional[str],
    guard: str,
) -> str:
    """Render a DROP INDEX statement. `target` is required (already quoted) when
    `drop_scope` is TABLE_SCOPED."""
    name_sql = quote_identifier(index_name, dialect)
    if drop_scope is DropScope.TABLE_SCOPED:
        return f"DROP INDEX {guard}{name_sql} ON {target}"
    return f"DROP INDEX {guard}{name_sql}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py`
Expected: PASS (predicate + builder classes, all green).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/_index.py tests/test_unit/backends/ibis/test_index_render.py
git commit -m "feat(ibis): pure CREATE/DROP INDEX builders with dialect USING placement"
```

---

## Task 5: Harden + add `get_index_exists_sql` introspection SQL

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py` (add `_sql_literal`; harden `sqlite_/duckdb_/motherduck_get_index_exists_sql` at lines 235-325; add 5 new functions)
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py` (import + wire the 5 new functions; re-broaden invariant)
- Test: `tests/test_unit/backends/ibis/test_index_render.py` (append)

**Interfaces:**
- Consumes: `sqlglot.exp` (already imported in operations.py).
- Produces: `_sql_literal(value: str) -> str`; `postgres_get_index_exists_sql`, `mysql_get_index_exists_sql`, `mssql_get_index_exists_sql`, `oracle_get_index_exists_sql`, `singlestore_get_index_exists_sql`, each `(index_name: str, table_name: str | None, database: str | None) -> str` returning a `SELECT COUNT(*) AS count ...` query with **escaped** literals.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_index_render.py`:

```python
from mountainash_data.backends.ibis.operations import (
    _sql_literal,
    postgres_get_index_exists_sql,
    mysql_get_index_exists_sql,
    mssql_get_index_exists_sql,
    oracle_get_index_exists_sql,
    singlestore_get_index_exists_sql,
    sqlite_get_index_exists_sql,
)


class TestIntrospectionSql:
    def test_sql_literal_escapes_quote(self):
        assert _sql_literal("x'y") == "'x''y'"

    def test_existing_sqlite_now_escapes(self):
        sql = sqlite_get_index_exists_sql("a'b", "t", None)
        assert "'a''b'" in sql
        assert "count" in sql.lower()

    def test_postgres_shape_and_escaping(self):
        sql = postgres_get_index_exists_sql("idx", "t", "public")
        assert "pg_indexes" in sql
        assert "'idx'" in sql and "'t'" in sql and "'public'" in sql
        assert "count" in sql.lower()

    def test_mysql_is_table_scoped(self):
        sql = mysql_get_index_exists_sql("idx", "t", None)
        assert "STATISTICS" in sql.upper()
        assert "'idx'" in sql and "'t'" in sql

    def test_mssql_uses_object_id(self):
        sql = mssql_get_index_exists_sql("idx", "t", None)
        assert "sys.indexes" in sql and "OBJECT_ID" in sql.upper()

    def test_oracle_upper_folds(self):
        sql = oracle_get_index_exists_sql("idx", "t", None)
        assert "user_indexes" in sql.lower() and "UPPER" in sql.upper()

    def test_singlestore_shape(self):
        sql = singlestore_get_index_exists_sql("idx", "t", None)
        assert "STATISTICS" in sql.upper() and "'t'" in sql

    @pytest.mark.parametrize("fn", [
        postgres_get_index_exists_sql, mysql_get_index_exists_sql,
        mssql_get_index_exists_sql, oracle_get_index_exists_sql,
        singlestore_get_index_exists_sql,
    ])
    def test_injection_payload_is_escaped_not_broken(self, fn):
        sql = fn("x'; DROP TABLE t; --", "t", None)
        # the payload is contained in an escaped literal (doubled quote), not raw
        assert "''" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py::TestIntrospectionSql`
Expected: FAIL with `ImportError: cannot import name '_sql_literal'`.

- [ ] **Step 3: Add `_sql_literal`, harden existing 3, add 5 new**

In `operations.py`, after the imports (top of file), confirm `from sqlglot import exp` is present (it is — used by `build_rename_sql`). Add near the other helpers (after `_validate_simple_identifier`, ~line 159):

```python
def _sql_literal(value: str) -> str:
    """Render `value` as an escaped SQL string literal (defense-in-depth for the
    catalog-introspection queries; identifiers are also allowlist-validated by
    the generic dispatcher before reaching here)."""
    return exp.Literal.string(value).sql()
```

Replace the bodies of the three existing functions (`sqlite_get_index_exists_sql`, `duckdb_get_index_exists_sql`, `motherduck_get_index_exists_sql`) to use `_sql_literal` instead of raw f-string interpolation. Example for sqlite:

```python
def sqlite_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """SQLite uses the sqlite_master system table. `database` is unused (no
    cross-database queries)."""
    where_clauses = ["type = 'index'", f"name = {_sql_literal(index_name)}"]
    if table_name:
        where_clauses.append(f"tbl_name = {_sql_literal(table_name)}")
    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) AS count FROM sqlite_master WHERE {where_sql}"
```

Apply the same `_sql_literal` substitution to `duckdb_get_index_exists_sql` (keys `index_name`/`table_name`/`database_name`) and `motherduck_get_index_exists_sql` (identical to duckdb).

Add the 5 new functions (place them grouped with the existing introspection functions, ~after line 325):

```python
# --- PostgreSQL ---

def postgres_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """PostgreSQL pg_indexes catalog view. `database` maps to schemaname."""
    where = [f"indexname = {_sql_literal(index_name)}"]
    if table_name:
        where.append(f"tablename = {_sql_literal(table_name)}")
    if database:
        where.append(f"schemaname = {_sql_literal(database)}")
    return f"SELECT COUNT(*) AS count FROM pg_indexes WHERE {' AND '.join(where)}"


# --- MySQL / MariaDB ---

def mysql_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """information_schema.STATISTICS (table-scoped). Defaults schema to the
    current database when `database` is omitted."""
    where = [f"INDEX_NAME = {_sql_literal(index_name)}"]
    if table_name:
        where.append(f"TABLE_NAME = {_sql_literal(table_name)}")
    schema_pred = (
        f"TABLE_SCHEMA = {_sql_literal(database)}" if database else "TABLE_SCHEMA = DATABASE()"
    )
    where.append(schema_pred)
    return (
        "SELECT COUNT(*) AS count FROM information_schema.STATISTICS "
        f"WHERE {' AND '.join(where)}"
    )


# --- SQL Server ---

def mssql_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """sys.indexes joined to the table via OBJECT_ID (table-scoped)."""
    obj = table_name if table_name else ""
    if database and table_name:
        obj = f"{database}.{table_name}"
    return (
        "SELECT COUNT(*) AS count FROM sys.indexes "
        f"WHERE name = {_sql_literal(index_name)} "
        f"AND object_id = OBJECT_ID({_sql_literal(obj)})"
    )


# --- Oracle ---

def oracle_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """user_indexes (schema-global). Oracle folds unquoted identifiers to
    upper case, so match UPPER()."""
    where = [f"index_name = UPPER({_sql_literal(index_name)})"]
    if table_name:
        where.append(f"table_name = UPPER({_sql_literal(table_name)})")
    return f"SELECT COUNT(*) AS count FROM user_indexes WHERE {' AND '.join(where)}"


# --- SingleStore ---

def singlestore_get_index_exists_sql(
    index_name: str, table_name: str | None, database: str | None
) -> str:
    """information_schema.STATISTICS (MySQL-compatible, table-scoped)."""
    where = [f"INDEX_NAME = {_sql_literal(index_name)}"]
    if table_name:
        where.append(f"TABLE_NAME = {_sql_literal(table_name)}")
    if database:
        where.append(f"TABLE_SCHEMA = {_sql_literal(database)}")
    return (
        "SELECT COUNT(*) AS count FROM information_schema.STATISTICS "
        f"WHERE {' AND '.join(where)}"
    )
```

In `_registry.py`, extend the import block (line 645) and wire each spec's `get_index_exists_sql`:

```python
from mountainash_data.backends.ibis.operations import (  # noqa: E402
    duckdb_get_index_exists_sql,
    duckdb_get_list_indexes_sql,
    sqlite_get_index_exists_sql,
    sqlite_get_list_indexes_sql,
    motherduck_get_index_exists_sql,
    motherduck_get_list_indexes_sql,
    postgres_get_index_exists_sql,
    mysql_get_index_exists_sql,
    mssql_get_index_exists_sql,
    oracle_get_index_exists_sql,
    singlestore_get_index_exists_sql,
    duckdb_family_create_index,
    duckdb_family_drop_index,
)
```

Add `get_index_exists_sql=postgres_get_index_exists_sql` to the `postgres` spec, `=mysql_get_index_exists_sql` to `mysql`, `=mssql_get_index_exists_sql` to `mssql`, `=oracle_get_index_exists_sql` to `oracle`, `=singlestore_get_index_exists_sql` to `singlestoredb`.

Re-broaden the invariant test in `test_index_capability.py` to all 8:

```python
@pytest.mark.parametrize("name", _EXPECTED.keys())
def test_invariant_caps_implies_exists_sql(name):
    spec = DIALECTS[name]
    assert spec.index_caps is not None
    assert spec.get_index_exists_sql is not None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_render.py::TestIntrospectionSql tests/test_unit/backends/ibis/test_index_capability.py`
Expected: PASS (introspection golden + full 8-dialect invariant).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/operations.py src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_index_render.py tests/test_unit/backends/ibis/test_index_capability.py
git commit -m "feat(ibis): escape+harden index introspection SQL; add pg/mysql/mssql/oracle/singlestore"
```

---

## Task 6: Generic dispatchers `_generic_create_index` / `_generic_drop_index` / `_generic_index_exists`

**Files:**
- Modify: `src/mountainash_data/backends/ibis/_index.py` (append)
- Test: `tests/test_unit/backends/ibis/test_index_ops.py`

**Interfaces:**
- Consumes: `_render.dialect_of`, `_render.qualified_name`, `_render.compile_index_predicate`, `_render.IndexPredicate`; `operations._validate_simple_identifier`, `operations._normalize_columns`, `operations._generate_index_name`; `_registry.IndexCapability`, `_registry.DropScope`.
- Produces:
  - `_generic_index_exists(ibis_conn, index_name, *, table_name=None, database=None, exists_sql_fn) -> bool`
  - `_generic_create_index(ibis_conn, table_name, columns, *, index_name=None, unique=False, index_type=None, where=None, database=None, if_not_exists=True, caps, exists_sql_fn) -> None`
  - `_generic_drop_index(ibis_conn, index_name, *, table_name=None, database=None, if_exists=True, caps, exists_sql_fn) -> None`

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_index_ops.py`:

```python
"""Generic index dispatchers, exercised on in-memory sqlite/duckdb."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
from mountainash_data.backends.ibis.dialects._registry import DIALECTS

_SQLITE = DIALECTS["sqlite"].index_caps
_SQLITE_FN = DIALECTS["sqlite"].get_index_exists_sql
_DUCKDB = DIALECTS["duckdb"].index_caps
_DUCKDB_FN = DIALECTS["duckdb"].get_index_exists_sql


def _seed_sqlite():
    con = ibis.sqlite.connect()
    con.create_table("t", pl.DataFrame({"id": [1, 2], "active": [True, False]}))
    return con


class TestCreateDropExistsRoundtrip:
    def test_create_then_exists_then_drop(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_t_id", caps=_SQLITE,
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True
        _generic_drop_index(con, "idx_t_id", table_name="t", caps=_SQLITE,
                            exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is False

    def test_create_if_not_exists_is_idempotent_native(self):
        con = _seed_sqlite()
        for _ in range(2):
            _generic_create_index(
                con, "t", ["id"], index_name="idx_t_id", if_not_exists=True,
                caps=_SQLITE, exists_sql_fn=_SQLITE_FN,
            )  # second call must not raise (native IF NOT EXISTS)

    def test_default_index_name_generated(self):
        con = _seed_sqlite()
        _generic_create_index(con, "t", ["id"], caps=_SQLITE, exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True


class TestPartialIndex:
    def test_partial_where_on_sqlite(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_active",
            where=lambda r: r.active == True, caps=_SQLITE,  # noqa: E712
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_active", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True

    def test_where_on_non_partial_dialect_raises(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "active": [True]}))
        with pytest.raises(ValueError, match="partial"):
            _generic_create_index(
                con, "t", ["id"], where=lambda r: r.active, caps=_DUCKDB,
                exists_sql_fn=_DUCKDB_FN,
            )


class TestValidationErrors:
    def test_unsupported_index_type_raises(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="index_type"):
            _generic_create_index(
                con, "t", ["id"], index_type="hash", caps=_SQLITE,
                exists_sql_fn=_SQLITE_FN,
            )

    def test_table_scoped_drop_requires_table_name(self):
        con = _seed_sqlite()
        mysql_caps = DIALECTS["mysql"].index_caps
        with pytest.raises(ValueError, match="table_name"):
            _generic_drop_index(con, "idx", table_name=None, caps=mysql_caps,
                                exists_sql_fn=DIALECTS["mysql"].get_index_exists_sql)

    def test_bad_identifier_rejected(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="simple identifier"):
            _generic_create_index(con, "t", ["id"], index_name="x; DROP",
                                  caps=_SQLITE, exists_sql_fn=_SQLITE_FN)

    def test_drop_if_exists_absent_is_noop_native(self):
        con = _seed_sqlite()
        _generic_drop_index(con, "nope", table_name="t", if_exists=True,
                            caps=_SQLITE, exists_sql_fn=_SQLITE_FN)  # no raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_ops.py`
Expected: FAIL with `ImportError: cannot import name '_generic_create_index'`.

- [ ] **Step 3: Implement the dispatchers**

Append to `_index.py` (add the imports at the top of the file first):

```python
import ibis  # add to top-of-file imports

from mountainash_data.backends.ibis._render import (
    compile_index_predicate,
    dialect_of,
    qualified_name,
)
from mountainash_data.backends.ibis.dialects._registry import IndexCapability
from mountainash_data.backends.ibis.operations import (
    _generate_index_name,
    _normalize_columns,
    _validate_simple_identifier,
)
```

```python
def _generic_index_exists(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: t.Optional[str] = None,
    database: t.Optional[str] = None,
    exists_sql_fn: t.Any,
) -> bool:
    """Run the dialect's introspection SQL and return whether the index exists."""
    if exists_sql_fn is None:
        raise NotImplementedError("dialect has no get_index_exists_sql")
    _validate_simple_identifier(index_name, kind="index_name")
    if table_name is not None:
        _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    result = ibis_conn.sql(exists_sql_fn(index_name, table_name, database))
    if result is None:
        return False
    import mountainash as ma

    return ma.relation(result).to_dict()["count"][0] > 0


def _target_ref(ibis_conn: t.Any, table_name: str, database: t.Optional[str]) -> str:
    dialect = dialect_of(ibis_conn)
    parts = [database, table_name] if database else [table_name]
    return qualified_name(parts, dialect)


def _generic_create_index(
    ibis_conn: t.Any,
    table_name: str,
    columns: t.Union[list[str], str],
    *,
    index_name: t.Optional[str] = None,
    unique: bool = False,
    index_type: t.Optional[str] = None,
    where: t.Any = None,
    database: t.Optional[str] = None,
    if_not_exists: bool = True,
    caps: IndexCapability,
    exists_sql_fn: t.Any,
) -> None:
    """Render and execute a CREATE INDEX via the generic path (spec §5-§8)."""
    _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    cols = _normalize_columns(columns)
    for c in cols:
        _validate_simple_identifier(c, kind="column")

    if index_type is not None and index_type not in caps.index_types:
        raise ValueError(
            f"index_type {index_type!r} not supported by this dialect; "
            f"valid: {sorted(caps.index_types) or 'none'}"
        )
    if where is not None and not caps.partial:
        raise ValueError("this dialect does not support partial indexes (where=)")

    if index_name is None:
        index_name = _generate_index_name(table_name, cols, unique=unique)
    _validate_simple_identifier(index_name, kind="index_name")

    # Idempotency: native guard, or emulate via precheck.
    guard = ""
    if if_not_exists:
        if caps.native_if_not_exists:
            guard = "IF NOT EXISTS "
        elif _generic_index_exists(
            ibis_conn, index_name, table_name=table_name, database=database,
            exists_sql_fn=exists_sql_fn,
        ):
            return  # emulated: already present

    where_sql = None
    if where is not None:
        schema = ibis_conn.table(table_name, database=database).schema()
        where_sql = compile_index_predicate(ibis_conn, schema, table_name, where)

    sql = build_create_index_sql(
        dialect=dialect_of(ibis_conn),
        target=_target_ref(ibis_conn, table_name, database),
        index_name=index_name, cols=cols, unique=unique,
        index_type=index_type, guard=guard, where_sql=where_sql,
    )
    ibis_conn.raw_sql(sql)


def _generic_drop_index(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: t.Optional[str] = None,
    database: t.Optional[str] = None,
    if_exists: bool = True,
    caps: IndexCapability,
    exists_sql_fn: t.Any,
) -> None:
    """Render and execute a DROP INDEX via the generic path (spec §5-§8)."""
    _validate_simple_identifier(index_name, kind="index_name")
    if caps.drop_scope is DropScope.TABLE_SCOPED and table_name is None:
        raise ValueError(
            "drop_index requires table_name for this dialect (DROP INDEX ... ON tbl)"
        )
    if table_name is not None:
        _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")

    guard = ""
    if if_exists:
        if caps.native_if_exists:
            guard = "IF EXISTS "
        elif not _generic_index_exists(
            ibis_conn, index_name, table_name=table_name, database=database,
            exists_sql_fn=exists_sql_fn,
        ):
            return  # emulated: already absent

    target = _target_ref(ibis_conn, table_name, database) if table_name else None
    sql = build_drop_index_sql(
        dialect=dialect_of(ibis_conn), drop_scope=caps.drop_scope,
        index_name=index_name, target=target, guard=guard,
    )
    ibis_conn.raw_sql(sql)
```

Note: `import ibis` is needed only if referenced; the dispatchers use `ibis_conn` directly, so the `import ibis` line may be unnecessary — include it only if a linter flags an undefined name (it is not used in the code above; omit it if `ruff` reports F401).

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_ops.py`
Expected: PASS (roundtrip, partial, validation errors all green).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/_index.py tests/test_unit/backends/ibis/test_index_ops.py
git commit -m "feat(ibis): generic create/drop/exists dispatchers with emulation + validation"
```

---

## Task 7: Backend wiring — dispatch + `where` predicate + table-scoped validation

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py` (`create_index` lines 576-599, `create_unique_index` 601-614, `drop_index` 616-633, `index_exists` 635-654)
- Test: `tests/test_unit/backends/ibis/test_index_ops.py` (append a backend-level class)

**Interfaces:**
- Consumes: `_index._generic_create_index`, `_index._generic_drop_index`, `_index._generic_index_exists`.
- Produces: `IbisBackend.create_index(table_name, columns, *, index_name=None, unique=False, index_type=None, where=None, database=None, if_not_exists=True) -> IbisBackend`; `drop_index(index_name, *, table_name=None, database=None, if_exists=True) -> IbisBackend`; `index_exists(index_name, *, table_name=None, database=None) -> bool`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_index_ops.py`:

```python
from mountainash_data import IbisBackend


class TestBackendDispatch:
    def test_create_exists_drop_via_backend(self):
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            be._ibis_conn.create_table(
                "t", pl.DataFrame({"id": [1], "active": [True]})
            )
            assert be.create_index("t", ["id"], index_name="ix") is be
            assert be.index_exists("ix", table_name="t") is True
            assert be.drop_index("ix", table_name="t") is be
            assert be.index_exists("ix", table_name="t") is False
        finally:
            be.close()

    def test_where_predicate_via_backend(self):
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            be._ibis_conn.create_table(
                "t", pl.DataFrame({"id": [1], "active": [True]})
            )
            be.create_index("t", ["id"], index_name="ixp",
                            where=lambda r: r.active == True)  # noqa: E712
            assert be.index_exists("ixp", table_name="t") is True
        finally:
            be.close()

    def test_unsupported_dialect_raises_notimplemented(self):
        from mountainash_data.backends.ibis.dialects._registry import DialectSpec
        be = IbisBackend(dialect="sqlite", database=":memory:")
        be.connect()
        try:
            # Rebind the INSTANCE's _spec to a fresh no-index spec (index_caps and
            # create_index_hook default to None). Never mutate the shared frozen
            # singleton in DIALECTS — that would corrupt other tests.
            be._spec = DialectSpec(
                ibis_backend_name="sqlite",
                connection_mode="connection_string",
                connection_string_scheme="sqlite://",
            )
            with pytest.raises(NotImplementedError):
                be.create_index("t", ["id"])
        finally:
            be.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_ops.py::TestBackendDispatch`
Expected: FAIL — `create_index` still routes only through the hook and rejects the `where=` kwarg (`TypeError: unexpected keyword argument 'where'`).

- [ ] **Step 3: Rewrite the four backend methods**

Add the import near the other `_index`/operations imports at the top of `backend.py`:

```python
from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
```

Replace `create_index`:

```python
    def create_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        unique: bool = False,
        index_type: str | None = None,
        where: t.Any = None,  # IndexPredicate | None
        database: str | None = None,
        if_not_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.create_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, database=database, if_not_exists=if_not_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_create_index(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, database=database, if_not_exists=if_not_exists,
                caps=self._spec.index_caps,
                exists_sql_fn=self._spec.get_index_exists_sql,
            )
        else:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support create_index"
            )
        return self
```

Replace `create_unique_index` (drop the removed `where_condition`, use `where`):

```python
    def create_unique_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where: t.Any = None,  # IndexPredicate | None
        database: str | None = None,
    ) -> IbisBackend:
        return self.create_index(
            table_name, columns,
            index_name=index_name, unique=True, where=where, database=database,
        )
```

Replace `drop_index`:

```python
    def drop_index(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.drop_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, index_name,
                table_name=table_name, database=database, if_exists=if_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_drop_index(
                conn._ibis_conn, index_name,
                table_name=table_name, database=database, if_exists=if_exists,
                caps=self._spec.index_caps,
                exists_sql_fn=self._spec.get_index_exists_sql,
            )
        else:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support drop_index"
            )
        return self
```

Replace `index_exists` body to delegate to the shared dispatcher (keeps the `count` extraction in one place):

```python
    def index_exists(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
    ) -> bool:
        if self._spec.get_index_exists_sql is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support index_exists"
            )
        conn = self._require_connected()
        return _generic_index_exists(
            conn._ibis_conn, index_name,
            table_name=table_name, database=database,
            exists_sql_fn=self._spec.get_index_exists_sql,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_index_ops.py::TestBackendDispatch`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_index_ops.py
git commit -m "feat(ibis): wire create/drop/exists to generic dispatch; where predicate param"
```

---

## Task 8: Cutover — retire `duckdb_family_*`, update mechanism tests, `where_condition` audit

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py` (remove `create_index_hook=`/`drop_index_hook=` from sqlite/duckdb/motherduck; drop the two names from the import block)
- Modify: `src/mountainash_data/backends/ibis/operations.py` (delete `duckdb_family_create_index` lines 362-398 and `duckdb_family_drop_index` lines 401-414; remove now-unused imports `contextlib`/`warnings`/`CONST_INDEX_TYPE` IF they are unused after deletion — verify first)
- Modify: `tests/test_unit/backends/ibis/test_backend.py` (lines 229-232 hook-mechanism assertion; 313-351 functional tests should keep passing through the generic path)
- Test: full unit suite

- [ ] **Step 1: Write/adjust the failing test**

In `tests/test_unit/backends/ibis/test_backend.py`, replace `test_sqlite_dialect_has_create_index_hook` (line 229) with a generic-dispatch assertion:

```python
def test_sqlite_dialect_uses_generic_index_path():
    """After cutover, sqlite has no index hooks and dispatches via index_caps."""
    from mountainash_data.backends.ibis.dialects._registry import DIALECTS
    spec = DIALECTS["sqlite"]
    assert spec.create_index_hook is None
    assert spec.drop_index_hook is None
    assert spec.index_caps is not None
```

Add a guard test that the retired symbols are gone:

```python
def test_duckdb_family_index_hooks_removed():
    import mountainash_data.backends.ibis.operations as ops
    assert not hasattr(ops, "duckdb_family_create_index")
    assert not hasattr(ops, "duckdb_family_drop_index")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_backend.py::test_duckdb_family_index_hooks_removed"`
Expected: FAIL — the symbols still exist.

- [ ] **Step 3: Delete the functions and registrations**

In `operations.py`, delete `duckdb_family_create_index` and `duckdb_family_drop_index` (lines ~362-414). Then check whether `contextlib`, `warnings`, and `CONST_INDEX_TYPE` are still referenced elsewhere in the file:

Run: `grep -n "contextlib\|warnings\.\|CONST_INDEX_TYPE" src/mountainash_data/backends/ibis/operations.py`
Remove any import that is now unused (only if grep shows no remaining references).

In `_registry.py`, remove `duckdb_family_create_index` and `duckdb_family_drop_index` from the import block (lines 652-653), and remove the `create_index_hook=duckdb_family_create_index,` and `drop_index_hook=duckdb_family_drop_index,` lines from the sqlite, duckdb, and motherduck specs.

- [ ] **Step 4: Run the audits and the full unit suite**

`where_condition` removal audit:

Run: `grep -rn "where_condition" src/ tests/ docs/`
Expected: no matches in `src/` or `tests/` (docs spec may reference it historically — acceptable). Fix any live callsite found.

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/ tests/test_unit/backends/ibis/test_backend.py`
Expected: PASS — including the previously-existing functional `test_create_index_returns_self` / `test_drop_index_returns_self` / `test_index_exists` (now flowing through the generic path).

Run: `hatch run ruff:check` then `hatch run mypy:check`
Expected: ruff clean; mypy Success.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/operations.py src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_backend.py
git commit -m "refactor(ibis): cutover index ops to generic dispatch; retire duckdb_family hooks"
```

---

## Task 9: Live integration — postgres + mariadb round-trips, partial, table-scoped, emulation

**Files:**
- Create: `tests/test_integration/test_index_ops_live.py`
- Uses: existing `tests/fixtures/database_fixtures.py` (`postgres_backend`, `mysql_backend`) and `compose.yaml`.

**Interfaces:**
- Consumes: `postgres_backend`, `mysql_backend` fixtures (skip-if-unreachable; fail-closed under `MOUNTAINASH_REQUIRE_LIVE_DB=1`).

- [ ] **Step 1: Write the live test**

Create `tests/test_integration/test_index_ops_live.py`:

```python
"""Live index ops against postgres (native) and mariadb (table-scoped + emulated)."""

import polars as pl
import pytest

pytestmark = pytest.mark.integration


def _fresh_table(be, name):
    conn = be._ibis_conn
    try:
        conn.drop_table(name, force=True)
    except Exception:  # noqa: BLE001
        pass
    conn.create_table(name, pl.DataFrame({"id": [1, 2, 3], "active": [True, False, True]}))


class TestPostgresLive:
    def test_roundtrip_and_partial(self, postgres_backend):
        be = postgres_backend
        _fresh_table(be, "ix_live")
        be.create_index("ix_live", ["id"], index_name="ix_live_id")
        assert be.index_exists("ix_live_id", table_name="ix_live") is True
        # partial (filtered) index — postgres supports WHERE
        be.create_index("ix_live", ["id"], index_name="ix_live_active",
                        where=lambda r: r.active == True)  # noqa: E712
        assert be.index_exists("ix_live_active", table_name="ix_live") is True
        be.drop_index("ix_live_id")           # schema-global: no table needed
        assert be.index_exists("ix_live_id", table_name="ix_live") is False

    def test_using_gin_index_type(self, postgres_backend):
        be = postgres_backend
        _fresh_table(be, "ix_gin")
        be.create_index("ix_gin", ["id"], index_name="ix_gin_btree", index_type="btree")
        assert be.index_exists("ix_gin_btree", table_name="ix_gin") is True


class TestMariaDBLive:
    def test_table_scoped_drop_requires_table(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_my")
        be.create_index("ix_my", ["id"], index_name="ix_my_id")
        assert be.index_exists("ix_my_id", table_name="ix_my") is True
        # schema-global drop must be rejected for a TABLE_SCOPED dialect
        with pytest.raises(ValueError, match="table_name"):
            be.drop_index("ix_my_id")
        be.drop_index("ix_my_id", table_name="ix_my")
        assert be.index_exists("ix_my_id", table_name="ix_my") is False

    def test_emulated_if_not_exists_is_idempotent(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_emu")
        # mysql dialect emulates IF NOT EXISTS via precheck; double-create is a no-op
        be.create_index("ix_emu", ["id"], index_name="ix_emu_id", if_not_exists=True)
        be.create_index("ix_emu", ["id"], index_name="ix_emu_id", if_not_exists=True)
        assert be.index_exists("ix_emu_id", table_name="ix_emu") is True

    def test_emulated_if_exists_drop_absent_is_noop(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_emu2")
        be.drop_index("nope", table_name="ix_emu2", if_exists=True)  # no raise
```

- [ ] **Step 2: Start the live databases**

```bash
docker compose -f compose.yaml up -d
```
Expected: postgres and mariadb containers healthy.

- [ ] **Step 3: Run the live suite (fail-closed)**

Run: `MOUNTAINASH_REQUIRE_LIVE_DB=1 hatch run test:test-target tests/test_integration/test_index_ops_live.py`
Expected: PASS (postgres + mariadb classes green). If a fixture skips under this flag, the env is misconfigured — fix connectivity, do not weaken the test.

- [ ] **Step 4: Run the full suite + gates**

```bash
hatch run test:test-target-quick tests/test_unit/
hatch run ruff:check
hatch run mypy:check
```
Expected: unit green; ruff clean; mypy Success.

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration/test_index_ops_live.py
git commit -m "test(ibis): live index round-trips (postgres native + mariadb emulated/table-scoped)"
```

---

## Self-Review

**1. Spec coverage:**
- §2 scope (conventional only, None sentinel) → Task 2 (`_UNSUPPORTED`).
- §3 capability model + invariant → Tasks 1, 2, 5.
- §4 verified matrix → Task 2 (`_EXPECTED`), Global Constraints.
- §5.1 builders + USING placement → Task 4.
- §5.2 predicate compiler (AST strip, full-schema bind, non-indexed cols) → Task 3.
- §6 idempotency/emulation + injection contract → Tasks 5 (escaping), 6 (validation + precheck).
- §7 public API (`where` predicate) → Task 7.
- §8 error table (all rows) → Task 6 tests (`TestValidationErrors`), Task 7.
- §9 testing (golden render-only, introspection golden, live, registry-consistency) → Tasks 2, 4, 5, 9.
- §10 cutover (retire family, `where_condition` audit, new introspection) → Tasks 5, 8.
- §11 file structure → matches.
- mssql/oracle/singlestore are render-only (no live container) — covered by Task 4/5 golden tests; documented as render-only in the spec.

**2. Placeholder scan:** No TBD/TODO. Every code step shows full code. The only conditional instruction (remove unused imports in Task 8) is gated on an explicit `grep` check.

**3. Type consistency:** `IndexCapability` fields, `DropScope` members, dispatcher signatures (`caps=`, `exists_sql_fn=`), and the `where`/`IndexPredicate` param name are identical across Tasks 1→2→6→7. `get_index_exists_sql` signature `(index_name, table_name, database)` matches the existing `GetIndexExistsSql` type alias and all 8 implementations. `count` column alias is consistent between the introspection builders (Task 5) and `_generic_index_exists` (Task 6).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-30-generic-default-index-operations.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks.
**2. Inline Execution** — batch execution with checkpoints.
