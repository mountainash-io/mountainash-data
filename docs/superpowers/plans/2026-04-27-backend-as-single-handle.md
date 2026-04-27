# Backend as Single Handle — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `IbisBackend` the single public handle for all ibis interaction — lifecycle, inspection, and operations — then delete all legacy factories, utils, and class hierarchies.

**Architecture:** `IbisBackend` composes an internal `IbisConnection` for inspection, delegates thin wrapper operations to the raw ibis connection, and dispatches per-dialect operations (upsert, indexes) via callable hooks on `DialectSpec`. Fluent methods return `self`; terminal methods return data.

**Tech Stack:** Python 3.12, ibis-framework 10.4.0, pytest 8.3.5, hatch

**Spec:** `docs/superpowers/specs/2026-04-27-backend-as-single-handle-design.md`
**Branch:** `feature/settings-aware-ibis-backend` (continuing from Phase 1)
**Test command:** `hatch run test:test-quick`

---

## File Structure

### Modified

| File | Responsibility |
|------|----------------|
| `src/mountainash_data/core/protocol.py` | Updated `Backend` protocol (connect→Self, context manager, inspection). Remove `Connection` protocol. |
| `src/mountainash_data/backends/ibis/backend.py` | Lifecycle (`connect`/`close`/context manager), accessor methods, thin wrappers, hook-dispatched operations |
| `src/mountainash_data/backends/ibis/dialects/_registry.py` | New hook fields on `DialectSpec`, wiring to standalone functions |
| `src/mountainash_data/backends/ibis/operations.py` | Extract mixin methods to standalone hook functions. Delete class hierarchy (keep functions only). |
| `src/mountainash_data/backends/iceberg/backend.py` | `connect()` returns `Self`, add `__enter__`/`__exit__`/`close()` |
| `src/mountainash_data/__init__.py` | Remove factory/utils/Connection exports |
| `tests/test_unit/backends/ibis/test_backend.py` | Expand with lifecycle + operations tests |
| `tests/test_unit/test_mountainash_data.py` | Update import assertions for new public API |
| `tests/test_unit/databases/settings/test_settings_parametrized.py` | Replace factory/utils calls with `IbisBackend` |
| `tests/test_integration/test_end_to_end_workflows.py` | Rewrite to use `IbisBackend` |

### Deleted

| File/Directory | Reason |
|----------------|--------|
| `src/mountainash_data/core/factories/` | Entire directory — all factories replaced |
| `src/mountainash_data/core/utils.py` | `DatabaseUtils` replaced by `IbisBackend` |
| `src/mountainash_data/backends/ibis/connection.py` | `BaseIbisConnection` + 12 subclasses replaced |
| `tests/test_unit/factories/` | All factory tests |
| `tests/test_unit/test_database_utils.py` | `DatabaseUtils` tests |
| `tests/test_unit/databases/test_database_connections.py` | Legacy connection tests |
| `tests/test_unit/databases/connections/` | Legacy connection lifecycle tests |
| `tests/test_unit/databases/test_ibis_backends.py` | Legacy ibis backend tests |
| `tests/test_unit/databases/operations/` | Legacy operations tests (rewritten in test_backend.py) |

### Kept (no changes)

| File | Reason |
|------|--------|
| `src/mountainash_data/core/connection.py` | Iceberg depends on `BaseDBConnection` |
| `src/mountainash_data/core/inspection.py` | Unchanged |
| `src/mountainash_data/core/settings/` | Unchanged |
| `src/mountainash_data/backends/ibis/inspect.py` | Unchanged |

---

### Task 1: Update Backend protocol and add lifecycle to IbisBackend

**Files:**
- Modify: `src/mountainash_data/core/protocol.py`
- Modify: `src/mountainash_data/backends/ibis/backend.py`
- Test: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing lifecycle tests**

Add these tests to `tests/test_unit/backends/ibis/test_backend.py`:

```python
# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_connect_returns_self():
    """connect() must return the backend instance itself."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    result = backend.connect()
    assert result is backend


def test_close_returns_self():
    """close() must return the backend instance itself."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    result = backend.close()
    assert result is backend


def test_context_manager():
    """with IbisBackend(...) as backend: must connect and close."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        assert backend.list_tables() == []
    # After exit, should be closed
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_double_close_is_idempotent():
    """Calling close() twice must not raise."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    backend.close()
    backend.close()  # Must not raise


def test_use_before_connect_raises():
    """Calling methods before connect() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_use_after_close_raises():
    """Calling methods after close() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    backend.close()
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_ibis_connection_accessor():
    """ibis_connection() returns the raw ibis backend object."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        raw = backend.ibis_connection()
        assert hasattr(raw, "list_tables")


def test_ibis_connection_before_connect_raises():
    """ibis_connection() before connect() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    with pytest.raises(RuntimeError, match="not connected"):
        backend.ibis_connection()


def test_get_connection_accessor():
    """get_connection() returns our IbisConnection wrapper."""
    from mountainash_data.backends.ibis.backend import IbisConnection
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        conn = backend.get_connection()
        assert isinstance(conn, IbisConnection)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_connect_returns_self -v`
Expected: FAIL (connect() returns IbisConnection, not self)

- [ ] **Step 3: Update Backend protocol**

Replace the entire contents of `src/mountainash_data/core/protocol.py` with:

```python
"""Backend protocol.

This is the structural contract every backend implementation must
satisfy. Implementations are plain classes — there is no inheritance.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)


@t.runtime_checkable
class Backend(t.Protocol):
    """The single handle for interacting with a backend service.

    Backends are constructed with config, connected via connect(),
    used for inspection and operations, then closed.
    """

    name: str

    def connect(self) -> t.Self: ...
    def close(self) -> t.Self: ...
    def __enter__(self) -> t.Self: ...
    def __exit__(self, *args: t.Any) -> None: ...

    def list_tables(self, namespace: str | None = None) -> list[str]: ...
    def list_namespaces(self) -> list[str]: ...

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo: ...

    def inspect_namespace(self, name: str) -> NamespaceInfo: ...
    def inspect_catalog(self) -> CatalogInfo: ...
```

- [ ] **Step 4: Add lifecycle and accessor methods to IbisBackend**

In `src/mountainash_data/backends/ibis/backend.py`, make these changes:

4a. Add `_conn: IbisConnection | None = None` initialisation to each `_init_from_*` method (set `self._conn = None`).

4b. Replace the existing `connect()` method and add `close()`, `__enter__`, `__exit__`, accessor methods, and a `_require_connected` helper. Replace everything from `def connect(self)` to end of file with:

```python
    def _require_connected(self) -> IbisConnection:
        if self._conn is None:
            raise RuntimeError(
                "IbisBackend is not connected. Call connect() first."
            )
        return self._conn

    def connect(self) -> IbisBackend:
        """Build a live ibis connection. Returns self for fluent chaining."""
        if self._conn is not None:
            return self
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        if self._url is not None:
            import ibis
            ibis_conn = ibis.connect(self._url, **self._config)
        else:
            cleaned_config = {
                k: v for k, v in self._config.items()
                if not (isinstance(v, (list, tuple)) and len(v) == 0)
            }
            ibis_conn = self._spec.connection_builder(**cleaned_config)
        self._conn = IbisConnection(ibis_conn, self._spec)
        return self

    def close(self) -> IbisBackend:
        """Release the connection. Idempotent. Returns self."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        return self

    def __enter__(self) -> IbisBackend:
        self.connect()
        return self

    def __exit__(self, *args: t.Any) -> None:
        self.close()

    def ibis_connection(self) -> t.Any:
        """Return the raw ibis backend object."""
        return self._require_connected()._ibis_conn

    def get_connection(self) -> IbisConnection:
        """Return the internal IbisConnection wrapper."""
        return self._require_connected()

    # --- Inspection (terminal — delegates to IbisConnection) ---

    def list_tables(self, namespace: str | None = None) -> list[str]:
        return self._require_connected().list_tables(namespace=namespace)

    def list_namespaces(self) -> list[str]:
        return self._require_connected().list_namespaces()

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo:
        return self._require_connected().inspect_table(name, namespace=namespace)

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return self._require_connected().inspect_namespace(name)

    def inspect_catalog(self) -> CatalogInfo:
        return self._require_connected().inspect_catalog()
```

4c. In each `_init_from_*` method, add `self._conn = None` after setting `self._config`:
- `_init_from_dialect`: after `self._config = config`, add `self._conn = None`
- `_init_from_url`: after `self._config = config`, add `self._conn = None`
- `_init_from_settings`: after `self._config = driver_kwargs`, add `self._conn = None`

- [ ] **Step 5: Update existing tests that use the old connect() return**

In `tests/test_unit/backends/ibis/test_backend.py`, update `test_in_memory_sqlite_connect_and_inspect` — it currently does `conn = backend.connect()` expecting an `IbisConnection`. Change it to use the backend directly:

```python
def test_in_memory_sqlite_connect_and_inspect():
    """End-to-end test with the only dialect that needs no external service."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    try:
        assert backend.list_tables() == []
    finally:
        backend.close()
```

Update the settings path tests similarly:

```python
def test_settings_path_sqlite():
    """Construct IbisBackend from SQLite SettingsParameters and connect."""
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    assert backend.dialect == "sqlite"
    backend.connect()
    tables = backend.list_tables()
    assert isinstance(tables, list)
    backend.close()


def test_settings_path_duckdb_empty_extensions():
    """DuckDB settings with default EXTENSIONS=[] must not crash ibis."""
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import DuckDBAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=DuckDBAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    assert backend.dialect == "duckdb"
    backend.connect()  # Must not raise — empty-list filter active
    backend.close()
```

Update URL path tests:

```python
def test_url_path_sqlite():
    """Construct IbisBackend from sqlite:// URL and connect."""
    backend = IbisBackend("sqlite://")
    assert backend.dialect == "sqlite"
    backend.connect()
    backend.close()


def test_url_path_duckdb():
    """Construct IbisBackend from duckdb:// URL and connect."""
    backend = IbisBackend("duckdb://")
    assert backend.dialect == "duckdb"
    backend.connect()
    backend.close()


def test_url_path_preserves_database(tmp_path):
    """URL database component must reach the driver, not be discarded."""
    db_file = tmp_path / "test.db"
    backend = IbisBackend(f"sqlite:///{db_file}")
    assert backend.dialect == "sqlite"
    backend.connect()
    backend.close()
    assert db_file.exists()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add src/mountainash_data/core/protocol.py src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_backend.py
git commit -m "feat(backend): add lifecycle, context manager, and accessor methods to IbisBackend

connect() and close() return self for fluent chaining. Context manager
support via __enter__/__exit__. Inspection methods delegate to internal
IbisConnection. Backend protocol updated: connect() returns Self,
Connection protocol removed."
```

---

### Task 2: Expand DialectSpec with operation hooks and extract standalone functions

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py`
- Modify: `src/mountainash_data/backends/ibis/operations.py`
- Test: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing test for hook presence on DialectSpec**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
# ---------------------------------------------------------------------------
# DialectSpec hooks
# ---------------------------------------------------------------------------

def test_duckdb_dialect_has_upsert_hook():
    """DuckDB DialectSpec must have upsert_hook wired."""
    spec = DIALECTS["duckdb"]
    assert spec.upsert_hook is not None


def test_sqlite_dialect_has_create_index_hook():
    """SQLite DialectSpec must have create_index_hook wired."""
    spec = DIALECTS["sqlite"]
    assert spec.create_index_hook is not None


def test_postgres_dialect_has_no_upsert_hook():
    """Postgres DialectSpec has no upsert_hook (not DuckDB family)."""
    spec = DIALECTS["postgres"]
    assert spec.upsert_hook is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_duckdb_dialect_has_upsert_hook -v`
Expected: FAIL (DialectSpec has no `upsert_hook` attribute)

- [ ] **Step 3: Add hook fields to DialectSpec**

In `src/mountainash_data/backends/ibis/dialects/_registry.py`, add new type aliases and fields.

After the existing type aliases (line 27), add:

```python
UpsertHook = t.Callable[..., None]
CreateIndexHook = t.Callable[..., None]
DropIndexHook = t.Callable[..., None]
RenameTableHook = t.Callable[..., None]
```

Add new fields to the `DialectSpec` dataclass, after `get_list_indexes_sql` and before `extras`:

```python
    upsert_hook: t.Optional[UpsertHook] = None
    create_index_hook: t.Optional[CreateIndexHook] = None
    drop_index_hook: t.Optional[DropIndexHook] = None
    rename_table_hook: t.Optional[RenameTableHook] = None
```

- [ ] **Step 4: Extract standalone hook functions from operations.py**

In `src/mountainash_data/backends/ibis/operations.py`, convert the `_DuckDBFamilyOperationsMixin` class methods into standalone functions. Add these after the existing per-dialect SQL functions (after `motherduck_list_tables`, around line 213) and before the `_DuckDBFamilyOperationsMixin` class:

```python
# ===========================================================================
# STANDALONE HOOK FUNCTIONS
# Extracted from _DuckDBFamilyOperationsMixin for DialectSpec wiring.
# ===========================================================================

def duckdb_family_create_index(
    ibis_conn: t.Any,
    table_name: str,
    columns: list[str] | str,
    *,
    index_name: str | None = None,
    unique: bool = False,
    index_type: str | None = None,
    where_condition: str | None = None,
    database: str | None = None,
    if_not_exists: bool = True,
) -> None:
    """Create an index using DuckDB/SQLite syntax."""
    columns_list = _normalize_columns(columns)

    if index_name is None:
        index_name = _generate_index_name(table_name, columns_list, unique=unique)

    qualified_table = _format_qualified_table(table_name, database=database)
    columns_sql = ", ".join(columns_list)

    unique_sql = "UNIQUE " if unique else ""
    if_not_exists_sql = "IF NOT EXISTS " if if_not_exists else ""
    where_sql = f" WHERE {where_condition}" if where_condition else ""

    if index_type and index_type != CONST_INDEX_TYPE.BTREE:
        warnings.warn(
            f"Index type {index_type} not supported, using default BTREE"
        )

    create_sql = (
        f"CREATE {unique_sql}INDEX {if_not_exists_sql}{index_name} "
        f"ON {qualified_table} ({columns_sql}){where_sql}"
    )

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(create_sql)


def duckdb_family_drop_index(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: str | None = None,
    database: str | None = None,
    if_exists: bool = True,
) -> None:
    """Drop an index using DuckDB/SQLite syntax."""
    if_exists_sql = "IF EXISTS " if if_exists else ""
    drop_sql = f"DROP INDEX {if_exists_sql}{index_name}"

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(drop_sql)


def duckdb_family_upsert(
    ibis_conn: t.Any,
    table_name: str,
    df: t.Any,
    *,
    conflict_columns: list[str] | str,
    update_columns: list[str] | str | None = None,
    conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
    update_condition: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> None:
    """Perform upsert using INSERT ... ON CONFLICT syntax (DuckDB/SQLite)."""
    conflict_cols = _normalize_columns(conflict_columns)
    all_columns = ma.relation(df).columns

    if update_columns is None:
        update_cols = [col for col in all_columns if col not in conflict_cols]
    else:
        update_cols = _normalize_columns(update_columns)

    tables = ibis_conn.list_tables()
    if table_name not in tables:
        raise ValueError(f"Target table '{table_name}' does not exist")

    if conflict_action not in [CONST_CONFLICT_ACTION.UPDATE, CONST_CONFLICT_ACTION.NOTHING]:
        raise ValueError(
            f"conflict_action must be '{CONST_CONFLICT_ACTION.UPDATE}' or "
            f"'{CONST_CONFLICT_ACTION.NOTHING}', got '{conflict_action}'"
        )

    if conflict_action == CONST_CONFLICT_ACTION.NOTHING:
        if update_cols or update_condition:
            warnings.warn(
                "update_columns and update_condition are ignored when "
                "conflict_action='NOTHING'"
            )

    staging_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
    qualified_table = _format_qualified_table(table_name, database=database, schema=schema)

    all_cols_sql = ", ".join(all_columns)
    conflict_cols_sql = ", ".join(conflict_cols)

    if conflict_action == CONST_CONFLICT_ACTION.UPDATE:
        if not update_cols:
            raise ValueError(
                "No columns to update. Either provide update_columns or ensure "
                "dataframe has columns beyond conflict_columns"
            )
        update_set_sql = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        where_sql = f" WHERE {update_condition}" if update_condition else ""
        on_conflict_sql = (
            f"ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {update_set_sql}{where_sql}"
        )
    else:
        on_conflict_sql = f"ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    upsert_sql = f"""
        INSERT INTO {qualified_table} ({all_cols_sql})
        SELECT {all_cols_sql} FROM {staging_table}
        WHERE true
        {on_conflict_sql}
    """

    if hasattr(ibis_conn.con, 'register'):
        with contextlib.closing(ibis_conn.con.cursor()) as cur:
            cur.execute("BEGIN TRANSACTION")
            cur.register(staging_table, df)
            cur.execute(upsert_sql)
            cur.unregister(staging_table)
            cur.execute("COMMIT")
    else:
        ibis_conn.create_table(staging_table, df, temp=True, overwrite=True)
        try:
            with contextlib.closing(ibis_conn.con.cursor()) as cur:
                cur.execute(upsert_sql)
                ibis_conn.con.commit()
        finally:
            try:
                ibis_conn.drop_table(staging_table, force=True)
            except Exception:
                pass
```

- [ ] **Step 5: Wire hooks in DIALECTS registry**

In `src/mountainash_data/backends/ibis/dialects/_registry.py`, add imports for the new hook functions alongside the existing SQL function imports:

```python
from mountainash_data.backends.ibis.operations import (  # noqa: E402
    duckdb_get_index_exists_sql,
    duckdb_get_list_indexes_sql,
    sqlite_get_index_exists_sql,
    sqlite_get_list_indexes_sql,
    motherduck_get_index_exists_sql,
    motherduck_get_list_indexes_sql,
    duckdb_family_upsert,
    duckdb_family_create_index,
    duckdb_family_drop_index,
)
```

Then add hook fields to the `sqlite`, `duckdb`, and `motherduck` entries in `DIALECTS`:

For `"sqlite"`:
```python
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
```

For `"duckdb"`:
```python
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
```

For `"motherduck"`:
```python
        upsert_hook=duckdb_family_upsert,
        create_index_hook=duckdb_family_create_index,
        drop_index_hook=duckdb_family_drop_index,
```

- [ ] **Step 6: Run tests**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add src/mountainash_data/backends/ibis/dialects/_registry.py src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_backend.py
git commit -m "feat(registry): expand DialectSpec with operation hooks

Add upsert_hook, create_index_hook, drop_index_hook, rename_table_hook
to DialectSpec. Extract standalone hook functions from
_DuckDBFamilyOperationsMixin. Wire DuckDB/SQLite/MotherDuck entries."
```

---

### Task 3: Add thin wrapper and hook-dispatched operations to IbisBackend

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py`
- Test: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing tests for thin wrapper operations**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
import polars as pl

# ---------------------------------------------------------------------------
# Thin wrapper operations (fluent)
# ---------------------------------------------------------------------------

def test_create_table_returns_self():
    """create_table() must return self for fluent chaining."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        result = backend.create_table("t", {"id": [1, 2]})
        assert result is backend
        assert "t" in backend.list_tables()


def test_drop_table_returns_self():
    """drop_table() must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.drop_table("t")
        assert result is backend
        assert "t" not in backend.list_tables()


def test_insert_returns_self():
    """insert() must return self."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.insert("t", {"id": [2]})
        assert result is backend


def test_truncate_returns_self():
    """truncate() must return self."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.truncate("t")
        assert result is backend


def test_table_returns_ibis_table():
    """table() must return an ibis table expression."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1, 2]})
        tbl = backend.table("t")
        assert tbl is not None


def test_run_sql_returns_result():
    """run_sql() must return an ibis table expression."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1, 2, 3]})
        result = backend.run_sql("SELECT COUNT(*) as cnt FROM t")
        assert result is not None


def test_table_exists_returns_bool():
    """table_exists() must return True/False."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        assert backend.table_exists("nope") is False
        backend.create_table("t", {"id": [1]})
        assert backend.table_exists("t") is True


def test_fluent_chaining():
    """Multiple fluent calls can be chained."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("a", {"id": [1]}).create_table("b", {"id": [2]})
        assert sorted(backend.list_tables()) == ["a", "b"]
```

- [ ] **Step 2: Write failing tests for hook-dispatched operations**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
# ---------------------------------------------------------------------------
# Hook-dispatched operations
# ---------------------------------------------------------------------------

def test_create_index_returns_self():
    """create_index() via hook must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        result = backend.create_index("t", ["name"])
        assert result is backend


def test_create_unique_index_returns_self():
    """create_unique_index() must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        result = backend.create_unique_index("t", ["name"])
        assert result is backend


def test_drop_index_returns_self():
    """drop_index() via hook must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        backend.create_index("t", ["name"], index_name="idx_name")
        result = backend.drop_index("idx_name")
        assert result is backend


def test_index_exists():
    """index_exists() must detect created indexes."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        backend.create_index("t", ["id"], index_name="idx_id")
        assert backend.index_exists("idx_id") is True
        assert backend.index_exists("no_such_idx") is False


def test_list_indexes():
    """list_indexes() must return index info."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        backend.create_index("t", ["id"], index_name="idx_id")
        indexes = backend.list_indexes("t")
        assert isinstance(indexes, list)
        assert len(indexes) >= 1


def test_upsert_duckdb():
    """upsert() must work on DuckDB via hook."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        initial = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        backend.create_table("t", initial)
        backend.create_unique_index("t", ["id"])

        update = pl.DataFrame({"id": [2, 3], "val": [25, 30]})
        result = backend.upsert("t", update, conflict_columns=["id"])
        assert result is backend

        count_result = backend.run_sql("SELECT COUNT(*) as cnt FROM t")
        count = count_result.to_polars()["cnt"][0]
        assert count == 3


def test_upsert_unsupported_dialect_raises():
    """upsert() on a dialect without upsert_hook must raise NotImplementedError."""
    backend = IbisBackend(dialect="postgres")
    # Don't connect (can't connect to postgres anyway) — just check the method
    backend._conn = type("FakeConn", (), {"_ibis_conn": None, "_dialect_spec": DIALECTS["postgres"], "_closed": False})()
    with pytest.raises(NotImplementedError, match="does not support upsert"):
        backend.upsert("t", {}, conflict_columns=["id"])
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_create_table_returns_self -v`
Expected: FAIL (IbisBackend has no `create_table` method)

- [ ] **Step 4: Add thin wrapper operations to IbisBackend**

In `src/mountainash_data/backends/ibis/backend.py`, add these methods to the `IbisBackend` class after the inspection methods:

```python
    # --- Thin wrapper operations (fluent — return self) ---

    def create_table(
        self,
        name: str,
        obj: t.Any,
        *,
        schema: t.Any | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.create_table(
            name, obj=obj, schema=schema, database=database,
            temp=temp, overwrite=overwrite,
        )
        return self

    def drop_table(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.drop_table(name, database=database, force=force)
        return self

    def create_view(
        self,
        name: str,
        obj: t.Any,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.create_view(name, obj=obj, database=database, overwrite=overwrite)
        return self

    def drop_view(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.drop_view(name, database=database, force=force)
        return self

    def insert(
        self,
        name: str,
        obj: t.Any,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.insert(name, obj=obj, database=database, overwrite=overwrite)
        return self

    def truncate(
        self,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.truncate_table(name, schema=schema, database=database)
        return self

    def rename_table(self, old_name: str, new_name: str) -> IbisBackend:
        if self._spec.rename_table_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support rename_table"
            )
        conn = self._require_connected()
        self._spec.rename_table_hook(conn._ibis_conn, old_name, new_name)
        return self

    # --- Terminal operations (return data) ---

    def table(self, name: str, *, database: str | None = None) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.table(name, database=database)

    def table_exists(
        self, name: str, database: str | None = None
    ) -> bool:
        tables = self.list_tables()
        return name in tables

    def run_sql(
        self,
        query: str,
        *,
        schema: t.Any | None = None,
        dialect: str | None = None,
    ) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.sql(query, schema=schema, dialect=dialect)

    def run_expr(
        self,
        expr: t.Any,
        *,
        params: dict | None = None,
        limit: str | None = "default",
        **kwargs: t.Any,
    ) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.execute(expr, params=params, limit=limit, **kwargs)

    def to_sql(
        self,
        expr: t.Any,
        *,
        params: t.Any = None,
        limit: str | None = None,
        pretty: bool = False,
        **kwargs: t.Any,
    ) -> str | None:
        conn = self._require_connected()
        return conn._ibis_conn.compile(expr, params=params, limit=limit, pretty=pretty, **kwargs)
```

- [ ] **Step 5: Add hook-dispatched operations to IbisBackend**

Continue adding methods in `src/mountainash_data/backends/ibis/backend.py`:

```python
    # --- Hook-dispatched operations (fluent — return self) ---

    def upsert(
        self,
        name: str,
        obj: t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = "UPDATE",
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        if self._spec.upsert_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support upsert"
            )
        conn = self._require_connected()
        self._spec.upsert_hook(
            conn._ibis_conn, name, obj,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            conflict_action=conflict_action,
            update_condition=update_condition,
            database=database,
            schema=schema,
        )
        return self

    def create_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        unique: bool = False,
        index_type: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
        if_not_exists: bool = True,
    ) -> IbisBackend:
        if self._spec.create_index_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support create_index"
            )
        conn = self._require_connected()
        self._spec.create_index_hook(
            conn._ibis_conn, table_name, columns,
            index_name=index_name, unique=unique, index_type=index_type,
            where_condition=where_condition, database=database,
            if_not_exists=if_not_exists,
        )
        return self

    def create_unique_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
    ) -> IbisBackend:
        return self.create_index(
            table_name, columns,
            index_name=index_name, unique=True,
            where_condition=where_condition, database=database,
        )

    def drop_index(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True,
    ) -> IbisBackend:
        if self._spec.drop_index_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support drop_index"
            )
        conn = self._require_connected()
        self._spec.drop_index_hook(
            conn._ibis_conn, index_name,
            table_name=table_name, database=database, if_exists=if_exists,
        )
        return self

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
        check_sql = self._spec.get_index_exists_sql(index_name, table_name, database)
        result = conn._ibis_conn.sql(check_sql)
        if result is None:
            return False
        import mountainash as ma
        count = ma.relation(result).to_dict()["count"][0]
        return count > 0

    def list_indexes(
        self,
        table_name: str,
        *,
        database: str | None = None,
    ) -> list[dict]:
        if self._spec.get_list_indexes_sql is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support list_indexes"
            )
        conn = self._require_connected()
        list_sql = self._spec.get_list_indexes_sql(table_name, database)
        result = conn._ibis_conn.sql(list_sql)
        if result is None:
            return []
        import mountainash as ma
        return ma.relation(result).to_dicts()
```

- [ ] **Step 6: Run all backend tests**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_backend.py
git commit -m "feat(backend): add thin wrapper and hook-dispatched operations

Fluent methods (create_table, insert, upsert, create_index, etc.) return
self. Terminal methods (table, run_sql, list_indexes, etc.) return data.
Hook dispatch: upsert/index operations via DialectSpec callables."
```

---

### Task 4: Update IcebergBackend for protocol compliance

**Files:**
- Modify: `src/mountainash_data/backends/iceberg/backend.py`
- Test: Run existing iceberg tests

- [ ] **Step 1: Update IcebergBackend**

Replace the contents of `src/mountainash_data/backends/iceberg/backend.py` with:

```python
"""IcebergBackend — implements core.protocol.Backend for iceberg catalogs."""

from __future__ import annotations

import typing as t

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase


_CATALOG_REGISTRY: dict[str, type[IcebergConnectionBase]] = {
    "rest": IcebergRestConnection,
}


class IcebergBackend:
    """Iceberg backend — single entry point for iceberg catalog interaction.

    Construction takes a catalog type (e.g. ``'rest'``) and config kwargs.
    ``connect()`` returns ``self``. Use as a context manager.
    """

    name = "iceberg"

    def __init__(self, catalog: str, **config: t.Any) -> None:
        if catalog not in _CATALOG_REGISTRY:
            raise KeyError(
                f"Unknown iceberg catalog type {catalog!r}. "
                f"Available: {sorted(_CATALOG_REGISTRY)}"
            )
        self._catalog_cls = _CATALOG_REGISTRY[catalog]
        self._config = config
        self._conn: IcebergConnectionBase | None = None

    def connect(self) -> IcebergBackend:
        """Open a connection. Returns self for fluent chaining."""
        if self._conn is None:
            self._conn = self._catalog_cls(**self._config)
        return self

    def close(self) -> IcebergBackend:
        """Release the connection. Idempotent. Returns self."""
        if self._conn is not None:
            if hasattr(self._conn, "close"):
                self._conn.close()
            self._conn = None
        return self

    def __enter__(self) -> IcebergBackend:
        self.connect()
        return self

    def __exit__(self, *args: t.Any) -> None:
        self.close()

    def _require_connected(self) -> IcebergConnectionBase:
        if self._conn is None:
            raise RuntimeError(
                "IcebergBackend is not connected. Call connect() first."
            )
        return self._conn

    def list_tables(self, namespace: str | None = None) -> list[str]:
        return self._require_connected().list_tables(namespace=namespace)

    def list_namespaces(self) -> list[str]:
        return self._require_connected().list_namespaces()

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> t.Any:
        return self._require_connected().inspect_table(name, namespace=namespace)

    def inspect_namespace(self, name: str) -> t.Any:
        return self._require_connected().inspect_namespace(name)

    def inspect_catalog(self) -> t.Any:
        return self._require_connected().inspect_catalog()
```

- [ ] **Step 2: Run iceberg tests (if any pass without external services)**

Run: `hatch run test:test-target tests/test_unit/backends/iceberg/ -v`
Expected: PASS (or skip if pyiceberg not installed)

- [ ] **Step 3: Commit**

```bash
git add src/mountainash_data/backends/iceberg/backend.py
git commit -m "feat(iceberg): update IcebergBackend for new Backend protocol

connect() returns self, context manager support, inspection methods
delegate to internal connection."
```

---

### Task 5: Delete legacy code and update public API

**Files:**
- Delete: `src/mountainash_data/core/factories/` (entire directory)
- Delete: `src/mountainash_data/core/utils.py`
- Delete: `src/mountainash_data/backends/ibis/connection.py`
- Modify: `src/mountainash_data/__init__.py`
- Modify: `src/mountainash_data/backends/ibis/operations.py` (delete class hierarchy, keep functions)

- [ ] **Step 1: Delete factory directory, utils, and legacy connection module**

```bash
rm -rf src/mountainash_data/core/factories/
rm src/mountainash_data/core/utils.py
rm src/mountainash_data/backends/ibis/connection.py
```

- [ ] **Step 2: Clean up operations.py — delete class hierarchy, keep hook functions**

In `src/mountainash_data/backends/ibis/operations.py`, delete everything from the `_DuckDBFamilyOperationsMixin` class definition onwards (from line ~221 to end of file). This removes:
- `_DuckDBFamilyOperationsMixin` class
- `_BaseIbisMixin` compatibility shim
- `BaseIbisOperations` abstract class
- All concrete operations subclasses (`DuckDB_IbisOperations`, `SQLite_IbisOperations`, etc.)

Keep:
- All imports at the top
- All module-level helper functions (`_generate_index_name`, `_format_qualified_table`, `_normalize_columns`)
- All per-dialect SQL functions (`duckdb_get_index_exists_sql`, `sqlite_get_index_exists_sql`, etc.)
- All standalone hook functions (`duckdb_family_create_index`, `duckdb_family_drop_index`, `duckdb_family_upsert`)
- The `motherduck_list_tables` function

Remove the imports that are only used by the deleted classes. After cleanup, the remaining imports should be:

```python
import typing as t
import contextlib
import warnings
import uuid

import mountainash as ma

from mountainash_data.core.constants import (
    CONST_CONFLICT_ACTION,
    CONST_INDEX_TYPE,
)
```

Remove: `from abc import abstractmethod, ABC`, `import ibis`, `import ibis.expr.types.relations as ir`, `from ibis.expr.schema import SchemaLike`, `from ibis.backends.sql import SQLBackend`, `from mountainash_settings import SettingsParameters`, `from mountainash_data.core.constants import CONST_DB_BACKEND`.

- [ ] **Step 3: Update `__init__.py`**

Replace `src/mountainash_data/__init__.py` with:

```python
"""mountainash-data: physical access to backend data services.

Public API:
    Backend — protocol (core.protocol)
    IbisBackend — ibis-style relational backends (backends.ibis.backend)
    IcebergBackend — iceberg-style table-format catalogs (backends.iceberg.backend)
    CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo — inspection model
"""

from mountainash_data.__version__ import __version__
from mountainash_data.core.protocol import Backend
from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.backends.ibis.backend import IbisBackend

try:
    from mountainash_data.backends.iceberg.backend import IcebergBackend
except ImportError:
    IcebergBackend = None  # type: ignore[assignment,misc]

__all__ = [
    "__version__",
    "Backend",
    "CatalogInfo",
    "ColumnInfo",
    "NamespaceInfo",
    "TableInfo",
    "IbisBackend",
    "IcebergBackend",
]
```

- [ ] **Step 4: Verify import works**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py -v`
Expected: ALL PASS (the backend tests don't import deleted modules)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: delete legacy factories, DatabaseUtils, and connection hierarchies

Remove ConnectionFactory, OperationsFactory, SettingsFactory, DatabaseUtils,
BaseIbisConnection + 12 subclasses, BaseIbisOperations + concrete subclasses.
Keep BaseDBConnection (Iceberg depends on it).
Update __init__.py to export only Backend, IbisBackend, IcebergBackend,
and inspection model."
```

---

### Task 6: Delete legacy test files

**Files:**
- Delete: `tests/test_unit/factories/`
- Delete: `tests/test_unit/test_database_utils.py`
- Delete: `tests/test_unit/databases/test_database_connections.py`
- Delete: `tests/test_unit/databases/connections/`
- Delete: `tests/test_unit/databases/test_ibis_backends.py`
- Delete: `tests/test_unit/databases/operations/`

- [ ] **Step 1: Delete legacy test files and directories**

```bash
rm -rf tests/test_unit/factories/
rm tests/test_unit/test_database_utils.py
rm tests/test_unit/databases/test_database_connections.py
rm -rf tests/test_unit/databases/connections/
rm tests/test_unit/databases/test_ibis_backends.py
rm -rf tests/test_unit/databases/operations/
```

- [ ] **Step 2: Update test_mountainash_data.py**

Replace `tests/test_unit/test_mountainash_data.py` with:

```python
"""Tests for main mountainash_data package."""

import pytest
import mountainash_data


class TestPackageImports:
    """Test package-level imports and structure."""

    def test_version_import(self):
        assert hasattr(mountainash_data, '__version__')
        assert isinstance(mountainash_data.__version__, str)
        assert len(mountainash_data.__version__) > 0

    def test_version_format(self):
        version_parts = mountainash_data.__version__.split('.')
        assert len(version_parts) >= 2
        assert version_parts[0].isdigit()
        assert version_parts[1].isdigit()

    def test_core_imports_available(self):
        from mountainash_data.core.connection import BaseDBConnection
        assert BaseDBConnection is not None


class TestPackageStructure:
    """Test package structure and organization."""

    def test_package_has_init(self):
        assert hasattr(mountainash_data, '__file__')

    def test_new_submodules_exist(self):
        import mountainash_data.core
        import mountainash_data.backends
        assert hasattr(mountainash_data, 'core')
        assert hasattr(mountainash_data, 'backends')

    def test_public_api_ibis_backend(self):
        from mountainash_data import IbisBackend
        assert IbisBackend is not None

    def test_public_api_backend_protocol(self):
        from mountainash_data import Backend
        assert Backend is not None

    def test_public_api_inspection_model(self):
        from mountainash_data import CatalogInfo, ColumnInfo, NamespaceInfo, TableInfo
        assert CatalogInfo is not None
        assert ColumnInfo is not None
        assert NamespaceInfo is not None
        assert TableInfo is not None

    def test_removed_exports_not_available(self):
        assert not hasattr(mountainash_data, 'ConnectionFactory')
        assert not hasattr(mountainash_data, 'OperationsFactory')
        assert not hasattr(mountainash_data, 'SettingsFactory')
        assert not hasattr(mountainash_data, 'DatabaseUtils')
```

- [ ] **Step 3: Update test_settings_parametrized.py**

In `tests/test_unit/databases/settings/test_settings_parametrized.py`, find the tests that use `ConnectionFactory` and `DatabaseUtils` (around lines 148-175) and replace them with `IbisBackend` equivalents:

Replace the `ConnectionFactory` test block (around line 148) with:

```python
    def test_settings_work_with_ibis_backend(self, settings_params):
        """Test that settings work with IbisBackend."""
        from mountainash_data.backends.ibis.backend import IbisBackend
        backend = IbisBackend(settings_params)
        assert backend.dialect is not None
```

Replace the `DatabaseUtils` test block (around line 163) with:

```python
    def test_settings_work_with_ibis_backend_connect(self, settings_params):
        """Test that settings can create a connected backend via IbisBackend."""
        from mountainash_data.backends.ibis.backend import IbisBackend
        backend = IbisBackend(settings_params)
        backend.connect()
        tables = backend.list_tables()
        assert isinstance(tables, list)
        backend.close()
```

- [ ] **Step 4: Rewrite integration tests**

Replace `tests/test_integration/test_end_to_end_workflows.py` with:

```python
"""End-to-end integration tests using IbisBackend."""

import pytest
import polars as pl
from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.core.settings import SQLiteAuthSettings, DuckDBAuthSettings
from mountainash_settings import SettingsParameters


@pytest.mark.integration
class TestIbisBackendWorkflow:
    """Test complete workflows through IbisBackend."""

    def test_sqlite_dialect_workflow(self):
        """Full workflow with dialect= keyword."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("users", {"id": [1, 2], "name": ["a", "b"]})
            assert "users" in backend.list_tables()
            tbl = backend.table("users")
            assert tbl is not None

    def test_sqlite_url_workflow(self, tmp_path):
        """Full workflow from URL."""
        db_file = tmp_path / "test.db"
        with IbisBackend(f"sqlite:///{db_file}") as backend:
            backend.create_table("t", {"id": [1, 2, 3]})
            assert "t" in backend.list_tables()
        assert db_file.exists()

    def test_duckdb_settings_workflow(self):
        """Full workflow from SettingsParameters."""
        from mountainash_data.core.settings import NoAuth
        params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            DATABASE=":memory:",
            auth=NoAuth(),
        )
        with IbisBackend(params) as backend:
            backend.create_table("t", {"id": [1, 2]})
            assert "t" in backend.list_tables()

    def test_fluent_chaining_workflow(self):
        """Fluent API chaining across multiple operations."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            (
                backend
                .create_table("users", {"id": [1], "email": ["a@b.com"]})
                .create_index("users", ["email"], unique=True)
                .create_table("orders", {"id": [1], "user_id": [1]})
            )
            assert sorted(backend.list_tables()) == ["orders", "users"]

    def test_inspect_workflow(self):
        """Inspection methods work through the backend."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", {"id": [1], "name": ["a"]})
            info = backend.inspect_table("t")
            assert info.name == "t"
            assert len(info.columns) == 2

    def test_ibis_connection_seam(self):
        """ibis_connection() provides the seam to mountainash-expressions."""
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", {"id": [1, 2]})
            raw = backend.ibis_connection()
            tbl = raw.table("t")
            assert tbl is not None


@pytest.mark.integration
class TestDuckDBOperationsWorkflow:
    """Test DuckDB-specific operations (upsert, indexes) through IbisBackend."""

    def test_upsert_insert_new_rows(self):
        """Upsert inserts new rows when no conflicts exist."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            initial = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            backend.create_table("users", initial)
            backend.create_unique_index("users", ["id"])

            new_data = pl.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
            backend.upsert("users", new_data, conflict_columns=["id"])

            count = backend.run_sql("SELECT COUNT(*) as cnt FROM users").to_polars()["cnt"][0]
            assert count == 4

    def test_upsert_update_existing_rows(self):
        """Upsert updates existing rows on conflict."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            initial = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "score": [100, 200]})
            backend.create_table("users", initial)
            backend.create_unique_index("users", ["id"])

            update = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "score": [150, 250]})
            backend.upsert("users", update, conflict_columns=["id"])

            result = backend.run_sql("SELECT score FROM users ORDER BY id").to_polars()
            assert list(result["score"]) == [150, 250]

    def test_index_lifecycle(self):
        """Create, check, list, drop index."""
        with IbisBackend(dialect="duckdb", database=":memory:") as backend:
            backend.create_table("t", {"id": [1, 2], "name": ["a", "b"]})

            backend.create_index("t", ["name"], index_name="idx_name")
            assert backend.index_exists("idx_name") is True

            indexes = backend.list_indexes("t")
            assert any(idx.get("name") == "idx_name" for idx in indexes)

            backend.drop_index("idx_name")
            assert backend.index_exists("idx_name") is False
```

- [ ] **Step 5: Run full test suite**

Run: `hatch run test:test-quick`
Expected: ALL PASS (some tests will have been removed, count should be lower but zero failures)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: rewrite all tests to use IbisBackend, delete legacy test files

Delete factory tests, DatabaseUtils tests, legacy connection tests,
and legacy operations tests. Rewrite integration tests and settings
parametrized tests. Update package structure tests."
```

---

### Task 7: Full test suite validation and cleanup

**Files:**
- Verify: All files

- [ ] **Step 1: Run full test suite**

Run: `hatch run test:test-quick`
Expected: ALL PASS

- [ ] **Step 2: Check for any remaining imports of deleted modules**

```bash
grep -rn "from mountainash_data.core.factories" src/ tests/ --include="*.py"
grep -rn "from mountainash_data.core.utils" src/ tests/ --include="*.py"
grep -rn "from mountainash_data.backends.ibis.connection import" src/ tests/ --include="*.py"
grep -rn "ConnectionFactory\|OperationsFactory\|SettingsFactory\|DatabaseUtils" src/ tests/ --include="*.py"
grep -rn "BaseIbisOperations\|BaseIbisConnection" src/ tests/ --include="*.py"
```

Expected: No matches in src/ or tests/ (only in docs/ specs/plans which is fine).

- [ ] **Step 3: Fix any remaining references found in step 2**

If any stale imports are found, update them. Common places to check:
- `conftest.py` files
- `__init__.py` files in test directories
- Fixture files in `tests/fixtures/`

- [ ] **Step 4: Run full test suite one final time**

Run: `hatch run test:test-quick`
Expected: ALL PASS

- [ ] **Step 5: Commit any cleanup**

```bash
git add -A
git commit -m "chore: clean up stale imports and references to deleted modules"
```

(Skip this commit if no changes were needed.)

---

## Self-Review

**Spec coverage check:**
- ✅ IbisBackend lifecycle (connect/close/context manager) — Task 1
- ✅ Fluent API (return self) — Task 3
- ✅ Terminal methods (return data) — Task 3
- ✅ DialectSpec hooks — Task 2
- ✅ Hook-dispatched operations (upsert, indexes) — Task 3
- ✅ Thin wrapper operations — Task 3
- ✅ Accessor methods (ibis_connection, get_connection) — Task 1
- ✅ Protocol update (Backend, remove Connection) — Task 1
- ✅ IcebergBackend update — Task 4
- ✅ Delete factories/utils — Task 5
- ✅ Delete legacy hierarchies — Task 5
- ✅ Public API cleanup — Task 5
- ✅ Test rewrite — Task 6
- ✅ BaseDBConnection kept — Task 5 (only deletes ibis connection.py, not core/connection.py)
- ✅ Error handling (no silent bool/None, NotImplementedError for unsupported) — Task 3

**Placeholder scan:** No TBD/TODO/placeholders found.

**Type consistency:** `IbisBackend` return type used consistently across all tasks. Hook function signatures match between Task 2 (extraction) and Task 3 (dispatch).
