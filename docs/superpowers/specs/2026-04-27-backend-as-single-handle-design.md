# Backend as Single Handle — Design Spec

> **Status:** APPROVED
> **Date:** 2026-04-27
> **Branch:** `feature/settings-aware-ibis-backend` (continues Phase 1 work)
> **Supersedes:** Connection consolidation Phases 2+3 (collapsed into one)

## Goal

Make `IbisBackend` the single public handle for all ibis interaction —
construction, connection lifecycle, inspection, and operations. Delete
`ConnectionFactory`, `OperationsFactory`, `DatabaseUtils`, the 12 concrete
`BaseIbisConnection` subclasses, and the separate `BaseIbisOperations` class
hierarchy.

## Architecture

`IbisBackend` composes three internal concerns:

1. **Config resolution** (existing, unchanged) — three-way constructor
   dispatch: `SettingsParameters`, URL, or `dialect=` keyword.
2. **Connection lifecycle** — `connect()` returns `self`, `close()` returns
   `self`, context manager support.
3. **Operations dispatch** — instance methods on `IbisBackend` that delegate
   to the internal `IbisConnection` (inspection), the raw ibis connection
   (thin wrappers), or `DialectSpec` callable hooks (per-dialect operations).

`IbisConnection` stays as an **internal** class. The consumer never imports
or interacts with it directly. `IbisBackend` exposes two accessor methods:

- `ibis_connection()` → raw ibis backend object (for the seam with
  mountainash-expressions: `backend.ibis_connection().table("users")`)
- `get_connection()` → our `IbisConnection` wrapper (for internal use)

Both raise `RuntimeError` if not connected.

### What Gets Deleted

| Target | Location | Reason |
|--------|----------|--------|
| `ConnectionFactory` | `core/factories/connection_factory.py` | Replaced by `IbisBackend` constructor |
| `OperationsFactory` | `core/factories/operations_factory.py` | Replaced by operations on `IbisBackend` |
| `SettingsFactory` | `core/factories/settings_factory.py` | URL detection now in `_SCHEME_TO_DIALECT`; settings creation via `IbisBackend(settings_params)` |
| `DatabaseUtils` | `core/utils.py` | Entire class — `IbisBackend` is the API |
| `BaseDBConnection` | `core/connection.py` | Abstract base no longer needed |
| `BaseIbisConnection` + 12 subclasses | `backends/ibis/connection.py` | Replaced by `IbisConnection` (internal) + `DialectSpec` registry |
| `BaseIbisOperations` + concrete subclasses | `backends/ibis/operations.py` | Replaced by operations on `IbisBackend` + `DialectSpec` hooks |
| `_DuckDBFamilyOperationsMixin` | `backends/ibis/operations.py` | Implementations become `DialectSpec` hooks |
| `_BaseIbisMixin` | `backends/ibis/operations.py` | Already a deprecated shim |

### What Stays

| Component | Location | Role |
|-----------|----------|------|
| `IbisBackend` | `backends/ibis/backend.py` | Single public handle (expanded) |
| `IbisConnection` | `backends/ibis/backend.py` | Internal wrapper — inspection delegation |
| `DialectSpec` registry | `backends/ibis/dialects/_registry.py` | Expanded with operation hooks |
| Module-level operation functions | `backends/ibis/operations.py` | Wired as `DialectSpec` hooks |
| `IcebergBackend` | `backends/iceberg/backend.py` | Unchanged (separate backend) |
| `Backend` protocol | `core/protocol.py` | Updated — `connect()` returns `Self` |
| Inspection model | `core/inspection.py` | Unchanged |
| Settings classes | `core/settings/` | Unchanged |

## Protocol Changes

### `Backend` protocol (updated)

```python
@t.runtime_checkable
class Backend(t.Protocol):
    name: str

    def connect(self) -> Self: ...
    def close(self) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args) -> None: ...

    # Inspection (terminal — return data)
    def list_tables(self, namespace: str | None = None) -> list[str]: ...
    def list_namespaces(self) -> list[str]: ...
    def inspect_table(self, name: str, namespace: str | None = None) -> TableInfo: ...
    def inspect_namespace(self, name: str) -> NamespaceInfo: ...
    def inspect_catalog(self) -> CatalogInfo: ...
```

### `Connection` protocol — REMOVED

No longer part of the public API. `IbisConnection` is internal.

## Connection Lifecycle

```python
# Explicit connect/close
backend = IbisBackend(dialect="sqlite", database=":memory:")
backend.connect()
tables = backend.list_tables()
backend.close()

# Context manager (recommended)
with IbisBackend(dialect="sqlite", database=":memory:") as backend:
    tables = backend.list_tables()

# Fluent chaining
with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("users", df).create_index("users", ["email"], unique=True)
    rows = backend.list_tables()
```

`__enter__` calls `connect()` and returns `self`.
`__exit__` calls `close()`.

Calling any method before `connect()` or after `close()` raises `RuntimeError`.

## Fluent API

Methods are categorised into two groups:

### Fluent (return `self` — chainable mutations)

| Method | Signature |
|--------|-----------|
| `connect()` | `() -> Self` |
| `close()` | `() -> Self` |
| `create_table()` | `(name, obj, *, schema?, database?, temp?, overwrite?) -> Self` |
| `drop_table()` | `(name, *, database?, force?) -> Self` |
| `create_view()` | `(name, obj, *, database?, overwrite?) -> Self` |
| `drop_view()` | `(name, *, database?, force?) -> Self` |
| `insert()` | `(name, obj, *, database?, overwrite?) -> Self` |
| `upsert()` | `(name, obj, *, conflict_columns, update_columns?, conflict_action?, ...) -> Self` |
| `truncate()` | `(name, *, database?, schema?) -> Self` |
| `rename_table()` | `(old_name, new_name) -> Self` |
| `create_index()` | `(table, columns, *, index_name?, unique?, ...) -> Self` |
| `create_unique_index()` | `(table, columns, *, index_name?, ...) -> Self` |
| `drop_index()` | `(index_name, *, table_name?, database?, if_exists?) -> Self` |

### Terminal (return data — end of chain)

| Method | Returns |
|--------|---------|
| `list_tables(namespace?)` | `list[str]` |
| `list_namespaces()` | `list[str]` |
| `table_exists(name, database?)` | `bool` |
| `inspect_table(name, namespace?)` | `TableInfo` |
| `inspect_namespace(name)` | `NamespaceInfo` |
| `inspect_catalog()` | `CatalogInfo` |
| `table(name, *, database?)` | `ir.Table` |
| `run_sql(query, *, schema?, dialect?)` | `ir.Table | None` |
| `run_expr(expr, *, params?, limit?)` | `Any` |
| `to_sql(expr, *, params?, limit?, pretty?)` | `str | None` |
| `index_exists(index_name, *, table_name?, database?)` | `bool` |
| `list_indexes(table_name, *, database?)` | `list[dict]` |
| `ibis_connection()` | raw ibis backend object |
| `get_connection()` | `IbisConnection` (internal wrapper) |

## DialectSpec Expansion

`DialectSpec` gains operation hook fields:

```python
@dataclass(frozen=True)
class DialectSpec:
    # Existing
    ibis_backend_name: str
    connection_mode: str
    connection_string_scheme: str
    connection_builder: Callable | None = None
    get_index_exists_sql: Callable | None = None      # existing
    get_list_indexes_sql: Callable | None = None       # existing

    # New operation hooks
    upsert_hook: Callable | None = None
    create_index_hook: Callable | None = None
    drop_index_hook: Callable | None = None
    rename_table_hook: Callable | None = None

    extras: Mapping[str, Any] = field(default_factory=dict)
```

### Hook wiring

The existing `_DuckDBFamilyOperationsMixin` methods become standalone
functions and are wired as hooks:

| Hook | DuckDB/MotherDuck | SQLite | Others |
|------|-------------------|--------|--------|
| `upsert_hook` | `duckdb_family_upsert` | `duckdb_family_upsert` | `None` |
| `create_index_hook` | `duckdb_family_create_index` | `duckdb_family_create_index` | `None` |
| `drop_index_hook` | `duckdb_family_drop_index` | `duckdb_family_drop_index` | `None` |
| `rename_table_hook` | `None` (not implemented) | `None` (not implemented) | `None` |
| `get_index_exists_sql` | `duckdb_get_index_exists_sql` | `sqlite_get_index_exists_sql` | `None` |
| `get_list_indexes_sql` | `duckdb_get_list_indexes_sql` | `sqlite_get_list_indexes_sql` | `None` |

`IbisBackend.upsert()` checks `self._spec.upsert_hook`; if `None`, raises
`NotImplementedError(f"Dialect {self.dialect!r} does not support upsert")`.

### Hook function signatures

Hooks receive the raw ibis connection as their first argument (not the
`IbisBackend` instance), plus the method's arguments:

```python
# upsert_hook signature
def duckdb_family_upsert(
    ibis_conn: Any,
    table_name: str,
    df: Any,
    *,
    conflict_columns: list[str] | str,
    update_columns: list[str] | str | None = None,
    conflict_action: str = "UPDATE",
    update_condition: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> None: ...

# create_index_hook signature
def duckdb_family_create_index(
    ibis_conn: Any,
    table_name: str,
    columns: list[str] | str,
    *,
    index_name: str | None = None,
    unique: bool = False,
    index_type: str | None = None,
    where_condition: str | None = None,
    database: str | None = None,
    if_not_exists: bool = True,
) -> None: ...
```

Note: hooks return `None`, not `bool`. The `IbisBackend` wrapper converts
the call to fluent `return self`. Errors raise exceptions (no silent
`False` returns).

### Thin wrapper methods

Methods that are simple ibis delegations don't need hooks — they work the
same across all dialects:

- `create_table` → `ibis_conn.create_table(...)`
- `drop_table` → `ibis_conn.drop_table(...)`
- `create_view` → `ibis_conn.create_view(...)`
- `drop_view` → `ibis_conn.drop_view(...)`
- `insert` → `ibis_conn.insert(...)`
- `truncate` → `ibis_conn.truncate_table(...)`
- `table` → `ibis_conn.table(...)`
- `run_sql` → `ibis_conn.sql(...)`
- `run_expr` → `ibis_conn.execute(...)`
- `to_sql` → `ibis_conn.compile(...)`
- `list_tables` → delegates to `IbisConnection.list_tables()`
- `list_namespaces` → delegates to `IbisConnection.list_namespaces()`
- `inspect_*` → delegates to `IbisConnection.inspect_*()`

## Public API Changes (`__init__.py`)

### Removed exports

- `Connection` (protocol removed)
- `ConnectionFactory`
- `OperationsFactory`
- `SettingsFactory`
- `DatabaseUtils`

### Retained exports

- `Backend` (protocol, updated)
- `IbisBackend` (expanded)
- `IcebergBackend`
- `CatalogInfo`, `ColumnInfo`, `NamespaceInfo`, `TableInfo`

## IcebergBackend Impact

`IcebergBackend` must also satisfy the updated `Backend` protocol
(`connect()` returns `Self`, context manager). This is a minor change —
same pattern as `IbisBackend`. Operations that don't apply (upsert, indexes)
are simply not on the protocol.

## Test Strategy

### New tests for `IbisBackend` operations

Test with SQLite and DuckDB (in-memory, no external services):

- **Lifecycle**: `connect()` → use → `close()`, context manager, double-close idempotent, use-before-connect raises
- **Fluent API**: chain `create_table().create_index()`, verify returns `self`
- **Thin wrappers**: `create_table`, `drop_table`, `insert`, `list_tables`, `table`, `run_sql`
- **Hook-based operations**: `upsert` (DuckDB), `create_index`/`drop_index`/`index_exists`/`list_indexes` (SQLite + DuckDB)
- **Unsupported operations**: `upsert` on Trino dialect raises `NotImplementedError`
- **Accessor methods**: `ibis_connection()` returns raw ibis, `get_connection()` returns `IbisConnection`

### Existing tests — rewrite

Factory test files (`test_connection_factory.py`, `test_operations_factory.py`)
are deleted. `test_database_utils.py` is deleted. Operations tests
(`test_base_ibis_operations.py`, `test_upsert_and_indexes.py`) are rewritten
to use `IbisBackend` directly. `test_backend.py` is expanded.

### Existing tests — verify unchanged

- Inspection tests remain valid
- Settings tests remain valid
- Iceberg tests need minor update for protocol change

## Files Modified or Deleted

### Modified

| File | Change |
|------|--------|
| `backends/ibis/backend.py` | Add lifecycle, operations methods, accessor methods |
| `backends/ibis/dialects/_registry.py` | Add operation hook fields to `DialectSpec`, wire hooks |
| `backends/ibis/operations.py` | Extract mixin methods into standalone hook functions; delete class hierarchy |
| `backends/iceberg/backend.py` | Update to return `Self` from `connect()`, add context manager |
| `core/protocol.py` | Update `Backend` protocol, remove `Connection` protocol |
| `__init__.py` | Remove factory/utils exports, remove `Connection` |
| `tests/test_unit/backends/ibis/test_backend.py` | Expand with operations + lifecycle tests |

### Deleted

| File | Reason |
|------|--------|
| `core/factories/` (entire directory) | All factories replaced by `IbisBackend` |
| `core/utils.py` | `DatabaseUtils` replaced by `IbisBackend` |
| `core/connection.py` | `BaseDBConnection` no longer needed |
| `backends/ibis/connection.py` | `BaseIbisConnection` + 12 subclasses replaced |
| `tests/test_unit/factories/` | All factory tests |
| `tests/test_unit/test_database_utils.py` | `DatabaseUtils` tests |
| `tests/test_unit/databases/test_database_connections.py` | Legacy connection tests |
| `tests/test_unit/databases/connections/` | Legacy connection lifecycle tests |

## Error Handling

- Methods that previously returned `bool` or `None` now raise on failure
  (no silent swallowing). The `print(f"Error: ...")` pattern throughout
  `BaseIbisOperations` is replaced with proper exception propagation.
- Unsupported operations (hook is `None`) raise `NotImplementedError`
  with dialect name in the message.
- Use-before-connect and use-after-close raise `RuntimeError`.

## Consumer Migration

Before:
```python
from mountainash_data import DatabaseUtils, ConnectionFactory
conn = DatabaseUtils.create_connection(settings_params)
backend = conn.connect()
ops = DatabaseUtils.create_operations(settings_params)
ops.create_table(backend, "users", df)
ops.upsert(backend, "users", new_df, conflict_columns=["id"])
tables = ops.list_tables(backend)
```

After:
```python
from mountainash_data import IbisBackend
with IbisBackend(settings_params) as backend:
    backend.create_table("users", df).upsert("users", new_df, conflict_columns=["id"])
    tables = backend.list_tables()
    tbl = backend.ibis_connection().table("users")
```
