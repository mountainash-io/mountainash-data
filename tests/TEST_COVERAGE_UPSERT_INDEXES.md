# Upsert and Index Management Test Coverage

## Overview

The Ibis backend tests cover upsert rendering, index DDL, index existence, and
index catalog inspection. The tests use the current `IbisBackend` API and the
generic Ibis implementation under `src/mountainash_data/backends/ibis/`.

## Test Files

### Upsert

- `tests/test_unit/backends/ibis/test_upsert_render.py`
- `tests/test_unit/backends/ibis/test_upsert_condition_render.py`
- `tests/test_unit/backends/ibis/test_upsert_style_registry.py`
- `tests/test_integration/test_end_to_end_workflows.py`

These tests cover insert-or-update, insert-or-ignore, update-column selection,
conditional updates, merge rendering, duplicate-key rendering, and complete
SQLite and DuckDB workflows.

### Index management

- `tests/test_unit/backends/ibis/test_index_ops.py`
- `tests/test_unit/backends/ibis/test_index_render.py`
- `tests/test_unit/backends/ibis/test_index_capability.py`
- `tests/test_unit/backends/ibis/test_backend.py`
- `tests/test_integration/test_end_to_end_workflows.py`
- `tests/test_integration/test_index_ops_live.py`

These tests cover index creation, unique indexes, composite indexes, partial
indexes, idempotent DDL, index deletion, existence checks, and index listing.

## Index Listing Contract

`IbisBackend.list_indexes()` returns `list[IndexInfo]`. Each `IndexInfo` object
contains the index name, ordered key columns, uniqueness, primary-key status,
index type, included columns, validity, definition, and dialect metadata.

The generic list-index builders use one positional ten-column row contract:

1. `index_name`
2. `is_unique`
3. `is_primary`
4. `is_valid`
5. `index_type`
6. `definition`
7. `col_name`
8. `col_expr`
9. `is_included`
10. `position`

The generic row grouper rejects malformed rows, duplicate positions,
conflicting metadata, and indexes without key columns. DuckDB uses a dedicated
hook because it combines explicit indexes with index-backed constraints.

The route matrix is:

| Route | Dialects |
|---|---|
| Generic ten-column catalog query | SQLite, PostgreSQL, MySQL, MSSQL, Oracle, SingleStoreDB |
| Dedicated inspection hook | DuckDB, MotherDuck |

## Running the Tests

Run the focused upsert and index suites:

```bash
hatch run test:test-target-quick \
  tests/test_unit/backends/ibis/test_upsert_render.py \
  tests/test_unit/backends/ibis/test_upsert_condition_render.py \
  tests/test_unit/backends/ibis/test_index_ops.py \
  tests/test_unit/backends/ibis/test_index_render.py \
  tests/test_unit/backends/ibis/test_index_capability.py \
  tests/test_unit/backends/ibis/test_backend.py \
  tests/test_integration/test_end_to_end_workflows.py
```

Run the complete suite:

```bash
hatch run test:test-quick
```

Run live database tests when the required services are available:

```bash
hatch run test:test-live tests/test_integration/test_index_ops_live.py
```

## Expected API

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="sqlite", database=":memory:") as backend:
    backend.create_table("users", {"id": [1], "email": ["a@example.com"]})
    backend.create_unique_index("users", ["email"], index_name="ux_users_email")

    exists = backend.index_exists("ux_users_email", table_name="users")
    indexes = backend.list_indexes("users")

    assert exists
    assert indexes[0].name == "ux_users_email"
    assert indexes[0].columns == ("email",)

    backend.drop_index("ux_users_email")
```

Pass `namespace="sales"` to index methods when the table is in a non-default
schema. Catalog-qualified namespaces are rejected by the Ibis index API.

## Test Data

Tests use Polars DataFrames for frame-based write operations:

```python
data = pl.DataFrame({
    "id": [1, 2, 3],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "name": ["Alice", "Bob", "Charlie"],
})
```

## Related Implementation

- `src/mountainash_data/backends/ibis/backend.py`
- `src/mountainash_data/backends/ibis/_index.py`
- `src/mountainash_data/backends/ibis/_index_inspection.py`
- `src/mountainash_data/backends/ibis/_render.py`
- `src/mountainash_data/backends/ibis/dialects/_registry.py`
