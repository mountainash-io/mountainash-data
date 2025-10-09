# Upsert and Index Management Test Coverage

## Overview

Comprehensive test suite for the new upsert and index management functionality in `mountainash-data`. Tests cover all scenarios for DuckDB, MotherDuck, and SQLite backends.

## Test File

**Location**: `tests/test_unit/databases/operations/test_upsert_and_indexes.py`

## Test Coverage

### 1. Upsert Operations (`TestUpsertOperations`)

Tests cover all upsert scenarios with parameterization for SQLite and DuckDB:

#### Basic Operations
- **`test_simple_upsert_insert_new_rows`**: Verify upsert inserts new rows when no conflicts exist
- **`test_simple_upsert_update_existing_rows`**: Verify upsert updates existing rows on conflict
- **`test_upsert_mixed_insert_and_update`**: Test handling both inserts and updates in same operation

#### Conflict Handling
- **`test_upsert_with_conflict_action_nothing`**: Test `DO NOTHING` ignores conflicts (insert-ignore pattern)
- **`test_upsert_with_specific_update_columns`**: Update only specified columns, leaving others unchanged
- **`test_upsert_with_conditional_update`**: Conditional updates with WHERE clause

#### Advanced Scenarios
- **`test_upsert_composite_conflict_columns`**: Test composite keys (multiple conflict columns)
- **`test_upsert_auto_column_detection`**: Auto-detect update columns when not specified

#### Validation
- **`test_upsert_validation_table_not_exists`**: Error when target table doesn't exist
- **`test_upsert_validation_empty_conflict_columns`**: Error when conflict_columns is empty

### 2. Index Management (`TestIndexManagement`)

Tests cover index creation, deletion, and querying with parameterization for SQLite and DuckDB:

#### Index Creation
- **`test_create_simple_index`**: Create basic single-column index
- **`test_create_unique_index`**: Create unique constraint index
- **`test_create_composite_index`**: Create multi-column composite index
- **`test_create_index_with_custom_name`**: Custom index naming
- **`test_create_partial_index`**: Partial indexes with WHERE clause
- **`test_create_index_if_not_exists`**: IF NOT EXISTS prevents duplicate errors

#### Index Operations
- **`test_drop_index`**: Drop existing index
- **`test_drop_index_if_exists`**: Drop with IF EXISTS doesn't fail on non-existent
- **`test_index_exists_returns_false_for_nonexistent`**: Check index existence
- **`test_list_indexes`**: List all indexes for a table

#### Convenience Methods
- **`test_create_unique_index_convenience_method`**: Test unique index convenience wrapper

### 3. Integration Tests (`TestUpsertIndexIntegration`)

Tests combining upsert and index operations:

- **`test_upsert_requires_unique_index_on_conflict_columns`**: Upsert with unique index on conflict columns
- **`test_multiple_indexes_and_upserts`**: Multiple indexes don't interfere with upserts

## Running the Tests

### Prerequisites

Due to dependency resolution, ensure you're in the correct hatch environment:

```bash
# Run all upsert/index tests
hatch run test:test-target tests/test_unit/databases/operations/test_upsert_and_indexes.py -v

# Run specific test class
hatch run test:test-target tests/test_unit/databases/operations/test_upsert_and_indexes.py::TestUpsertOperations -v

# Run specific test
hatch run test:test-target tests/test_unit/databases/operations/test_upsert_and_indexes.py::TestUpsertOperations::test_simple_upsert_insert_new_rows -v

# Run with coverage
hatch run test:test tests/test_unit/databases/operations/test_upsert_and_indexes.py
```

### Test Matrix

All tests are parametrized to run against:
- ✅ SQLite (in-memory)
- ✅ DuckDB (in-memory)
- 🔄 MotherDuck (future - requires cloud credentials)

## Test Scenarios Covered

### Upsert Scenarios

| Scenario | Description | Test Method |
|----------|-------------|-------------|
| Insert new | No conflicts, all rows inserted | `test_simple_upsert_insert_new_rows` |
| Update existing | All rows conflict, all updated | `test_simple_upsert_update_existing_rows` |
| Mixed | Some new, some conflicts | `test_upsert_mixed_insert_and_update` |
| DO NOTHING | Skip conflicts | `test_upsert_with_conflict_action_nothing` |
| Partial update | Update specific columns only | `test_upsert_with_specific_update_columns` |
| Conditional | Update with WHERE clause | `test_upsert_with_conditional_update` |
| Composite key | Multiple conflict columns | `test_upsert_composite_conflict_columns` |
| Auto-detect | Infer update columns | `test_upsert_auto_column_detection` |

### Index Scenarios

| Scenario | Description | Test Method |
|----------|-------------|-------------|
| Simple | Single column index | `test_create_simple_index` |
| Unique | Unique constraint | `test_create_unique_index` |
| Composite | Multi-column index | `test_create_composite_index` |
| Custom name | User-specified name | `test_create_index_with_custom_name` |
| Partial | Index with WHERE clause | `test_create_partial_index` |
| Idempotent | IF NOT EXISTS | `test_create_index_if_not_exists` |
| Drop | Remove index | `test_drop_index` |
| List | Query all indexes | `test_list_indexes` |

## Expected Behavior

### Upsert

```python
# Simple upsert - auto-updates all non-conflict columns
operations.upsert(
    backend, "users", df,
    conflict_columns=["email"]
)

# Insert-ignore pattern
operations.upsert(
    backend, "users", df,
    conflict_columns=["email"],
    conflict_action=CONST_CONFLICT_ACTION.NOTHING
)

# Partial update
operations.upsert(
    backend, "users", df,
    conflict_columns=["email"],
    update_columns=["last_login"]
)

# Conditional update
operations.upsert(
    backend, "users", df,
    conflict_columns=["email"],
    update_columns=["score"],
    update_condition="users.score < EXCLUDED.score"
)
```

### Index Management

```python
# Create unique index
operations.create_unique_index(backend, "users", ["email"])

# Create partial index
operations.create_index(
    backend, "orders", ["customer_id"],
    where_condition="status = 'active'"
)

# Check existence
exists = operations.index_exists(backend, "idx_users_email")

# List all indexes
indexes = operations.list_indexes(backend, "users")

# Drop index
operations.drop_index(backend, "idx_users_email")
```

## Known Issues

### Circular Import in Dependencies

There's a circular import in `mountainash-settings` that prevents direct Python execution:
```
ImportError: cannot import name 'SettingsParameters' from partially initialized module
```

**Workaround**: Tests run correctly in the hatch test environment which has proper dependency isolation.

### Factory Class Name Mismatch (Resolved)

The operations factory expects class names without underscores (e.g., `SQLiteIbisOperations`), which are provided via the `__init__.py` exports using lazy loading.

## Test Data

Tests use Polars DataFrames as the standard format:

```python
data = pl.DataFrame({
    "id": [1, 2, 3],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "name": ["Alice", "Bob", "Charlie"]
})
```

## Future Enhancements

1. **PostgreSQL tests**: Add when PostgreSQL backend operations are implemented
2. **Error scenario tests**: More edge cases and error conditions
3. **Performance tests**: Benchmark upsert vs delete+insert
4. **Concurrent upserts**: Test thread safety
5. **Large dataset tests**: Test with 10K+ rows

## Related Files

- **Implementation**: `src/mountainash_data/databases/operations/ibis/_duckdb_family_mixin.py`
- **Base interface**: `src/mountainash_data/databases/operations/ibis/base_ibis_operations.py`
- **Constants**: `src/mountainash_data/databases/constants.py`
- **DuckDB ops**: `src/mountainash_data/databases/operations/ibis/duckdb_ibis_operations.py`
- **SQLite ops**: `src/mountainash_data/databases/operations/ibis/sqlite_ibis_operations.py`
- **MotherDuck ops**: `src/mountainash_data/databases/operations/ibis/motherduck_ibis_operations.py`
