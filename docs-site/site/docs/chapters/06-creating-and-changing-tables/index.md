---
title: Creating and changing tables
description: Create, insert into, truncate, upsert, add columns to and rename tables.
date: 2026-10-29
---

# Creating and changing tables

`IbisBackend` creates, fills, empties, merges into, widens and renames tables with the same method calls on every dialect it supports. Creating tables and views, inserting and truncating go straight to Ibis. Upsert, `add_columns` and `rename_table` are built by mountainash-data itself: it renders dialect-specific SQL from the live connection, so one call produces `ON CONFLICT`, `MERGE` or `ON DUPLICATE KEY` depending on the engine. The examples on this page run on in-memory DuckDB, with SQLite differences noted in the text, and each block continues from the one before it. The anchor recipe is [`examples/mutations`](https://github.com/mountainash-io/mountainash-data/tree/main/examples/mutations).

Which operations a given dialect supports is listed in [Supported databases and limits](../10-supported-databases-and-limits/index.md). Inside an explicit transaction scope every method on this page is a protected package call; see [Transactions](../08-transactions/index.md).

## Creating tables and views

<!-- concept:23 -->
The DDL methods on `IbisBackend` are `create_table`, `drop_table`, `create_view` and `drop_view`. Each needs a connected backend and returns the backend itself, so calls chain. Each accepts a `namespace=` argument that targets a schema or database instead of the connection's default. It takes a string (one namespace level) or a `Namespace` (see [Inspecting databases](../04-inspecting-databases/index.md) for the value object). These four methods hand the namespace to Ibis's own `database=` argument, so they accept a catalog-qualified `Namespace` with one path level. Paths deeper than one level raise `ValueError`, because Ibis models only `catalog -> database -> table`. Errors from the engine, such as creating a table that already exists, pass through unchanged.

<!-- concept:24 -->
`create_table(name, obj, *, schema=None, namespace=None, temp=False, overwrite=False)` creates a table from data or from a schema alone. `obj` is anything Ibis can load: a list of dicts, a Polars, pandas or PyArrow frame, or an Ibis table expression. Column types are inferred from it. Pass `obj=None` with an `ibis.schema(...)` to create an empty table with exact types. An existing table raises the engine's error unless `overwrite=True` replaces it, and `temp=True` creates a temporary table where the engine supports one.

```python
import ibis
import polars as pl
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="duckdb", database=":memory:").connect()

backend.create_table(
    "regions", None, schema=ibis.schema({"code": "string", "name": "string"})
)
backend.create_table(
    "sales",
    pl.DataFrame({"id": [1, 2], "region": ["north", "south"], "total": [120, 95]}),
)

try:
    backend.create_table("regions", [{"code": "n", "name": "North"}])
except Exception as exc:
    print(type(exc).__name__, exc)
# → CatalogException Catalog Error: Table with name "regions" already exists!

backend.create_table("regions", [{"code": "n", "name": "North"}], overwrite=True)
```

The `namespace=` argument places a table in another schema, here a DuckDB schema addressed with its catalog:

```python
from mountainash_data import Namespace

backend.ibis_connection().create_database("analytics")
backend.create_table(
    "daily",
    [{"day": "2026-10-01", "total": 215}],
    namespace=Namespace(catalog="memory", path=("analytics",)),
)
print(backend.list_tables(namespace="analytics"))
# → ['daily']
```

Creating the schema itself is not a backend method; the example reaches through `ibis_connection()` to the Ibis backend's `create_database`. The upsert, `add_columns` and index methods treat namespaces more strictly, as described under [Upsert](#upsert).

`drop_table(name, *, namespace=None, force=False)` removes a table; `force=True` makes a missing table a no-op instead of an engine error.

<!-- concept:29 -->
`create_view(name, obj, *, namespace=None, overwrite=False)` stores an Ibis expression as a view in the database. Build the expression from `backend.table(...)` with the [Ibis expression API](https://ibis-project.org/reference/expression-tables); the view is compiled to the engine's SQL once, at creation. `drop_view(name, *, namespace=None, force=False)` removes it.

```python
sales = backend.table("sales")
backend.create_view("large_sales", sales.filter(sales.total > 100))

print(backend.table("large_sales").to_pyarrow().to_pylist())
# → [{'id': 1, 'region': 'north', 'total': 120}]
```

A view reads like a table through `backend.table()` and appears in `list_tables()`.

## Inserting and truncating

<!-- concept:25 -->
The DML methods change rows rather than structure. `insert` and `truncate` delegate to Ibis (SQLite's truncate is the one exception, described below), while `upsert` builds its own statement. All three return the backend for chaining and take the same `namespace=` argument as the DDL methods. `insert` and `truncate` accept a catalog-qualified namespace; `upsert` does not. Writes run on the backend's connection with whatever autocommit behaviour the driver has: mountainash-data does not change autocommit, and a write outside an explicit `transaction()` scope is committed the way the driver normally commits it.

<!-- concept:26 -->
`insert(name, obj, *, namespace=None, overwrite=False)` appends rows from the same kinds of source that `create_table` accepts. With `overwrite=True` the table's existing rows are replaced by the new ones; the table definition stays.

```python
backend.insert("sales", [{"id": 3, "region": "west", "total": 80}])
print(backend.table("sales").count().execute())
# → 3

backend.insert("sales", [{"id": 4, "region": "east", "total": 60}], overwrite=True)
print(backend.table("sales").to_pyarrow().to_pylist())
# → [{'id': 4, 'region': 'east', 'total': 60}]
```

Source columns are matched to the table by Ibis. Inserting rows that should update existing keys is an upsert, not an insert.

<!-- concept:28 -->
`truncate(name, *, namespace=None)` deletes every row and keeps the table. Most dialects run Ibis's `truncate_table`, which emits `TRUNCATE TABLE`. SQLite has no `TRUNCATE` statement, so its dialect entry registers a replacement that deletes all rows instead (`src/mountainash_data/backends/ibis/dialects/_registry.py`). The call is the same either way:

```python
backend.truncate("regions").drop_view("large_sales")
print(backend.table("regions").count().execute())
# → 0
```

## Upsert

<!-- concept:27 -->
`upsert` inserts rows whose key is new and updates rows whose key already exists, in one SQL statement. The signature is:

```text
upsert(name, obj, *, conflict_columns, update_columns=None,
       conflict_action="UPDATE", update_condition=None, namespace=None)
```

`conflict_columns` is the match key: one column name or a list. `obj` is the incoming rows, as a list of dicts, a frame or an Ibis table.

### A first upsert

On DuckDB and SQLite the engine decides whether a row conflicts, and it only does so against a unique index or a primary key on exactly the conflict columns. Without one, the engine rejects the statement:

```python
def rows(name):
    return backend.table(name).order_by("id").to_pyarrow().to_pylist()

backend.create_table(
    "orders",
    [
        {"id": 1, "region": "north", "total": 120},
        {"id": 2, "region": "south", "total": 95},
    ],
)
try:
    backend.upsert(
        "orders", [{"id": 1, "region": "north", "total": 140}], conflict_columns="id"
    )
except Exception as exc:
    print(type(exc).__name__, exc)
# → BinderException Binder Error: The specified columns as conflict target are not
#   referenced by a UNIQUE/PRIMARY KEY CONSTRAINT or INDEX
```

SQLite fails the same way with `OperationalError: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint`. Adding a unique index (see [Indexes](../07-indexes/index.md)) fixes it:

```python
backend.create_unique_index("orders", "id")
backend.upsert(
    "orders",
    [
        {"id": 1, "region": "north", "total": 140},
        {"id": 3, "region": "west", "total": 80},
    ],
    conflict_columns="id",
)
for row in rows("orders"):
    print(row)
# → {'id': 1, 'region': 'north', 'total': 140}
#   {'id': 2, 'region': 'south', 'total': 95}
#   {'id': 3, 'region': 'west', 'total': 80}
```

Row 1 was updated, row 3 was inserted and row 2, which was not in the input, was left alone. Upsert never deletes.

### Which columns are written

The incoming rows may carry fewer columns than the table, but not more: a column absent from the table raises `ValueError` before any SQL runs. Incoming values are cast to the target column types. When `update_columns` is omitted, the columns updated on a match are the incoming columns that are not part of the key. A table column missing from the input is not touched on a match, and is left `NULL` (or its default) on an insert:

```python
backend.upsert(
    "orders", [{"id": 2, "total": 99}, {"id": 4, "total": 10}], conflict_columns="id"
)
for row in rows("orders"):
    print(row)
# → {'id': 1, 'region': 'north', 'total': 140}
#   {'id': 2, 'region': 'south', 'total': 99}
#   {'id': 3, 'region': 'west', 'total': 80}
#   {'id': 4, 'region': None, 'total': 10}
```

Pass `update_columns` to update fewer columns than the input carries. Every column named there must exist in both the table and the input. `conflict_action="NOTHING"` turns the call into insert-or-ignore: new keys are inserted and existing rows are untouched.

```python
backend.upsert(
    "orders",
    [{"id": 1, "region": "NORTH", "total": 150}],
    conflict_columns="id",
    update_columns="total",
)
print(rows("orders")[0])
# → {'id': 1, 'region': 'north', 'total': 150}

backend.upsert(
    "orders",
    [{"id": 1, "region": "north", "total": 0}, {"id": 5, "region": "east", "total": 7}],
    conflict_columns="id",
    conflict_action="NOTHING",
)
print(rows("orders")[0], rows("orders")[-1])
# → {'id': 1, 'region': 'north', 'total': 150} {'id': 5, 'region': 'east', 'total': 7}
```

If the update set comes out empty (for example, the input carries only the key columns), the call degrades to `NOTHING` and emits a `UserWarning` saying there are no columns to update.

### Conditional updates

`update_condition` takes a function of two Ibis tables, `(incoming, existing)`, that returns a boolean expression. A matched row is updated only where the expression is true:

```python
backend.upsert(
    "orders",
    [{"id": 1, "region": "north", "total": 500}, {"id": 2, "region": "south", "total": 50}],
    conflict_columns="id",
    update_condition=lambda incoming, existing: incoming.total > existing.total,
)
print(rows("orders")[0], rows("orders")[1])
# → {'id': 1, 'region': 'north', 'total': 500} {'id': 2, 'region': 'south', 'total': 99}
```

The condition must be a scalar, row-level predicate. Aggregations, window functions and subqueries raise `ValueError`. The condition is ignored, with a warning, when the action is `NOTHING`, and it is not available on the MySQL family (see below).

### Validation before execution

The package checks the request against the target table before it sends SQL, and reports problems as `ValueError`:

```python
attempts = [
    dict(name="returns", rows=[{"id": 1}], conflict_columns="id"),
    dict(name="orders", rows=[{"id": 1, "colour": "red"}], conflict_columns="id"),
    dict(name="orders", rows=[{"id": 1, "total": 3}], conflict_columns="id",
         update_columns="region"),
    dict(name="orders", rows=[{"id": 1, "total": 3}], conflict_columns="id",
         update_condition=lambda incoming, existing: incoming.total > existing.total.sum()),
    dict(name="orders", rows=[{"id": 1, "total": 3}], conflict_columns="id",
         namespace=Namespace(catalog="memory", path=("main",))),
]
for attempt in attempts:
    try:
        backend.upsert(attempt.pop("name"), attempt.pop("rows"), **attempt)
    except ValueError as exc:
        print(exc)
```

```text
target table 'returns' does not exist
source columns absent from target: ['colour']
update_columns not present in source frame: ['region']
update_condition must be a scalar row predicate; found Sum (aggregation/window). Use the upsert_hook override for conditions outside this grammar.
upsert does not support catalog-qualified namespaces (catalog='memory'): it builds engine-native SQL that cannot address a foreign catalog. Use a native-delegating op, or omit the catalog.
```

Key columns missing from the table, and a `conflict_action` other than `"UPDATE"` or `"NOTHING"`, raise `ValueError` too. Table and namespace names must be simple identifiers (letters, digits, `_` and `$`, not starting with a digit); dotted or quoted names raise `ValueError`. The last line shows the namespace rule for statements the package builds itself: one namespace level, with no catalog.

### Match-key semantics by dialect

Each dialect declares which SQL family its upsert uses (`upsert_style` in `src/mountainash_data/backends/ibis/dialects/_registry.py`). The statements are built in `src/mountainash_data/backends/ibis/operations.py`, using the quoting, type-casting and predicate compilation helpers in `src/mountainash_data/backends/ibis/_render.py`, and dispatched from `IbisBackend.upsert` in `src/mountainash_data/backends/ibis/backend.py`. The family decides what "the key matches" means:

| Family | Dialects | How a match is found | `NOTHING` | `update_condition` |
|---|---|---|---|---|
| `INSERT ... ON CONFLICT` | SQLite, DuckDB, MotherDuck, PostgreSQL, RisingWave | The engine's unique index or primary key on exactly the conflict columns; required | `DO NOTHING` | `DO UPDATE ... WHERE` |
| `MERGE` | SQL Server, Oracle, Snowflake, BigQuery, Redshift, Trino, Databricks, Exasol | Equality on every conflict column in the `MERGE ... ON` clause; no constraint required by the statement | Only the `WHEN NOT MATCHED` insert clause | `WHEN MATCHED AND ...` |
| `INSERT ... ON DUPLICATE KEY UPDATE` | MySQL, SingleStoreDB | Any unique index on the table, so the package proves the key first | A self-assignment of the first key column | Not supported (`ValueError`) |

The consequences differ in ways that matter for portable code:

- With `ON CONFLICT`, the constraint is the contract. Create the unique index or primary key that matches `conflict_columns`, as in the examples above.
- With `MERGE`, matching is plain equality in the join condition. Because `NULL = NULL` is not true in SQL, an incoming row with a `NULL` key never matches and is inserted. The statement is rendered per engine: Oracle omits the `AS` keyword before aliases and SQL Server gets a terminating semicolon.
- MySQL's `ON DUPLICATE KEY` fires on a collision with *any* unique index, not the one you meant. Before running it, the package reads `information_schema` and raises `ValueError` unless the table has exactly one unique index, its columns equal `conflict_columns` in order, it is not a prefix or functional index, and none of the conflict columns is nullable. With `conflict_action="NOTHING"` the matched row is still touched by a no-op assignment.

ClickHouse, Impala, Materialize, Druid and PySpark declare no upsert family, and `upsert` raises `NotImplementedError` for them. A dialect entry can also register its own upsert implementation instead of a family; none does at this revision. The full per-dialect picture is in [Supported databases and limits](../10-supported-databases-and-limits/index.md), and the dispatch design in [How one backend serves 20 dialects](../11-how-one-backend-serves-20-dialects/index.md).

## Adding columns

<!-- concept:119 -->
`add_columns(name, source, *, namespace=None)` adds the columns in `source` that the table does not have yet. It is additive: columns that already exist are skipped, even when `source` gives them a different type, and nothing is dropped or altered. Calling it twice with the same source is safe. That makes it the tool for widening a table before loading data that carries new fields.

`source` takes two shapes:

| Shape | Example | How types are chosen |
|---|---|---|
| Mapping of column name to type | `{"note": "string", "shipped_at": "timestamp"}` | Each value is an Ibis `DataType`, an Ibis type string, or a `MountainashDtype` when the [`mountainash`](https://github.com/mountainash-io/mountainash) package is installed |
| A frame | a Polars, pandas or PyArrow frame, or a list of dicts | Inferred by Ibis, exactly as `create_table` would infer them; the rows themselves are not inserted |

```python
backend.add_columns(
    "orders", {"note": "string", "shipped_at": "timestamp", "total": "float64"}
)
print(backend.table("orders").schema())
```

```text
ibis.Schema {
  id          int64
  region      string
  total       int64
  note        string
  shipped_at  timestamp(6)
}
```

`total` already existed, so it keeps its `int64` type. The frame form reads only the frame's schema:

```python
extra = pl.DataFrame({"id": [1], "channel": ["web"], "weight": [1.5], "flag": [None]})
backend.add_columns("orders", extra)
backend.add_columns("orders", extra)  # second call finds nothing to add
print(backend.table("orders").schema())
print(rows("orders")[0])
```

```text
ibis.Schema {
  id          int64
  region      string
  total       int64
  note        string
  shipped_at  timestamp(6)
  channel     string
  weight      float64
  flag        string
}
{'id': 1, 'region': 'north', 'total': 500, 'note': None, 'shipped_at': None, 'channel': None, 'weight': None, 'flag': None}
```

New columns are nullable and existing rows hold `NULL` in them. A column whose inferred type is null (the all-`None` `flag` column) is created as the dialect's string type. Column types are rendered by the connection's own Ibis type mapper, so they match what `create_table` would produce on the same engine: the same mapping run on SQLite creates `shipped_at` as `timestamp` rather than DuckDB's `timestamp(6)`.

The method issues one `ALTER TABLE ... ADD COLUMN` per new column, because SQLite accepts only one column per statement (`src/mountainash_data/backends/ibis/operations.py`). The set of missing columns is computed once, up front. Two consequences follow:

- On engines without transactional DDL, a failure part-way leaves the earlier columns added. Re-running the call adds the rest.
- Two processes widening the same table at once can both decide a column is missing; the second `ALTER` then fails with the engine's error.

The same identifier and namespace rules as upsert apply: simple table and namespace names, one namespace level, no catalog.

```python
for target, ns in [("main.orders", None), ("orders", Namespace(catalog="memory", path=("main",)))]:
    try:
        backend.add_columns(target, {"x": "int64"}, namespace=ns)
    except ValueError as exc:
        print(exc)
```

```text
table_name 'main.orders' must be a simple identifier (letters, digits, underscore, $; starting with a letter or underscore); dotted, quoted, or whitespace-bearing names are out of scope.
add_columns does not support catalog-qualified namespaces (catalog='memory'): it builds engine-native SQL that cannot address a foreign catalog. Use a native-delegating op, or omit the catalog.
```

To target a schema, pass it as `namespace="analytics"` rather than dotting it into the table name. No dialect declares a capability gate for `add_columns`: every dialect runs the same `ALTER TABLE ... ADD COLUMN`, and an engine that refuses it (a read-only or append-only service, for example) raises its own error.

## Renaming tables

<!-- concept:120 -->
`rename_table(old_name, new_name)` renames a table in the connection's current database or schema. It has no `namespace=` argument, and both names must be simple identifiers. The statement is rendered with sqlglot for the connected dialect: `ALTER TABLE ... RENAME TO ...` on most engines, `EXEC sp_rename` on SQL Server, `ALTER TABLE ... RENAME ...` on MySQL and SingleStoreDB, and Exasol's `RENAME TABLE ... TO ...` (`src/mountainash_data/backends/ibis/operations.py`).

```python
backend.rename_table("sales", "sales_2026")
print(backend.table_exists("sales"), backend.table_exists("sales_2026"))
# → False True

try:
    backend.rename_table("sales_2026", "archive.sales")
except ValueError as exc:
    print(exc)
# → new_name 'archive.sales' must be a simple identifier (letters, digits, underscore, $;
#   starting with a letter or underscore); dotted, quoted, or whitespace-bearing names are
#   out of scope.
```

Engine rules still apply after the SQL is rendered. DuckDB refuses to rename a table that has an index on it, so the indexed `orders` table cannot be renamed until its index is dropped:

```python
try:
    backend.rename_table("orders", "orders_2026")
except Exception as exc:
    print(type(exc).__name__, exc)
# → DependencyException Dependency Error: Cannot alter entry "orders" because there
#   are entries that depend on it.
```

SQLite renames an indexed table and keeps the index attached to the new name.
