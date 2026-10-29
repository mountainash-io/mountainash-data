---
title: Indexes
description: Create, list, check and drop indexes.
date: 2026-10-29
---

# Indexes

`IbisBackend` manages conventional secondary indexes with five methods: `create_index`, `create_unique_index`, `drop_index`, `list_indexes` and `index_exists`. Ibis has no index API, so mountainash-data renders the `CREATE INDEX` and `DROP INDEX` statements and the catalog queries itself, using each dialect's declared index capability. The capability says whether the engine has secondary indexes at all, whether it supports partial indexes and index methods, and whether `DROP INDEX` needs the table name. The same calls therefore work across engines, and an option an engine cannot honour raises `ValueError` before any SQL is sent.

The examples run on in-memory SQLite, with DuckDB for contrast, and each block continues from the one before it. The anchor recipe is [`examples/indexes`](https://github.com/mountainash-io/mountainash-data/tree/main/examples/indexes).

## Creating indexes

<!-- concept:115 -->
`create_index(table_name, columns, *, index_name=None, unique=False, index_type=None, where=None, namespace=None, if_not_exists=True)` creates an index and returns the backend. `create_unique_index(table_name, columns, *, index_name=None, where=None, namespace=None)` is the same call with `unique=True`; a unique index is also what `upsert` needs on SQLite, DuckDB and PostgreSQL (see [Upsert](../06-creating-and-changing-tables/index.md#upsert)).

```python
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="sqlite", database=":memory:").connect()
backend.create_table(
    "sales",
    [
        {"id": 1, "region": "north", "total": 120, "sold_on": "2026-01-02"},
        {"id": 2, "region": "south", "total": 95, "sold_on": "2026-01-03"},
    ],
)

backend.create_index("sales", "region", index_name="ix_sales_region")
backend.create_index("sales", ["sold_on", "region"])
backend.create_unique_index("sales", "id")
backend.create_index(
    "sales", "total", index_name="ix_sales_large", where=lambda t: t.total > 100
)
backend.create_index("sales", "region", index_name="ix_sales_region")  # no-op

print([index.name for index in backend.list_indexes("sales")])
# → ['idx_sales_region_sold_on', 'ix_sales_large', 'ix_sales_region', 'uidx_sales_id']
```

Without `index_name`, the name is `idx_<table>_<columns>`, or `uidx_<table>_<columns>` for a unique index, with the column names sorted alphabetically and joined by underscores. The index itself keeps the column order you passed: the second index above covers `("sold_on", "region")`.

The arguments map onto the dialect's capability as follows:

| Argument | Effect | Raises `ValueError` when |
|---|---|---|
| `columns` | One column name or a list, in index order | The list is empty, or a name is not a simple identifier |
| `index_name` | The index name; generated when omitted | The name is not a simple identifier |
| `index_type` | Adds a `USING <method>` clause, placed where each engine expects it | The method is not one the dialect accepts. PostgreSQL accepts `btree`, `hash`, `gist`, `gin`, `brin` and `spgist`, MySQL `btree`, SingleStoreDB `btree` and `hash`; the others accept none |
| `where` | A partial (filtered) index. Pass a function of the Ibis table that returns a boolean expression; it is compiled to the dialect's SQL | The dialect has no partial indexes. SQLite, PostgreSQL and SQL Server support them; DuckDB, MotherDuck, MySQL, Oracle and SingleStoreDB do not |
| `if_not_exists` | When `True` (the default), creating an index that already exists is a no-op | |
| `namespace` | One schema level, as a string or a `Namespace` without a catalog | The namespace has a catalog or more than one level |

"Simple identifier" means letters, digits, `_` and `$`, not starting with a digit. The `where` predicate follows the same grammar as an upsert `update_condition`: a row-level expression that may reference any column of the table, with no aggregations, window functions or subqueries.

```python
try:
    backend.create_index("sales", "total", index_type="hash")
except ValueError as exc:
    print(exc)
# → index_type 'hash' not supported by this dialect; valid: none

with IbisBackend(dialect="duckdb", database=":memory:") as duck:
    duck.create_table("sales", [{"id": 1, "total": 120}])
    try:
        duck.create_index("sales", "total", where=lambda t: t.total > 100)
    except ValueError as exc:
        print(exc)
# → this dialect does not support partial indexes (where=)
```

SQLite, DuckDB, MotherDuck and PostgreSQL have a native `CREATE INDEX IF NOT EXISTS`. On MySQL, SQL Server, Oracle and SingleStoreDB the package runs `index_exists` first and skips the `CREATE` when the index is there. That check and the create are two statements, so a concurrent session can still create the same index in between, and the engine's error is then raised, not suppressed. The engine's DDL rules also apply: MySQL and Oracle commit implicitly around DDL, so an index statement issued inside a [transaction](../08-transactions/index.md) is not undone by a rollback.

Dialects without a declared index capability raise `NotImplementedError` from every index method. ClickHouse, for example, has data-skipping indexes rather than secondary indexes, so it declares none. `index_exists` checks this before it needs a connection:

```python
clickhouse = IbisBackend(dialect="clickhouse", host="localhost")
try:
    clickhouse.index_exists("ix_sales_region")
except NotImplementedError as exc:
    print(exc)
# → Dialect 'clickhouse' does not support index_exists
```

Oracle declares index support, but its index operations still have unresolved identifier and owner-resolution issues and should not be relied on. The full per-dialect index support is in [Supported databases and limits](../10-supported-databases-and-limits/index.md).

## Dropping indexes

<!-- concept:116 -->
`drop_index(index_name, *, table_name=None, namespace=None, if_exists=True)` drops an index by name and returns the backend. With the default `if_exists=True`, dropping an index that does not exist is a no-op; with `if_exists=False` the engine reports it:

```python
backend.drop_index("ix_sales_region")
backend.drop_index("ix_sales_region")  # already gone: no-op

try:
    backend.drop_index("ix_sales_region", if_exists=False)
except Exception as exc:
    print(type(exc).__name__, exc)
# → OperationalError no such index: ix_sales_region
```

Engines disagree about where an index name lives. On SQLite, DuckDB, MotherDuck, PostgreSQL and Oracle, index names are unique within a schema and the statement is `DROP INDEX name`. On MySQL, SQL Server and SingleStoreDB, index names are unique per table and the statement is `DROP INDEX name ON table`; those dialects raise `ValueError` when `table_name` is missing. Passing `table_name` everywhere keeps the call portable, since schema-scoped dialects accept it too. `IF EXISTS` is native on SQLite, DuckDB, MotherDuck, PostgreSQL and SQL Server, and emulated with an existence check on MySQL, Oracle and SingleStoreDB, with the same race as index creation.

## Listing indexes and IndexInfo

<!-- concept:114 -->
`list_indexes(table_name, *, namespace=None)` returns one `IndexInfo` per index on the table. `IndexInfo` is a frozen dataclass exported from `mountainash_data` and shared with the rest of the [inspection model](../04-inspecting-databases/index.md). Its fields are:

| Field | Type | Meaning |
|---|---|---|
| `name` | `str` | Index name as the engine reports it |
| `unique` | `bool` | Whether the index enforces uniqueness |
| `is_primary` | `bool` | Whether it backs the primary key |
| `columns` | tuple of `str` | Key columns, in index order |
| `index_type` | `str` or `None` | The engine's access method, such as `btree` (SQLite) or `art` (DuckDB) |
| `included_columns` | tuple of `str` | Non-key columns carried by the index, on engines with `INCLUDE` columns (PostgreSQL, SQL Server); empty elsewhere |
| `is_valid` | `bool` or `None` | Validity where the engine reports it (PostgreSQL, SQL Server, Oracle); `None` where it does not |
| `definition` | `str` or `None` | The engine's own DDL or filter text when available |
| `metadata` | mapping | Engine-specific extras |

```python
for index in backend.list_indexes("sales"):
    print(index.name, index.columns, index.unique, index.index_type)
# → idx_sales_region_sold_on ('sold_on', 'region') False btree
#   ix_sales_large ('total',) False btree
#   uidx_sales_id ('id',) True btree

print(backend.list_indexes("sales")[1].definition)
# → CREATE INDEX "ix_sales_large" ON "sales" ("total") WHERE "total" > 100
```

On DuckDB the list also includes the indexes behind primary-key and unique constraints. Their `metadata["source_kind"]` is `"constraint"`, and indexes created with `CREATE INDEX` have `"index"`:

```python
with IbisBackend(dialect="duckdb", database=":memory:") as duck:
    duck.ibis_connection().raw_sql(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT)"
    )
    duck.create_index("customers", "email")
    for index in duck.list_indexes("customers"):
        print(index.name, index.is_primary, index.index_type, index.metadata["source_kind"])
# → constraint_primary_key_customers_0 True art constraint
#   idx_customers_email False art index
```

On SQLite, an `INTEGER PRIMARY KEY` column is the table's rowid rather than an index, so it does not appear in the list. The table and namespace names must be simple identifiers, and a dialect without index support raises `NotImplementedError`.

## index_exists

<!-- concept:117 -->
`index_exists(index_name, *, table_name=None, namespace=None)` returns `True` or `False` by querying the engine's catalog. It is the same check the emulated `if_not_exists` and `if_exists` paths use, and it suits code that needs to decide something before acting, such as whether a migration step has already run.

```python
print(backend.index_exists("uidx_sales_id"))
# → True
print(backend.index_exists("uidx_sales_id", table_name="sales"))
# → True
print(backend.index_exists("ix_sales_region"))
# → False

backend.close()
```

With `table_name`, only an index on that table counts. SQL Server resolves the index through its table, so there `index_exists` returns `False` unless `table_name` is given; pass it for portable code. Without `namespace`, most dialects look in the connection's current schema (SQLite's `main`, DuckDB's current schema, the current MySQL database), while PostgreSQL matches the name in any schema. The names must be simple identifiers, and a namespace with a catalog raises `ValueError`.
