# Quickstart

## Installation

```bash
pip install mountainash-data
```

For optional backend drivers, install the relevant extra:

```bash
pip install mountainash-data[postgres]     # PostgreSQL
pip install mountainash-data[mssql]        # SQL Server
pip install mountainash-data[snowflake]    # Snowflake
pip install mountainash-data[bigquery]     # BigQuery
pip install mountainash-data[pyspark]      # Apache Spark
pip install mountainash-data[trino]        # Trino
```

SQLite and DuckDB work out of the box — no extra needed.

---

## Connecting

`IbisBackend` is the entry point for all relational database connections. There are three ways to construct one.

### Form 1 — dialect + kwargs (scripts, tests)

```python
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="sqlite", database=":memory:")
```

### Form 2 — connection URL

```python
backend = IbisBackend("postgresql://app:s3cret@db.example:5432/mydb")
backend = IbisBackend("duckdb:///path/to/local.ddb")
backend = IbisBackend("duckdb://md:my_db?motherduck_token=TOKEN")  # MotherDuck
```

### Form 3 — settings object (env-driven deployment)

```python
from mountainash_data.core.settings import PostgreSQLAuthSettings, PasswordAuth
from mountainash_settings import SettingsParameters

settings = PostgreSQLAuthSettings(
    HOST="db.example",
    PORT=5432,
    DATABASE="mydb",
    auth=PasswordAuth(username="app", password="s3cret"),
)
params = SettingsParameters(settings_class=PostgreSQLAuthSettings)
backend = IbisBackend(params)
```

---

## Connecting and closing

Call `connect()` before use and `close()` when done. Both return `self` for chaining.

```python
backend = IbisBackend(dialect="duckdb").connect()
# ... use backend ...
backend.close()
```

The context manager handles this automatically:

```python
with IbisBackend(dialect="sqlite", database=":memory:") as backend:
    tables = backend.list_tables()
```

---

## Inspecting schema

```python
with IbisBackend("postgresql://user:pass@host/db") as backend:
    # List tables in the default namespace
    tables = backend.list_tables()

    # List tables in a specific schema
    tables = backend.list_tables(namespace="reporting")

    # Inspect a single table
    info = backend.inspect_table("orders")
    print(info.name)            # "orders"
    print(info.column_names)    # ["id", "customer_id", "total", ...]
    print(info.qualified_name)  # "mydb.public.orders"

    for col in info.columns:
        print(col.name, col.type_name, col.nullable)

    # Inspect a namespace (schema/database)
    ns = backend.inspect_namespace("reporting")
    print(ns.name, ns.tables)

    # Inspect the full catalog
    catalog = backend.inspect_catalog()
    for ns in catalog.namespaces:
        print(ns.name, ns.tables)
```

---

## Running queries

```python
with IbisBackend(dialect="duckdb") as backend:
    # Get an ibis Table expression
    orders = backend.table("orders")
    result = backend.run_expr(orders.filter(orders.total > 100))

    # Raw SQL
    result = backend.run_sql("SELECT count(*) FROM orders")

    # Compile an ibis expression to SQL (without executing)
    sql = backend.to_sql(orders.count())
```

---

## Creating and loading tables

```python
import pandas as pd

df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})

with IbisBackend(dialect="duckdb") as backend:
    backend.create_table("my_table", df)

    # Append rows
    backend.insert("my_table", df)

    # Overwrite
    backend.create_table("my_table", df, overwrite=True)

    # Drop
    backend.drop_table("my_table")
```

---

## Upsert and indexes

Not all backends support these operations. `IbisBackend` raises `NotImplementedError` if the dialect does not have the capability.

```python
# Upsert (DuckDB, SQLite, MotherDuck)
backend.upsert(
    "orders",
    new_rows_df,
    conflict_columns="id",
    update_columns=["total", "status"],
)

# Create an index
backend.create_index("orders", ["customer_id"])
backend.create_unique_index("orders", ["id"])

# Check existence
exists = backend.index_exists("idx_orders_customer_id")

# List typed index metadata
indexes = backend.list_indexes("orders")
for index in indexes:
    print(index.name, index.columns, index.unique, index.is_primary)

# Drop
backend.drop_index("idx_orders_customer_id", table_name="orders")
```

`list_indexes()` returns frozen `IndexInfo` objects. Each object includes the
index name, key columns, uniqueness, primary-key status, index type, included
columns, validity, definition, and dialect metadata.

Index listing is available for SQLite, DuckDB, MotherDuck, PostgreSQL, MySQL,
MSSQL, Oracle, and SingleStoreDB.

Pass `namespace="sales"` to `list_indexes()` when the table is not in the
default schema.

---

## Accessing the raw ibis connection

For operations not covered by `IbisBackend`, retrieve the underlying ibis backend object:

```python
with IbisBackend(dialect="duckdb") as backend:
    conn = backend.ibis_connection()   # raw ibis DuckDB backend
    result = conn.read_parquet("s3://bucket/file.parquet")
```

---

## Supported databases

| Dialect | URL scheme | Notes |
|---------|-----------|-------|
| `sqlite` | `sqlite://` | No extra required |
| `duckdb` | `duckdb://` | No extra required |
| `motherduck` | `duckdb://md:` | MotherDuck cloud DuckDB |
| `postgres` | `postgres://` | Requires `[postgres]` extra |
| `mysql` | `mysql://` | |
| `mssql` | `mssql://` | Requires `[mssql]` extra |
| `oracle` | `oracle://` | |
| `snowflake` | `snowflake://` | Requires `[snowflake]` extra |
| `bigquery` | `bigquery://` | Requires `[bigquery]` extra |
| `redshift` | `postgres://` | Uses postgres protocol |
| `trino` | `trino://` | Requires `[trino]` extra |
| `clickhouse` | `clickhouse://` | |
| `databricks` | — | kwargs only, no URL form |
| `singlestoredb` | `singlestoredb://` | |
| `exasol` | `exasol://` | |
| `impala` | `impala://` | |
| `materialize` | `materialize://` | |
| `risingwave` | `risingwave://` | |
| `druid` | `druid://` | |
| `pyspark` | `pyspark://` | Requires `[pyspark]` extra |

