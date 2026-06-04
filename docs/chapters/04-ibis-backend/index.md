---
title: Ibis Backend
description: IbisBackend class covering connection, fluent queries, raw SQL, DDL/DML operations, table inspection, and context management
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Ibis Backend

## Summary

This chapter covers the IbisBackend class — the primary backend for connecting to 20+ SQL databases through the Ibis library. It begins with the IbisBackend class structure and connection management, then covers querying via both the fluent Ibis API and raw SQL. DDL operations (create table, create view) and DML operations (insert, upsert, truncate) are presented in detail. The chapter concludes with Ibis-specific implementations of table listing, table/namespace/catalog inspection using the Driver Metadata Conversion layer, and context manager usage for automatic cleanup.

## Concepts Covered

- IbisBackend Class
- Ibis Connection
- Fluent Query API
- Raw SQL Queries
- DDL Operations
- Create Table
- DML Operations
- Insert Data
- Upsert Data
- Truncate Table
- Create View
- List Tables Ibis
- Table Inspection Ibis
- Namespace Inspection Ibis
- Catalog Inspection Ibis
- Context Manager Ibis

## Prerequisites

- Chapter 1: Foundation Concepts (SQL Databases, Connection Management, Ibis Library)
- Chapter 2: Backend Protocol (Backend Protocol Definition, Connect Method, Close Method, List Tables Method, Inspect Table Method, Inspect Namespace Method, Inspect Catalog Method)
- Chapter 3: Inspection Model (Driver Metadata Conversion)

---

<!-- concept:19 -->
## IbisBackend Class

The **IbisBackend class** is the primary entry point for connecting to SQL databases through mountainash-data. It implements the `Backend` protocol by providing a `name` attribute (the string `"ibis"`) and a `connect()` method that returns an `IbisConnection`. The class acts as a factory: it accepts a dialect name and configuration parameters at construction time, validates that the dialect exists in the registry, and stores the configuration for later use when `connect()` is called.

The constructor performs eager validation of the dialect name, raising a `KeyError` immediately if the requested dialect is not registered. This fail-fast behavior ensures that configuration errors surface at construction time rather than at the point of first use, which might be much later in the application lifecycle.

```python
class IbisBackend:
    name = "ibis"

    def __init__(self, dialect: str, **config: t.Any):
        if dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect
        self._spec: DialectSpec = DIALECTS[dialect]
        self._config = config
```

The `_spec` attribute holds the `DialectSpec` for the chosen dialect, which contains the connection builder function and capability hooks (such as dialect-specific index SQL). The `_config` dictionary stores all keyword arguments that will be forwarded to the connection builder when `connect()` is called.

A typical instantiation looks like this, where the dialect and backend-specific parameters are provided as keyword arguments.

```python
# SQLite in-memory
backend = IbisBackend(dialect="sqlite", database=":memory:")

# PostgreSQL with credentials
backend = IbisBackend(
    dialect="postgres",
    host="db.example.com",
    port=5432,
    user="analyst",
    password="secret",
    database="warehouse",
)
```

<!-- concept:20 -->
## Ibis Connection

The **IbisConnection class** wraps a live Ibis backend object and adapts it to satisfy the `Connection` protocol. It is constructed by `IbisBackend.connect()` and should not be instantiated directly by consumer code.

Internally, `IbisConnection` holds three pieces of state: the raw Ibis connection object (`_ibis_conn`), the dialect specification (`_dialect_spec`), and a closed flag (`_closed`). The raw Ibis connection provides the actual database communication capabilities, while the dialect spec is retained for metadata operations like determining the backend name for `inspect_catalog()`.

The connection creation flow is straightforward. `IbisBackend.connect()` invokes the dialect's `connection_builder` callable with the stored configuration, then wraps the result.

```python
def connect(self) -> IbisConnection:
    """Build and return a live ibis connection."""
    if self._spec.connection_builder is None:
        raise NotImplementedError(
            f"Dialect {self.dialect!r} has no connection_builder configured"
        )
    ibis_conn = self._spec.connection_builder(**self._config)
    return IbisConnection(ibis_conn, self._spec)
```

#### Diagram: IbisBackend Connection Flow
<iframe src="../../sims/ibis-connection-flow/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>IbisBackend Connection Flow</summary>
Type: workflow
**sim-id:** ibis-connection-flow<br/>
**Library:** vis-network<br/>
**Status:** Specified

A sequential flow diagram showing the connection creation process. Five nodes: (1) "Consumer code calls IbisBackend(dialect, **config)", (2) "IbisBackend validates dialect in DIALECTS registry", (3) "connect() invokes dialect.connection_builder(**config)", (4) "Ibis library opens connection to database engine", (5) "IbisConnection wrapper returned to consumer". Arrows connect nodes sequentially. Error paths branch off from nodes 2 and 3 showing KeyError and NotImplementedError respectively. Clicking a node shows the code executed at that stage. Learning objective: Apply knowledge of the connection flow to debug connection failures (Bloom: Apply). Controls: click nodes for code detail, hover for descriptions. Colors: SteelBlue for consumer, DarkSlateBlue for validation, DarkGreen for Ibis library, Gold for result.
</details>

<!-- concept:21 -->
## Fluent Query API

The **fluent query API** refers to Ibis's expression-based approach to building analytical queries. Instead of writing SQL strings, you chain method calls on table objects to compose queries. Ibis compiles these expressions into the appropriate SQL dialect for the connected backend at execution time.

The fluent API is accessible through the raw Ibis connection wrapped inside `IbisConnection`. The operations module provides methods that accept an Ibis backend object and execute expressions against it.

```python
# Access the Ibis table reference
ibis_table = conn._ibis_conn.table("orders")

# Chain fluent operations
result = (
    ibis_table
    .filter(ibis_table.status == "completed")
    .group_by("region")
    .aggregate(total=ibis_table.amount.sum())
    .order_by("total", ascending=False)
)
```

Key characteristics of the fluent query API include:

- **Deferred execution**: Expressions are built lazily. No database query is sent until results are explicitly requested via `.execute()` or equivalent.
- **Backend portability**: The same expression tree compiles to different SQL dialects depending on the connected backend.
- **Type safety**: Ibis validates column names and operation compatibility at expression construction time.
- **Composability**: Intermediate expressions can be stored in variables and combined later.

<!-- concept:22 -->
## Raw SQL Queries

When the fluent API does not cover a specific query pattern, mountainash-data supports **raw SQL queries** through the `run_sql()` class method on `BaseIbisOperations`. This method accepts a SQL string and returns an Ibis table expression representing the result set.

The method signature includes optional parameters for schema hints (when the result schema cannot be inferred) and dialect specification (for multi-dialect environments).

```python
@classmethod
def run_sql(cls, ibis_backend, query, /, *, schema=None, dialect=None):
    try:
        return ibis_backend.sql(query, schema=schema, dialect=dialect)
    except Exception as e:
        print(f"Error executing SQL: {e}")
        return None
```

Raw SQL is essential for operations that Ibis does not support natively, such as database-specific administrative commands, system catalog queries, or complex CTE expressions that are more naturally written in SQL. The `run_sql()` method returns `None` on failure rather than raising an exception, allowing calling code to handle errors gracefully.

The `to_sql()` companion method provides the inverse operation: it compiles an Ibis expression into a SQL string without executing it, which is useful for debugging or logging.

<!-- concept:23 -->
## DDL Operations

**DDL (Data Definition Language) operations** modify the structure of the database. In mountainash-data, DDL operations include creating and dropping tables, creating and dropping views, and managing indexes. These operations are exposed through the `BaseIbisOperations` class, which provides class methods that accept an Ibis backend connection and perform the structural modification.

DDL operations differ from queries in that they change the database state rather than returning data. They typically require elevated permissions and should be used with care in production environments.

The primary DDL methods available in mountainash-data are:

| Method | Purpose | Key Parameters |
|---|---|---|
| `create_table()` | Create a new table | table_name, dataframe/schema, temp, overwrite |
| `drop_table()` | Remove a table | table_name, database, force |
| `create_view()` | Create a named view | view_name, ibis_table_expr, overwrite |
| `drop_view()` | Remove a view | view_name, database, force |
| `create_index()` | Create a table index | table_name, columns, unique, if_not_exists |
| `drop_index()` | Remove an index | index_name, if_exists |

<!-- concept:24 -->
## Create Table

The **create_table** operation constructs a new table in the database, optionally populating it with data from a dataframe or defining its schema without data.

```python
@classmethod
def create_table(cls, ibis_backend, table_name, df, /,
                 schema=None, database=None, temp=False, overwrite=False):
    ibis_backend.create_table(
        table_name, obj=df, schema=schema,
        database=database, temp=temp, overwrite=overwrite
    )
```

The method delegates to Ibis's native `create_table()`, which handles SQL generation for the specific backend. The `temp` parameter creates a temporary table that is automatically dropped when the connection closes. The `overwrite` parameter allows replacing an existing table with the same name.

Temporary tables are particularly useful for staging data during upsert operations, where incoming data must be compared against existing rows before being merged.

<!-- concept:25 -->
## DML Operations

**DML (Data Manipulation Language) operations** modify the data within existing tables. mountainash-data provides three DML operations: insert, upsert, and truncate. These operations form the write path of the library, complementing the read path provided by the fluent query API and raw SQL.

DML operations accept data in flexible formats. The `df` parameter can be an Ibis table expression, a Polars dataframe, a Pandas dataframe, or any other format supported by the underlying Ibis backend. This flexibility allows mountainash-data to serve as a write sink for diverse data pipelines.

<!-- concept:26 -->
## Insert Data

The **insert** operation appends rows from a dataframe to an existing table. It is the simplest DML operation, requiring only the target table name and source data.

```python
@classmethod
def insert(cls, ibis_backend, table_name, /, df,
           database=None, schema=None, overwrite=False):
    ibis_backend.insert(table_name, obj=df, database=database, overwrite=overwrite)
```

The `overwrite` parameter controls whether existing data is replaced. When `overwrite=True`, the operation is equivalent to truncate followed by insert. When `overwrite=False` (the default), new rows are appended without affecting existing data.

<!-- concept:27 -->
## Upsert Data

The **upsert** operation (also known as "merge" or "INSERT ... ON CONFLICT") combines insert and update semantics. Rows that do not conflict with existing data are inserted; rows that conflict on specified key columns are updated. This is the most complex DML operation in mountainash-data.

The upsert implementation uses a staging table pattern. Incoming data is first written to a temporary staging table, then an INSERT ... ON CONFLICT SQL statement merges the staged data into the target table.

```python
# Simplified upsert flow
staging_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
upsert_sql = f"""
    INSERT INTO {target_table} ({all_cols})
    SELECT {all_cols} FROM {staging_table}
    WHERE true
    ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
"""
```

The operation accepts several control parameters:

- `conflict_columns`: Columns that define uniqueness (the "natural key").
- `update_columns`: Which columns to update on conflict (defaults to all non-key columns).
- `conflict_action`: Either `"UPDATE"` (merge) or `"NOTHING"` (skip conflicts).
- `update_condition`: Optional WHERE clause applied to the update.

!!! warning "Upsert memory considerations"
    The staging table approach loads all incoming data into a temporary table before merging. For very large datasets, this can consume significant memory. See Chapter 9 for a discussion of memory-intensive upsert patterns and alternatives.

<!-- concept:28 -->
## Truncate Table

The **truncate** operation removes all rows from a table without dropping the table structure itself. Unlike `DELETE FROM table` (which logs individual row deletions), truncate is a bulk operation that is typically much faster for clearing large tables.

```python
@classmethod
def truncate(cls, ibis_backend, table_name, /, database=None, schema=None):
    ibis_backend.truncate_table(table_name, schema=schema, database=database)
```

Truncate is commonly used in ETL pipelines that follow a "full refresh" pattern: clear the table, then reload all data from the source.

<!-- concept:29 -->
## Create View

The **create_view** operation creates a named view backed by an Ibis table expression. A view is a stored query that behaves like a table for read operations but does not store data independently. Views are useful for encapsulating complex query logic that multiple consumers need to share.

```python
@classmethod
def create_view(cls, ibis_backend, view_name, ibis_table_expr, /,
                database=None, schema=None, overwrite=False):
    ibis_backend.create_view(view_name, obj=ibis_table_expr,
                             database=database, overwrite=overwrite)
```

The `ibis_table_expr` parameter is the Ibis expression that defines the view's query. This expression is compiled to SQL and stored in the database. Subsequent queries against the view name execute the stored expression.

#### Diagram: DDL and DML Operations
<iframe src="../../sims/ddl-dml-operations/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>DDL and DML Operations</summary>
Type: chart
**sim-id:** ddl-dml-operations<br/>
**Library:** vis-network<br/>
**Status:** Specified

A two-column layout showing DDL operations (left, structural changes) and DML operations (right, data changes). DDL column shows create_table, drop_table, create_view, drop_view, create_index, drop_index as nodes connected to a central "Schema" node. DML column shows insert, upsert, truncate as nodes connected to a central "Data" node. Each operation node shows its key parameters on hover. The upsert node is larger to indicate its complexity, with a sub-flow showing the staging table pattern. Clicking an operation highlights its parameters and shows example SQL. Learning objective: Evaluate when to use each operation type (Bloom: Evaluate). Controls: click operation for details, hover for parameters. Colors: DarkSlateBlue for DDL, DarkGreen for DML, Gold for shared concepts.
</details>

<!-- concept:30 -->
## List Tables Ibis

The **List Tables Ibis** implementation delegates to the underlying Ibis connection's `list_tables()` method, passing the namespace as the `database` parameter. The Ibis library uses "database" terminology where mountainash-data uses "namespace" to refer to schema-level groupings.

```python
def list_tables(self, namespace: str | None = None) -> list[str]:
    try:
        if namespace is not None:
            return self._ibis_conn.list_tables(database=namespace)
        return self._ibis_conn.list_tables()
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []
```

Error handling returns an empty list rather than propagating exceptions, which allows exploration code to gracefully handle cases where a namespace does not exist or the connection has been interrupted.

<!-- concept:31 -->
## Table Inspection Ibis

**Table Inspection Ibis** implements the `inspect_table()` protocol method by loading the Ibis table reference and converting its schema to a `TableInfo` through the `table_to_info()` helper from `backends.ibis.inspect`.

The implementation first obtains the Ibis table object (which triggers schema introspection on the backend), then passes it to the conversion function along with the table name and namespace context.

```python
def inspect_table(self, name: str, namespace: str | None = None) -> TableInfo:
    from mountainash_data.backends.ibis.inspect import table_to_info
    ibis_table = self._ibis_conn.table(name, database=namespace)
    return table_to_info(ibis_table, name=name, namespace=namespace)
```

If the table does not exist or cannot be accessed, the method raises a `ValueError` with a descriptive message identifying the problematic table name.

<!-- concept:32 -->
## Namespace Inspection Ibis

**Namespace Inspection Ibis** builds a `NamespaceInfo` by combining the namespace name with a `list_tables()` call scoped to that namespace. This approach reuses the list_tables implementation rather than duplicating its logic.

```python
def inspect_namespace(self, name: str) -> NamespaceInfo:
    tables = self.list_tables(namespace=name)
    return NamespaceInfo(name=name, tables=tables)
```

<!-- concept:33 -->
## Catalog Inspection Ibis

**Catalog Inspection Ibis** constructs a complete `CatalogInfo` by iterating over all namespaces and building a `NamespaceInfo` for each one. The catalog name is taken from the dialect spec's `ibis_backend_name` field.

```python
def inspect_catalog(self) -> CatalogInfo:
    namespaces = self.list_namespaces()
    ns_infos = [
        NamespaceInfo(name=ns, tables=self.list_tables(namespace=ns))
        for ns in namespaces
    ]
    return CatalogInfo(name=self._dialect_spec.ibis_backend_name, namespaces=ns_infos)
```

For backends with many schemas, this operation issues one `list_tables` call per namespace and may be slow. Consumers performing repeated catalog inspections should cache the result.

<!-- concept:34 -->
## Context Manager Ibis

The **context manager** pattern in `IbisConnection` ensures that database resources are released even when exceptions occur. The implementation uses Python's `__enter__` and `__exit__` dunder methods to support `with` statement usage.

```python
def __enter__(self):
    return self

def __exit__(self, *args):
    self.close()
```

The context manager delegates to `close()`, which is idempotent. This means it is safe to call `close()` explicitly inside the `with` block (e.g., for early release) without causing errors when the block exits.

The recommended usage pattern combines Backend instantiation with a context-managed connection.

```python
backend = IbisBackend(dialect="duckdb", database="analytics.db")
with backend.connect() as conn:
    tables = conn.list_tables()
    info = conn.inspect_table("orders")
    # Connection is automatically closed when block exits
```

#### Diagram: Connection Lifecycle States
<iframe src="../../sims/connection-lifecycle-states/main.html" width="100%" height="400px" scrolling="no"></iframe>
<details markdown="1">
<summary>Connection Lifecycle States</summary>
Type: diagram
**sim-id:** connection-lifecycle-states<br/>
**Library:** vis-network<br/>
**Status:** Specified

A state machine diagram showing three states: "Created" (after IbisBackend instantiation), "Connected" (after connect() is called, transition labeled "connect()"), and "Closed" (after close() is called, transition labeled "close()"). The "Connected" state has a self-loop labeled "Operations (list, inspect, query)" showing that operations happen while in the connected state. An error edge from "Connected" to "Closed" shows that exceptions in __exit__ trigger close(). The "Closed" state has a self-loop labeled "close() (no-op)" showing idempotency. Clicking a state shows what methods are available. Learning objective: Understand connection lifecycle state transitions (Bloom: Understand). Controls: click states for available methods, hover transitions for trigger details. Colors: SteelBlue for Created, DarkGreen for Connected, MediumPurple for Closed.
</details>

## Key Takeaways

- **IbisBackend** is the primary backend factory, supporting 12+ SQL dialects through the DIALECTS registry and validating configuration eagerly at construction time.
- **IbisConnection** wraps a raw Ibis connection and adapts it to the `Connection` protocol, providing list, inspect, and lifecycle methods.
- The **fluent query API** enables portable, lazily-evaluated analytical queries that compile to backend-specific SQL at execution time.
- **Raw SQL queries** via `run_sql()` provide an escape hatch for operations not covered by the fluent API, returning `None` on failure for graceful error handling.
- **DDL operations** (create_table, create_view, create_index) modify database structure, while **DML operations** (insert, upsert, truncate) modify data within existing tables.
- The **upsert** operation uses a staging table pattern and supports configurable conflict resolution strategies.
- **Inspection methods** delegate to the Ibis connection's introspection capabilities and convert results through the `table_to_info()` helper.
- The **context manager** pattern provides automatic resource cleanup and is the recommended way to manage connection lifecycles.
