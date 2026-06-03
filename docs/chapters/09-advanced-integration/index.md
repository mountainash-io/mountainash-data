---
title: Advanced Integration
description: Iceberg operations, cross-backend queries, capability matrix, and known limitations
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Advanced Integration

## Summary

This chapter covers advanced topics that span both the Ibis and Iceberg backends. It begins with Iceberg-specific operations — listing tables, inspecting tables, namespaces, and catalogs through the IcebergBackend. Then it addresses cross-backend integration patterns: querying across Ibis and Iceberg backends, understanding the backend capability matrix, DDL index support variations, memory-intensive upsert considerations, REST catalog cursor pagination, and handling unimplemented operations gracefully.

## Concepts Covered

- Iceberg Operations
- List Tables Iceberg
- Table Inspection Iceberg
- Namespace Inspection Iceberg
- Catalog Inspection Iceberg
- Cross Backend Queries
- Backend Capability Matrix
- DDL Index Support
- Memory Intensive Upsert
- REST Catalog Cursors
- Unimplemented Operations

## Prerequisites

- Chapter 3: Inspection Model (Driver Metadata Conversion)
- Chapter 4: Ibis Backend (IbisBackend Class, DDL Operations, DML Operations, Upsert Data)
- Chapter 8: Iceberg Backend (IcebergBackend Class, REST Catalog Type)

---

## Iceberg Operations

**Iceberg operations** encompass the read-side inspection methods that the IcebergBackend provides through its connection objects. While the IcebergBackend satisfies the same Connection protocol as the Ibis backend, the underlying implementation differs significantly because Iceberg catalogs organize data around namespaces and table identifiers rather than SQL schemas and qualified table names.

The Iceberg connection delegates all inspection operations to the PyIceberg Catalog handle, converting native PyIceberg types into the unified inspection model dataclasses (CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo). This conversion step is critical because it allows consumer code to work with Iceberg metadata using the same interfaces used for SQL databases.

Iceberg operations also include mutations (create_table, drop_table, insert, upsert, truncate) that are delegated to a separate operations module. However, Iceberg does not support raw SQL queries; calling `run_sql()` on an Iceberg connection raises `NotImplementedError`. This is a fundamental architectural difference: Iceberg is a table format with catalog-level operations, not a query engine.

## List Tables Iceberg

The **List Tables Iceberg** implementation returns the names of tables within a specified namespace. Unlike the Ibis implementation (which delegates to the Ibis connection's `list_tables` method), the Iceberg implementation calls the PyIceberg catalog's table listing API.

```python
def list_tables(self, namespace=None):
    self.connect()
    return self._list_tables(namespace=namespace)
```

The `_list_tables()` hook is implemented by each concrete catalog type (e.g., `IcebergRestConnection`). The implementation calls `catalog.list_tables(namespace)` on the PyIceberg catalog handle and converts the results from PyIceberg's tuple-based identifiers to simple string names.

Iceberg namespaces are hierarchical in some catalog implementations, meaning a namespace like `"analytics"` might contain sub-namespaces. The `list_tables()` method returns only the tables at the specified namespace level, not tables in nested namespaces.

## Table Inspection Iceberg

**Table Inspection Iceberg** loads a table reference through the PyIceberg catalog and converts its schema into a `TableInfo` dataclass. The implementation uses the `table_to_info()` helper from `backends.iceberg.inspect`.

```python
def inspect_table(self, name, namespace=None):
    from mountainash_data.backends.iceberg.inspect import table_to_info
    identifier = (namespace, name) if namespace else name
    iceberg_table = self.table(identifier)
    if iceberg_table is None:
        raise ValueError(f"Table not found: {identifier!r}")
    catalog_name = getattr(self.catalog_backend, "name", None)
    return table_to_info(iceberg_table, name=name, namespace=namespace, catalog=catalog_name)
```

The conversion iterates over the PyIceberg schema's fields, extracting each field's name, type (as a string), and required/nullable status. The key difference from the Ibis conversion is the nullability convention: Iceberg uses `.required` (True means NOT nullable), which is inverted when creating `ColumnInfo(nullable=not field.required)`.

The `table()` method includes built-in retry logic with configurable `max_attempts` and `retry_delay` parameters. This addresses a common failure mode with REST catalogs where transient network issues can cause `load_table()` to return `None` on the first attempt.

#### Diagram: Iceberg Inspection Pipeline
<iframe src="../../sims/iceberg-inspection-pipeline/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Iceberg Inspection Pipeline</summary>
Type: workflow
**sim-id:** iceberg-inspection-pipeline<br/>
**Library:** vis-network<br/>
**Status:** Specified

A comparison workflow showing Ibis inspection (top path) and Iceberg inspection (bottom path) side by side, converging at the unified TableInfo output. The Ibis path shows: Ibis Table -> schema() -> names/types lists -> ColumnInfo(nullable=type.nullable). The Iceberg path shows: PyIceberg Table -> schema() -> NestedField list -> ColumnInfo(nullable=not field.required). Highlighted annotations call out the nullability inversion. Both paths converge to the same TableInfo dataclass. Clicking either path highlights the differences in detail. Learning objective: Compare Ibis and Iceberg inspection pipelines and identify key differences (Bloom: Analyze). Controls: click paths to highlight differences, hover nodes for data examples. Colors: DarkGreen for Ibis path, LimeGreen for Iceberg path, Gold for unified output.
</details>

## Namespace Inspection Iceberg

**Namespace Inspection Iceberg** builds a `NamespaceInfo` from the tables listed within a namespace and the catalog name. The implementation uses the `namespace_to_info()` helper function.

```python
def inspect_namespace(self, name):
    from mountainash_data.backends.iceberg.inspect import namespace_to_info
    table_names = self._list_tables(namespace=name)
    catalog_name = getattr(self.catalog_backend, "name", None)
    return namespace_to_info(name, table_names, catalog=catalog_name)
```

The helper function simply assembles the `NamespaceInfo` dataclass from the provided arguments. Unlike the Ibis implementation (which has no catalog context), the Iceberg implementation populates the `catalog` field because Iceberg namespaces always exist within a named catalog.

## Catalog Inspection Iceberg

**Catalog Inspection Iceberg** provides a complete hierarchical view of the Iceberg catalog by iterating over all namespaces and collecting their table lists. The implementation calls `catalog_backend.list_namespaces()` to discover all top-level namespaces, then builds a `NamespaceInfo` for each one.

```python
def inspect_catalog(self):
    self.connect()
    raw_namespaces = self.catalog_backend.list_namespaces()
    catalog_name = getattr(self.catalog_backend, "name", "iceberg")

    namespace_infos = []
    for ns in raw_namespaces:
        ns_name = ns[0] if isinstance(ns, (tuple, list)) else str(ns)
        try:
            table_names = self._list_tables(namespace=ns_name)
        except NotImplementedError:
            table_names = []
        namespace_infos.append(namespace_to_info(ns_name, table_names, catalog=catalog_name))

    return catalog_to_info(catalog_name, namespace_infos)
```

The implementation handles the fact that PyIceberg returns namespaces as tuples (e.g., `("analytics",)`) by extracting the first element. It also catches `NotImplementedError` from `_list_tables()` for catalog types that do not fully implement table listing, falling back to an empty table list.

## Cross Backend Queries

**Cross backend queries** refer to scenarios where data from an Ibis-backed SQL database and an Iceberg-backed data lake need to be combined in a single analytical workflow. mountainash-data does not provide a built-in cross-backend query engine; instead, it facilitates cross-backend workflows through the unified protocol interface.

The common patterns for cross-backend data integration are:

1. **Extract and Load**: Read data from one backend, materialize it as a dataframe, and write it to the other backend. This is the simplest approach but requires materializing the full dataset in memory.

2. **Metadata Comparison**: Use the unified inspection API to compare schemas across backends (e.g., validating that a SQL staging table matches the target Iceberg table's schema).

3. **Federated Query Engine**: Use a federated engine like Trino that can query both SQL databases and Iceberg catalogs, with mountainash-data handling connection management for each.

```python
# Pattern 1: Extract from PostgreSQL, load to Iceberg
ibis_backend = IbisBackend(dialect="postgres", host="db.example.com", ...)
ice_backend = IcebergBackend(catalog="rest", uri="http://catalog:8181")

with ibis_backend.connect() as sql_conn:
    # Read source data via Ibis
    source_table = sql_conn._ibis_conn.table("events")
    df = source_table.execute()

with ice_backend.connect() as ice_conn:
    # Write to Iceberg
    ice_conn.insert("analytics.events", df)
```

The unified protocol makes it possible to write generic functions that operate on any backend. A schema validation function, for example, can accept any Connection object and compare its TableInfo output regardless of the underlying backend type.

## Backend Capability Matrix

The **backend capability matrix** documents which operations each backend supports. Not all backends implement every operation: SQL databases support raw SQL queries but not Iceberg-style time travel, while Iceberg catalogs support schema evolution but not raw SQL.

| Capability | Ibis (SQL) | Iceberg |
|---|---|---|
| list_namespaces | Yes | Yes |
| list_tables | Yes | Yes |
| inspect_table | Yes | Yes |
| inspect_namespace | Yes | Yes |
| inspect_catalog | Yes | Yes |
| create_table | Yes | Yes |
| drop_table | Yes | Yes |
| insert | Yes | Yes |
| upsert | Partial (dialect-dependent) | Yes |
| truncate | Yes | Yes |
| create_view | Yes | Yes |
| drop_view | Yes | Yes |
| run_sql | Yes | No |
| create_index | Partial (DuckDB, SQLite) | No |
| time_travel | No | Yes (via PyIceberg) |
| schema_evolution | No (requires DDL) | Yes (native) |

The "Partial" entries deserve special attention. Upsert on the Ibis side is only implemented for backends that support `INSERT ... ON CONFLICT` syntax (DuckDB, SQLite, PostgreSQL). Trino raises `NotImplementedError` for upsert. Similarly, index management is only implemented for DuckDB/SQLite/MotherDuck, which provide the dialect-specific SQL hooks described in Chapter 5.

#### Diagram: Backend Capability Matrix
<iframe src="../../sims/backend-capability-matrix/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Backend Capability Matrix</summary>
Type: chart
**sim-id:** backend-capability-matrix<br/>
**Library:** Chart.js<br/>
**Status:** Specified

An interactive heatmap with operations on the Y axis and backend types on the X axis (Ibis-SQLite, Ibis-DuckDB, Ibis-PostgreSQL, Ibis-Snowflake, Ibis-Trino, Iceberg-REST). Cells are color-coded: green for fully supported, yellow for partial support, red for not supported. Clicking a cell shows implementation details and any caveats. A summary bar at the bottom shows the total number of supported operations per backend. Filtering controls allow showing only specific operation categories (lifecycle, inspection, DDL, DML, advanced). Learning objective: Evaluate which backend to choose based on required capabilities (Bloom: Evaluate). Controls: click cells for details, filter by operation category. Colors: DarkGreen for supported, Gold for partial, Crimson for unsupported.
</details>

## DDL Index Support

**DDL index support** varies significantly across database backends, and mountainash-data makes this variation explicit through the operation hooks system described in Chapter 5. Only backends that provide `get_index_exists_sql` and `get_list_indexes_sql` hooks support index management operations.

Currently, three dialect families provide index support:

- **DuckDB**: Uses the `duckdb_indexes()` system function for index introspection.
- **SQLite**: Uses the `sqlite_master` system table for index introspection.
- **MotherDuck**: Uses DuckDB's index functions (same as DuckDB).

The operations module provides a unified interface for index management (`create_index`, `drop_index`, `index_exists`, `list_indexes`) through the `_DuckDBFamilyOperationsMixin` class. This mixin generates standard `CREATE INDEX` and `DROP INDEX` SQL using dialect-specific SQL for introspection.

```python
# DuckDB index existence check
"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE index_name = 'idx_users_email'"

# SQLite index existence check
"SELECT COUNT(*) as count FROM sqlite_master WHERE type = 'index' AND name = 'idx_users_email'"
```

Backends that do not provide index hooks (PostgreSQL, Snowflake, BigQuery, Trino) raise `NotImplementedError` when index operations are attempted. Cloud data warehouses like Snowflake deliberately do not expose user-defined indexes, relying instead on automatic micro-partitioning and clustering for query optimization.

## Memory Intensive Upsert

The **memory-intensive upsert** limitation arises from the staging table approach used by the Ibis backend's upsert implementation. As described in Chapter 4, the upsert operation writes all incoming data to a temporary staging table, then executes an `INSERT ... ON CONFLICT` statement to merge the staged data into the target table.

For large datasets, this approach has two memory implications:

1. **Staging table size**: The entire incoming dataset must be materialized in the staging table, which consumes disk space (or memory for in-memory databases) proportional to the dataset size.
2. **Merge operation**: The `INSERT ... ON CONFLICT` statement processes all rows in the staging table, which requires holding both the staging and target table data in the query engine's working memory.

For DuckDB (which keeps data in memory by default), a large upsert can exhaust available RAM. For disk-based databases like PostgreSQL, the staging table consumes disk space but the merge operation is bounded by available buffer pool memory.

Strategies for mitigating memory-intensive upserts include:

- **Batch processing**: Split the incoming data into smaller chunks and upsert each chunk separately.
- **Pre-filtering**: Remove rows from the incoming data that have not changed (by comparing checksums or timestamps) before upserting.
- **Direct SQL**: For very large datasets, write custom SQL that avoids the staging table pattern entirely.

!!! warning "Upsert is not supported on all backends"
    The upsert operation requires `INSERT ... ON CONFLICT` syntax, which is not available on all backends. Trino, BigQuery, and MSSQL do not support this syntax natively. Attempting upsert on these backends raises `NotImplementedError`. Check the backend capability matrix before planning upsert-based ETL workflows.

## REST Catalog Cursors

**REST catalog cursors** are the pagination mechanism used by Iceberg REST catalogs to handle large result sets. When a namespace contains thousands of tables, the catalog service returns results in pages rather than as a single response, using cursor tokens to track pagination state.

PyIceberg's `RestCatalog` implementation handles cursor-based pagination transparently. When `list_tables()` or `list_namespaces()` is called, PyIceberg issues an initial request and follows pagination links automatically until all results have been collected. The consumer sees a complete list without needing to manage pagination manually.

However, cursor-based pagination introduces considerations for large catalogs:

- **Latency**: Listing tables in a namespace with thousands of entries requires multiple HTTP round-trips, each adding network latency.
- **Consistency**: If the catalog is modified between pagination requests, the result set may include or exclude tables that were created or dropped during iteration.
- **Rate limiting**: Some managed catalog services impose rate limits on API calls. Rapid pagination of large namespaces may trigger throttling.

The `inspect_catalog()` method is particularly sensitive to these concerns because it issues a `list_tables()` call for every namespace. For catalogs with many namespaces, this can result in a large number of paginated API calls.

## Unimplemented Operations

**Unimplemented operations** are methods that exist in the protocol or base class interface but are not yet implemented for a specific backend or dialect. mountainash-data handles unimplemented operations by raising `NotImplementedError` with a descriptive message that identifies both the operation and the backend.

The library follows a consistent pattern for marking operations as unimplemented:

```python
# In Trino operations class
@classmethod
def _upsert(cls, *args, **kwargs) -> None:
    raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: upsert not supported")

@classmethod
def create_index(cls, *args, **kwargs) -> bool:
    raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: create_index not supported")
```

This approach has two advantages. First, it provides clear feedback when a consumer attempts an unsupported operation, including which backend does not support it. Second, it preserves the uniform protocol interface: all backends expose the same methods, and consumers can catch `NotImplementedError` to implement fallback logic.

The recommended pattern for handling unimplemented operations in consumer code is:

```python
def safe_create_index(conn, table_name, columns):
    """Create an index if the backend supports it."""
    try:
        conn.create_index(table_name, columns)
        return True
    except NotImplementedError:
        # Backend does not support indexes; skip silently
        return False
```

#### Diagram: Operation Support Decision Flow
<iframe src="../../sims/operation-support-decision/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Operation Support Decision Flow</summary>
Type: workflow
**sim-id:** operation-support-decision<br/>
**Library:** vis-network<br/>
**Status:** Specified

A decision tree diagram guiding consumers through handling operation support across backends. The root node is "Attempt Operation". It branches to "Success" (operation completed) and "NotImplementedError" (operation unsupported). The NotImplementedError branch further branches to three strategies: "Fallback" (use an alternative approach), "Skip" (proceed without the operation), and "Error" (fail with a clear message). Each leaf node shows example code for the strategy. Clicking a strategy shows when it is appropriate. Learning objective: Apply knowledge of unimplemented operations to design robust cross-backend code (Bloom: Apply). Controls: click strategies for code examples, hover for guidance. Colors: DarkGreen for success, Gold for fallback, SteelBlue for skip, Crimson for error.
</details>

Consumer code that needs to work across multiple backends should query the backend capability matrix programmatically (by catching `NotImplementedError`) or consult the matrix documentation to plan around known limitations.

## Key Takeaways

- **Iceberg operations** (list, inspect) follow the same protocol as Ibis operations but delegate to PyIceberg's catalog API rather than SQL introspection.
- **Table Inspection Iceberg** inverts the nullability convention (Iceberg uses `required`, mountainash-data uses `nullable`) during conversion.
- **Cross backend queries** are facilitated through the unified protocol but require manual materialization; mountainash-data does not include a cross-backend query engine.
- The **backend capability matrix** documents which operations each backend supports, with key gaps in upsert (Trino, BigQuery) and index management (all cloud warehouses).
- **DDL index support** is limited to DuckDB, SQLite, and MotherDuck, which provide dialect-specific SQL hooks for index introspection.
- **Memory-intensive upsert** is a known limitation of the staging table approach; batch processing and pre-filtering are recommended mitigations.
- **REST catalog cursors** handle pagination transparently but introduce latency and consistency considerations for large catalogs.
- **Unimplemented operations** raise `NotImplementedError` with descriptive messages, and consumer code should use try/except patterns for cross-backend compatibility.
