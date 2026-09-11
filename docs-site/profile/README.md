# mountainash-data

**The gateway to every database and lakehouse -- one typed interface to connect, inspect, query, and write.**

## Vision

mountainash-data connects the mountainash expression engine to the world's databases. Every expression, rule evaluation, data contract, and conformance pipeline that compiles through Ibis gains access to whatever backend mountainash-data is connected to. The same logic runs on SQLite during development, DuckDB for analytics, Snowflake or BigQuery for enterprise scale, and Iceberg for the lakehouse.

IbisBackend provides a unified connection, inspection, and query interface across every SQL and analytical engine Ibis supports -- DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, MySQL, Oracle, SQL Server, Databricks, and more. When Ibis adds a new backend, mountainash-data inherits it. IcebergBackend connects to Apache Iceberg catalogs -- REST, Hive, Glue, SQL -- so the same expressions that validate and transform data can store and query it in a production lakehouse.

Each database dialect has typed settings classes with auto-derived connection parameters. Authentication spans the full spectrum -- password, token, OAuth2, IAM, service accounts, certificates, and more. Settings compose with mountainash-settings for config file loading and secrets resolution. The Backend protocol defines connect, close, inspect, and query operations, so any data store that implements this protocol participates in the mountainash ecosystem automatically.

## Installation

```bash
pip install mountainash-data
```

## Use Cases

### Development to Production Without Rewrites

A data engineer develops pipelines against DuckDB on their laptop. In staging, the same pipeline points at Snowflake. In production, BigQuery. The expressions compile through Ibis to each backend's native dialect. The backend is a deployment config, not a code change. Switching environments is a one-line change at the top of the script.

### The Lakehouse That Speaks Expressions

Rule tables, validated data, and flattened hierarchical data all land in Iceberg. The same mountainash expressions that validated the data can query it later. The lakehouse is not a separate world -- it is another backend the expression engine compiles to. Tables, metadata inspection, and operations all work through the Backend protocol.

### Multi-Warehouse Data Quality

A platform team runs the same data contracts against every warehouse in the organisation -- Snowflake for the analytics team, BigQuery for the ML team, PostgreSQL for the application team. One contract definition, one library version, every backend. Quality validation travels with the expressions, not with the connection code.

## Key Capabilities

### IbisBackend

IbisBackend is re-exported at the package root for convenient access. You can construct one from a typed settings object, a connection URL, or dialect-specific keyword arguments -- whichever fits your workflow best. It provides methods for reading tables, executing raw SQL, evaluating Ibis expressions, creating tables, inserting rows, and upserting data. The query interface lets you stay in Ibis expressions or drop to SQL when you need to.

### IcebergBackend

IcebergBackend connects to Apache Iceberg catalogs (REST, Hive, Glue, SQL) and exposes the same Backend interface, so your code works the same way regardless of the underlying catalog type. List namespaces and tables, inspect schemas, and read data -- all through the same interface you already know from IbisBackend.

### Typed Connection Settings

Each supported database has a dedicated typed settings class (e.g. PostgresAuthSettings, SnowflakeAuthSettings) that knows how to produce driver keyword arguments and connection strings. Settings classes surface all connection parameters as typed fields, making configuration discoverable and validated at construction time. Auth modes are shared from mountainash-auth-client and cover common patterns: NoAuth, PasswordAuth, OAuth2Auth, IAMAuth, and more.

### Metadata Inspection

The inspection API returns structured, frozen dataclasses -- CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo -- so you can explore catalogs, namespaces, tables, and columns programmatically. The same models are returned by every backend, giving you a consistent way to understand your data regardless of where it lives.

## Architecture

mountainash-data is built around a two-tier architecture. A runtime-checkable Backend protocol defines the structural contract that any data store can satisfy -- implement the methods and your object is a valid backend, no inheritance required. Below this protocol, data-driven dispatch layers route operations to the right driver: DialectSpec for Ibis SQL dialects (a frozen dataclass capturing connection builders and hook callables) and a catalog registry for Iceberg (mapping catalog type strings to connection subclasses).

A parallel settings registry maps typed configuration to connection parameters through adapter pipelines. BackendSpec descriptors register themselves via a `@register` decorator, and ConnectionProfile subclasses provide `to_driver_kwargs()` and `to_connection_string()` for driver-native output. When a settings class needs non-trivial credential transformation, it declares an adapter callable that handles backend-specific concerns like token exchange, SSL bundling, or cloud-provider credential resolution. Per-backend converter modules normalise driver-specific metadata into a shared frozen dataclass hierarchy, enabling consistent introspection regardless of the underlying data store.

## Extending

mountainash-data provides clear extension points for integrating new data stores. The Backend protocol is the primary integration contract -- implement its methods and your object is a valid backend. You can add new Ibis dialects by creating a DialectSpec entry with a connection builder, add Iceberg catalog types by subclassing IcebergConnectionBase, and register typed settings classes via the `@register` decorator. Existing implementations serve as complete, copy-and-adapt templates. Backend settings can be published as separate packages that self-register on import.

## Contributing

Contributing a new database dialect, Iceberg catalog type, or backend settings class follows clear registration patterns with existing implementations as templates. The project uses hatch for environment management with preconfigured environments for testing, linting (ruff), type checking (mypy), and complexity analysis. Each backend has a dedicated test file following a consistent structure, and shared fixtures with parametrised invariant tests ensure cross-backend consistency.

## Maintaining

The maintenance surface spans three layers: the Backend protocol and connection contracts, the registry-driven settings architecture with typed descriptors and adapters, and the two backend implementations. When upgrading an Ibis dialect after a library release, locate the DialectSpec, update the connection builder, adjust the adapter if auth parameters changed, and run the per-backend tests. The dialect isolation keeps the change contained.

Settings registration issues typically trace to import paths -- the self-registration pattern means the fix is usually ensuring the module import triggers the `@register` decorator. Iceberg operation debugging follows the delegation chain: IcebergBackend delegates to IcebergConnectionBase, which delegates to the operations module, letting you scope the fix to either catalog-specific connection handling or shared operation logic.

The core.registry module is a placeholder for a unified dispatch layer that will route to both IbisBackend and IcebergBackend. Iceberg operations have room to grow, and these are active areas where changes should be reviewed carefully against the existing per-backend test patterns.
