---
title: Iceberg Backend
description: IcebergBackend for Apache Iceberg catalogs with REST, Hive, Glue, and SQL catalog types via PyIceberg
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Iceberg Backend

## Summary

This chapter introduces the IcebergBackend — the second major backend type in mountainash-data, providing access to Apache Iceberg table format catalogs. It begins with an overview of Apache Iceberg and the PyIceberg library, then covers the IcebergBackend class and its connection model. The Catalog Type Registry and its four implementations (REST, Hive, Glue, SQL) demonstrate how different catalog backends are discovered and instantiated through the registry pattern.

## Concepts Covered

- Apache Iceberg Overview
- PyIceberg Library
- IcebergBackend Class
- Iceberg Connection Base
- Catalog Type Registry
- REST Catalog Type
- Hive Catalog Type
- Glue Catalog Type
- SQL Catalog Type

## Prerequisites

- Chapter 1: Foundation Concepts (Database Catalogs, Connection Management, Registry Pattern)
- Chapter 2: Backend Protocol (Backend Protocol Definition)

---

## Apache Iceberg Overview

**Apache Iceberg** is an open table format designed for large-scale analytical datasets. Unlike traditional SQL databases that tightly couple storage, metadata, and compute in a single engine, Iceberg separates these concerns. The table format defines how data files (typically Parquet or ORC) are organized in object storage (S3, GCS, ADLS), while a separate catalog service manages table metadata, including schema evolution, partition layouts, and snapshot history.

This separation enables several capabilities that traditional databases cannot easily provide:

- **Engine independence**: Multiple compute engines (Spark, Trino, Flink, Dremio) can read and write the same Iceberg tables concurrently.
- **Schema evolution**: Columns can be added, renamed, or reordered without rewriting existing data files.
- **Time travel**: Every write operation creates a new snapshot, allowing queries against historical versions of a table.
- **Hidden partitioning**: Partition layouts can change without breaking queries or requiring data reorganization.

In the mountainash-data architecture, Iceberg represents the "lakehouse" half of the library's dual nature. Where the Ibis backend connects to traditional SQL databases, the Iceberg backend connects to data lake catalog services that manage Iceberg table format data.

#### Diagram: Iceberg Architecture Layers
<iframe src="../../sims/iceberg-architecture-layers/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Iceberg Architecture Layers</summary>
Type: infographic
**sim-id:** iceberg-architecture-layers<br/>
**Library:** vis-network<br/>
**Status:** Specified

A layered architecture diagram showing four Iceberg layers from top to bottom: (1) Catalog Layer (REST, Hive, Glue, SQL catalogs), (2) Metadata Layer (manifest lists, manifest files, table metadata JSON), (3) Data Layer (Parquet files in object storage), (4) Storage Layer (S3, GCS, ADLS, local filesystem). Each layer is a horizontal band with representative nodes. Vertical arrows show the navigation path from catalog lookup through metadata resolution to data file access. Clicking a layer expands it to show more detail about its components. A side panel shows how mountainash-data's IcebergBackend interacts with only the catalog layer. Learning objective: Understand how Iceberg separates catalog, metadata, and data layers (Bloom: Understand). Controls: click layers to expand, hover for component descriptions. Colors: SteelBlue for catalog, Gold for metadata, DarkGreen for data, Teal for storage.
</details>

## PyIceberg Library

The **PyIceberg library** is the official Python implementation for interacting with Apache Iceberg tables. It provides Python-native APIs for catalog operations (create, load, drop tables), schema manipulation, and data read/write operations. mountainash-data uses PyIceberg as the driver for its Iceberg backend, analogous to how it uses Ibis for SQL databases.

PyIceberg provides several key abstractions that mountainash-data builds upon:

- **Catalog**: The entry point for all table management operations. Different catalog implementations (RestCatalog, HiveCatalog, GlueCatalog, SqlCatalog) provide the same interface backed by different metadata stores.
- **Table**: A reference to an Iceberg table that provides access to its schema, snapshots, partitioning, and data.
- **Schema**: Defines the column structure of a table, including nested types and field metadata.
- **PartitionSpec**: Defines how data is partitioned within the table's storage layout.

The relationship between mountainash-data and PyIceberg mirrors the Ibis relationship. mountainash-data adds a protocol-conformant wrapper, a unified inspection model, and a settings/configuration system on top of PyIceberg's raw capabilities.

```python
# Direct PyIceberg usage (without mountainash-data)
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(name="warehouse", uri="http://localhost:8181")
table = catalog.load_table("analytics.events")
schema = table.schema()

# Via mountainash-data's unified protocol
backend = IcebergBackend(catalog="rest", uri="http://localhost:8181")
conn = backend.connect()
info = conn.inspect_table("events", namespace="analytics")
```

## IcebergBackend Class

The **IcebergBackend class** is the Iceberg counterpart to `IbisBackend`. It implements the `Backend` protocol by providing a `name` attribute (the string `"iceberg"`) and a `connect()` method that returns a Connection-conformant object.

The class follows the same factory pattern as IbisBackend: it accepts a catalog type and configuration at construction time, validates the catalog type against a registry, and produces connections on demand.

```python
class IcebergBackend:
    name = "iceberg"

    def __init__(self, catalog: str, **config: t.Any) -> None:
        if catalog not in _CATALOG_REGISTRY:
            raise KeyError(
                f"Unknown iceberg catalog type {catalog!r}. "
                f"Available: {sorted(_CATALOG_REGISTRY)}"
            )
        self._catalog_cls = _CATALOG_REGISTRY[catalog]
        self._config = config

    def connect(self) -> Connection:
        return self._catalog_cls(**self._config)
```

Like IbisBackend, IcebergBackend performs eager validation of the catalog type at construction time, failing fast with a descriptive error if the requested catalog type is not registered. The error message includes the list of available catalog types to guide users toward valid options.

The key architectural difference from IbisBackend is that IcebergBackend's `connect()` instantiates a connection class directly (passing config as constructor arguments), whereas IbisBackend invokes a connection builder function. This difference reflects the fact that Iceberg connections carry more stateful initialization logic (catalog discovery, namespace caching) that is better encapsulated in a class constructor.

## Iceberg Connection Base

The **IcebergConnectionBase** is an abstract base class that all Iceberg catalog connection implementations extend. Unlike the Ibis side (where `IbisConnection` is a single concrete class), the Iceberg side uses inheritance because different catalog types require significantly different initialization, authentication, and lifecycle management.

`IcebergConnectionBase` provides:

- **Lifecycle management**: `connect()`, `disconnect()`, `close()`, and `is_connected()` methods that manage the PyIceberg Catalog handle.
- **Schema caching**: A `_schema_cache` dictionary that avoids redundant schema introspection calls.
- **Table loading with retry**: A `table()` method with built-in retry logic for transient catalog failures.
- **Inspection methods**: Implementations of `list_namespaces()`, `list_tables()`, `inspect_table()`, `inspect_namespace()`, and `inspect_catalog()` that satisfy the Connection protocol.
- **Mutation delegation**: Thin wrappers around operations module functions for create_table, drop_table, insert, upsert, and truncate.

The class maintains a lazy connection model. The `catalog_backend` property (implemented by subclasses) returns the live PyIceberg Catalog handle, or `None` if not yet connected. Most methods call `self.connect()` as their first action, ensuring the catalog is initialized before any operation is attempted.

```python
class IcebergConnectionBase(BaseDBConnection):
    def __init__(self, db_auth_settings_parameters):
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)
        self._schema_cache: dict = {}

    def connect(self, connection_string=None, connection_kwargs=None, **kwargs):
        if self.catalog_backend is None:
            self.connect_default(**kwargs)
        return self.catalog_backend

    def table(self, table_name, max_attempts=3, retry_delay=0.5):
        """Load a table reference with built-in retry logic."""
        self.connect()
        for attempt in range(max_attempts):
            table_ref = self.catalog_backend.load_table(table_name)
            if table_ref is not None:
                return table_ref
            sleep(retry_delay)
        return None
```

The retry logic in `table()` handles transient failures that are common with network-based catalogs. REST catalogs may experience brief unavailability during deployments or network partitions, and automatic retries prevent these transient issues from surfacing as errors to consumer code.

#### Diagram: IcebergConnectionBase Methods
<iframe src="../../sims/iceberg-connection-methods/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>IcebergConnectionBase Methods</summary>
Type: graph-model
**sim-id:** iceberg-connection-methods<br/>
**Library:** vis-network<br/>
**Status:** Specified

A method dependency graph for IcebergConnectionBase. Central node is the class name. Surrounding nodes are grouped by category: Lifecycle (connect, disconnect, close, is_connected), Inspection (list_namespaces, list_tables, inspect_table, inspect_namespace, inspect_catalog), Mutations (create_table, drop_table, insert, upsert, truncate), and Utilities (table, get_schema, clear_schema_cache). Edges show internal call dependencies (e.g., inspect_catalog calls list_namespaces and list_tables). Node size reflects method complexity. Clicking a method shows its signature and which other methods it depends on. Learning objective: Analyze the internal method dependencies of IcebergConnectionBase (Bloom: Analyze). Controls: click methods for signatures, hover for descriptions. Colors: SteelBlue for lifecycle, Gold for inspection, DarkGreen for mutations, Teal for utilities.
</details>

## Catalog Type Registry

The **Catalog Type Registry** is a module-level dictionary (`_CATALOG_REGISTRY`) that maps catalog type names to their connection class implementations. This is a direct application of the registry pattern described in Chapter 1, scoped to the Iceberg backend.

```python
_CATALOG_REGISTRY: dict[str, type[IcebergConnectionBase]] = {
    "rest": IcebergRestConnection,
}
```

The registry currently contains one implemented catalog type (`"rest"`), with Hive, Glue, and SQL catalog types defined as planned extensions. The architecture supports adding new catalog types by implementing an `IcebergConnectionBase` subclass and adding it to the registry.

The registry lookup is performed in `IcebergBackend.__init__()`, which stores the resolved class for later instantiation in `connect()`. This two-phase approach (resolve at init, instantiate at connect) allows validation to fail early while deferring the expensive network connection until actually needed.

## REST Catalog Type

The **REST Catalog type** implements the Iceberg REST Catalog API specification. REST catalogs communicate with a remote catalog service over HTTP, making them the most common deployment model for production Iceberg installations. Services like AWS Athena, Tabular, and Polaris implement the REST Catalog specification.

The `IcebergRestConnection` class extends `IcebergConnectionBase` and provides the concrete `catalog_backend` implementation using PyIceberg's `RestCatalog` class.

```python
class IcebergRestConnection(IcebergConnectionBase):
    @property
    def catalog_backend(self):
        return self._catalog_backend

    def connect_default(self, **kwargs):
        if self.catalog_backend is None:
            connection_kwargs = self.get_connection_kwargs()
            self._catalog_backend = RestCatalog(**connection_kwargs)
        return self.catalog_backend
```

REST catalog connections require at minimum a `uri` parameter pointing to the catalog service endpoint. Authentication varies by service provider: some require OAuth tokens, others use API keys, and some support anonymous access for read-only operations.

The REST catalog supports pagination for large namespaces (tables listed in pages via cursor-based pagination), which is handled transparently by PyIceberg's `RestCatalog` implementation.

## Hive Catalog Type

The **Hive Catalog type** uses Apache Hive Metastore as the metadata backend. The Hive Metastore was the original catalog implementation for Iceberg tables in Hadoop-based data lakes. It stores table metadata in a relational database (typically MySQL or PostgreSQL) and exposes it via the Thrift protocol.

Hive catalog connections require the Thrift URI of the Hive Metastore service. The connection model is similar to REST but uses Thrift RPC rather than HTTP/REST for communication.

Key characteristics of the Hive catalog type:

- Commonly deployed in existing Hadoop/Spark environments.
- Requires a running Hive Metastore service.
- Supports the full range of Iceberg operations (create, drop, rename, alter).
- May have higher latency than REST catalogs due to Thrift serialization overhead.

## Glue Catalog Type

The **Glue Catalog type** uses AWS Glue Data Catalog as the metadata backend. AWS Glue is a fully managed service that eliminates the need to run a separate catalog server. Tables registered in Glue are automatically available to AWS services like Athena, EMR, and Redshift Spectrum.

Glue catalog connections authenticate through AWS IAM credentials (access key + secret key, or IAM role assumption). The catalog does not require a URI because it is accessed through the AWS SDK.

Key characteristics of the Glue catalog type:

- Fully managed by AWS; no infrastructure to operate.
- Integrates natively with AWS analytics services.
- Requires AWS credentials and appropriate IAM permissions.
- Subject to AWS Glue API rate limits for high-throughput operations.

## SQL Catalog Type

The **SQL Catalog type** stores Iceberg table metadata directly in a SQL database (typically SQLite for testing or PostgreSQL for production). This catalog type is useful for environments that do not have access to a dedicated catalog service but need Iceberg's table format features.

The SQL catalog stores table metadata (schema, snapshots, manifest lists) as rows in SQL tables. This makes it self-contained and easy to deploy but limits scalability compared to purpose-built catalog services.

Key characteristics of the SQL catalog type:

- Self-contained; no external service dependencies.
- Useful for development, testing, and small-scale deployments.
- Backed by any JDBC-compatible database.
- Limited concurrent write support (depends on the backing SQL database's isolation level).

The following table compares all four catalog types across key dimensions.

| Catalog Type | Protocol | Managed | Use Case | Scalability |
|---|---|---|---|---|
| REST | HTTP/REST | Varies (can be managed) | Production; multi-engine | High |
| Hive | Thrift RPC | Self-hosted | Hadoop/Spark environments | Medium |
| Glue | AWS SDK | Fully managed (AWS) | AWS-native analytics | High (with rate limits) |
| SQL | JDBC/SQL | Self-hosted | Development; small scale | Low-Medium |

#### Diagram: Catalog Types Comparison
<iframe src="../../sims/catalog-types-comparison/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Catalog Types Comparison</summary>
Type: chart
**sim-id:** catalog-types-comparison<br/>
**Library:** Chart.js<br/>
**Status:** Specified

A radar chart comparing the four catalog types across five dimensions: Scalability, Ease of Setup, Cloud Integration, Multi-Engine Support, and Operational Complexity (inverted so lower is better). Each catalog type is a colored polygon on the radar. A legend identifies each type. Clicking a polygon highlights it and displays a detail panel with deployment requirements and best-fit scenarios. A toggle button switches between radar view and grouped bar chart view for accessibility. Learning objective: Evaluate which catalog type best fits a given deployment scenario (Bloom: Evaluate). Controls: click polygons for details, toggle chart type. Colors: Crimson for REST, DarkGreen for Hive, Orange for Glue, SteelBlue for SQL.
</details>

!!! tip "Start with REST for production"
    Unless you have an existing Hive Metastore or are locked into AWS Glue, the REST catalog type is the recommended choice for new deployments. It provides the best balance of scalability, engine independence, and operational simplicity. The SQL catalog type is excellent for local development and testing.

## Key Takeaways

- **Apache Iceberg** is an open table format that separates storage, metadata, and compute, enabling engine independence, schema evolution, and time travel.
- **PyIceberg** is the Python library that mountainash-data uses to interact with Iceberg catalogs, providing catalog, table, and schema abstractions.
- **IcebergBackend** is a protocol-conformant factory that validates catalog type at construction and produces connections via its registry.
- **IcebergConnectionBase** provides lifecycle management, schema caching, retry logic, and protocol-conformant inspection methods that all catalog types inherit.
- The **Catalog Type Registry** maps catalog type names to connection classes, following the same registry pattern used throughout mountainash-data.
- **REST catalogs** communicate over HTTP and are the most common production deployment model for Iceberg.
- **Hive**, **Glue**, and **SQL** catalog types serve different deployment contexts: Hadoop environments, AWS-native analytics, and lightweight/development scenarios respectively.
- The retry logic in `table()` provides resilience against transient catalog service failures without requiring consumer code to implement retry patterns.
