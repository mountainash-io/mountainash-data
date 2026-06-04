---
title: Foundation Concepts
description: Core prerequisites including SQL databases, schemas, catalogs, connection management, Python protocols, Pydantic models, decorators, and the registry pattern
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Foundation Concepts

## Summary

This chapter establishes the foundational knowledge required for understanding mountainash-data. It covers SQL database fundamentals, schema and catalog organization, connection management patterns, Python's protocol system for structural typing, Pydantic models for data validation, decorators for metaprogramming, the registry pattern for extensible discovery, and the Ibis library that powers the SQL backend.

## Concepts Covered

- SQL Databases
- Database Schemas
- Database Catalogs
- Connection Management
- Python Protocols
- Runtime Checkable Protocol
- Pydantic Models
- Decorators
- Registry Pattern
- Ibis Library

## Prerequisites

- Intermediate Python programming experience
- Basic familiarity with relational databases

---

<!-- concept:1 -->
## SQL Databases

A **SQL database** is a structured data store that organizes information into tables composed of rows and columns. Each table represents an entity type, and SQL (Structured Query Language) provides the standard interface for creating, reading, updating, and deleting data within these tables. mountainash-data treats SQL databases as the primary storage tier, wrapping connections to over 20 database engines through a unified Python interface.

The library supports a wide range of SQL database engines, from lightweight embedded databases like SQLite to enterprise-scale cloud data warehouses such as Snowflake and BigQuery. Despite their differences in scale, concurrency models, and query optimizers, all SQL databases share a common structural hierarchy that mountainash-data relies on to provide consistent behavior.

The following table summarizes the database engines that mountainash-data supports through its Ibis backend, grouped by deployment model.

| Deployment Model | Database Engines | Key Characteristics |
|---|---|---|
| Embedded / Local | SQLite, DuckDB | Single-file or in-memory; no server required |
| Self-Hosted Server | PostgreSQL, MySQL, MSSQL, Oracle | Client-server architecture; network connections |
| Cloud Data Warehouse | Snowflake, BigQuery, Redshift, Databricks | Managed infrastructure; elastic compute |
| Federated / Query Engine | Trino, PySpark | Query across heterogeneous data sources |
| Hybrid | MotherDuck | Cloud-hosted DuckDB with local compute |

<!-- concept:2 -->
## Database Schemas

Within a SQL database, a **database schema** (sometimes called a namespace) provides a logical grouping mechanism for related tables, views, and other database objects. Schemas serve a purpose analogous to directories in a filesystem: they organize objects by function, ownership, or access control without affecting the underlying storage. A PostgreSQL database, for example, creates a default schema named `public` where tables reside unless explicitly placed elsewhere.

mountainash-data uses schema awareness throughout its inspection layer. When the library lists tables or inspects metadata, it preserves schema context so that consumers can distinguish between identically named tables in different schemas. The three-part naming convention used throughout the library follows the pattern `catalog.schema.table`, where each level provides progressively finer granularity.

#### Diagram: Database Hierarchy
<iframe src="../../sims/database-hierarchy/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Database Hierarchy</summary>
Type: diagram
**sim-id:** database-hierarchy<br/>
**Library:** vis-network<br/>
**Status:** Specified

A hierarchical tree diagram showing the three-level organization of database objects. The root node represents a Catalog (e.g., "analytics_warehouse"), which branches into multiple Schema nodes (e.g., "public", "staging", "raw"). Each Schema node branches into Table nodes (e.g., "users", "orders", "events"). Nodes are color-coded by level: catalogs in SteelBlue, schemas in Gold, tables in LimeGreen. Clicking a node highlights its children and displays a tooltip with level description. Users can drag nodes to rearrange the layout. Learning objective: Understand the hierarchical organization of database objects (Bloom: Understand). Controls: click to select node, drag to rearrange, hover for tooltip. Colors: SteelBlue for catalogs, Gold for schemas, LimeGreen for tables.
</details>

<!-- concept:3 -->
## Database Catalogs

A **database catalog** is the top level of the structural hierarchy, acting as a container for one or more schemas. In single-database systems like SQLite, the catalog is implicit (there is only one). In multi-database platforms such as Snowflake, Trino, or Databricks, catalogs enable cross-database queries and unified metadata browsing. mountainash-data captures this three-tier structure through its `CatalogInfo`, `NamespaceInfo`, and `TableInfo` dataclasses, which are introduced fully in Chapter 3.

Not every database engine exposes catalogs in the same way. Some engines treat the database name itself as the catalog (PostgreSQL), while others maintain a distinct catalog layer above the database (Trino). The mountainash-data inspection layer normalizes these differences, mapping engine-specific terminology into the consistent `catalog > namespace > table` hierarchy.

<!-- concept:4 -->
## Connection Management

**Connection management** refers to the lifecycle of a database connection: establishing a connection with the appropriate credentials, maintaining its state while operations are performed, and releasing resources when the connection is no longer needed. Poor connection management causes resource leaks, stale connections, and concurrency issues that can degrade application performance.

mountainash-data models connection lifecycle through two protocol-level abstractions that you will encounter in Chapter 2.

- A **Backend** is a factory that accepts configuration and produces connections. It is stateless from the consumer's perspective.
- A **Connection** is a live, owned handle to a backend service. It exposes introspection and lifecycle methods, and the caller is responsible for closing it.

The pattern is familiar to anyone who has used Python's `sqlite3` module or SQLAlchemy's engine/session split. The following pseudocode illustrates the pattern.

```python
# Backend is the factory; Connection is the live handle
backend = IbisBackend(dialect="sqlite", database=":memory:")
conn = backend.connect()
try:
    tables = conn.list_tables()
finally:
    conn.close()
```

mountainash-data connections also support Python's context manager protocol, allowing you to use `with` statements for automatic cleanup. This is the recommended approach because it guarantees that `close()` is called even when exceptions occur.

```python
backend = IbisBackend(dialect="sqlite", database=":memory:")
with backend.connect() as conn:
    tables = conn.list_tables()
# conn.close() is called automatically
```

<!-- concept:5 -->
## Python Protocols

A **Python protocol** is a mechanism for structural typing introduced in PEP 544 and available in the `typing` module. Unlike abstract base classes, which require explicit inheritance, protocols define an interface purely in terms of method signatures and attributes. Any class that happens to have the right methods satisfies the protocol, regardless of whether it inherits from a common base class. This approach is sometimes called "duck typing with type checking."

mountainash-data uses protocols as the foundation of its backend system. The `Backend` and `Connection` protocols define the contract that all backends must satisfy, but neither `IbisBackend` nor `IcebergBackend` inherits from them. Instead, conformance is verified structurally by type checkers and at runtime through the `@runtime_checkable` decorator.

The key advantage of protocols over inheritance for a library like mountainash-data is that new backends can be added without modifying any existing code. A third-party backend only needs to implement the correct method signatures; it never needs to import or subclass anything from mountainash-data's core module.

<!-- concept:6 -->
## Runtime Checkable Protocol

The `@runtime_checkable` decorator, applied to a protocol class, enables `isinstance()` and `issubclass()` checks at runtime. Without this decorator, protocols are purely a static analysis concept and cannot be used for dynamic dispatch.

In mountainash-data, both the `Backend` and `Connection` protocols are decorated with `@runtime_checkable`. This allows code to verify at runtime that a given object conforms to the protocol contract.

```python
import typing as t

@t.runtime_checkable
class Backend(t.Protocol):
    name: str

    def connect(self) -> "Connection":
        ...
```

The runtime check verifies that the object has the required methods and attributes, but it does not verify argument types or return types. Full type safety requires a static type checker such as mypy or pyright.

!!! note "Runtime checks are structural, not behavioral"
    A `runtime_checkable` protocol confirms that an object has the right *shape* (methods and attributes exist), but it cannot verify that those methods behave correctly. Think of it as checking that a key fits the lock, not that it opens the right door.

<!-- concept:7 -->
## Pydantic Models

**Pydantic** is a Python library for data validation and settings management that uses type annotations to define data structures. A Pydantic model is a class that inherits from `BaseModel` (or, in mountainash-data's case, from a custom `MountainAshBaseSettings` base) and declares typed fields. When an instance is created, Pydantic validates each field against its declared type and applies any custom validators.

mountainash-data uses Pydantic models in two distinct areas of its architecture.

- **Inspection dataclasses**: The `CatalogInfo`, `NamespaceInfo`, `TableInfo`, and `ColumnInfo` dataclasses use Python's `@dataclass(frozen=True)` decorator rather than Pydantic's `BaseModel`, but they follow the same principles of typed, validated data structures. They are frozen (immutable) to ensure metadata snapshots remain consistent.
- **Settings classes**: Authentication and connection settings for each database engine are Pydantic models that validate credentials, connection parameters, and configuration at construction time. For example, `SnowflakeAuthSettings` validates that the account identifier matches the expected format and that the correct credentials are provided for the chosen authentication method.

The following list highlights the key features that Pydantic provides to mountainash-data's configuration system:

- **Type coercion**: Automatically converts string port numbers to integers.
- **Field validators**: Per-field validation logic using `@field_validator`.
- **Model validators**: Cross-field validation using `@model_validator` (e.g., ensuring that password auth includes both username and password).
- **Secret types**: `SecretStr` prevents accidental logging of passwords and tokens.
- **Default values**: Sensible defaults reduce boilerplate for common configurations.

<!-- concept:8 -->
## Decorators

A **decorator** in Python is a callable that wraps or modifies another callable (function, method, or class) without changing its source code. Decorators use the `@` syntax and are evaluated at definition time, making them a powerful tool for metaprogramming patterns like logging, caching, access control, and registration.

mountainash-data uses decorators in two important patterns. First, `@t.runtime_checkable` is applied to protocol classes to enable runtime `isinstance()` checks, as described above. Second, the `@register` decorator (covered in detail in Chapter 6) is the mechanism through which new backend settings classes announce their presence to the global `DATABASES` registry at import time.

The registration decorator pattern works as follows. When a module defining a new settings class is imported, the decorator fires immediately and adds the class to a central dictionary. This means that backend discovery is automatic: simply importing a module is sufficient to make its backend available.

```python
# Simplified illustration of the register decorator pattern
DATABASES = {}

def register(name):
    def decorator(cls):
        DATABASES[name] = cls
        return cls
    return decorator

@register("sqlite")
class SQLiteAuthSettings:
    ...
```

<!-- concept:9 -->
## Registry Pattern

The **registry pattern** is a design pattern where a central dictionary (the registry) maps string keys to factories or class references. Consumer code looks up entries by name rather than importing specific classes, which decouples the consumer from the concrete implementations and makes the system extensible without modifying existing code.

mountainash-data uses the registry pattern in three places, each serving a different purpose.

| Registry | Module | Purpose |
|---|---|---|
| `DIALECTS` | `backends.ibis.dialects._registry` | Maps dialect names (e.g., `"sqlite"`, `"postgres"`) to `DialectSpec` objects containing connection builders and capability hooks |
| `_CATALOG_REGISTRY` | `backends.iceberg.backend` | Maps catalog types (e.g., `"rest"`) to Iceberg connection classes |
| `_REGISTRY` | `core.registry` | A top-level backend registry that maps backend names to factory callables |

The benefit of this approach is that adding a new database backend requires only two steps: implementing the backend class and registering it in the appropriate registry. No existing code needs to change, and the new backend is immediately discoverable through the registry's lookup interface.

#### Diagram: Registry Pattern Flow
<iframe src="../../sims/registry-pattern-flow/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Registry Pattern Flow</summary>
Type: workflow
**sim-id:** registry-pattern-flow<br/>
**Library:** vis-network<br/>
**Status:** Specified

An animated workflow diagram showing three swim lanes: Registration (left), Registry (center), and Lookup (right). On the left, decorator calls flow into the central registry dictionary. On the right, consumer code sends a lookup key to the registry and receives back a factory/class reference. Clicking each step highlights the data flow with animated edges. The registry node in the center shows the current state of registered entries. Learning objective: Analyze how the registry pattern decouples registration from lookup (Bloom: Analyze). Controls: click step to animate flow, hover nodes for description. Colors: DarkSlateBlue for registration, Gold for registry, LimeGreen for lookup.
</details>

<!-- concept:10 -->
## Ibis Library

**Ibis** is an open-source Python library that provides a portable, pandas-like API for writing analytical queries that execute on remote SQL backends. Rather than writing raw SQL strings, you compose queries using Ibis's fluent expression API, and Ibis compiles those expressions into the appropriate SQL dialect for the connected backend. This means the same Python code can run against SQLite during local development and against Snowflake in production.

mountainash-data uses Ibis as the execution engine for its primary backend (`IbisBackend`). The Ibis library handles the complexity of connecting to different database engines, translating expressions into backend-specific SQL, and retrieving results. mountainash-data's role is to layer on top of Ibis a unified protocol interface, a metadata inspection model, and a configuration system that standardizes connection management across all supported backends.

Key capabilities that Ibis provides to mountainash-data include:

- **Multi-backend support**: Over 20 SQL backends through a consistent API.
- **Deferred execution**: Expressions are built lazily and executed only when results are needed.
- **SQL compilation**: Ibis can compile expressions to SQL strings for debugging.
- **Schema introspection**: Ibis exposes table schemas that mountainash-data converts into its own metadata model.

#### Diagram: Ibis Architecture Layer
<iframe src="../../sims/ibis-architecture-layer/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Ibis Architecture Layer</summary>
Type: infographic
**sim-id:** ibis-architecture-layer<br/>
**Library:** vis-network<br/>
**Status:** Specified

A layered architecture diagram showing four horizontal tiers. The top tier is "Application Code" with mountainash-data as the entry point. The second tier is "mountainash-data" showing the Backend protocol, inspection model, and settings system as side-by-side boxes. The third tier is "Ibis Library" showing the fluent API, SQL compiler, and backend adapters. The bottom tier shows individual database engines (SQLite, DuckDB, PostgreSQL, Snowflake, BigQuery, etc.) as colored nodes. Vertical arrows connect the layers. Clicking a database node highlights the full path from application code through Ibis to that engine. Learning objective: Understand how mountainash-data layers on top of Ibis (Bloom: Understand). Controls: click database to highlight path, hover for layer descriptions. Colors: SteelBlue for mountainash-data, DarkGreen for Ibis, Gold for databases.
</details>

The combination of Ibis's execution capabilities with mountainash-data's protocol-driven architecture means that consumers write backend-agnostic code that works across the entire range of supported databases, with type-safe configuration and consistent metadata inspection at every level.

## Key Takeaways

- **SQL databases** are the primary storage tier in mountainash-data, with support for 20+ engines spanning embedded, server-based, and cloud data warehouses.
- **Database schemas** and **catalogs** form a three-level hierarchy (catalog > schema > table) that mountainash-data normalizes across all backends.
- **Connection management** follows a factory pattern: Backends produce Connections, and Connections own the lifecycle of database handles.
- **Python protocols** provide structural typing, allowing backends to conform to a contract without inheritance, making the system extensible.
- **Runtime checkable protocols** enable `isinstance()` checks at runtime, supporting dynamic dispatch in addition to static type checking.
- **Pydantic models** power the configuration and validation layer, ensuring credentials and connection parameters are correct before a connection attempt.
- **Decorators** and the **registry pattern** work together to provide automatic discovery of new backends at import time, with no modifications to existing code.
- **Ibis** is the SQL execution engine that translates fluent Python expressions into backend-specific SQL, and mountainash-data layers a unified protocol on top.
