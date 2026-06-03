---
title: Backend Protocol
description: The runtime-checkable Backend protocol defining connect, close, list, and inspect methods as the universal backend contract
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Backend Protocol

## Summary

This chapter introduces the Backend protocol — a runtime-checkable structural typing contract that all mountainash-data backends must satisfy. It defines the required methods for connection lifecycle (connect, close), table and namespace listing, and catalog/schema/table inspection. Both IbisBackend and IcebergBackend implement this protocol, enabling polymorphic usage across all supported databases.

## Concepts Covered

- Backend Protocol Definition
- Connect Method
- Close Method
- List Tables Method
- Inspect Table Method
- List Namespaces Method
- Inspect Namespace Method
- Inspect Catalog Method

## Prerequisites

- Chapter 1: Foundation Concepts (Python Protocols, Runtime Checkable Protocol, Database Schemas, Database Catalogs, Connection Management)

---

## Backend Protocol Definition

The **Backend protocol** is the central architectural contract in mountainash-data. Defined in `mountainash_data.core.protocol`, it specifies the minimal interface that any backend implementation must provide. The protocol uses Python's structural typing system (PEP 544) rather than class inheritance, which means that conformance is determined by whether a class has the right methods with the right signatures, not by whether it appears in a specific class hierarchy.

The architecture separates concerns into two distinct protocols: `Backend` and `Connection`. A Backend is a factory object that holds configuration and can produce Connection instances. A Connection is a live handle to a database service that exposes introspection and lifecycle methods. This separation ensures that configuration (which is static) remains distinct from state (which is transient).

The actual protocol definitions in the codebase are concise. The `Backend` protocol requires only two things: a `name` attribute (a string identifying the backend type) and a `connect()` method that returns a `Connection`. The `Connection` protocol requires six methods: `list_namespaces()`, `list_tables()`, `inspect_table()`, `inspect_namespace()`, `inspect_catalog()`, and `close()`.

```python
@t.runtime_checkable
class Backend(t.Protocol):
    """A factory for Connections to a particular backend service."""
    name: str

    def connect(self) -> Connection:
        """Open a connection. Caller is responsible for closing it."""
        ...
```

Both protocols are decorated with `@runtime_checkable`, enabling runtime verification through `isinstance()` checks. This is particularly useful in routing logic where code must determine at runtime whether a given object satisfies the backend contract.

#### Diagram: Protocol Architecture
<iframe src="../../sims/protocol-architecture/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Protocol Architecture</summary>
Type: diagram
**sim-id:** protocol-architecture<br/>
**Library:** vis-network<br/>
**Status:** Specified

A UML-style class diagram showing the Backend and Connection protocols as interface boxes at the top, with IbisBackend, IbisConnection, IcebergBackend, and IcebergConnectionBase as implementing classes below. Dashed arrows labeled "satisfies" connect the implementations to the protocols. The Backend box shows the `name: str` attribute and `connect()` method. The Connection box shows all six required methods. Clicking a protocol highlights all its implementors; clicking an implementor shows which protocol methods it provides. Learning objective: Analyze the relationship between protocols and their implementations (Bloom: Analyze). Controls: click to highlight relationships, hover for method signatures. Colors: DarkSlateBlue for protocols, DarkGreen for Ibis classes, LimeGreen for Iceberg classes.
</details>

The following table contrasts the two protocols and their responsibilities.

| Protocol | Responsibility | Statefulness | Key Methods |
|---|---|---|---|
| `Backend` | Factory; holds configuration | Stateless (from consumer's perspective) | `connect()` |
| `Connection` | Live database handle; owns session state | Stateful; must be closed | `list_tables()`, `inspect_table()`, `close()`, etc. |

## Connect Method

The **connect method** is the single method required by the `Backend` protocol. It constructs and returns a `Connection` object representing a live session with the backing database or catalog service. The caller assumes ownership of the returned connection and is responsible for closing it when finished.

The method takes no arguments because all connection configuration is provided at Backend construction time. This design decision means that a Backend object fully encapsulates its connection requirements, making it safe to pass around as a value without leaking credentials or connection details.

In the `IbisBackend` implementation, `connect()` delegates to the dialect's `connection_builder` function, passing the stored configuration kwargs. The result is wrapped in an `IbisConnection` that adapts the raw Ibis connection object to the `Connection` protocol.

```python
class IbisBackend:
    name = "ibis"

    def __init__(self, dialect: str, **config: t.Any):
        self._spec: DialectSpec = DIALECTS[dialect]
        self._config = config

    def connect(self) -> IbisConnection:
        """Build and return a live ibis connection."""
        ibis_conn = self._spec.connection_builder(**self._config)
        return IbisConnection(ibis_conn, self._spec)
```

If the dialect's `connection_builder` is not configured (for backends that are registered but not yet implemented), `connect()` raises `NotImplementedError` with a clear message identifying the incomplete dialect.

## Close Method

The **close method** releases all resources held by a Connection. It is defined on the `Connection` protocol and must be idempotent: calling `close()` multiple times on the same connection must not raise an error.

The idempotency requirement is important for defensive programming patterns. Code that catches exceptions in a `finally` block should be able to call `close()` unconditionally without worrying about whether the connection was already closed due to an earlier error.

In the `IbisConnection` implementation, close is tracked by a `_closed` boolean flag. The first call attempts to disconnect the underlying Ibis backend; subsequent calls are no-ops.

```python
def close(self) -> None:
    """Release the connection. Idempotent."""
    if not self._closed:
        try:
            if hasattr(self._ibis_conn, "disconnect"):
                self._ibis_conn.disconnect()
        except Exception:
            pass
        finally:
            self._closed = True
```

The `IbisConnection` also implements `__enter__` and `__exit__` to support context manager usage. This means `close()` is called automatically at the end of a `with` block, which is the recommended usage pattern.

## List Tables Method

The **list_tables method** returns the names of all tables visible within a given namespace (schema). It accepts an optional `namespace` parameter; when omitted, it returns tables from the connection's default namespace.

The method signature on the `Connection` protocol is:

```python
def list_tables(self, namespace: str | None = None) -> list[str]:
    """Return the names of tables in the given namespace."""
    ...
```

This method is one of the most frequently used entry points for data exploration. A typical workflow begins with listing namespaces, then listing tables within a chosen namespace, and finally inspecting individual tables for their column structure.

The Ibis implementation delegates to the underlying Ibis connection object, passing the namespace as the `database` parameter (Ibis uses "database" where mountainash-data uses "namespace" for the schema-level grouping). Error handling wraps the call in a try/except block and returns an empty list on failure, ensuring that transient connection issues do not propagate as unhandled exceptions.

## Inspect Table Method

The **inspect_table method** returns detailed structural metadata for a single table. Rather than returning raw driver-specific metadata, it produces a `TableInfo` dataclass from the unified inspection model (covered in Chapter 3).

The method signature requires the table name and accepts an optional namespace qualifier:

```python
def inspect_table(
    self, name: str, namespace: str | None = None
) -> TableInfo:
    """Return shared-model metadata for one table."""
    ...
```

The returned `TableInfo` includes the table name, its list of `ColumnInfo` objects (each with name, type, and nullability), and optional catalog/namespace qualifiers. This provides enough information to understand a table's structure without needing to query its data.

In the Ibis implementation, the method retrieves the Ibis table reference, extracts its schema (column names and types), and converts each column into a `ColumnInfo` dataclass through the `table_to_info()` helper function.

#### Diagram: Inspection Flow
<iframe src="../../sims/inspection-flow/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Inspection Flow</summary>
Type: workflow
**sim-id:** inspection-flow<br/>
**Library:** vis-network<br/>
**Status:** Specified

A directed flow diagram showing the data transformation pipeline from raw driver metadata to the unified inspection model. Five nodes connected by arrows: (1) "Driver-Specific Schema" (raw Ibis or PyIceberg metadata), (2) "Conversion Helper" (table_to_info function), (3) "ColumnInfo" dataclass, (4) "TableInfo" dataclass, (5) "Consumer Code" that receives the standardized metadata. Each node shows the data shape at that stage. Clicking a node displays example data at that transformation step. Learning objective: Understand the metadata conversion pipeline (Bloom: Understand). Controls: click node to see example data, hover for descriptions. Colors: Orange for driver data, Gold for conversion, LimeGreen for unified model, SteelBlue for consumer.
</details>

## List Namespaces Method

The **list_namespaces method** returns the names of all namespaces (schemas or databases, depending on the engine) visible to the current connection. This is the entry point for hierarchical exploration of database structure.

```python
def list_namespaces(self) -> list[str]:
    """Return the names of all namespaces (schemas) visible to this connection."""
    ...
```

Different database engines expose namespace information through different mechanisms. Some provide a `list_databases()` method (DuckDB, MotherDuck), while others provide `list_schemas()` (PostgreSQL). The Ibis backend implementation checks for both methods and returns whichever is available, falling back to an empty list if neither exists.

This graceful degradation is important because some embedded databases (like SQLite) have no concept of multiple namespaces. For these backends, `list_namespaces()` returns an empty list, and all tables are accessed without a namespace qualifier.

## Inspect Namespace Method

The **inspect_namespace method** returns metadata about a single namespace, including the list of tables it contains. The method returns a `NamespaceInfo` dataclass.

```python
def inspect_namespace(self, name: str) -> NamespaceInfo:
    """Return shared-model metadata for one namespace."""
    ...
```

The Ibis implementation builds a `NamespaceInfo` by combining the namespace name with the result of `list_tables(namespace=name)`. This means that inspecting a namespace triggers a table listing operation internally.

The `NamespaceInfo` dataclass contains:

- `name`: The namespace identifier string.
- `tables`: A sequence of table name strings within this namespace.
- `catalog`: An optional catalog name that this namespace belongs to.
- `metadata`: An extensible mapping for backend-specific properties.

## Inspect Catalog Method

The **inspect_catalog method** provides a complete view of the connection's top-level catalog, including all namespaces and their tables. It returns a `CatalogInfo` dataclass that aggregates multiple `NamespaceInfo` objects.

```python
def inspect_catalog(self) -> CatalogInfo:
    """Return shared-model metadata for the connection's catalog."""
    ...
```

The Ibis implementation builds the catalog view by iterating over all namespaces, calling `list_tables()` for each one, and assembling the results into a hierarchical structure. The catalog name is derived from the dialect spec's `ibis_backend_name` field.

```python
def inspect_catalog(self) -> CatalogInfo:
    namespaces = self.list_namespaces()
    ns_infos = [
        NamespaceInfo(name=ns, tables=self.list_tables(namespace=ns))
        for ns in namespaces
    ]
    return CatalogInfo(
        name=self._dialect_spec.ibis_backend_name,
        namespaces=ns_infos,
    )
```

This method can be expensive for backends with many namespaces and tables, as it triggers a separate list_tables call for each namespace. Consumers should cache the result if they need to reference it multiple times.

#### Diagram: Protocol Methods Map
<iframe src="../../sims/protocol-methods-map/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Protocol Methods Map</summary>
Type: graph-model
**sim-id:** protocol-methods-map<br/>
**Library:** vis-network<br/>
**Status:** Specified

A directed graph showing the Connection protocol's six methods as nodes, with edges representing data flow dependencies between them. `inspect_catalog` depends on `list_namespaces` and `list_tables`. `inspect_namespace` depends on `list_tables`. `inspect_table` is independent. `close` is independent. Nodes are sized by how many other methods depend on them. Hovering a method node shows its signature, return type, and which other methods it calls internally. Clicking a node highlights its dependency chain. Learning objective: Evaluate the dependency relationships between protocol methods (Bloom: Evaluate). Controls: click to highlight dependency chain, hover for signature details. Colors: DarkSlateBlue for lifecycle methods (connect, close), Gold for listing methods, LimeGreen for inspection methods.
</details>

The complete set of protocol methods forms a coherent API surface where listing methods provide discovery, inspection methods provide detailed metadata, and lifecycle methods manage the connection state. The following summary table captures the full interface.

| Method | Category | Parameters | Returns | Purpose |
|---|---|---|---|---|
| `connect()` | Lifecycle (Backend) | None | `Connection` | Create a live connection |
| `close()` | Lifecycle (Connection) | None | `None` | Release connection resources |
| `list_namespaces()` | Discovery | None | `list[str]` | Enumerate available schemas |
| `list_tables()` | Discovery | `namespace: str \| None` | `list[str]` | Enumerate tables in a schema |
| `inspect_table()` | Inspection | `name: str`, `namespace: str \| None` | `TableInfo` | Detailed table metadata |
| `inspect_namespace()` | Inspection | `name: str` | `NamespaceInfo` | Namespace + table listing |
| `inspect_catalog()` | Inspection | None | `CatalogInfo` | Full catalog structure |

!!! tip "Design principle: Backend is configuration, Connection is state"
    Keeping configuration separate from connection state means you can safely serialize Backend objects, pass them across thread boundaries, or retry connection attempts without worrying about stale state. The Connection holds the mutable state and must be managed within a well-defined scope.

## Key Takeaways

- The **Backend protocol** is a structural typing contract requiring only a `name` attribute and a `connect()` method, making it trivial for new backends to conform.
- The **Connection protocol** defines six methods covering lifecycle (close), discovery (list_namespaces, list_tables), and inspection (inspect_table, inspect_namespace, inspect_catalog).
- Both protocols are `@runtime_checkable`, enabling dynamic dispatch via `isinstance()` checks without requiring inheritance.
- The **connect method** takes no arguments because all configuration is captured at Backend construction time, keeping the factory stateless.
- The **close method** is idempotent, supporting defensive cleanup patterns in `finally` blocks and context managers.
- **Inspection methods** return frozen dataclass instances from the unified inspection model, converting driver-specific metadata into a backend-agnostic format.
- The protocol architecture cleanly separates concerns: the Backend owns configuration, and the Connection owns session state.
