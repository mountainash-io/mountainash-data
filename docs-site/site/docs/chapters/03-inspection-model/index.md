---
title: Inspection Model
description: Frozen Pydantic metadata dataclasses for backend-agnostic catalog, namespace, table, and column introspection
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Inspection Model

## Summary

This chapter covers the four frozen metadata dataclasses that provide a backend-agnostic view of database structure: CatalogInfo, NamespaceInfo, TableInfo, and ColumnInfo. It explains the frozen metadata model pattern, how driver-specific metadata is converted into these unified structures, and the unified inspection API that ties everything together. These dataclasses are central to the Backend protocol's inspect methods.

---

## The Problem of Heterogeneous Metadata

Every database engine represents table structure differently. Ibis exposes schemas as `Schema` objects with `.names` and `.types` attributes. PyIceberg uses a `Schema` containing `NestedField` objects with `.name`, `.field_type`, and `.required` attributes. PostgreSQL's information_schema returns rows from system tables. Without a normalization layer, consumer code would need to understand the internals of each driver to perform basic metadata operations.

mountainash-data solves this problem with a set of four frozen dataclasses that serve as the common language for structural metadata. Regardless of whether the metadata originated from a DuckDB schema introspection or an Iceberg catalog scan, it arrives at the consumer in the same shape. This chapter introduces those dataclasses from the bottom up, starting with the smallest unit (a column) and building toward the full catalog view.

<!-- concept:52 -->
## ColumnInfo Dataclass

The **ColumnInfo dataclass** represents the physical metadata for a single column within a table. It captures the minimal information needed to understand a column's structure: its name, its data type (as a string representation), and whether it accepts null values.

```python
@dataclass(frozen=True)
class ColumnInfo:
    """Physical metadata for a single column."""
    name: str
    type_name: str
    nullable: bool
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
```

The `type_name` field stores the data type as a string rather than a typed enum or type object. This design choice accommodates the wide variety of type systems across database engines. A DuckDB `VARCHAR` column, a Snowflake `STRING` column, and an Iceberg `string` field all have different native type representations, but each is stored as a readable string in `ColumnInfo`.

The optional `description` field captures column-level documentation when available (some databases support column comments). The `metadata` mapping provides an escape hatch for backend-specific properties that do not fit the standard fields, such as Iceberg field IDs or Snowflake column policies.

#### Diagram: ColumnInfo Structure
<iframe src="../../sims/columninfo-structure/main.html" width="100%" height="400px" scrolling="no"></iframe>
<details markdown="1">
<summary>ColumnInfo Structure</summary>
Type: diagram
**sim-id:** columninfo-structure<br/>
**Library:** vis-network<br/>
**Status:** Specified

An interactive card-style visualization showing a ColumnInfo instance as a structured card with labeled fields. The card displays name="user_id", type_name="int64", nullable=False, description=None, metadata={}. Below the card, three example source formats are shown (Ibis Schema entry, PyIceberg NestedField, raw SQL column definition) with animated arrows flowing into the ColumnInfo card to show convergence. Clicking a source format highlights the field mapping. Learning objective: Remember the fields of the ColumnInfo dataclass (Bloom: Remember). Controls: click source format to see mapping, hover fields for type info. Colors: Gold for ColumnInfo card, DarkGreen for Ibis source, LimeGreen for Iceberg source, SteelBlue for SQL source.
</details>

<!-- concept:51 -->
## TableInfo Dataclass

The **TableInfo dataclass** aggregates column metadata into a complete description of a single table or view. It is the primary return type of the `inspect_table()` protocol method.

```python
@dataclass(frozen=True)
class TableInfo:
    """Physical metadata for a single table or view."""
    name: str
    columns: t.Sequence[ColumnInfo]
    namespace: t.Optional[str] = None
    catalog: t.Optional[str] = None
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
```

`TableInfo` provides two computed properties that simplify common operations. The `column_names` property returns a flat list of column name strings, useful for quick schema comparisons. The `qualified_name` property constructs the fully qualified table identifier by joining the non-None parts of catalog, namespace, and name with dots.

```python
@property
def column_names(self) -> list[str]:
    return [c.name for c in self.columns]

@property
def qualified_name(self) -> str:
    parts = [p for p in (self.catalog, self.namespace, self.name) if p]
    return ".".join(parts)
```

These properties mean that a `TableInfo` for a table named `orders` in the `public` schema of the `analytics` catalog would produce `qualified_name = "analytics.public.orders"`. For backends without catalogs (like SQLite), only the table name appears.

<!-- concept:50 -->
## NamespaceInfo Dataclass

The **NamespaceInfo dataclass** represents a namespace (schema) and the tables it contains. It sits one level above `TableInfo` in the structural hierarchy.

```python
@dataclass(frozen=True)
class NamespaceInfo:
    """Physical metadata for a namespace (schema/database/dataset)."""
    name: str
    tables: t.Sequence[str]
    catalog: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
```

Note that `tables` is a sequence of table name strings, not a sequence of `TableInfo` objects. This is a deliberate design choice that keeps the namespace inspection lightweight. Fetching full table metadata for every table in a namespace could be expensive for schemas containing hundreds of tables. Consumers who need detailed column information for specific tables can call `inspect_table()` individually.

The following list shows what information each inspection level provides:

- **list_namespaces()**: Just namespace names (cheapest).
- **inspect_namespace()**: Namespace name + table names within it.
- **inspect_table()**: Full column structure for one table (most detailed).
- **inspect_catalog()**: All namespaces with their table name lists.

<!-- concept:49 -->
## CatalogInfo Dataclass

The **CatalogInfo dataclass** is the top-level container that represents an entire catalog (or backend instance). It aggregates multiple `NamespaceInfo` objects into a single hierarchical view.

```python
@dataclass(frozen=True)
class CatalogInfo:
    """Physical metadata for a top-level catalog or backend instance."""
    name: str
    namespaces: t.Sequence[NamespaceInfo]
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
```

A `CatalogInfo` produced by `inspect_catalog()` gives a complete structural map of everything accessible through a connection. For example, a Snowflake connection might produce a `CatalogInfo` with name `"snowflake"`, containing `NamespaceInfo` entries for schemas like `"raw"`, `"staging"`, and `"analytics"`, each listing the tables within that schema.

The hierarchical relationship between all four dataclasses can be visualized as a containment tree.

| Level | Dataclass | Contains |
|---|---|---|
| Catalog | `CatalogInfo` | Multiple `NamespaceInfo` |
| Namespace | `NamespaceInfo` | Table name strings |
| Table | `TableInfo` | Multiple `ColumnInfo` |
| Column | `ColumnInfo` | Scalar field values |

<!-- concept:53 -->
## Frozen Metadata Model

All four inspection dataclasses use `@dataclass(frozen=True)`, which makes their instances **immutable** after construction. Attempting to assign a new value to any field raises a `FrozenInstanceError`. This design choice has three important benefits for the mountainash-data architecture.

First, immutability provides **safety guarantees**. When a `TableInfo` is returned from an inspection method, the consumer can be confident that no other code will modify it. This is particularly important in concurrent contexts where multiple threads might inspect the same table simultaneously.

Second, frozen dataclasses are **hashable by default** (provided all fields are hashable), which means they can be used as dictionary keys or stored in sets. This enables efficient caching of metadata.

Third, the frozen pattern communicates **intent**: inspection results are snapshots of the database state at a point in time. They are not live references that update automatically when the underlying schema changes. If the database schema changes, a new inspection call must be made to obtain fresh metadata.

```python
# Frozen dataclasses cannot be mutated
info = ColumnInfo(name="id", type_name="int64", nullable=False)
info.name = "new_name"  # Raises FrozenInstanceError
```

#### Diagram: Metadata Model Hierarchy
<iframe src="../../sims/metadata-model-hierarchy/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Metadata Model Hierarchy</summary>
Type: diagram
**sim-id:** metadata-model-hierarchy<br/>
**Library:** vis-network<br/>
**Status:** Specified

A nested containment diagram showing CatalogInfo at the top containing NamespaceInfo boxes, which contain TableInfo boxes, which contain ColumnInfo rows. The diagram uses a tree-map or Russian-doll style layout. Each level is color-coded and shows the actual field names from the dataclass. Users can click to expand/collapse levels, showing or hiding the contained elements. An ice crystal icon on each box indicates the frozen (immutable) property. Learning objective: Understand the hierarchical containment of the inspection model (Bloom: Understand). Controls: click to expand/collapse levels, hover for field type information. Colors: SteelBlue for CatalogInfo, Teal for NamespaceInfo, Gold for TableInfo, Orange for ColumnInfo.
</details>

<!-- concept:54 -->
## Backend Agnostic Metadata

The concept of **backend-agnostic metadata** is the principle that inspection results must not expose any driver-specific details in their standard fields. A consumer reading a `TableInfo` should not need to know whether it was produced by an Ibis connection to PostgreSQL or a PyIceberg connection to a REST catalog. The structural information (table name, column names and types, nullability) should be directly comparable across backends.

This principle drives several design decisions in the inspection model:

- **Type names as strings**: Rather than using backend-specific type objects, all column types are represented as readable strings. An Ibis `Int64` type becomes the string `"int64"`, and a PyIceberg `IntegerType` becomes the string `"int"`.
- **Optional qualifiers**: The `namespace` and `catalog` fields on `TableInfo` are optional because not all backends have these concepts. SQLite tables have neither; PostgreSQL tables have a namespace but typically a single implicit catalog.
- **Extensible metadata mappings**: Backend-specific information that does not fit the standard fields can be stored in the `metadata` dictionary without polluting the standard interface.

The following comparison illustrates how two different backends produce equivalent `TableInfo` for the same logical table.

```python
# From Ibis (PostgreSQL)
TableInfo(
    name="users",
    columns=[
        ColumnInfo(name="id", type_name="int64", nullable=False),
        ColumnInfo(name="email", type_name="string", nullable=True),
    ],
    namespace="public",
    catalog=None,
)

# From Iceberg (REST catalog)
TableInfo(
    name="users",
    columns=[
        ColumnInfo(name="id", type_name="long", nullable=False),
        ColumnInfo(name="email", type_name="string", nullable=True),
    ],
    namespace="default",
    catalog="warehouse",
)
```

!!! note "Type name normalization is not performed"
    mountainash-data does not normalize type names across backends. PostgreSQL's `"int64"` and Iceberg's `"long"` refer to the same logical type but retain their backend-specific string representations. This preserves fidelity to the source system while still providing a structurally consistent interface.

<!-- concept:55 -->
## Driver Metadata Conversion

**Driver metadata conversion** is the process of transforming raw driver-specific schema objects into the unified inspection dataclasses. Each backend implements its own conversion helper module (`backends.ibis.inspect` and `backends.iceberg.inspect`) that contains functions named `table_to_info`, `namespace_to_info`, and `catalog_to_info`.

The Ibis conversion helper works with Ibis's `Schema` object, which provides parallel lists of column names and types. The conversion iterates over these lists and constructs a `ColumnInfo` for each pair.

```python
def table_to_info(ibis_table, *, name, namespace=None, catalog=None):
    """Convert an ibis Table object into a TableInfo."""
    schema = ibis_table.schema()
    columns = [
        ColumnInfo(
            name=col_name,
            type_name=str(col_type),
            nullable=col_type.nullable,
        )
        for col_name, col_type in zip(schema.names, schema.types)
    ]
    return TableInfo(name=name, columns=columns, namespace=namespace, catalog=catalog)
```

The Iceberg conversion helper works with PyIceberg's `Schema` object, which provides a list of `NestedField` objects. Each field has `.name`, `.field_type`, and `.required` attributes. Note that Iceberg uses `.required` (True means NOT nullable), which is the inverse of the `nullable` field in `ColumnInfo`.

```python
def table_to_info(iceberg_table, *, name, namespace=None, catalog=None):
    """Convert a pyiceberg Table object into a TableInfo."""
    columns = [
        ColumnInfo(
            name=field.name,
            type_name=str(field.field_type),
            nullable=not field.required,  # Iceberg uses inverse convention
        )
        for field in iceberg_table.schema().fields
    ]
    return TableInfo(name=name, columns=columns, namespace=namespace, catalog=catalog)
```

#### Diagram: Conversion Pipeline
<iframe src="../../sims/conversion-pipeline/main.html" width="100%" height="450px" scrolling="no"></iframe>
<details markdown="1">
<summary>Conversion Pipeline</summary>
Type: workflow
**sim-id:** conversion-pipeline<br/>
**Library:** vis-network<br/>
**Status:** Specified

A side-by-side comparison workflow showing the Ibis conversion path (left) and Iceberg conversion path (right) converging into the same TableInfo output (center bottom). Each path shows three stages: (1) raw driver object with example attributes, (2) conversion helper function call, (3) unified TableInfo result. Animated arrows trace the data flow. A toggle control switches between showing the Ibis path, the Iceberg path, or both simultaneously for comparison. Learning objective: Apply knowledge of both conversion paths to predict outputs (Bloom: Apply). Controls: toggle between Ibis/Iceberg/Both views, click stages for detail. Colors: DarkGreen for Ibis path, LimeGreen for Iceberg path, Gold for unified output.
</details>

<!-- concept:56 -->
## Unified Inspection API

The **unified inspection API** is the collection of protocol methods (`inspect_table`, `inspect_namespace`, `inspect_catalog`) combined with the shared dataclass model that together provide a single, consistent way to explore database structure across all backends. The "unified" qualifier emphasizes that the same method calls and return types work identically whether the connection is to a local SQLite file, a remote PostgreSQL server, or an Iceberg REST catalog.

The API supports three levels of inspection granularity, each representing a trade-off between breadth and depth.

| Level | Method | Returns | Cost |
|---|---|---|---|
| Broad + Shallow | `inspect_catalog()` | All namespaces and table names | One call per namespace |
| Medium | `inspect_namespace(name)` | Table names in one namespace | Single list_tables call |
| Narrow + Deep | `inspect_table(name)` | Full column-level metadata | Schema introspection |

Consumer code typically starts broad and narrows down. The following example demonstrates the common exploration pattern.

```python
# Explore a database structure top-down
with backend.connect() as conn:
    # Level 1: What namespaces exist?
    catalog = conn.inspect_catalog()
    for ns in catalog.namespaces:
        print(f"Namespace: {ns.name} ({len(ns.tables)} tables)")

    # Level 2: What tables are in 'public'?
    public = conn.inspect_namespace("public")
    print(f"Tables: {public.tables}")

    # Level 3: What columns does 'users' have?
    users = conn.inspect_table("users", namespace="public")
    for col in users.columns:
        print(f"  {col.name}: {col.type_name} (nullable={col.nullable})")
```

The unified API makes it possible to write generic tooling (schema diffing, documentation generators, migration validators) that works across all mountainash-data backends without conditional logic for each database engine.

## Key Takeaways

- The inspection model consists of four frozen dataclasses: **ColumnInfo**, **TableInfo**, **NamespaceInfo**, and **CatalogInfo**, arranged in a hierarchical containment structure.
- All dataclasses use `@dataclass(frozen=True)` to ensure immutability, providing safety guarantees and communicating that inspection results are point-in-time snapshots.
- **Backend-agnostic metadata** means inspection results use the same field names and structure regardless of the source backend, enabling generic tooling.
- **Driver metadata conversion** is handled by per-backend helper modules that transform raw Ibis schemas or PyIceberg fields into the unified dataclass format.
- The `metadata` mapping on each dataclass provides an extensibility mechanism for backend-specific properties without polluting the standard interface.
- The **unified inspection API** supports three granularity levels (catalog, namespace, table), allowing consumers to trade breadth for depth based on their needs.
- Type names are stored as strings rather than typed objects, accommodating the wide variety of type systems across database engines without imposing a universal type taxonomy.
