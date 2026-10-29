# Graph reconciliation — 2026-10-29 (book replacement)

Existing graph: baseline `4c07766`, 100 concepts. Proposed: 139 concepts from the confirmed editorial brief and profile at `2d0782b`. `ibook graph reconcile` ran against a reviewed copy of the existing graph with the identity decisions below applied explicitly. It reported no conflicts.

## Removed concepts (subject retired; provenance retained here)

| ID | Label | Former source module | Former chapter |
|---|---|---|---|
| 35 | IcebergBackend Class | `mountainash_data.backends.iceberg.backend` | 08-iceberg-backend |
| 36 | Apache Iceberg Overview | `None` | 08-iceberg-backend |
| 37 | Iceberg Connection Base | `mountainash_data.backends.iceberg.connection` | 08-iceberg-backend |
| 38 | REST Catalog Type | `None` | 08-iceberg-backend |
| 39 | Hive Catalog Type | `None` | 08-iceberg-backend |
| 40 | Glue Catalog Type | `None` | 08-iceberg-backend |
| 41 | SQL Catalog Type | `None` | 08-iceberg-backend |
| 42 | Catalog Type Registry | `mountainash_data.core.settings` | 08-iceberg-backend |
| 43 | Iceberg Operations | `mountainash_data.backends.ibis.operations` | 09-advanced-integration |
| 44 | List Tables Iceberg | `None` | 09-advanced-integration |
| 45 | Table Inspection Iceberg | `mountainash_data.backends.ibis.inspect` | 09-advanced-integration |
| 46 | Namespace Inspection Iceberg | `mountainash_data.core.registry` | 09-advanced-integration |
| 47 | Catalog Inspection Iceberg | `mountainash_data.backends.ibis.inspect` | 09-advanced-integration |
| 48 | PyIceberg Library | `None` | 08-iceberg-backend |
| 95 | Cross Backend Queries | `mountainash_data.core.protocol` | 09-advanced-integration |
| 98 | Memory Intensive Upsert | `mountainash_data.backends.iceberg.operations` | 09-advanced-integration |
| 99 | REST Catalog Cursors | `None` | 09-advanced-integration |

34 dependency edges touching removed concepts were dropped with them.

## Renamed concepts (same meaning, current API name)

| ID | From | To |
|---|---|---|
| 67 | ConnectionProfile Base | BackendProfile Class |
| 68 | To Driver Kwargs Method | build_driver_kwargs Function |
| 72 | DATABASES REGISTRY | DATABASES_REGISTRY |
| 75 | SQLiteAuthSettings | SQLiteBackendProfile |
| 76 | DuckDBAuthSettings | DuckDBBackendProfile |
| 77 | PostgreSQLAuthSettings | PostgreSQLBackendProfile |
| 78 | BigQueryAuthSettings | BigQueryBackendProfile |
| 79 | SnowflakeAuthSettings | SnowflakeBackendProfile |
| 80 | RedshiftAuthSettings | RedshiftBackendProfile |
| 81 | ClickhouseAuthSettings | ClickHouseBackendProfile |
| 82 | DatabricksAuthSettings | DatabricksBackendProfile |
| 83 | MSSQLAuthSettings | MSSQLBackendProfile |
| 84 | TrinoAuthSettings | TrinoBackendProfile |
| 91 | NoAuth Settings | NoAuthProfile |
| 92 | PasswordAuth Settings | PasswordAuthProfile |
| 93 | TokenAuth Settings | TokenAuthProfile |
| 94 | IAMAuth Settings | IAMAuthProfile |

## Source remapping

Every node was mapped to a current profile module by review rather than by the label-matching heuristic (`mapping_basis: reviewed-2026-10-29`). Foundation concepts and process concepts have no module. Changed mappings:

| ID | Label | From | To |
|---|---|---|---|
| 3 | Database Catalogs | `mountainash_data.backends.iceberg.catalogs` | `None` |
| 4 | Connection Management | `mountainash_data.backends.iceberg.connection` | `None` |
| 5 | Python Protocols | `mountainash_data.core.protocol` | `None` |
| 6 | Runtime Checkable Protocol | `mountainash_data.core.protocol` | `None` |
| 9 | Registry Pattern | `mountainash_data.core.settings` | `None` |
| 12 | Connect Method | `None` | `mountainash_data.core.protocol` |
| 13 | Close Method | `None` | `mountainash_data.core.protocol` |
| 14 | List Tables Method | `None` | `mountainash_data.core.protocol` |
| 15 | Inspect Table Method | `mountainash_data.backends.ibis.inspect` | `mountainash_data.core.protocol` |
| 16 | List Namespaces Method | `mountainash_data.core.registry` | `mountainash_data.core.protocol` |
| 17 | Inspect Namespace Method | `mountainash_data.core.registry` | `mountainash_data.core.protocol` |
| 18 | Inspect Catalog Method | `mountainash_data.backends.ibis.inspect` | `mountainash_data.core.protocol` |
| 21 | Fluent Query API | `None` | `mountainash_data.backends.ibis.backend` |
| 22 | Raw SQL Queries | `None` | `mountainash_data.backends.ibis.backend` |
| 23 | DDL Operations | `mountainash_data.backends.ibis.operations` | `mountainash_data.backends.ibis.backend` |
| 24 | Create Table | `mountainash_data.backends.iceberg.operations` | `mountainash_data.backends.ibis.backend` |
| 25 | DML Operations | `mountainash_data.backends.ibis.operations` | `mountainash_data.backends.ibis.backend` |
| 26 | Insert Data | `mountainash_data.backends.iceberg.operations` | `mountainash_data.backends.ibis.backend` |
| 27 | Upsert Data | `mountainash_data.backends.iceberg.operations` | `mountainash_data.backends.ibis.backend` |
| 28 | Truncate Table | `mountainash_data.backends.iceberg.operations` | `mountainash_data.backends.ibis.backend` |
| 29 | Create View | `mountainash_data.backends.iceberg.operations` | `mountainash_data.backends.ibis.backend` |
| 30 | List Tables Ibis | `None` | `mountainash_data.backends.ibis.backend` |
| 31 | Table Inspection Ibis | `mountainash_data.backends.ibis.inspect` | `mountainash_data.backends.ibis.backend` |
| 32 | Namespace Inspection Ibis | `mountainash_data.core.registry` | `mountainash_data.backends.ibis.backend` |
| 33 | Catalog Inspection Ibis | `mountainash_data.backends.ibis.inspect` | `mountainash_data.backends.ibis.backend` |
| 34 | Context Manager Ibis | `None` | `mountainash_data.backends.ibis.backend` |
| 53 | Frozen Metadata Model | `None` | `mountainash_data.core.inspection` |
| 54 | Backend Agnostic Metadata | `mountainash_data.core.protocol` | `mountainash_data.core.inspection` |
| 55 | Driver Metadata Conversion | `mountainash_data.init` | `mountainash_data.backends.ibis.inspect` |
| 56 | Unified Inspection API | `mountainash_data.backends.ibis.inspect` | `mountainash_data.core.inspection` |
| 58 | Dialect Registry | `mountainash_data.core.settings` | `mountainash_data.backends.ibis.dialects` |
| 59 | Dialect Name Key | `None` | `mountainash_data.backends.ibis.dialects` |
| 60 | Connection Builder | `mountainash_data.backends.iceberg.connection` | `mountainash_data.backends.ibis.dialects` |
| 61 | Operation Hooks | `None` | `mountainash_data.backends.ibis.operations` |
| 62 | Per Dialect Configuration | `None` | `mountainash_data.backends.ibis.dialects` |
| 63 | SQLite Dialect | `None` | `mountainash_data.backends.ibis.dialects` |
| 64 | DuckDB Dialect | `None` | `mountainash_data.backends.ibis.dialects` |
| 65 | PostgreSQL Dialect | `None` | `mountainash_data.backends.ibis.dialects` |
| 66 | Snowflake Dialect | `None` | `mountainash_data.backends.ibis.dialects` |
| 67 | BackendProfile Class | `mountainash_data.core.settings` | `mountainash_data.core.settings.profile` |
| 68 | build_driver_kwargs Function | `None` | `mountainash_data.core.factories` |
| 69 | BackendSpec Class | `mountainash_data.core.protocol` | `mountainash_data.core.settings.descriptor` |
| 71 | Parameter Tiers | `None` | `mountainash_data.core.settings.descriptor` |
| 72 | DATABASES_REGISTRY | `mountainash_data.core.settings` | `mountainash_data.core.settings.registry` |
| 73 | Register Decorator | `mountainash_data.core.registry` | `mountainash_data.core.settings.registry` |
| 74 | Auto Registration | `None` | `mountainash_data.core.settings.registry` |
| 85 | Adapter Pipeline | `None` | `mountainash_data.core.settings.adapters` |
| 86 | Credential Transformation | `None` | `mountainash_data.core.settings.adapters` |
| 87 | OAuth Adapter | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings.adapters` |
| 88 | JWT Adapter | `None` | `mountainash_data.core.settings.adapters` |
| 89 | Cloud Native Auth | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings.adapters` |
| 90 | SSL Bundle Adapter | `None` | `mountainash_data.core.settings.adapters` |
| 91 | NoAuthProfile | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings` |
| 92 | PasswordAuthProfile | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings` |
| 93 | TokenAuthProfile | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings` |
| 94 | IAMAuthProfile | `mountainash_data.core.settings.auth` | `mountainash_data.core.settings` |
| 96 | Backend Capability Matrix | `mountainash_data.core.protocol` | `mountainash_data.backends.ibis.dialects` |
| 97 | DDL Index Support | `None` | `mountainash_data.backends.ibis.dialects` |
| 100 | Unimplemented Operations | `mountainash_data.backends.ibis.operations` | `mountainash_data.backends.ibis.dialects` |

## Chapter fields

Surviving concepts keep their old `chapter` values; new concepts have none. `book-chapter-generator` remaps all of them after chapter-plan approval.

## Metadata

The licence changed to Apache-2.0 (confirmed brief). Version 2.0. `source_commit` is `2d0782b`, with `previous_source_commit` `4c07766`. `profile_dir` is `docs-site/profile`.
