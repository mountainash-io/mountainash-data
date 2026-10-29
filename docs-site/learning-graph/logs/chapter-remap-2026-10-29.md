# Chapter remap — 2026-10-29 (approved chapter plan)

An editorial change, recorded separately from the mechanical reconciliation in `reconciliation-2026-10-29.md`. Implements `docs-site/chapter-plan.md`, approved 2026-10-29. Source fields and enrichments are unchanged.

## Dependency edges dropped (approved with the plan)

| Dependent | Prerequisite | Reason |
|---|---|---|
| 102 Missing Driver ImportError | 60 Connection Builder | missing-driver error is user-visible at install/connect; builder internals not needed to read it |
| 103 Connection URL Construction | 69 BackendSpec Class | URL usage needs only the scheme table, not BackendSpec internals |
| 147 Rides-On Backends | 62 Per Dialect Configuration | rides-on is a settings fact; per-dialect config is architecture |
| 75 SQLiteBackendProfile | 63 SQLite Dialect | a settings profile does not require the dialect internals |
| 76 DuckDBBackendProfile | 64 DuckDB Dialect | same |
| 77 PostgreSQLBackendProfile | 65 PostgreSQL Dialect | same |
| 79 SnowflakeBackendProfile | 66 Snowflake Dialect | same |
| 94 IAMAuthProfile | 89 Cloud Native Auth | IAMAuthProfile usage does not require the adapter internals |
| 106 Connect-Time Auth Composition | 68 build_driver_kwargs Function | connect-time auth composition is user-level; build_driver_kwargs is the internal mechanism |
| 27 Upsert Data | 121 UpsertStyle Descriptor | upsert usage precedes the UpsertStyle descriptor (architecture) |
| 119 Add Columns | 122 Generic-Default Operations | add_columns usage precedes generic-default internals |
| 120 Rename Table | 122 Generic-Default Operations | rename usage precedes generic-default internals |
| 115 Create Index | 118 IndexCapability Descriptor | index usage precedes IndexCapability internals |
| 116 Drop Index | 118 IndexCapability Descriptor | same |
| 114 List Indexes | 118 IndexCapability Descriptor | same |
| 125 Transaction Scope | 130 TransactionSupport Descriptor | transaction usage precedes the TransactionSupport descriptor |
| 133 Transaction Errors | 131 Protected Package Calls | transaction errors are user-facing; protection internals explain them later |
| 136 from_raw_connection | 137 Session Option Preservation | from_raw_connection usage precedes snapshot/restore internals |
| 96 Backend Capability Matrix | 57 DialectSpec Overview | the user support matrix precedes DialectSpec internals |
| 96 Backend Capability Matrix | 118 IndexCapability Descriptor | same |
| 96 Backend Capability Matrix | 121 UpsertStyle Descriptor | same |
| 96 Backend Capability Matrix | 130 TransactionSupport Descriptor | same |
| 96 Backend Capability Matrix | 145 DropScope Descriptor | same |
| 97 DDL Index Support | 118 IndexCapability Descriptor | same |
| 100 Unimplemented Operations | 123 Hook Dispatch Order | NotImplementedError is user-facing; dispatch order is architecture |
| 112 Catalog-Qualified Namespace Limits | 122 Generic-Default Operations | namespace limits are user-facing; generic-default internals later |
| 144 BackendConnectionError | 143 Credential Redaction | BackendConnectionError is user-facing; redaction internals later |

## Chapter assignments

| ID | Concept | Former chapter | New chapter |
|---|---|---|---|
| 1 | SQL Databases | 01-foundation-concepts | 01-what-mountainash-data-is |
| 2 | Database Schemas | 01-foundation-concepts | 01-what-mountainash-data-is |
| 3 | Database Catalogs | 01-foundation-concepts | 01-what-mountainash-data-is |
| 4 | Connection Management | 01-foundation-concepts | 01-what-mountainash-data-is |
| 5 | Python Protocols | 01-foundation-concepts | 01-what-mountainash-data-is |
| 6 | Runtime Checkable Protocol | 01-foundation-concepts | 01-what-mountainash-data-is |
| 7 | Pydantic Models | 01-foundation-concepts | 01-what-mountainash-data-is |
| 8 | Decorators | 01-foundation-concepts | 01-what-mountainash-data-is |
| 9 | Registry Pattern | 01-foundation-concepts | 01-what-mountainash-data-is |
| 10 | Ibis Library | 01-foundation-concepts | 01-what-mountainash-data-is |
| 11 | Backend Protocol Definition | 02-backend-protocol | 01-what-mountainash-data-is |
| 12 | Connect Method | 02-backend-protocol | 02-installing-and-connecting |
| 13 | Close Method | 02-backend-protocol | 02-installing-and-connecting |
| 14 | List Tables Method | 02-backend-protocol | 04-inspecting-databases |
| 15 | Inspect Table Method | 02-backend-protocol | 04-inspecting-databases |
| 16 | List Namespaces Method | 02-backend-protocol | 04-inspecting-databases |
| 17 | Inspect Namespace Method | 02-backend-protocol | 04-inspecting-databases |
| 18 | Inspect Catalog Method | 02-backend-protocol | 04-inspecting-databases |
| 19 | IbisBackend Class | 04-ibis-backend | 02-installing-and-connecting |
| 20 | Ibis Connection | 04-ibis-backend | 02-installing-and-connecting |
| 21 | Fluent Query API | 04-ibis-backend | 05-reading-tables-and-running-queries |
| 22 | Raw SQL Queries | 04-ibis-backend | 05-reading-tables-and-running-queries |
| 23 | DDL Operations | 04-ibis-backend | 06-creating-and-changing-tables |
| 24 | Create Table | 04-ibis-backend | 06-creating-and-changing-tables |
| 25 | DML Operations | 04-ibis-backend | 06-creating-and-changing-tables |
| 26 | Insert Data | 04-ibis-backend | 06-creating-and-changing-tables |
| 27 | Upsert Data | 04-ibis-backend | 06-creating-and-changing-tables |
| 28 | Truncate Table | 04-ibis-backend | 06-creating-and-changing-tables |
| 29 | Create View | 04-ibis-backend | 06-creating-and-changing-tables |
| 30 | List Tables Ibis | 04-ibis-backend | 04-inspecting-databases |
| 31 | Table Inspection Ibis | 04-ibis-backend | 04-inspecting-databases |
| 32 | Namespace Inspection Ibis | 04-ibis-backend | 04-inspecting-databases |
| 33 | Catalog Inspection Ibis | 04-ibis-backend | 04-inspecting-databases |
| 34 | Context Manager Ibis | 04-ibis-backend | 02-installing-and-connecting |
| 49 | CatalogInfo Dataclass | 03-inspection-model | 04-inspecting-databases |
| 50 | NamespaceInfo Dataclass | 03-inspection-model | 04-inspecting-databases |
| 51 | TableInfo Dataclass | 03-inspection-model | 04-inspecting-databases |
| 52 | ColumnInfo Dataclass | 03-inspection-model | 04-inspecting-databases |
| 53 | Frozen Metadata Model | 03-inspection-model | 04-inspecting-databases |
| 54 | Backend Agnostic Metadata | 03-inspection-model | 04-inspecting-databases |
| 55 | Driver Metadata Conversion | 03-inspection-model | 04-inspecting-databases |
| 56 | Unified Inspection API | 03-inspection-model | 04-inspecting-databases |
| 57 | DialectSpec Overview | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 58 | Dialect Registry | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 59 | Dialect Name Key | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 60 | Connection Builder | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 61 | Operation Hooks | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 62 | Per Dialect Configuration | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 63 | SQLite Dialect | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 64 | DuckDB Dialect | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 65 | PostgreSQL Dialect | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 66 | Snowflake Dialect | 05-dialect-system | 11-how-one-backend-serves-20-dialects |
| 67 | BackendProfile Class | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 68 | build_driver_kwargs Function | 06-settings-and-configuration | 12-from-settings-to-driver-arguments |
| 69 | BackendSpec Class | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 70 | ParameterSpec Class | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 71 | Parameter Tiers | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 72 | DATABASES_REGISTRY | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 73 | Register Decorator | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 74 | Auto Registration | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 75 | SQLiteBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 76 | DuckDBBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 77 | PostgreSQLBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 78 | BigQueryBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 79 | SnowflakeBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 80 | RedshiftBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 81 | ClickHouseBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 82 | DatabricksBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 83 | MSSQLBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 84 | TrinoBackendProfile | 06-settings-and-configuration | 03-connection-profiles-and-authentication |
| 85 | Adapter Pipeline | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 86 | Credential Transformation | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 87 | OAuth Adapter | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 88 | JWT Adapter | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 89 | Cloud Native Auth | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 90 | SSL Bundle Adapter | 07-adapters-and-auth | 12-from-settings-to-driver-arguments |
| 91 | NoAuthProfile | 07-adapters-and-auth | 03-connection-profiles-and-authentication |
| 92 | PasswordAuthProfile | 07-adapters-and-auth | 03-connection-profiles-and-authentication |
| 93 | TokenAuthProfile | 07-adapters-and-auth | 03-connection-profiles-and-authentication |
| 94 | IAMAuthProfile | 07-adapters-and-auth | 03-connection-profiles-and-authentication |
| 96 | Backend Capability Matrix | 09-advanced-integration | 10-supported-databases-and-limits |
| 97 | DDL Index Support | 09-advanced-integration | 10-supported-databases-and-limits |
| 100 | Unimplemented Operations | 09-advanced-integration | 10-supported-databases-and-limits |
| 101 | Installation Extras | — | 02-installing-and-connecting |
| 102 | Missing Driver ImportError | — | 02-installing-and-connecting |
| 103 | Connection URL Construction | — | 02-installing-and-connecting |
| 104 | SettingsParameters Construction | — | 03-connection-profiles-and-authentication |
| 105 | AuthProfile Types | — | 03-connection-profiles-and-authentication |
| 106 | Connect-Time Auth Composition | — | 03-connection-profiles-and-authentication |
| 107 | ConnectionFactory | — | 12-from-settings-to-driver-arguments |
| 108 | build_connection_string Function | — | 12-from-settings-to-driver-arguments |
| 109 | Supported Auth Validation | — | 12-from-settings-to-driver-arguments |
| 110 | Namespace Value Object | — | 04-inspecting-databases |
| 111 | NamespaceLike Alias | — | 04-inspecting-databases |
| 112 | Catalog-Qualified Namespace Limits | — | 10-supported-databases-and-limits |
| 113 | IndexInfo Dataclass | — | 04-inspecting-databases |
| 114 | List Indexes | — | 07-indexes |
| 115 | Create Index | — | 07-indexes |
| 116 | Drop Index | — | 07-indexes |
| 117 | Index Exists | — | 07-indexes |
| 118 | IndexCapability Descriptor | — | 11-how-one-backend-serves-20-dialects |
| 119 | Add Columns | — | 06-creating-and-changing-tables |
| 120 | Rename Table | — | 06-creating-and-changing-tables |
| 121 | UpsertStyle Descriptor | — | 11-how-one-backend-serves-20-dialects |
| 122 | Generic-Default Operations | — | 11-how-one-backend-serves-20-dialects |
| 123 | Hook Dispatch Order | — | 11-how-one-backend-serves-20-dialects |
| 124 | sqlglot Rendering | — | 11-how-one-backend-serves-20-dialects |
| 125 | Transaction Scope | — | 08-transactions |
| 126 | Transaction Ownership | — | 08-transactions |
| 127 | Flat Nested Scopes | — | 08-transactions |
| 128 | in_transaction Predicate | — | 08-transactions |
| 129 | native_transaction_open Probe | — | 08-transactions |
| 130 | TransactionSupport Descriptor | — | 11-how-one-backend-serves-20-dialects |
| 131 | Protected Package Calls | — | 13-transaction-and-adoption-internals |
| 132 | Raw Driver Connection | — | 08-transactions |
| 133 | Transaction Errors | — | 08-transactions |
| 134 | Connection Adoption | — | 09-adopting-existing-connections |
| 135 | from_ibis_connection | — | 09-adopting-existing-connections |
| 136 | from_raw_connection | — | 09-adopting-existing-connections |
| 137 | Session Option Preservation | — | 13-transaction-and-adoption-internals |
| 138 | Connection Ownership Flag | — | 09-adopting-existing-connections |
| 139 | run_expr Execution | — | 05-reading-tables-and-running-queries |
| 140 | to_sql Compilation | — | 05-reading-tables-and-running-queries |
| 141 | table Expression Access | — | 05-reading-tables-and-running-queries |
| 142 | Bridge To mountainash Relations | — | 05-reading-tables-and-running-queries |
| 143 | Credential Redaction | — | 12-from-settings-to-driver-arguments |
| 144 | BackendConnectionError | — | 10-supported-databases-and-limits |
| 145 | DropScope Descriptor | — | 11-how-one-backend-serves-20-dialects |
| 146 | Dialect Compatibility Shims | — | 11-how-one-backend-serves-20-dialects |
| 147 | Rides-On Backends | — | 03-connection-profiles-and-authentication |
| 148 | Auth Adapter Registry | — | 12-from-settings-to-driver-arguments |
| 149 | Protocol Conformance Testing | — | 14-extending-mountainash-data |
| 150 | Adding A Dialect | — | 14-extending-mountainash-data |
| 151 | Adding A Settings Backend | — | 14-extending-mountainash-data |
| 152 | Implementing A Backend | — | 14-extending-mountainash-data |
| 153 | Test Tiers | — | 15-contributing-and-testing |
| 154 | Live Database Harness | — | 15-contributing-and-testing |
| 155 | Backend Name Registry | — | 14-extending-mountainash-data |
| 156 | Memtable Rollback Caveat | — | 08-transactions |

## Source mapping corrections during chapter writing

The generic builders live in `backends/ibis/operations.py` (`_generic_add_columns`, `_generic_rename_table`, `_generic_upsert`). `_render.py` holds only shared rendering helpers.

| ID | Concept | From | To |
|---|---|---|---|
| 119 | Add Columns | `mountainash_data.backends.ibis._render` | `mountainash_data.backends.ibis.operations` |
| 120 | Rename Table | `mountainash_data.backends.ibis._render` | `mountainash_data.backends.ibis.operations` |
| 122 | Generic-Default Operations | `mountainash_data.backends.ibis._render` | `mountainash_data.backends.ibis.operations` |

## Revision 2 — ecosystem positioning (approved 2026-10-29)

The chapter plan was revised to 18 chapters (brief revision 2). Chapters from 4 on were renumbered and their slugs updated in the graph's `chapter` fields; chapter 1's slug became `01-mountainash-data-in-the-ecosystem`.

New taxonomy `ECO` (mountainash Ecosystem Integration). New concepts 157–162 and 164–169 (163 unused). Concept 142 (Bridge To mountainash Relations) moved from `06-reading-tables-and-running-queries` to `07-running-mountainash-expressions-against-databases`, with its group changed to `ECO`. Concepts 167 and 168 map to `backends.ibis.backend`; the others are cross-package integration concepts with no module in this package.
