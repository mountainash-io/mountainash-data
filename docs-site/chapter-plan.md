# Chapter plan: mountainash-data

Status: approved — 2026-10-29. The repository owner approved the plan as proposed in the agent session: 15 chapters in four parts, indexes (ch 7) and contributing (ch 15) kept as separate chapters, and the 27 graph-edge drops listed below.
Implements: `docs-site/editorial-brief.md` (confirmed 2026-10-29).
Basis: source `2d0782b`; profile `docs-site/profile` (validated at `2d0782b`, 30 modules, 6 facets); graph `docs-site/learning-graph/learning-graph.json` (139 concepts, reconciled 2026-10-29).

## Structure

| # | Part | Chapter (title) | Audiences |
|---|---|---|---|
| 1 | Overview | What mountainash-data is (`01-what-mountainash-data-is`) | all facets (entry point); broader-hype framing is on the home page |
| 2 | Using the package | Installing and connecting (`02-installing-and-connecting`) | users |
| 3 | Using the package | Connection profiles and authentication (`03-connection-profiles-and-authentication`) | users, extension-authors (reference for profile fields) |
| 4 | Using the package | Inspecting databases (`04-inspecting-databases`) | users, backend-architecture, extension-authors |
| 5 | Using the package | Reading tables and running queries (`05-reading-tables-and-running-queries`) | users, broader-hype (relation bridge) |
| 6 | Using the package | Creating and changing tables (`06-creating-and-changing-tables`) | users |
| 7 | Using the package | Indexes (`07-indexes`) | users |
| 8 | Using the package | Transactions (`08-transactions`) | users, maintainers |
| 9 | Using the package | Adopting existing connections (`09-adopting-existing-connections`) | users, maintainers |
| 10 | Using the package | Supported databases and limits (`10-supported-databases-and-limits`) | users, broader-hype (credible capability claims) |
| 11 | Architecture and internals | How one backend serves 20 dialects (`11-how-one-backend-serves-20-dialects`) | backend-architecture, maintainers, extension-authors, contributors |
| 12 | Architecture and internals | From settings to driver arguments (`12-from-settings-to-driver-arguments`) | backend-architecture, maintainers, extension-authors, contributors |
| 13 | Architecture and internals | Transaction and adoption internals (`13-transaction-and-adoption-internals`) | maintainers, backend-architecture |
| 14 | Extending | Extending mountainash-data (`14-extending-mountainash-data`) | extension-authors, contributors |
| 15 | Extending | Contributing and testing (`15-contributing-and-testing`) | contributors, maintainers |

Appendices: **FAQ** (`docs/faq.md`) and **Glossary** (`docs/glossary.md`), each entry linking to its primary chapter section. Home and About pages carry the broader-hype framing (step 8).

The progression is concepts (ch 1) → package use (ch 2–10) → internals (ch 11–13) → extending (ch 14–15). Readers enter at their depth: chapter 1 is the shared entry point, the user part is self-contained, and internals chapters link back to the user chapter where each concept was introduced instead of repeating it.

## Audience coverage

| Facet | Served by | Notes |
|---|---|---|
| users | Ch 1–10 | Each task chapter is anchored on a verified `examples/` recipe. |
| backend-architecture | Ch 11–13, plus ch 4 (inspection model) | |
| maintainers | Ch 11–13, plus ch 8 (transaction semantics) and ch 15 | Known debt is linked (DEBT-25, profile coverage), not repeated. |
| extension-authors | Ch 14, plus reference material in ch 3, 11 and 12 | `_`-prefixed modules are marked private. |
| contributors | Ch 14–15 | Links to CONTRIBUTING.md and TESTING.md. |
| broader-hype | Home and About pages; ch 5 (relation bridge) and ch 10 (support matrix) as proof points | No dedicated chapter, per the brief. |

## Chapters

### 1. What mountainash-data is

- **Slug:** `01-what-mountainash-data-is` — **Part:** Overview
- **Purpose:** Explain what the package is for, its layers (Backend protocol → IbisBackend → DialectSpec; settings/auth from sibling packages; Ibis for expressions; mountainash for relations), and the few general ideas the rest of the book relies on.
- **Audiences:** all facets (entry point); broader-hype framing is on the home page
- **Prerequisite chapters:** none
- **Sections:** What the package does and does not do; Databases, schemas and catalogs as the package models them; Where Ibis fits; The Backend protocol in one page; Python ideas the package leans on (structural protocols, pydantic settings, decorators and registries); How the book is organised
- **Why a chapter:** A single entry chapter: the foundation ideas are short and only matter as background for later chapters.
- **Reuse (verify first):** Old ch01 explanations of protocols/registries, after trimming course register; README intro.
- **Visuals:** Mermaid layer diagram.
- **Sources:** `src/mountainash_data/core/protocol.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 1 | SQL Databases | 378 | elaborate | — |
| 2 | Database Schemas | 51 | standard | — |
| 3 | Database Catalogs | 17 | standard | — |
| 4 | Connection Management | 93 | elaborate | — |
| 10 | Ibis Library | 212 | elaborate | — |
| 5 | Python Protocols | 314 | elaborate | — |
| 6 | Runtime Checkable Protocol | 158 | elaborate | — |
| 11 | Backend Protocol Definition | 155 | elaborate | core.protocol |
| 7 | Pydantic Models | 168 | elaborate | — |
| 8 | Decorators | 158 | elaborate | — |
| 9 | Registry Pattern | 139 | elaborate | — |

### 2. Installing and connecting

- **Slug:** `02-installing-and-connecting` — **Part:** Using the package
- **Purpose:** Install the right extra and open, use and close a connection by dialect kwargs or URL.
- **Audiences:** users
- **Prerequisite chapters:** 1
- **Sections:** Choosing extras; Missing-driver errors; IbisBackend by dialect and keyword arguments; IbisBackend from a URL; connect, close and the context manager; The live IbisConnection
- **Why a chapter:** Separate from profiles: the shortest path to a working connection, with no settings classes needed.
- **Reuse (verify first):** examples/connections (dialect and URL parts), README Installation.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`, `src/mountainash_data/core/protocol.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 101 | Installation Extras | 2 | brief | — |
| 102 | Missing Driver ImportError | 1 | brief | backends.ibis.backend |
| 19 | IbisBackend Class | 76 | elaborate | backends.ibis.backend |
| 12 | Connect Method | 41 | standard | core.protocol |
| 13 | Close Method | 2 | brief | core.protocol |
| 20 | Ibis Connection | 38 | standard | backends.ibis.backend |
| 34 | Context Manager Ibis | 1 | brief | backends.ibis.backend |
| 103 | Connection URL Construction | 4 | brief | backends.ibis.backend |

### 3. Connection profiles and authentication

- **Slug:** `03-connection-profiles-and-authentication` — **Part:** Using the package
- **Purpose:** Configure connections with typed *BackendProfile classes and supply credentials separately as *AuthProfile objects at connect time.
- **Audiences:** users, extension-authors (reference for profile fields)
- **Prerequisite chapters:** 1, 2
- **Sections:** BackendProfile and SettingsParameters; BackendSpec, ParameterSpec and parameter tiers; The settings registry and automatic registration; Rides-on backends (MotherDuck, Redshift); Backend profile reference (20 backends, as a table with notable fields); Auth profiles and connect-time composition; Which auth types each backend accepts
- **Why a chapter:** Settings and auth are one user task (configure a connection); the mechanism behind them is chapter 12.
- **Reuse (verify first):** examples/connections (settings part); old ch06 ParameterSpec/tier prose after renames; old ch07 is not reused.
- **Visuals:** Mermaid: profile + auth → connect().
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`, `src/mountainash_data/core/settings`, `src/mountainash_data/core/settings/__init__.py`, `src/mountainash_data/core/settings/descriptor.py`, `src/mountainash_data/core/settings/profile.py`, `src/mountainash_data/core/settings/registry.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 67 | BackendProfile Class | 33 | standard | core.settings.profile |
| 104 | SettingsParameters Construction | 1 | brief | backends.ibis.backend |
| 69 | BackendSpec Class | 39 | standard | core.settings.descriptor |
| 70 | ParameterSpec Class | 2 | brief | core.settings.descriptor |
| 71 | Parameter Tiers | 1 | brief | core.settings.descriptor |
| 72 | DATABASES_REGISTRY | 19 | standard | core.settings.registry |
| 73 | Register Decorator | 18 | standard | core.settings.registry |
| 74 | Auto Registration | 14 | standard | core.settings.registry |
| 147 | Rides-On Backends | 2 | brief | core.settings.descriptor |
| 75 | SQLiteBackendProfile | 1 | brief | core.settings.backends |
| 76 | DuckDBBackendProfile | 1 | brief | core.settings.backends |
| 77 | PostgreSQLBackendProfile | 1 | brief | core.settings.backends |
| 78 | BigQueryBackendProfile | 1 | brief | core.settings.backends |
| 79 | SnowflakeBackendProfile | 1 | brief | core.settings.backends |
| 80 | RedshiftBackendProfile | 1 | brief | core.settings.backends |
| 81 | ClickHouseBackendProfile | 1 | brief | core.settings.backends |
| 82 | DatabricksBackendProfile | 1 | brief | core.settings.backends |
| 83 | MSSQLBackendProfile | 1 | brief | core.settings.backends |
| 84 | TrinoBackendProfile | 1 | brief | core.settings.backends |
| 105 | AuthProfile Types | 40 | standard | core.settings |
| 91 | NoAuthProfile | 1 | brief | core.settings |
| 92 | PasswordAuthProfile | 1 | brief | core.settings |
| 93 | TokenAuthProfile | 1 | brief | core.settings |
| 94 | IAMAuthProfile | 1 | brief | core.settings |
| 106 | Connect-Time Auth Composition | 1 | brief | backends.ibis.backend |

### 4. Inspecting databases

- **Slug:** `04-inspecting-databases` — **Part:** Using the package
- **Purpose:** List and inspect catalogs, namespaces, tables, columns and indexes through the typed inspection model.
- **Audiences:** users, backend-architecture, extension-authors
- **Prerequisite chapters:** 1, 2
- **Sections:** The inspection methods; Namespace and NamespaceLike; CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo, IndexInfo; Frozen, backend-agnostic metadata; How driver metadata becomes TableInfo; Inspecting through IbisBackend
- **Why a chapter:** One chapter for read-only metadata; it shares the Namespace explanation that later chapters link to.
- **Reuse (verify first):** examples/inspection; old ch03 dataclass prose after checking it against IndexInfo and location; old ch02 method prose.
- **Visuals:** Mermaid: catalog → namespace → table → column hierarchy.
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`, `src/mountainash_data/backends/ibis/inspect.py`, `src/mountainash_data/core/inspection.py`, `src/mountainash_data/core/namespace.py`, `src/mountainash_data/core/protocol.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 14 | List Tables Method | 3 | brief | core.protocol |
| 15 | Inspect Table Method | 3 | brief | core.protocol |
| 16 | List Namespaces Method | 2 | brief | core.protocol |
| 17 | Inspect Namespace Method | 3 | brief | core.protocol |
| 18 | Inspect Catalog Method | 3 | brief | core.protocol |
| 110 | Namespace Value Object | 4 | brief | core.namespace |
| 111 | NamespaceLike Alias | 1 | brief | core.namespace |
| 49 | CatalogInfo Dataclass | 9 | standard | core.inspection |
| 50 | NamespaceInfo Dataclass | 9 | standard | core.inspection |
| 51 | TableInfo Dataclass | 9 | standard | core.inspection |
| 52 | ColumnInfo Dataclass | 9 | standard | core.inspection |
| 113 | IndexInfo Dataclass | 11 | standard | core.inspection |
| 53 | Frozen Metadata Model | 8 | standard | core.inspection |
| 54 | Backend Agnostic Metadata | 7 | brief | core.inspection |
| 55 | Driver Metadata Conversion | 4 | brief | backends.ibis.inspect |
| 56 | Unified Inspection API | 1 | brief | core.inspection |
| 30 | List Tables Ibis | 1 | brief | backends.ibis.backend |
| 31 | Table Inspection Ibis | 1 | brief | backends.ibis.backend |
| 32 | Namespace Inspection Ibis | 1 | brief | backends.ibis.backend |
| 33 | Catalog Inspection Ibis | 1 | brief | backends.ibis.backend |

### 5. Reading tables and running queries

- **Slug:** `05-reading-tables-and-running-queries` — **Part:** Using the package
- **Purpose:** Get Ibis tables, run expressions and SQL, compile to SQL, and hand tables to mountainash.relation().
- **Audiences:** users, broader-hype (relation bridge)
- **Prerequisite chapters:** 1, 2
- **Sections:** table() and Ibis expressions; Fluent chaining; run_expr and to_sql; run_sql; Bridging to mountainash relations
- **Why a chapter:** Reading is distinct from writing; it is short but common.
- **Reuse (verify first):** examples/local_tables; README pull-side bridge note.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 141 | table Expression Access | 2 | brief | backends.ibis.backend |
| 21 | Fluent Query API | 3 | brief | backends.ibis.backend |
| 139 | run_expr Execution | 1 | brief | backends.ibis.backend |
| 140 | to_sql Compilation | 1 | brief | backends.ibis.backend |
| 22 | Raw SQL Queries | 1 | brief | backends.ibis.backend |
| 142 | Bridge To mountainash Relations | 1 | brief | backends.ibis.backend |

### 6. Creating and changing tables

- **Slug:** `06-creating-and-changing-tables` — **Part:** Using the package
- **Purpose:** Create, insert, truncate, view, upsert, add columns to and rename tables, with the dialect caveats that matter.
- **Audiences:** users
- **Prerequisite chapters:** 1, 2
- **Sections:** Creating tables and views; Inserting and truncating; Upsert; Adding columns; Renaming tables
- **Why a chapter:** Write operations share caveats (dialect support, namespaces) and belong together; indexes are split out because they have their own capability model.
- **Reuse (verify first):** examples/mutations; old ch04 DDL/DML prose after removing Iceberg.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/_render.py`, `src/mountainash_data/backends/ibis/backend.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 23 | DDL Operations | 12 | standard | backends.ibis.backend |
| 24 | Create Table | 2 | brief | backends.ibis.backend |
| 29 | Create View | 1 | brief | backends.ibis.backend |
| 25 | DML Operations | 8 | standard | backends.ibis.backend |
| 26 | Insert Data | 2 | brief | backends.ibis.backend |
| 28 | Truncate Table | 1 | brief | backends.ibis.backend |
| 27 | Upsert Data | 1 | brief | backends.ibis.backend |
| 119 | Add Columns | 1 | brief | backends.ibis._render |
| 120 | Rename Table | 1 | brief | backends.ibis._render |

### 7. Indexes

- **Slug:** `07-indexes` — **Part:** Using the package
- **Purpose:** Create, list, check and drop indexes, and know which index families each dialect supports.
- **Audiences:** users
- **Prerequisite chapters:** 2, 4, 6
- **Sections:** Creating indexes; Dropping indexes; Listing indexes and IndexInfo; index_exists
- **Why a chapter:** Its own chapter: indexes have a distinct recipe, capability descriptor and limits.
- **Reuse (verify first):** examples/indexes.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/_index.py`, `src/mountainash_data/backends/ibis/_index_inspection.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 115 | Create Index | 1 | brief | backends.ibis._index |
| 116 | Drop Index | 1 | brief | backends.ibis._index |
| 114 | List Indexes | 2 | brief | backends.ibis._index_inspection |
| 117 | Index Exists | 1 | brief | backends.ibis._index |

### 8. Transactions

- **Slug:** `08-transactions` — **Part:** Using the package
- **Purpose:** Use explicit transaction scopes, understand owned versus joined transactions, and avoid raw-handle pitfalls.
- **Audiences:** users, maintainers
- **Prerequisite chapters:** 1, 2, 6
- **Sections:** raw_driver_connection; transaction(); in_transaction and native_transaction_open; Owned and joined transactions; Nested scopes are flat; Transaction errors; The memtable rollback caveat
- **Why a chapter:** Transactions are a self-contained user capability with their own semantics; the internals are in chapter 13.
- **Reuse (verify first):** README 'Transaction ownership' (verified), examples/transactions.
- **Visuals:** Mermaid state diagram: idle → owned / joined → commit / rollback.
- **Sources:** `src/mountainash_data/backends/ibis/_raw.py`, `src/mountainash_data/backends/ibis/_transaction.py`, `src/mountainash_data/core/errors.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 132 | Raw Driver Connection | 6 | brief | backends.ibis._raw |
| 125 | Transaction Scope | 10 | standard | backends.ibis._transaction |
| 128 | in_transaction Predicate | 1 | brief | backends.ibis._transaction |
| 129 | native_transaction_open Probe | 2 | brief | backends.ibis._transaction |
| 126 | Transaction Ownership | 1 | brief | backends.ibis._transaction |
| 127 | Flat Nested Scopes | 1 | brief | backends.ibis._transaction |
| 133 | Transaction Errors | 1 | brief | core.errors |
| 156 | Memtable Rollback Caveat | 1 | brief | backends.ibis._transaction |

### 9. Adopting existing connections

- **Slug:** `09-adopting-existing-connections` — **Part:** Using the package
- **Purpose:** Wrap an existing Ibis or raw driver connection while keeping ownership with the caller.
- **Audiences:** users, maintainers
- **Prerequisite chapters:** 2, 8
- **Sections:** Connection ownership; from_ibis_connection; from_raw_connection and verified dialects
- **Why a chapter:** Separate from transactions: adoption concerns lifecycle ownership, not transaction state.
- **Reuse (verify first):** examples/connection_adoption.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 138 | Connection Ownership Flag | 4 | brief | backends.ibis.backend |
| 134 | Connection Adoption | 3 | brief | backends.ibis.backend |
| 135 | from_ibis_connection | 1 | brief | backends.ibis.backend |
| 136 | from_raw_connection | 1 | brief | backends.ibis.backend |

### 10. Supported databases and limits

- **Slug:** `10-supported-databases-and-limits` — **Part:** Using the package
- **Purpose:** The per-dialect support matrix, documented limits, and how unsupported operations fail.
- **Audiences:** users, broader-hype (credible capability claims)
- **Prerequisite chapters:** 2, 4, 6
- **Sections:** Support matrix (generated from DialectSpec); Index support by dialect; Namespace limits; NotImplementedError and other failure modes; BackendConnectionError
- **Why a chapter:** A reference chapter that ends the user part, so earlier chapters can link here instead of repeating caveats.
- **Reuse (verify first):** README 'Backend support and limits' (verified).
- **Visuals:** Table only.
- **Sources:** `src/mountainash_data/backends/ibis/backend.py`, `src/mountainash_data/backends/ibis/dialects/_registry.py`, `src/mountainash_data/core/errors.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 96 | Backend Capability Matrix | 3 | brief | backends.ibis.dialects |
| 97 | DDL Index Support | 1 | brief | backends.ibis.dialects |
| 100 | Unimplemented Operations | 1 | brief | backends.ibis.dialects |
| 112 | Catalog-Qualified Namespace Limits | 1 | brief | backends.ibis.backend |
| 144 | BackendConnectionError | 1 | brief | core.errors |

### 11. How one backend serves 20 dialects

- **Slug:** `11-how-one-backend-serves-20-dialects` — **Part:** Architecture and internals
- **Purpose:** How DialectSpec data drives connection building, capabilities, generic-default SQL and per-dialect hooks.
- **Audiences:** backend-architecture, maintainers, extension-authors, contributors
- **Prerequisite chapters:** 1
- **Sections:** DialectSpec and the DIALECTS registry; Connection builders; Capability descriptors (UpsertStyle, IndexCapability, TransactionSupport, DropScope); Generic-default operations and sqlglot rendering; Operation hooks and dispatch order; Compatibility shims; Worked dialects: SQLite, DuckDB, PostgreSQL, Snowflake
- **Why a chapter:** The core architecture chapter; the settings pipeline and transaction internals are separate concerns with their own modules.
- **Reuse (verify first):** Old ch05 DialectSpec overview and connection-builder prose (updated for capability fields and hooks).
- **Visuals:** Mermaid flow: operation → hook? → generic → NotImplementedError.
- **Sources:** `src/mountainash_data/backends/ibis/_render.py`, `src/mountainash_data/backends/ibis/backend.py`, `src/mountainash_data/backends/ibis/dialects/_registry.py`, `src/mountainash_data/backends/ibis/operations.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 57 | DialectSpec Overview | 103 | elaborate | backends.ibis.dialects |
| 58 | Dialect Registry | 15 | standard | backends.ibis.dialects |
| 59 | Dialect Name Key | 1 | brief | backends.ibis.dialects |
| 60 | Connection Builder | 11 | standard | backends.ibis.dialects |
| 62 | Per Dialect Configuration | 11 | standard | backends.ibis.dialects |
| 145 | DropScope Descriptor | 4 | brief | backends.ibis.dialects |
| 118 | IndexCapability Descriptor | 9 | standard | backends.ibis.dialects |
| 121 | UpsertStyle Descriptor | 5 | brief | backends.ibis.dialects |
| 130 | TransactionSupport Descriptor | 14 | standard | backends.ibis.dialects |
| 124 | sqlglot Rendering | 19 | standard | backends.ibis._render |
| 122 | Generic-Default Operations | 18 | standard | backends.ibis._render |
| 61 | Operation Hooks | 9 | standard | backends.ibis.operations |
| 123 | Hook Dispatch Order | 5 | brief | backends.ibis.backend |
| 146 | Dialect Compatibility Shims | 6 | brief | — |
| 63 | SQLite Dialect | 2 | brief | backends.ibis.dialects |
| 64 | DuckDB Dialect | 2 | brief | backends.ibis.dialects |
| 65 | PostgreSQL Dialect | 2 | brief | backends.ibis.dialects |
| 66 | Snowflake Dialect | 2 | brief | backends.ibis.dialects |

### 12. From settings to driver arguments

- **Slug:** `12-from-settings-to-driver-arguments` — **Part:** Architecture and internals
- **Purpose:** How BackendProfile config and AuthProfile credentials become driver kwargs and URLs.
- **Audiences:** backend-architecture, maintainers, extension-authors, contributors
- **Prerequisite chapters:** 2, 3
- **Sections:** ConnectionFactory; The auth adapter registry; Credential transformation and adapter families (OAuth, JWT, cloud-native, SSL bundle); build_driver_kwargs; Supported-auth validation; build_connection_string; Credential redaction
- **Why a chapter:** The internals behind chapter 3, kept apart so users never need them.
- **Reuse (verify first):** None from old ch07 (its API is gone); CLAUDE.md settings section as a checked outline.
- **Visuals:** Mermaid pipeline: profile.emit → adapter → kwargs.
- **Sources:** `src/mountainash_data/core/_redact.py`, `src/mountainash_data/core/factories/connection_factory.py`, `src/mountainash_data/core/settings/adapters`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 107 | ConnectionFactory | 12 | standard | core.factories |
| 86 | Credential Transformation | 16 | standard | core.settings.adapters |
| 148 | Auth Adapter Registry | 7 | brief | core.settings.adapters |
| 85 | Adapter Pipeline | 3 | brief | core.settings.adapters |
| 68 | build_driver_kwargs Function | 2 | brief | core.factories |
| 87 | OAuth Adapter | 1 | brief | core.settings.adapters |
| 88 | JWT Adapter | 1 | brief | core.settings.adapters |
| 89 | Cloud Native Auth | 2 | brief | core.settings.adapters |
| 90 | SSL Bundle Adapter | 1 | brief | core.settings.adapters |
| 109 | Supported Auth Validation | 4 | brief | core.factories |
| 108 | build_connection_string Function | 3 | brief | core.factories |
| 143 | Credential Redaction | 2 | brief | core._redact |

### 13. Transaction and adoption internals

- **Slug:** `13-transaction-and-adoption-internals` — **Part:** Architecture and internals
- **Purpose:** How protected calls, the ownership registry and session-option snapshot/restore work, and their invariants.
- **Audiences:** maintainers, backend-architecture
- **Prerequisite chapters:** 8, 11
- **Sections:** Protected package calls; The per-raw-handle scope registry; Session-option snapshot and restore; Invariants and known Ibis couplings
- **Why a chapter:** Maintainer depth behind chapters 8 and 9.
- **Reuse (verify first):** Profile risk notes for _transaction, _protected and _adoption.
- **Visuals:** Mermaid sequence: scope enter → protected call → exit.
- **Sources:** `src/mountainash_data/backends/ibis/_adoption.py`, `src/mountainash_data/backends/ibis/_protected.py`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 131 | Protected Package Calls | 2 | brief | backends.ibis._protected |
| 137 | Session Option Preservation | 2 | brief | backends.ibis._adoption |

### 14. Extending mountainash-data

- **Slug:** `14-extending-mountainash-data` — **Part:** Extending
- **Purpose:** Add a dialect, add a settings backend with auth adapters, or implement the Backend protocol for a new store.
- **Audiences:** extension-authors, contributors
- **Prerequisite chapters:** 1, 3, 4, 11, 12
- **Sections:** Protocol conformance testing; Implementing a Backend; The backend name registry (retained, unwired; DEBT-25); Adding a dialect; Adding a settings backend
- **Why a chapter:** One recipe-oriented chapter for every extension point.
- **Reuse (verify first):** tests/test_unit/core/test_protocol.py (_StubBackend) as template; CLAUDE.md structure.
- **Visuals:** None.
- **Sources:** `src/mountainash_data/backends/ibis/dialects/_registry.py`, `src/mountainash_data/core/protocol.py`, `src/mountainash_data/core/registry.py`, `src/mountainash_data/core/settings`

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 149 | Protocol Conformance Testing | 2 | brief | core.protocol |
| 152 | Implementing A Backend | 1 | brief | core.protocol |
| 155 | Backend Name Registry | 1 | brief | core.registry |
| 150 | Adding A Dialect | 3 | brief | backends.ibis.dialects |
| 151 | Adding A Settings Backend | 3 | brief | core.settings.backends |

### 15. Contributing and testing

- **Slug:** `15-contributing-and-testing` — **Part:** Extending
- **Purpose:** Repository layout, test tiers and the live-database harness; links to CONTRIBUTING.md and TESTING.md.
- **Audiences:** contributors, maintainers
- **Prerequisite chapters:** 14
- **Sections:** Repository layout; Test tiers; The live-database harness
- **Why a chapter:** Short and separate so the extension recipes stay focused.
- **Reuse (verify first):** TESTING.md, CONTRIBUTING.md (linked, not copied).
- **Visuals:** None.
- **Sources:** conceptual

| ID | Concept | CIS | Weight | Source module |
|---|---|---|---|---|
| 153 | Test Tiers | 2 | brief | — |
| 154 | Live Database Harness | 1 | brief | — |

## Graph edits required by this plan

Prerequisites must come earlier in reading order. These dependencies encoded internal mechanics rather than what a reader needs first, so they are dropped from the graph (on approval, through a reviewed reconcile). The concepts stay connected through their other dependencies.

| Dependent | Prerequisite dropped | Reason |
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

After the edits, every one of the 139 concepts has exactly one primary chapter, and every remaining prerequisite appears earlier (checked mechanically).

## Elaboration guidance

Weights use the book-wide CIS maximum (378, concept 1). **elaborate** means ≥15% of the maximum, **standard** 2–15%, **brief** under 2%. CIS guides depth within a section; it does not decide structure. Foundation concepts in chapter 1 score highly because everything depends on them, but they stay brief because the brief makes them background.

CIS measures how much depends on a concept, so leaf capabilities score low by construction. Transaction ownership, connection adoption, upsert, `add_columns` and indexes are all leaves, yet they are core user tasks. They are elaborated as the profile's `essential` priority and the brief require, whatever their CIS weight. Use the weight column to size explanations within a section, not to decide whether a capability gets one.

## Resolved questions

1. Indexes stay a separate chapter (ch 7). Approved 2026-10-29.
2. Contributing stays a separate chapter (ch 15). Approved 2026-10-29.
3. The graph edits above are approved. They are applied through a reviewed reconcile and logged in `docs-site/learning-graph/logs/chapter-remap-2026-10-29.md`.
