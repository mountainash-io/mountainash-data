# Taxonomy Distribution Report

## Overview

- **Total Concepts**: 139
- **Number of Taxonomies**: 12
- **Average Concepts per Taxonomy**: 11.6

## Distribution Summary

| Category | TaxonomyID | Count | Percentage | Status |
|----------|-----------|-------|------------|--------|
| Settings and Configuration | SETT | 21 | 15.1% | ✅ |
| Dialect System and Dispatch | DIAL | 18 | 12.9% | ✅ |
| Inspection Model | INSP | 15 | 10.8% | ✅ |
| Tables, Mutations and Indexes | WRITE | 13 | 9.4% | ✅ |
| Authentication and Adapters | AUTH | 13 | 9.4% | ✅ |
| Transactions and Connection Adoption | TXN | 13 | 9.4% | ✅ |
| Foundation Concepts | FOUND | 10 | 7.2% | ✅ |
| Ibis Backend Usage | IBIS | 9 | 6.5% | ✅ |
| Backend Protocol | PROTO | 8 | 5.8% | ✅ |
| Installing and Connecting | CONN | 7 | 5.0% | ✅ |
| Extending and Contributing | EXT | 7 | 5.0% | ✅ |
| Capabilities and Limits | LIMIT | 5 | 3.6% | ✅ |

## Visual Distribution

```
Settings and Configuratio ███████  21 ( 15.1%)
Dialect System and Dispat ██████  18 ( 12.9%)
Inspection Model          █████  15 ( 10.8%)
Tables, Mutations and Ind ████  13 (  9.4%)
Authentication and Adapte ████  13 (  9.4%)
Transactions and Connecti ████  13 (  9.4%)
Foundation Concepts       ███  10 (  7.2%)
Ibis Backend Usage        ███   9 (  6.5%)
Backend Protocol          ██   8 (  5.8%)
Installing and Connecting ██   7 (  5.0%)
Extending and Contributin ██   7 (  5.0%)
Capabilities and Limits   █   5 (  3.6%)
```

## Balance Analysis

### ✅ No Over-Represented Categories

All categories are under the 30% threshold. Good balance!

## Category Details

### Settings and Configuration (SETT)

**Count**: 21 concepts (15.1%)

**Concepts**:

- 67. BackendProfile Class
- 68. build_driver_kwargs Function
- 69. BackendSpec Class
- 70. ParameterSpec Class
- 71. Parameter Tiers
- 72. DATABASES_REGISTRY
- 73. Register Decorator
- 74. Auto Registration
- 75. SQLiteBackendProfile
- 76. DuckDBBackendProfile
- 77. PostgreSQLBackendProfile
- 78. BigQueryBackendProfile
- 79. SnowflakeBackendProfile
- 80. RedshiftBackendProfile
- 81. ClickHouseBackendProfile
- *...and 6 more*

### Dialect System and Dispatch (DIAL)

**Count**: 18 concepts (12.9%)

**Concepts**:

- 57. DialectSpec Overview
- 58. Dialect Registry
- 59. Dialect Name Key
- 60. Connection Builder
- 61. Operation Hooks
- 62. Per Dialect Configuration
- 63. SQLite Dialect
- 64. DuckDB Dialect
- 65. PostgreSQL Dialect
- 66. Snowflake Dialect
- 118. IndexCapability Descriptor
- 121. UpsertStyle Descriptor
- 122. Generic-Default Operations
- 123. Hook Dispatch Order
- 124. sqlglot Rendering
- *...and 3 more*

### Inspection Model (INSP)

**Count**: 15 concepts (10.8%)

**Concepts**:

- 30. List Tables Ibis
- 31. Table Inspection Ibis
- 32. Namespace Inspection Ibis
- 33. Catalog Inspection Ibis
- 49. CatalogInfo Dataclass
- 50. NamespaceInfo Dataclass
- 51. TableInfo Dataclass
- 52. ColumnInfo Dataclass
- 53. Frozen Metadata Model
- 54. Backend Agnostic Metadata
- 55. Driver Metadata Conversion
- 56. Unified Inspection API
- 110. Namespace Value Object
- 111. NamespaceLike Alias
- 113. IndexInfo Dataclass

### Tables, Mutations and Indexes (WRITE)

**Count**: 13 concepts (9.4%)

**Concepts**:

- 23. DDL Operations
- 24. Create Table
- 25. DML Operations
- 26. Insert Data
- 27. Upsert Data
- 28. Truncate Table
- 29. Create View
- 114. List Indexes
- 115. Create Index
- 116. Drop Index
- 117. Index Exists
- 119. Add Columns
- 120. Rename Table

### Authentication and Adapters (AUTH)

**Count**: 13 concepts (9.4%)

**Concepts**:

- 85. Adapter Pipeline
- 86. Credential Transformation
- 87. OAuth Adapter
- 88. JWT Adapter
- 89. Cloud Native Auth
- 90. SSL Bundle Adapter
- 91. NoAuthProfile
- 92. PasswordAuthProfile
- 93. TokenAuthProfile
- 94. IAMAuthProfile
- 105. AuthProfile Types
- 109. Supported Auth Validation
- 148. Auth Adapter Registry

### Transactions and Connection Adoption (TXN)

**Count**: 13 concepts (9.4%)

**Concepts**:

- 125. Transaction Scope
- 126. Transaction Ownership
- 127. Flat Nested Scopes
- 128. in_transaction Predicate
- 129. native_transaction_open Probe
- 131. Protected Package Calls
- 132. Raw Driver Connection
- 133. Transaction Errors
- 134. Connection Adoption
- 135. from_ibis_connection
- 136. from_raw_connection
- 137. Session Option Preservation
- 138. Connection Ownership Flag

### Foundation Concepts (FOUND)

**Count**: 10 concepts (7.2%)

**Concepts**:

- 1. SQL Databases
- 2. Database Schemas
- 3. Database Catalogs
- 4. Connection Management
- 5. Python Protocols
- 6. Runtime Checkable Protocol
- 7. Pydantic Models
- 8. Decorators
- 9. Registry Pattern
- 10. Ibis Library

### Ibis Backend Usage (IBIS)

**Count**: 9 concepts (6.5%)

**Concepts**:

- 19. IbisBackend Class
- 20. Ibis Connection
- 21. Fluent Query API
- 22. Raw SQL Queries
- 34. Context Manager Ibis
- 139. run_expr Execution
- 140. to_sql Compilation
- 141. table Expression Access
- 142. Bridge To mountainash Relations

### Backend Protocol (PROTO)

**Count**: 8 concepts (5.8%)

**Concepts**:

- 11. Backend Protocol Definition
- 12. Connect Method
- 13. Close Method
- 14. List Tables Method
- 15. Inspect Table Method
- 16. List Namespaces Method
- 17. Inspect Namespace Method
- 18. Inspect Catalog Method

### Installing and Connecting (CONN)

**Count**: 7 concepts (5.0%)

**Concepts**:

- 101. Installation Extras
- 102. Missing Driver ImportError
- 103. Connection URL Construction
- 104. SettingsParameters Construction
- 106. Connect-Time Auth Composition
- 143. Credential Redaction
- 144. BackendConnectionError

### Extending and Contributing (EXT)

**Count**: 7 concepts (5.0%)

**Concepts**:

- 149. Protocol Conformance Testing
- 150. Adding A Dialect
- 151. Adding A Settings Backend
- 152. Implementing A Backend
- 153. Test Tiers
- 154. Live Database Harness
- 155. Backend Name Registry

### Capabilities and Limits (LIMIT)

**Count**: 5 concepts (3.6%)

**Concepts**:

- 96. Backend Capability Matrix
- 97. DDL Index Support
- 100. Unimplemented Operations
- 112. Catalog-Qualified Namespace Limits
- 156. Memtable Rollback Caveat

## Recommendations

- ✅ **Excellent balance**: Categories are evenly distributed (spread: 11.5%)
- ✅ **MISC category minimal**: Good categorization specificity

### Educational Use Recommendations

- Use taxonomy categories for color-coding in graph visualizations
- Design curriculum modules based on taxonomy groupings
- Create filtered views for focused learning paths
- Use categories for assessment organization
- Enable navigation by topic area in interactive tools

---

*Report generated by ibook graph taxonomy-report*
