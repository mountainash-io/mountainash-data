# Concept List

Internal concept inventory for the mountainash-data book (source `2d0782b`). Canonical data: `learning-graph.csv` / `learning-graph.json`.

1. SQL Databases
2. Database Schemas
3. Database Catalogs
4. Connection Management
5. Python Protocols
6. Runtime Checkable Protocol
7. Pydantic Models
8. Decorators
9. Registry Pattern
10. Ibis Library
11. Backend Protocol Definition
12. Connect Method
13. Close Method
14. List Tables Method
15. Inspect Table Method
16. List Namespaces Method
17. Inspect Namespace Method
18. Inspect Catalog Method
19. IbisBackend Class
20. Ibis Connection
21. Fluent Query API
22. Raw SQL Queries
23. DDL Operations
24. Create Table
25. DML Operations
26. Insert Data
27. Upsert Data
28. Truncate Table
29. Create View
30. List Tables Ibis
31. Table Inspection Ibis
32. Namespace Inspection Ibis
33. Catalog Inspection Ibis
34. Context Manager Ibis
49. CatalogInfo Dataclass
50. NamespaceInfo Dataclass
51. TableInfo Dataclass
52. ColumnInfo Dataclass
53. Frozen Metadata Model
54. Backend Agnostic Metadata
55. Driver Metadata Conversion
56. Unified Inspection API
57. DialectSpec Overview
58. Dialect Registry
59. Dialect Name Key
60. Connection Builder
61. Operation Hooks
62. Per Dialect Configuration
63. SQLite Dialect
64. DuckDB Dialect
65. PostgreSQL Dialect
66. Snowflake Dialect
67. BackendProfile Class
68. build_driver_kwargs Function
69. BackendSpec Class
70. ParameterSpec Class
71. Parameter Tiers
72. DATABASES_REGISTRY
73. Register Decorator
74. Auto Registration
75. SQLiteBackendProfile
76. DuckDBBackendProfile
77. PostgreSQLBackendProfile
78. BigQueryBackendProfile
79. SnowflakeBackendProfile
80. RedshiftBackendProfile
81. ClickHouseBackendProfile
82. DatabricksBackendProfile
83. MSSQLBackendProfile
84. TrinoBackendProfile
85. Adapter Pipeline
86. Credential Transformation
87. OAuth Adapter
88. JWT Adapter
89. Cloud Native Auth
90. SSL Bundle Adapter
91. NoAuthProfile
92. PasswordAuthProfile
93. TokenAuthProfile
94. IAMAuthProfile
96. Backend Capability Matrix
97. DDL Index Support
100. Unimplemented Operations
101. Installation Extras
102. Missing Driver ImportError
103. Connection URL Construction
104. SettingsParameters Construction
105. AuthProfile Types
106. Connect-Time Auth Composition
107. ConnectionFactory
108. build_connection_string Function
109. Supported Auth Validation
110. Namespace Value Object
111. NamespaceLike Alias
112. Catalog-Qualified Namespace Limits
113. IndexInfo Dataclass
114. List Indexes
115. Create Index
116. Drop Index
117. Index Exists
118. IndexCapability Descriptor
119. Add Columns
120. Rename Table
121. UpsertStyle Descriptor
122. Generic-Default Operations
123. Hook Dispatch Order
124. sqlglot Rendering
125. Transaction Scope
126. Transaction Ownership
127. Flat Nested Scopes
128. in_transaction Predicate
129. native_transaction_open Probe
130. TransactionSupport Descriptor
131. Protected Package Calls
132. Raw Driver Connection
133. Transaction Errors
134. Connection Adoption
135. from_ibis_connection
136. from_raw_connection
137. Session Option Preservation
138. Connection Ownership Flag
139. run_expr Execution
140. to_sql Compilation
141. table Expression Access
142. Bridge To mountainash Relations
143. Credential Redaction
144. BackendConnectionError
145. DropScope Descriptor
146. Dialect Compatibility Shims
147. Rides-On Backends
148. Auth Adapter Registry
149. Protocol Conformance Testing
150. Adding A Dialect
151. Adding A Settings Backend
152. Implementing A Backend
153. Test Tiers
154. Live Database Harness
155. Backend Name Registry
156. Memtable Rollback Caveat
