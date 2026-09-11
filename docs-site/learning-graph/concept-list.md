# Concept List for Mountainash Data

Total concepts: 100

## Foundation Concepts (1-10)

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

## Backend Protocol (11-18)

11. Backend Protocol Definition
12. Connect Method
13. Close Method
14. List Tables Method
15. Inspect Table Method
16. List Namespaces Method
17. Inspect Namespace Method
18. Inspect Catalog Method

## Ibis Backend (19-34)

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

## Iceberg Backend (35-48)

35. IcebergBackend Class
36. Apache Iceberg Overview
37. Iceberg Connection Base
38. REST Catalog Type
39. Hive Catalog Type
40. Glue Catalog Type
41. SQL Catalog Type
42. Catalog Type Registry
43. Iceberg Operations
44. List Tables Iceberg
45. Table Inspection Iceberg
46. Namespace Inspection Iceberg
47. Catalog Inspection Iceberg
48. PyIceberg Library

## Inspection Model (49-56)

49. CatalogInfo Dataclass
50. NamespaceInfo Dataclass
51. TableInfo Dataclass
52. ColumnInfo Dataclass
53. Frozen Metadata Model
54. Backend Agnostic Metadata
55. Driver Metadata Conversion
56. Unified Inspection API

## Dialect System (57-66)

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

## Settings & Configuration (67-84)

67. ConnectionProfile Base
68. To Driver Kwargs Method
69. BackendSpec Class
70. ParameterSpec Class
71. Parameter Tiers
72. DATABASES REGISTRY
73. Register Decorator
74. Auto Registration
75. SQLiteAuthSettings
76. DuckDBAuthSettings
77. PostgreSQLAuthSettings
78. BigQueryAuthSettings
79. SnowflakeAuthSettings
80. RedshiftAuthSettings
81. ClickhouseAuthSettings
82. DatabricksAuthSettings
83. MSSQLAuthSettings
84. TrinoAuthSettings

## Adapters & Auth (85-94)

85. Adapter Pipeline
86. Credential Transformation
87. OAuth Adapter
88. JWT Adapter
89. Cloud Native Auth
90. SSL Bundle Adapter
91. NoAuth Settings
92. PasswordAuth Settings
93. TokenAuth Settings
94. IAMAuth Settings

## Advanced Features (95-100)

95. Cross Backend Queries
96. Backend Capability Matrix
97. DDL Index Support
98. Memory Intensive Upsert
99. REST Catalog Cursors
100. Unimplemented Operations
