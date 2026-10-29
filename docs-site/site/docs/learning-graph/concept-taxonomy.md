# Concept Taxonomy

This taxonomy organizes the 100 mountainash-data concepts into 9 categories.

## Categories

### FOUND — Foundation Concepts
Prerequisites: SQL, schemas, catalogs, connections, Python protocols, Pydantic, decorators.

### PROTO — Backend Protocol
Runtime-checkable Backend protocol defining the universal backend contract.

### IBIS — Ibis Backend
IbisBackend class for 20+ SQL databases with fluent queries, DDL, and DML.

### ICE — Iceberg Backend
IcebergBackend for Apache Iceberg catalogs (REST, Hive, Glue, SQL).

### INSP — Inspection Model
Four frozen metadata dataclasses for backend-agnostic catalog/schema/table/column info.

### DIAL — Dialect System
DialectSpec registry for backend-specific connection builders and operation hooks.

### SETT — Settings & Configuration
Per-backend AuthSettings with auto-registration, BackendSpec, ParameterSpec.

### ADAPT — Adapters & Auth
Credential transformation pipeline for OAuth, JWT, cloud-native auth, SSL.

### ADV — Advanced Features
Cross-backend queries, capability matrix, known limitations.

## Taxonomy Summary Table

| TaxonomyID | Category Name | Concept Range | Count |
|------------|---------------|---------------|-------|
| FOUND | Foundation Concepts | 1-10 | 10 |
| PROTO | Backend Protocol | 11-18 | 8 |
| IBIS | Ibis Backend | 19-34 | 16 |
| ICE | Iceberg Backend | 35-48 | 14 |
| INSP | Inspection Model | 49-56 | 8 |
| DIAL | Dialect System | 57-66 | 10 |
| SETT | Settings & Configuration | 67-84 | 18 |
| ADAPT | Adapters & Auth | 85-94 | 10 |
| ADV | Advanced Features | 95-100 | 6 |
