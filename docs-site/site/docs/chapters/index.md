# Chapters

This textbook covers the mountainash-data unified database and lakehouse connectivity library across 9 chapters, progressing from foundational concepts through the Backend protocol, inspection model, Ibis and Iceberg backends, dialect system, settings, adapters, and advanced integration patterns.

## Chapter List

1. [Foundation Concepts](./01-foundation-concepts/index.md) — SQL databases, Python protocols, Pydantic models, and the registry pattern
2. [Backend Protocol](./02-backend-protocol/index.md) — The runtime-checkable Backend protocol defining the universal contract
3. [Inspection Model](./03-inspection-model/index.md) — Frozen metadata dataclasses for backend-agnostic catalog introspection
4. [Ibis Backend](./04-ibis-backend/index.md) — IbisBackend class with fluent queries, DDL, DML, and inspection
5. [Dialect System](./05-dialect-system/index.md) — DialectSpec registry for backend-specific connection and operation routing
6. [Settings and Configuration](./06-settings-and-configuration/index.md) — ConnectionProfile, BackendSpec, ParameterSpec, DATABASES registry, and provider auth settings
7. [Adapters and Auth](./07-adapters-and-auth/index.md) — Credential transformation pipeline for OAuth, JWT, cloud-native auth, and SSL
8. [Iceberg Backend](./08-iceberg-backend/index.md) — IcebergBackend for Apache Iceberg catalogs with REST, Hive, Glue, and SQL catalog types
9. [Advanced Integration](./09-advanced-integration/index.md) — Cross-backend queries, capability matrix, Iceberg operations, and known limitations
