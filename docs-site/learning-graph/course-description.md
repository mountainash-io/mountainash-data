---
title: Mountainash Data Package Description
description: A detailed description of the mountainash-data unified database and lakehouse connectivity library
quality_score: 87
---

# Mountainash Data Package Description

## Title

Mountainash Data: Unified Database and Lakehouse Connectivity Library

## Target Audience

Python developers and data engineers who need to connect to multiple database backends (PostgreSQL, Snowflake, BigQuery, DuckDB, etc.) and Apache Iceberg catalogs through a single, consistent API with typed configuration.

## Prerequisites

- Intermediate Python (protocols, dataclasses, decorators)
- Basic SQL and database concepts (schemas, tables, catalogs)
- Familiarity with at least one database client library
- Understanding of connection pooling and credential management

## Topics Covered

1. **Backend Protocol** — Runtime-checkable structural protocol defining the universal backend contract
2. **Ibis Backend** — IbisBackend class for 20+ SQL databases with fluent queries and DDL/DML
3. **Iceberg Backend** — IcebergBackend for Apache Iceberg catalog access (REST, Hive, Glue)
4. **Inspection Model** — CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo metadata dataclasses
5. **Settings & Configuration** — Per-backend AuthSettings with auto-registration via @register decorator
6. **Dialect System** — DialectSpec registry for backend-specific connection and operation routing
7. **Adapters** — Credential transformation pipeline for OAuth, JWT, cloud-native auth, SSL
8. **BackendSpec & Registry** — Typed parameter specifications and centralized backend discovery

## Topics Excluded

- Database administration and performance tuning
- SQL query optimization
- Data warehouse modeling
- ETL/ELT pipeline orchestration
- Cloud provider account management

## Learning Outcomes

After studying this package, developers will be able to:

### Remember

- List the four inspection dataclasses (CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo)
- Name the Backend protocol's required methods
- Identify the two backend types (IbisBackend, IcebergBackend)

### Understand

- Explain the structural typing approach via runtime-checkable Backend protocol
- Describe dialect-driven routing through DialectSpec
- Explain the adapter pipeline for credential transformation

### Apply

- Connect to databases using typed settings classes
- Inspect catalogs, schemas, and tables via the unified API
- Execute queries using both fluent Ibis expressions and raw SQL

### Analyze

- Compare IbisBackend and IcebergBackend capabilities and tradeoffs
- Analyze the settings registration and auto-discovery pipeline

### Evaluate

- Assess which backend type suits a given data architecture
- Evaluate settings class designs for new database providers

### Create

- Implement new database backend settings via the @register pattern
- Build custom Iceberg catalog type implementations
- Design adapter functions for new authentication schemes

## Context

Mountainash-data provides a unified Python API for connecting to any supported database or lakehouse. The Backend protocol defines the universal contract, IbisBackend covers 20+ SQL databases through Ibis, and IcebergBackend handles Apache Iceberg catalogs. Typed settings classes with auto-registration enable consistent, validated configuration across all providers.
