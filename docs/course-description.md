---
title: Package Overview
description: 'What mountainash-data is, who it is for, what it covers, and what it does not cover.'
---

# Package Overview

Mountainash-data is a unified Python library for connecting to any supported database or lakehouse. It wraps every Ibis dialect — DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, and more — behind a single `IbisBackend` class, and provides `IcebergBackend` for Apache Iceberg catalog access. Typed settings classes handle connection configuration and authentication, while a runtime-checkable `Backend` protocol provides a uniform interface for inspecting metadata and running queries.

## Target Audience

Python developers and data engineers who need to connect to multiple database backends (PostgreSQL, Snowflake, BigQuery, DuckDB, etc.) and Apache Iceberg catalogs through a single, consistent API with typed configuration.

## Prerequisites

- Intermediate Python (protocols, dataclasses, decorators)
- Basic SQL and database concepts (schemas, tables, catalogs)
- Familiarity with at least one database client library
- Understanding of connection pooling and credential management

## What This Manual Covers

1. **Foundation Concepts** — Core abstractions and design decisions underpinning the library
2. **Backend Protocol** — Runtime-checkable structural protocol defining the universal backend contract
3. **Inspection Model** — CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo metadata dataclasses
4. **Ibis Backend** — IbisBackend class for 20+ SQL databases with fluent queries and DDL/DML
5. **Dialect System** — DialectSpec registry for backend-specific connection and operation routing
6. **Settings and Configuration** — Per-backend AuthSettings with auto-registration via @register decorator
7. **Adapters and Auth** — Credential transformation pipeline for OAuth, JWT, cloud-native auth, SSL
8. **Iceberg Backend** — IcebergBackend for Apache Iceberg catalog access (REST, Hive, Glue)
9. **Advanced Integration** — Combining backends, extending the library, and production deployment patterns

## What This Manual Does Not Cover

- Database administration and performance tuning
- SQL query optimization
- Data warehouse modeling
- ETL/ELT pipeline orchestration
- Cloud provider account management

## Key Capabilities

**Every Ibis dialect through one interface** — IbisBackend provides a unified connection, inspection, and query interface across every SQL and analytical engine Ibis supports: DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, MySQL, Oracle, SQL Server, Databricks, and more. When Ibis adds a new backend, mountainash-data inherits it.

**Native Iceberg lakehouse support** — IcebergBackend connects to Apache Iceberg catalogs (REST, Hive, Glue, SQL) so the same code that validates and transforms data can store and query it in a production lakehouse. Tables, metadata inspection, and operations all work through the Backend protocol.

**Typed connection settings for every backend** — Each database dialect has typed settings classes with auto-derived connection parameters. Authentication spans the full spectrum: password, token, OAuth2, IAM, service accounts, and certificates. Settings compose with mountainash-settings for config file loading and secrets resolution.

**A protocol that any data store can implement** — The Backend protocol defines connect, close, inspect, and query operations. Any data store that implements this protocol participates in the mountainash ecosystem — expressions, rules, and validation all work against it automatically.

**Development to production without rewrites** — Develop pipelines against DuckDB on your laptop, point at Snowflake in staging, and BigQuery in production. The expressions compile through Ibis to each backend's native dialect. The backend is a deployment config, not a code change.

## Context

Mountainash-data provides a unified Python API for connecting to any supported database or lakehouse. The Backend protocol defines the universal contract, IbisBackend covers 20+ SQL databases through Ibis, and IcebergBackend handles Apache Iceberg catalogs. Typed settings classes with auto-registration enable consistent, validated configuration across all providers.
