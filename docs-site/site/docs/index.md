---
title: 'Mountainash Data'
description: 'A unified Python interface for connecting to any database or lakehouse through typed settings, consistent metadata inspection, and fluent queries.'
---


[← Back to Ecosystem](https://docs.mountainash.io/)
# Mountainash Data

A single, consistent way to connect to any supported database or lakehouse — import `IbisBackend` for SQL databases across every Ibis dialect, or `IcebergBackend` for Apache Iceberg catalogs.

## Why a Guided Manual?

Most library documentation tells you *what* the API does. This manual explains *why* the library is designed the way it is — why there is a Backend protocol, why settings classes auto-register, why inspection returns frozen dataclasses — so that when you need to extend it or debug a connection, you understand the system rather than just the surface.

## What's Inside

- [**Chapters**](chapters/index.md) — 9 chapters covering everything from foundation concepts through advanced integration patterns

## Who This Is For

Python developers and data engineers who work with multiple database backends and want a single library instead of per-database boilerplate. Whether you are connecting to DuckDB locally, PostgreSQL in staging, or Snowflake in production, this manual will get you productive quickly. See the [About](about.md) page for prerequisites and a full reading guide.



# Package Overview

Mountainash-data is a unified Python library for connecting to any supported database or lakehouse. It wraps every Ibis dialect — DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, and more — behind a single `IbisBackend` class, and provides `IcebergBackend` for Apache Iceberg catalog access. Typed settings classes handle connection configuration and authentication, while a runtime-checkable `Backend` protocol provides a uniform interface for inspecting metadata and running queries.

## Target Audience

Python developers and data engineers who need to connect to multiple database backends (PostgreSQL, Snowflake, BigQuery, DuckDB, etc.) and Apache Iceberg catalogs through a single, consistent API with typed configuration.

## Who This Is For

Python developers and data engineers who need to connect to multiple database backends — PostgreSQL, Snowflake, BigQuery, DuckDB, and others — and Apache Iceberg catalogs through a single, consistent API with typed configuration. You might be building data pipelines that need to run against different backends in dev versus production, or you might be a platform engineer who wants one library for every warehouse in the organisation.

## What You Should Already Know

You should be comfortable with:

- Python at an intermediate level — protocols, dataclasses, and decorators in particular
- Basic SQL and database concepts: schemas, tables, catalogs
- At least one Python database client library (e.g. `psycopg2`, `snowflake-connector-python`, `duckdb`)
- General ideas around connection pooling and credential management

## What You'll Get Out of This

After working through this manual, you will know how to:

- Connect to any supported database with a typed settings object and start querying immediately
- Switch between dev, staging, and production backends by swapping a single settings object — no code changes
- Inspect catalogs, namespaces, tables, and columns through a uniform metadata API that works the same across every backend
- Configure authentication for real-world setups: OAuth2, IAM, service accounts, certificates
- Extend the library with new database backends by implementing the Backend protocol and registering settings classes
- Use IcebergBackend to connect to Apache Iceberg catalogs (REST, Hive, Glue) alongside your SQL backends

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
