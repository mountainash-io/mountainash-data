---
title: Package Overview
description: 'A syllabus-style overview of what mountainash-data does, who it is for, and how the chapters build on each other'
---

# Package Overview

This page is the course syllabus for the manual: what it assumes you already know, what each chapter adds, and what deliberately falls outside its scope. For a narrative introduction to the library itself, start with [Home](index.md); for the full chapter list, see [Chapters](chapters/index.md).

## Audience

The manual targets Python developers and data engineers who need one API in front of several database backends — SQL engines through Ibis (DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, and more) and Apache Iceberg lakehouse catalogs (REST, Hive, Glue) through `IcebergBackend`. It suits engineers who currently maintain separate connection code per database and want to collapse that into a single, typed, protocol-driven library — and just as much suits engineers meeting mountainash-data for the first time as consumers of another mountainash package that depends on it.

## Prerequisites

Before starting, you should be able to:

- Read and write intermediate Python, including protocols, dataclasses, and decorators
- Explain basic SQL and database concepts — schemas, tables, catalogs
- Point to hands-on experience with at least one Python database client (`psycopg2`, `snowflake-connector-python`, `duckdb`, or similar)
- Reason about connection pooling and credential management at a conceptual level

None of these need to be expert-level. The manual introduces the library's own abstractions from first principles; it assumes general programming and database fluency, not prior Ibis or Iceberg experience.

## Learning Outcomes

By the end of the manual, you will be able to:

1. Stand up a connection to any supported backend from a single typed settings object
2. Retarget a pipeline from DuckDB in development to Snowflake or BigQuery in production by swapping configuration, not code
3. Walk a catalog's namespaces, tables, and columns through one metadata API regardless of which backend produced it
4. Configure authentication correctly for password, token, OAuth2, IAM, service-account, and certificate-based setups
5. Implement the `Backend` protocol and register a settings class to add support for a new database
6. Read and write Apache Iceberg tables through `IcebergBackend` alongside your SQL backends

## Syllabus

The nine chapters build in dependency order — each assumes the ones before it:

1. **Foundation Concepts** — why the library is protocol-based rather than class-hierarchy-based, and the core abstractions everything else builds on
2. **Backend Protocol** — the runtime-checkable `Backend` contract every backend, present or future, must satisfy
3. **Inspection Model** — the frozen `CatalogInfo`, `NamespaceInfo`, `TableInfo`, and `ColumnInfo` dataclasses returned by every backend
4. **Ibis Backend** — `IbisBackend`, the single class fronting 20+ SQL dialects, with fluent queries and DDL/DML
5. **Dialect System** — the `DialectSpec` registry that routes connection and operation details per backend without a class per dialect
6. **Settings and Configuration** — auto-registering `AuthSettings` classes and how typed configuration reaches a connection
7. **Adapters and Auth** — the credential transformation pipeline behind OAuth, JWT, cloud-native auth, and SSL
8. **Iceberg Backend** — `IcebergBackend` and Apache Iceberg catalog access across REST, Hive, and Glue
9. **Advanced Integration** — combining backends, extending the library with new ones, and production deployment patterns

See [Chapters](chapters/index.md) for the linked table of contents, and [Home](index.md) for the fuller narrative on why each of these pieces exists.

## Out of Scope

This is a connectivity library, not a data platform, so the manual does not cover:

- Database administration or performance tuning
- SQL query optimization
- Data warehouse modeling
- ETL/ELT pipeline orchestration
- Cloud provider account management

Query building and expression logic over the connections this library provides live one layer up, in mountainash-expressions — mountainash-data deliberately stops at the physical layer of connecting, authenticating, and inspecting.
