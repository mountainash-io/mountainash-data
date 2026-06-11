---
title: About
description: 'Who this manual is for, what you need to know beforehand, and how to get the most out of it.'
---

# About

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

## How to Navigate

- **Read chapters in order** — concepts are introduced in dependency order, so prerequisites are always covered before they are used.
- **Use search** (top right) to jump to a specific class, method, or term.
- **Try the MicroSims** when you encounter them — they are the fastest way to build intuition.
- **Check the [Learning Graph](learning-graph/index.md)** to see how a concept fits into the larger picture.
- **Use the [API Reference](api/index.md)** when you need exact signatures, parameters, and return types.

## About Mountainash

Mountainash is an open-source expression engine and data platform. `mountainash-data` is its database connectivity layer — it connects the expression engine to the world's databases so that the same logic can run on SQLite during development, DuckDB for analytics, Snowflake or BigQuery at enterprise scale, and Iceberg for the lakehouse.

Source code and issue tracker: [github.com/mountainash-io/mountainash](https://github.com/mountainash-io/mountainash)

## Author

Nathaniel Ramm
