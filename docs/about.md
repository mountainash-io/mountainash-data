---
title: About
description: 'Who this manual is for, what you need to know beforehand, and how to get the most out of it'
---

# About

## Who This Is For

This manual is for Python developers and data engineers who connect to multiple database backends — PostgreSQL, Snowflake, BigQuery, DuckDB, and others — or Apache Iceberg catalogs, and want a single, consistent API instead of per-database boilerplate. You might be building data pipelines that need to run unchanged against dev, staging, and production backends, or you might be a platform engineer standardizing connectivity for every warehouse in the organisation. If you write code that talks to more than one database, this is for you.

## What You Should Already Know

You should be comfortable with:

- Python at an intermediate level — protocols, dataclasses, and decorators in particular
- Basic SQL and database concepts: schemas, tables, catalogs
- At least one Python database client library (e.g. `psycopg2`, `snowflake-connector-python`, `duckdb`)
- General ideas around connection pooling and credential management

## What You'll Get Out of This

After working through this manual, you'll know how to:

- Connect to any supported database with a typed settings object and start querying immediately
- Switch between dev, staging, and production backends by swapping a single settings object — no code changes
- Inspect catalogs, namespaces, tables, and columns through a uniform metadata API that works the same across every backend
- Configure authentication for real-world setups: OAuth2, IAM, service accounts, certificates
- Extend the library with new database backends by implementing the `Backend` protocol and registering settings classes
- Use `IcebergBackend` to connect to Apache Iceberg catalogs (REST, Hive, Glue) alongside your SQL backends

## How to Navigate

- **Read in order** — chapters are arranged in dependency order, so each builds on what came before
- **Use search** — the search bar (top right) indexes every page; use it to jump to a specific class or concept
- **Try the MicroSims** — interactive simulations are the fastest way to build intuition for how the pieces fit together
- **Check the Learning Graph** — the [Learning Graph](learning-graph/index.md) shows how concepts relate and where each fits in the bigger picture

## About Mountainash Data

Mountainash Data is the intelligent textbook for **mountainash-data**, a unified Python library for connecting to any supported database or lakehouse. It wraps every Ibis dialect — DuckDB, PostgreSQL, Snowflake, BigQuery, Trino, ClickHouse, and more — behind a single `IbisBackend` class, and provides `IcebergBackend` for Apache Iceberg catalog access. A runtime-checkable `Backend` protocol gives every backend the same connect, inspect, and query surface, and typed settings classes with auto-registration handle connection configuration and authentication.

Within the mountainash ecosystem, mountainash-data owns the *physical* layer — connections, authentication, schema inspection, and table enumeration. It deliberately does not build queries or expressions; that logical layer belongs to mountainash-expressions, which consumes the connections and metadata this library produces.

This manual explains the design as much as the API: why there is a `Backend` protocol instead of a class hierarchy, why settings classes auto-register instead of being wired up by hand, and why inspection returns frozen dataclasses. Understanding those decisions is what lets you extend the library or debug a connection with confidence, rather than just calling methods you don't fully trust.

Source code and issues: [github.com/mountainash-io/mountainash-data](https://github.com/mountainash-io/mountainash-data)

## License

Copyright &copy; 2026 Nathaniel Ramm. Licensed under [CC BY-NC-SA 4.0](license.md) for non-commercial use — see the [License](license.md) page for the full terms. Commercial licensing is reserved by the copyright holder; see [Contact](contact.md) for inquiries.

## Author

Nathaniel Ramm
