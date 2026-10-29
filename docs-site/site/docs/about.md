---
title: 'About'
description: 'Why mountainash-data exists, the design ideas behind it, where it fits, and who maintains it'
---

[← Back to Home](index.md)

# About mountainash-data

**Ibis compiles your queries for each database. mountainash-data decides how you connect, what you can see and change, and who owns the transaction.**

## Why it exists

[Ibis](https://ibis-project.org/) already gives Python one expression language over many SQL engines. Build a query once and Ibis compiles it for DuckDB, PostgreSQL, Snowflake or BigQuery. That covers reading. It covers less of the work around reading: opening the connection with the right arguments and credentials, finding out which schemas, tables and indexes exist, and changing tables in ways each engine spells differently.

That work is where database code tends to fork per engine. Connection arguments and credentials differ for each driver. An idempotent load needs `INSERT ... ON CONFLICT` on PostgreSQL, `INSERT ... ON DUPLICATE KEY UPDATE` on MySQL and `MERGE` on SQL Server or Snowflake. Index DDL and `ALTER TABLE` forms vary. When a library and its caller both issue `BEGIN` and `COMMIT` on the same connection, it becomes unclear which of them owns the transaction.

mountainash-data answers with one handle per connection, `IbisBackend`. Each database's differences are declared as data in a dialect registry, so the same method calls work across all 20 registered dialects, and a call that a dialect cannot honour raises an error instead of quietly doing something else.

## What it adds to Ibis

Ibis supplies the connections to each engine and the table expressions you query with. mountainash-data adds:

| Area | What you get |
|---|---|
| Connections | One constructor that takes a dialect name with driver arguments, a URL, or a typed connection profile, with credentials passed separately to `connect()` |
| Metadata | Typed, frozen `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo` and `IndexInfo` records, and a `Namespace` value for catalog and schema paths |
| Table changes | `upsert()` across three SQL families, `add_columns()`, `rename_table()` and `truncate()`, rendered for each dialect |
| Indexes | `create_index()`, `list_indexes()`, `index_exists()` and `drop_index()` on dialects that support them |
| Transactions | `transaction()` scopes that commit or roll back, `in_transaction()` and `native_transaction_open()` checks, and joining a transaction the caller already opened on SQLite, DuckDB and PostgreSQL |
| Adoption | `from_ibis_connection()` and `from_raw_connection()` wrap a connection you already have, without taking ownership of it by default |

## Design ideas

One handle per connection. You construct an `IbisBackend`, connect it, use it for inspection, reads and writes, and close it, either explicitly or with a `with` block. Every method on that object refers to the same live session, so the transaction you open and the upsert you run inside it share one driver connection. [Installing and connecting](chapters/02-installing-and-connecting/index.md) shows the lifecycle.

Database differences are data, not subclasses. Each dialect has one registry entry that declares its upsert style, index capabilities, transaction support and any dialect-specific hooks. An operation uses a dialect's hook if it has one, otherwise the shared implementation for the declared capability; a dialect that declares no support raises `NotImplementedError`. You can read what a database supports from the [support matrix](chapters/13-supported-databases-and-limits/index.md#support-matrix) rather than discovering it at run time. [How one backend serves 20 dialects](chapters/14-how-one-backend-serves-20-dialects/index.md) explains the dispatch.

The connection profile carries no credentials. A backend profile such as `PostgreSQLBackendProfile` describes where a database is and how to reach it. Passwords, tokens, certificates and service accounts arrive separately, as an auth profile from `mountainash-auth-client`, when you call `connect()`. The profile itself stays free of secrets, and the same profile works with whichever compatible credentials each environment supplies. See [Connection profiles and authentication](chapters/03-connection-profiles-and-authentication/index.md).

Ownership is explicit. A backend you construct owns its connection and closes it. A backend that adopts your connection leaves it open on `close()` unless you pass `owns_connection=True`. A `transaction()` scope that finds your own transaction already open on SQLite, DuckDB or PostgreSQL joins it and never commits or rolls it back for you. Nested scopes are flat, with no savepoints. [Transactions](chapters/10-transactions/index.md) and [Adopting existing connections](chapters/11-adopting-existing-connections/index.md) cover both.

The package stops at the physical table. `table()` returns a backend-native Ibis table. Query it with Ibis, or pass it to `mountainash.relation()` if you use the separate `mountainash` package; mountainash-data neither imports that package nor builds logical query plans. It renders SQL only for the physical changes you ask for, such as an upsert, an index or a new column. [Reading tables and running queries](chapters/06-reading-tables-and-running-queries/index.md#bridging-to-mountainash-relations) shows the handoff.

## Where it fits

mountainash-data is part of the [Mountain Ash ecosystem](https://github.com/mountainash-io) of Python packages, and owns physical access to databases: connections, metadata inspection, table changes, indexes and transactions. It builds on two other packages in the ecosystem. [`mountainash-settings`](https://github.com/mountainash-io/mountainash-settings) owns typed settings, configuration files and secret stores, and supplies the profile primitives the connection profiles are built on. [`mountainash-auth-client`](https://github.com/mountainash-io/mountainash-auth-client) owns the auth profile types and authentication flows.

Logical relations and expressions live in [`mountainash`](https://github.com/mountainash-io/mountainash), which consumes the Ibis tables this package returns. The package does not join tables across different database connections, and has no plans to.

## Who maintains it

mountainash-data is written and maintained by Nathaniel Ramm. The source code, issue tracker and pull requests are at [github.com/mountainash-io/mountainash-data](https://github.com/mountainash-io/mountainash-data). The repository's [`examples/`](https://github.com/mountainash-io/mountainash-data/tree/develop/examples) directory holds runnable recipes for connections, tables, inspection, table changes, indexes, transactions and connection adoption; each runs offline on SQLite or DuckDB and documents its expected output.

To correct a page in this documentation, use the edit icon at the top of the page to open its source on GitHub and propose a change. Questions and feedback can also go through the [Contact](contact.md) page.

## License

mountainash-data is licensed under the [Apache License, Version 2.0](license.md). Copyright 2024-2026 Mountain Ash Solutions Pty. Ltd.
