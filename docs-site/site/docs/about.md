---
title: 'About'
description: 'Why mountainash-data exists, the design ideas behind it, where it fits, and who maintains it'
---

[← Back to Home](index.md)

# About mountainash-data

**Ibis compiles your queries for each database. mountainash-data connects those databases to your configuration, your mountainash logic and the writes that land the results.**

!!! warning "Experimental"
    mountainash-data 0.1.0 is experimental and not yet ready for production use. Its APIs can change between versions, and it is not yet published on PyPI.

## Why it exists

The mountainash ecosystem lets you write data logic once and run it anywhere. A `mountainash` expression, conformance schema or data contract is defined without naming an engine, and runs on a Polars frame in a test or a notebook. mountainash-data is what connects that logic to the world's databases. Every relation built over one of its tables compiles to that database's SQL, so the same logic runs on SQLite or DuckDB during development and on PostgreSQL, Snowflake or BigQuery in production.

[Ibis](https://ibis-project.org/) already connects Python to those engines and compiles queries for them, and code that only needs a connection and a query can use Ibis directly. The work this package takes on sits around the query. A data engineer develops a pipeline against DuckDB on a laptop, and in staging and production the same pipeline should point at a warehouse by changing configuration, not code. The credentials for that warehouse should come from a secret store and never sit in the configuration file. A platform team wants to run the same contract against each team's database. The pipeline joins warehouse tables with reference data that lives in Python. And the results have to go back into the database idempotently, as one unit, on engines that spell upsert, `ALTER TABLE` and index DDL differently.

mountainash-data answers with one handle per connection, `IbisBackend`, built from `mountainash-settings` configuration and given `mountainash-auth-client` credentials at connect time. Its tables go to `mountainash` through one call, `ma.relation(backend.table(...))`, and its physical writes run inside transaction scopes. Each database's differences are declared as data in a dialect registry, so the same calls work across all 20 registered dialects, and a call a dialect cannot honour raises an error instead of quietly doing something else.

## What it adds to Ibis

Ibis supplies the connection to each engine, the table expressions you query with, and its own table operations, including `upsert`, `rename_table` and `truncate_table` in Ibis 12. mountainash-data adds:

| Area | What you get |
|---|---|
| Configuration | Typed backend profiles for 20 databases, loaded by `mountainash-settings` from YAML, TOML, JSON and dotenv files, prefixed environment variables and `secret:` references, and carried to workers as `SettingsParameters` recipes |
| Credentials | Auth profiles from `mountainash-auth-client`, checked against what each backend accepts and applied only at `connect()` |
| The mountainash handoff | `table()` returns the native Ibis table that `ma.relation()` takes, so `mountainash` expressions, joins with local data, conformance and contracts run against database tables |
| Writes | `upsert()` rendered as `ON CONFLICT`, `MERGE` or `ON DUPLICATE KEY` per dialect and validated against the target table before any SQL runs, `add_columns()`, and `create_index()`, `list_indexes()`, `index_exists()` and `drop_index()` |
| Transactions and adoption | `transaction()` scopes that own a transaction or join one the caller opened, `in_transaction()` and `native_transaction_open()` checks, and `from_ibis_connection()` and `from_raw_connection()` for connections you already have |
| Metadata | Typed, frozen `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo` and `IndexInfo` records and a `Namespace` value, the same for every backend |

## Design ideas

Configuration in, connection out. A connection is described by deployment sources rather than code, and the object your code carries is a `SettingsParameters` recipe, not resolved values. `IbisBackend(params)` resolves the recipe where the connection is opened, which may be a worker process that received only the recipe. Moving between engines and environments changes a file or an environment variable. [Configuring connections with mountainash-settings](chapters/04-configuring-connections-with-mountainash-settings/index.md) shows file sources, overrides, secret references and recipes crossing process boundaries.

The connection profile carries no credentials. A backend profile such as `PostgreSQLBackendProfile` describes where a database is and how to reach it. Passwords, tokens, certificates and service accounts arrive separately, as an auth profile from `mountainash-auth-client`, when you call `connect()`. The same profile works with whichever compatible credentials each environment supplies, and a backend holds no secrets until it connects. See [Connection profiles and authentication](chapters/03-connection-profiles-and-authentication/index.md).

The package stops at the physical table. mountainash-data owns connections, physical metadata and physical writes; `mountainash` owns expressions, relations and the semantics of combining sources. mountainash-data never imports `mountainash` and has no conversion method: the application makes the handoff with `ma.relation(backend.table(...))`, and results come back to the write methods as ordinary frames. A service that only needs connections and writes does not install `mountainash`. The SQL this package renders is for physical changes you ask for, such as an upsert, an index or a new column. [Running mountainash expressions against databases](chapters/07-running-mountainash-expressions-against-databases/index.md#a-mountainash-relation-over-a-backend-table) shows the handoff.

Database differences are data, not subclasses. Each dialect has one registry entry that declares its upsert family, index capabilities, transaction support and any dialect-specific hooks. An operation uses a dialect's hook if it has one, otherwise the shared implementation for the declared capability, and a dialect that declares no support raises `NotImplementedError`. You can read what a database supports from the [support matrix](chapters/13-supported-databases-and-limits/index.md#support-matrix) rather than discovering it at run time. [How one backend serves 20 dialects](chapters/14-how-one-backend-serves-20-dialects/index.md) explains the dispatch.

Ownership is explicit. A backend you construct owns its connection and closes it. A backend that adopts your connection leaves it open on `close()` unless you pass `owns_connection=True`. A `transaction()` scope that finds your own transaction already open on SQLite, DuckDB or PostgreSQL joins it and never commits or rolls it back for you, and nested scopes are flat, with no savepoints. [Transactions](chapters/10-transactions/index.md) and [Adopting existing connections](chapters/11-adopting-existing-connections/index.md) cover both.

## Where it fits

mountainash-data is part of the [Mountain Ash ecosystem](https://github.com/mountainash-io) of Python packages and is its physical data layer. Four packages split the work:

| Package | Owns |
|---|---|
| [`mountainash-settings`](https://github.com/mountainash-io/mountainash-settings) | The settings base the backend profiles build on, configuration files, environment loading, secret stores and `SettingsParameters` recipes |
| [`mountainash-auth-client`](https://github.com/mountainash-io/mountainash-auth-client) | Auth profile types and authentication flows such as OAuth |
| mountainash-data | Connections and their lifecycle, physical metadata, native table access, physical writes and transaction scopes |
| [`mountainash`](https://github.com/mountainash-io/mountainash) | Expressions, relations, schemas and contracts, and cross-source semantics, including joins between database tables, local data and other connections |

The dependency between the last two runs one way. The `mountainash[ibis]` extra depends on mountainash-data and borrows a relation's Ibis connection through its public API, `IbisBackend.from_ibis_connection()`, for SQL transactions and the owned copies behind `Relation.snapshot()`; base Polars work in `mountainash` does not import this package. [How mountainash borrows a connection](chapters/11-adopting-existing-connections/index.md#how-mountainash-borrows-a-connection) describes the calls it makes.

What deliberately lives elsewhere: Ibis builds query expressions; `mountainash` plans joins across sources and decides where each part of a plan runs; `mountainash-settings` loads configuration and resolves secrets; `mountainash-auth-client` obtains and refreshes tokens.

## Who maintains it

mountainash-data is written and maintained by Nathaniel Ramm. The source code, issue tracker and pull requests are at [github.com/mountainash-io/mountainash-data](https://github.com/mountainash-io/mountainash-data). The repository's [`examples/`](https://github.com/mountainash-io/mountainash-data/tree/develop/examples) directory holds runnable recipes for connections, tables, inspection, table changes, indexes, transactions and connection adoption; each runs offline on SQLite or DuckDB and documents its expected output. [An end-to-end pipeline](chapters/12-an-end-to-end-pipeline/index.md) runs the ecosystem workflow, from a configuration file to an upsert in a transaction, on DuckDB and SQLite.

To correct a page in this documentation, use the edit icon at the top of the page to open its source on GitHub and propose a change. Questions and feedback can also go through the [Contact](contact.md) page.

## License

mountainash-data is licensed under the [Apache License, Version 2.0](license.md). Copyright 2024-2026 Mountain Ash Solutions Pty. Ltd.
