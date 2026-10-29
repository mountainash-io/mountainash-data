# Chapters


## Overview

1. [mountainash-data in the mountainash ecosystem](./01-mountainash-data-in-the-ecosystem/index.md): Where the package sits between mountainash-settings, mountainash expressions and your databases, and what it adds beyond Ibis.

## Using the package

2. [Installing and connecting](./02-installing-and-connecting/index.md): Install the right extra and open a connection from a dialect name or a URL.
3. [Connection profiles and authentication](./03-connection-profiles-and-authentication/index.md): Configure connections with typed profiles and supply credentials separately at connect time.
4. [Configuring connections with mountainash-settings](./04-configuring-connections-with-mountainash-settings/index.md): Load connection settings from config files, the environment and secret stores, and carry them to where work runs.
5. [Inspecting databases](./05-inspecting-databases/index.md): List and inspect catalogs, namespaces, tables, columns and indexes.
6. [Reading tables and running queries](./06-reading-tables-and-running-queries/index.md): Read tables as Ibis expressions, run queries and SQL, and compile to SQL.
7. [Running mountainash expressions against databases](./07-running-mountainash-expressions-against-databases/index.md): Run the same mountainash expressions on database tables and local data, and join them in one plan.
8. [Creating and changing tables](./08-creating-and-changing-tables/index.md): Create, insert into, truncate, upsert, add columns to and rename tables.
9. [Indexes](./09-indexes/index.md): Create, list, check and drop indexes.
10. [Transactions](./10-transactions/index.md): Group operations in transactions and work safely with caller-owned transactions.
11. [Adopting existing connections](./11-adopting-existing-connections/index.md): Wrap a connection you already have while keeping control of its lifecycle.
12. [An end-to-end pipeline](./12-an-end-to-end-pipeline/index.md): Configure, connect, combine database and local data with mountainash expressions, and write results back in a transaction.
13. [Supported databases and limits](./13-supported-databases-and-limits/index.md): What each database supports and how unsupported operations fail.

## Architecture and internals

14. [How one backend serves 20 dialects](./14-how-one-backend-serves-20-dialects/index.md): How per-database behaviour is declared as data and dispatched.
15. [From settings to driver arguments](./15-from-settings-to-driver-arguments/index.md): How profiles and credentials become driver arguments and connection URLs.
16. [Transaction and adoption internals](./16-transaction-and-adoption-internals/index.md): How transaction scopes and connection adoption are implemented.

## Extending

17. [Extending mountainash-data](./17-extending-mountainash-data/index.md): Add a database dialect, a settings backend or a new kind of backend.
18. [Contributing and testing](./18-contributing-and-testing/index.md): Repository layout, test tiers and the live-database harness.
