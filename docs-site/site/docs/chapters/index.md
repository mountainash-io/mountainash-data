# Chapters


## Overview

1. [What mountainash-data is](./01-what-mountainash-data-is/index.md): What the package does, how its layers fit together, and the ideas the rest of the book builds on.

## Using the package

2. [Installing and connecting](./02-installing-and-connecting/index.md): Install the right extra and open a connection from a dialect name or a URL.
3. [Connection profiles and authentication](./03-connection-profiles-and-authentication/index.md): Configure connections with typed profiles and supply credentials separately at connect time.
4. [Inspecting databases](./04-inspecting-databases/index.md): List and inspect catalogs, namespaces, tables, columns and indexes.
5. [Reading tables and running queries](./05-reading-tables-and-running-queries/index.md): Read tables as Ibis expressions, run queries and SQL, and pass tables to mountainash.
6. [Creating and changing tables](./06-creating-and-changing-tables/index.md): Create, insert into, truncate, upsert, add columns to and rename tables.
7. [Indexes](./07-indexes/index.md): Create, list, check and drop indexes.
8. [Transactions](./08-transactions/index.md): Group operations in transactions and work safely with caller-owned transactions.
9. [Adopting existing connections](./09-adopting-existing-connections/index.md): Wrap a connection you already have while keeping control of its lifecycle.
10. [Supported databases and limits](./10-supported-databases-and-limits/index.md): What each database supports and how unsupported operations fail.

## Architecture and internals

11. [How one backend serves 20 dialects](./11-how-one-backend-serves-20-dialects/index.md): How per-database behaviour is declared as data and dispatched.
12. [From settings to driver arguments](./12-from-settings-to-driver-arguments/index.md): How profiles and credentials become driver arguments and connection URLs.
13. [Transaction and adoption internals](./13-transaction-and-adoption-internals/index.md): How transaction scopes and connection adoption are implemented.

## Extending

14. [Extending mountainash-data](./14-extending-mountainash-data/index.md): Add a database dialect, a settings backend or a new kind of backend.
15. [Contributing and testing](./15-contributing-and-testing/index.md): Repository layout, test tiers and the live-database harness.
