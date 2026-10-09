# mountainash-data

![Python](https://img.shields.io/badge/python-3.12%2B-blue)
[![License: Proprietary](https://img.shields.io/badge/license-proprietary-lightgrey)](LICENSE)

Connect to relational databases, inspect their physical metadata and manage tables
through `IbisBackend`. Ibis supplies backend-native table expressions; the package
adds connection profiles, namespace handling, dialect-specific writes and
transaction ownership behind a backend-neutral `Backend` protocol.

[Examples](examples/) · [Testing](TESTING.md) ·
[Contributing](CONTRIBUTING.md)

## What it provides

| Area | Capabilities | Explore |
|---|---|---|
| Connections | Dialect kwargs, connection URLs and settings recipes; separate authentication profiles | [Connections](examples/connections/) |
| Tables and queries | Create, insert, read and drop tables; execute Ibis expressions | [Local tables](examples/local_tables/) |
| Physical metadata | Catalogs, namespaces, tables and columns with typed inspection results | [Inspection](examples/inspection/) |
| Mutations | Upsert and add columns using dialect-specific SQL | [Mutations](examples/mutations/) |
| Indexes | Create, inspect and drop supported index types | [Indexes](examples/indexes/) |
| Transactions | Commit/rollback scopes and joining caller-owned transactions | [Transactions](examples/transactions/) |
| Connection adoption | Wrap existing Ibis or raw driver connections with explicit ownership | [Connection adoption](examples/connection_adoption/) |

`table()` returns an Ibis table. Build queries with Ibis, or pass that table to
`mountainash.relation()` when using the separate `mountainash` package. This
package does not plan automatic joins between different database connections.

## Installation

Requires **Python 3.12+** and **Ibis 12+**. The base package depends on
`mountainash-settings` 0.1 and `mountainash-auth-client` 0.1, but installs no database
driver or dataframe library. Add the extra for each backend you use.

This README describes the **0.1.0 development baseline**. Until the runtime
dependency chain is published, install development checkouts together. From the
directory that will contain the repositories:

```bash
git clone --branch develop https://github.com/mountainash-io/mountainash-settings.git
git clone --branch develop https://github.com/mountainash-io/mountainash-auth-client.git
git clone --branch develop https://github.com/mountainash-io/mountainash-data.git
cd mountainash-data
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e ../mountainash-settings -e ../mountainash-auth-client \
  -e '.[sqlite,duckdb]'
```

That installation runs the quick start and every local recipe. For Polars inputs,
add `polars` to the extra list. Each dialect extra installs its matching Ibis
backend dependencies; connecting without a required driver raises an `ImportError`
that names the package extra to install.

| Extra | Backend or input |
|---|---|
| `sqlite`, `duckdb` | Local databases used by the recipes |
| `motherduck` | MotherDuck through DuckDB |
| `postgres`, `redshift`, `materialize` | PostgreSQL-family backends; includes `psycopg[binary]` |
| `mysql`, `singlestoredb`, `mssql`, `oracle` | MySQL/MariaDB, SingleStore, SQL Server and Oracle |
| `snowflake`, `bigquery`, `databricks` | Hosted analytics services |
| `trino`, `clickhouse`, `exasol`, `impala`, `risingwave`, `druid`, `pyspark` | Other registered Ibis backends |
| `polars` | Polars dataframe input |
| `all` | Every backend extra and Polars |

Some extras also need system software: SQL Server needs an ODBC driver manager and
the Microsoft ODBC driver; MySQL may need MySQL/MariaDB client development
libraries when no wheel is available; local PySpark needs Java; a source build of
RisingWave's `psycopg2` needs `pg_config`. The `psycopg[binary]` extras bundle libpq.

## Quick start

Save this as `quickstart.py` and run `python quickstart.py`:

```python
import pyarrow as pa

from mountainash_data import IbisBackend

sales = pa.table({"id": [1, 2], "region": ["north", "south"], "total": [120, 95]})

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("sales", sales)
    table = backend.table("sales")
    rows = table.order_by("id").to_pyarrow().to_pylist()
    print(rows)
    print(f"Sales total: {backend.run_expr(table.total.sum())}")
```

Expected output:

```text
[{'id': 1, 'region': 'north', 'total': 120}, {'id': 2, 'region': 'south', 'total': 95}]
Sales total: 215
```

The context manager connects and closes the backend. The in-memory database is
private to this connection; the example needs no credentials or persistent files.
PyArrow is installed by the selected backend extras.

## Backend support and limits

A registered dialect or installable extra does not guarantee every operation.
Upsert styles, schema changes, indexes, namespaces and transaction support depend
on the backend and, for services such as Trino, its configured connector.
Unsupported operations can raise `NotImplementedError`; unsupported namespace
shapes raise `ValueError`.

- `Namespace` represents a catalog and namespace path. Ibis operations support at
  most one namespace level. Manual-SQL operations such as upsert, add-columns and
  indexes reject catalog-qualified namespaces.
- Oracle index operations have unresolved identifier and owner-resolution issues;
  do not treat them as supported workflows.
- Exasol's system-managed indexes and non-B-tree index families such as ClickHouse
  data-skipping indexes are outside the current index API.
- Raw connection adoption is restricted to verified dialects. Otherwise construct
  an Ibis connection and use `from_ibis_connection()`.
- The recipes exercise local SQLite and DuckDB. They do not qualify hosted
  services, every operation on every dialect, or a public-release Python matrix.

## Transaction ownership

On SQLite, DuckDB and PostgreSQL, `backend.transaction()` checks the driver's
native state. It begins and completes a transaction on an idle connection, or
joins an existing caller transaction without committing or rolling it back.
Nested scopes are flat, with no savepoints. An owned scope rolls back on failure,
including a failed protected package call whose exception you caught inside it.

`in_transaction()` reports an active package scope.
`native_transaction_open()` reports native driver state, or `None` when the
backend cannot determine it. PostgreSQL with autocommit disabled may open a
transaction implicitly on a read, so a later package scope joins that transaction.
The package does not change autocommit.

Package calls are protected inside a scope. Operations issued directly on a raw
handle or returned Ibis expression bypass those protections. Ending the native
transaction that way can raise `TransactionIntegrityError` when the scope exits;
it cannot undo an earlier raw commit. Other dialects retain their existing
outermost-scope begin/commit behavior where transactions are supported.

Ibis can retain stale memtable registration after rollback: reusing the same
in-memory expression may fail. No package workaround is installed. See the
[transaction recipe](examples/transactions/) for a local commit/rollback example
and [connection adoption](examples/connection_adoption/) for caller ownership.

## Learn more

The [database recipes](examples/) run offline and independently, using the same
small sales dataset. Each documents its command, requirements and expected output.

| Guide | Contents |
|---|---|
| [Examples](examples/) | Connections, tables, metadata, mutations, indexes and transaction ownership |
| [Testing](TESTING.md) | Test tiers, live-database harness and type checks |
| [Contributing](CONTRIBUTING.md) | Development and pull requests |
| [Textbook maintenance](docs-site/README.md) | Preview, publishing and refresh workflow for generated material |

Textbook: [production](https://docs.mountainash.io/mountainash-data/) ·
[development](https://docs.mountainash.io/mountainash-data/dev/).

## Contributing

Branch from `develop` and open a pull request targeting `develop`. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [TESTING.md](TESTING.md) for setup and checks.
Documentation changes should run the README quick start and affected recipes.

Part of the [Mountain Ash ecosystem](https://github.com/mountainash-io), alongside
`mountainash-settings`, `mountainash-auth-client`, `mountainash-transport`,
`mountainash-files` and `mountainash`. Settings owns configuration recipes;
auth-client owns authentication profiles and flows.

## License

Proprietary; see [LICENSE](LICENSE). Public-release licensing is a separate release
prerequisite; this documentation does not change the current license.
