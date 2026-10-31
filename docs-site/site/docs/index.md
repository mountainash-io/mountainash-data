---
title: 'Mountainash Data'
description: 'The physical data layer of the mountainash ecosystem: configuration-driven database connections, mountainash expressions over database tables, and portable writes back under transactions, across 20 Ibis dialects.'
---

[← Back to Ecosystem](https://docs.mountainash.io/)

# Mountainash Data

**The physical data layer of the mountainash ecosystem: connections from configuration, database tables for mountainash expressions, and portable writes back, across 20 databases from in-memory DuckDB to PostgreSQL, Snowflake and BigQuery.**

[Ibis](https://ibis-project.org/) already connects Python to each of these engines and compiles queries for them, and a script that only needs a connection and a query can use Ibis on its own. A pipeline needs more around the query. Connection details differ between a laptop, staging and production, and belong in configuration files and environment variables rather than code. Passwords and tokens belong in a secret store. The business logic should run on a local dataframe in tests and on a warehouse table in production, and it often has to join that table with reference data held in Python. Then the results go back to the database, idempotently, as one unit that either lands or rolls back.

`mountainash-data` is the part of the [mountainash ecosystem](https://github.com/mountainash-io) that reaches the database. It turns [`mountainash-settings`](https://github.com/mountainash-io/mountainash-settings) configuration and [`mountainash-auth-client`](https://github.com/mountainash-io/mountainash-auth-client) credentials into a live connection, one `IbisBackend` for every dialect. It hands each table to [`mountainash`](https://github.com/mountainash-io/mountainash), whose expressions are written once and run anywhere: the same `ma.col("total").gt(90)` filters a Polars frame or a database table, and a relation over a table joins Polars frames and lists of dicts in one plan. It then writes the results back with an upsert that renders `ON CONFLICT`, `MERGE` or `ON DUPLICATE KEY` for each dialect and validates the request first, with `add_columns()` and index management, inside `transaction()` scopes that own the transaction or join one the caller already opened. Inspection returns the same typed catalog, namespace, table, column and index records on every engine, and a call a database cannot honour fails with a clear error instead of running something different.

## Who it's for

<div class="grid cards" markdown>

-   **Teams running mountainash expressions on their databases**

    ---

    Hand a database table to `mountainash` with `ma.relation(backend.table(...))`. The same filters, conformance schemas and data contracts that run on Polars frames in tests run as SQL on the warehouse table, and the table joins local frames and Python data in one plan.

    [Run mountainash expressions against databases](chapters/07-running-mountainash-expressions-against-databases/index.md)

-   **Teams deploying by configuration**

    ---

    Describe each database in YAML, TOML, JSON or dotenv files, override it per environment with prefixed variables, and keep passwords as `secret:` references. A `SettingsParameters` recipe carries no resolved credentials and travels to the worker that opens the connection. Moving from DuckDB to PostgreSQL changes the file, not the pipeline.

    [Configure connections with mountainash-settings](chapters/04-configuring-connections-with-mountainash-settings/index.md)

-   **Engineers building pipelines and loaders**

    ---

    Load idempotently with `upsert()`, widen a table with `add_columns()` and manage indexes through the same calls on every dialect that supports them. Wrap the work in `transaction()` scopes, join a transaction your own code opened, or adopt a connection you already have.

    [Follow an end-to-end pipeline](chapters/12-an-end-to-end-pipeline/index.md)

-   **Python developers working with databases directly**

    ---

    Connect by dialect name, URL or typed profile, list and inspect catalogs, namespaces, tables and indexes, and read tables as Ibis expressions. Install only the driver extras you need; a missing driver raises an `ImportError` that names the extra to add.

    [Install and connect](chapters/02-installing-and-connecting/index.md)

-   **Architects and extension authors**

    ---

    See how one class serves 20 dialects by declaring each database's capabilities as data, then add a dialect, a settings backend or an entirely new kind of store behind the same `Backend` protocol.

    [How one backend serves 20 dialects](chapters/14-how-one-backend-serves-20-dialects/index.md)

</div>

## See it work

A one-line configuration file names the database:

```yaml
# warehouse.yaml
DATABASE: warehouse.duckdb
```

The script below reads it through `mountainash-settings`, filters a database table with a `mountainash` expression, joins the result with a local Polars frame, and upserts the joined rows into a second table inside a transaction. Save it as `daily_sales.py` next to the file.

```python
import mountainash as ma
import polars as pl
from mountainash_settings import SettingsParameters

from mountainash_data import IbisBackend
from mountainash_data.core.settings import DuckDBBackendProfile

params = SettingsParameters.create(
    settings_class=DuckDBBackendProfile,
    config_files=["warehouse.yaml"],
    env_prefix="WAREHOUSE_",
)
managers = pl.DataFrame({"region": ["north", "south"], "manager": ["Ada", "Lin"]})

with IbisBackend(params) as backend:
    # Stand-ins for tables another system maintains.
    backend.create_table("sales", [
        {"id": 1, "region": "north", "total": 120},
        {"id": 2, "region": "south", "total": 95},
        {"id": 3, "region": "north", "total": 40},
    ], overwrite=True)
    backend.create_table("big_sales", [{"id": 1, "region": "north", "total": 100}], overwrite=True)
    backend.create_unique_index("big_sales", "id")

    big = (
        ma.relation(backend.table("sales"))
        .filter(ma.col("total").gt(90))
        .join(ma.relation(managers), on="region")
        .sort("id")
        .to_polars()
    )

    with backend.transaction():
        backend.add_columns("big_sales", big)
        backend.upsert("big_sales", big, conflict_columns="id")

    print(backend.dialect, backend.table("big_sales").columns)
    for row in backend.table("big_sales").order_by("id").to_pyarrow().to_pylist():
        print(row)
```

```text
duckdb ('id', 'region', 'total', 'manager')
{'id': 1, 'region': 'north', 'total': 120, 'manager': 'Ada'}
{'id': 2, 'region': 'south', 'total': 95, 'manager': 'Lin'}
```

The dialect came from the profile class and the database file from the configuration; `WAREHOUSE_DATABASE=:memory: python daily_sales.py` overrides the file from the environment and prints the same rows. The filter compiled to DuckDB SQL, `mountainash` uploaded the managers frame and joined it in the database, and in one transaction this package added the `manager` column, updated row 1 and inserted row 2. A PostgreSQL deployment needs a PostgreSQL profile in the configuration and a `PasswordAuthProfile` passed to `connect(auth_profile=...)`; the relation and the writes stay as they are. The examples need `mountainash-data` with its `duckdb` and `polars` extras, plus `mountainash-settings`, `mountainash-auth-client` and `mountainash` (see [Installing and connecting](chapters/02-installing-and-connecting/index.md#choosing-extras)). Not every dialect supports every call, and [Supported databases and limits](chapters/13-supported-databases-and-limits/index.md#support-matrix) lists what each of the 20 supports.

## Where to go next

- Start with [mountainash-data in the mountainash ecosystem](chapters/01-mountainash-data-in-the-ecosystem/index.md) for what each package owns and what this one adds beyond Ibis, then [Installing and connecting](chapters/02-installing-and-connecting/index.md). Both assume intermediate Python and basic SQL; Ibis experience helps but isn't required.
- [An end-to-end pipeline](chapters/12-an-end-to-end-pipeline/index.md) builds the example above out from a configuration file to an inspected result, and runs it unchanged on DuckDB and SQLite.
- The [chapter list](chapters/index.md) shows everything the documentation covers, and the [FAQ](faq.md) and [glossary](glossary.md) answer common questions and define terms.
- [About](about.md) explains why the package exists and the design ideas behind it, and has the license and the link to the code repository.

!!! note "Development baseline"
    This documentation describes the `mountainash-data` 0.1.0 development baseline, which requires Python 3.12 or later and Ibis 12 or later. Until its dependencies are published, install it from development checkouts alongside `mountainash-settings`, `mountainash-auth-client` and, for the ecosystem examples, `mountainash`, as described in [Installing and connecting](chapters/02-installing-and-connecting/index.md).
