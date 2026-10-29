---
title: 'Mountainash Data'
description: 'One typed Python handle for connecting to, inspecting and writing to relational databases across 20 Ibis dialects, from SQLite and DuckDB to PostgreSQL, Snowflake and BigQuery.'
---

# Mountainash Data

**One Python handle for connecting to, inspecting and writing to relational databases, from an in-memory DuckDB to PostgreSQL, Snowflake or BigQuery.**

Database code usually starts with one driver and a few SQL strings. Then the tests move to SQLite or DuckDB while production runs on PostgreSQL. A second warehouse arrives with its own connection arguments and its own way of handling credentials. The upsert that worked on one engine needs `ON CONFLICT` here, `ON DUPLICATE KEY` there and `MERGE` somewhere else, and nobody is sure which code path owns the transaction.

`mountainash-data` puts all of that behind one class, `IbisBackend`. You choose a database by dialect name, URL or typed connection profile, pass credentials separately when you connect, and then use the same methods everywhere: list and inspect tables, read them as Ibis expressions, create and insert, upsert, add columns, manage indexes and group the work in a transaction. Where a database cannot do something, the call fails with a clear error instead of running something different.

## Who it's for

<div class="grid cards" markdown>

-   **Python developers working with databases**

    ---

    Write data-loading and inspection code once and run it on SQLite or DuckDB in tests and on a server database in production. Install only the driver extras you need; a missing driver raises an `ImportError` that names the extra to add.

    [Install and connect](chapters/02-installing-and-connecting/index.md)

-   **Teams running several databases**

    ---

    Describe each database as a typed connection profile, one of 20 such as `PostgreSQLBackendProfile` or `SnowflakeBackendProfile`, and supply passwords, tokens or service accounts as a separate auth profile at connect time. Configuration files and secret stores stay with `mountainash-settings`.

    [Configure connection profiles](chapters/03-connection-profiles-and-authentication/index.md)

-   **Engineers building pipelines and loaders**

    ---

    Load idempotently with `upsert()`, evolve a table with `add_columns()` and manage indexes through the same calls on every dialect that supports them. Wrap the work in `transaction()` scopes, or join a transaction your own code already opened.

    [Create and change tables](chapters/06-creating-and-changing-tables/index.md)

-   **Architects and extension authors**

    ---

    See how one class serves 20 dialects by declaring each database's capabilities as data, then add a dialect, a settings backend or an entirely new kind of store behind the same `Backend` protocol.

    [How one backend serves 20 dialects](chapters/11-how-one-backend-serves-20-dialects/index.md)

</div>

## See it work

Save this as `load_sales.py`. The function never mentions a database engine; the last two lines run it once on DuckDB and once on SQLite.

```python
from mountainash_data import IbisBackend


def load_sales(backend: IbisBackend) -> None:
    with backend:
        backend.create_table("sales", [
            {"id": 1, "region": "north", "total": 120},
            {"id": 2, "region": "south", "total": 95},
        ])
        backend.create_index("sales", "id", index_name="ux_sales_id", unique=True)
        backend.upsert(
            "sales",
            [
                {"id": 1, "region": "north", "total": 140},
                {"id": 3, "region": "west", "total": 80},
            ],
            conflict_columns="id",
        )
        backend.add_columns("sales", {"note": "string"})
        sales = backend.table("sales")
        print(backend.dialect, sales.columns, backend.run_expr(sales.total.sum()))


load_sales(IbisBackend(dialect="duckdb", database=":memory:"))
# → duckdb ('id', 'region', 'total', 'note') 315
load_sales(IbisBackend("sqlite://"))
# → sqlite ('id', 'region', 'total', 'note') 315
```

Run it with the `sqlite` and `duckdb` extras installed (see [Installing and connecting](chapters/02-installing-and-connecting/index.md#choosing-extras)). On both engines the upsert updated row 1 and inserted row 3, and the `note` column was added, with the index, upsert and schema-change SQL rendered for each dialect by the package. Pointing the function at a server database means changing the line that builds the backend: the package's live-database test suite runs the same public `upsert()` call against PostgreSQL, MySQL, SingleStoreDB, SQL Server and Trino. Not every dialect supports every call (Snowflake, for example, has no index operations), and [Supported databases and limits](chapters/10-supported-databases-and-limits/index.md#support-matrix) lists what each of the 20 dialects supports.

## Where to go next

- Start with [What mountainash-data is](chapters/01-what-mountainash-data-is/index.md) for the package's layers and where Ibis fits, then [Installing and connecting](chapters/02-installing-and-connecting/index.md). Both assume intermediate Python and basic SQL; Ibis experience helps but isn't required.
- The [chapter list](chapters/index.md) shows everything the documentation covers, and the [FAQ](faq.md) and [glossary](glossary.md) answer common questions and define terms.
- [About](about.md) explains why the package exists and the design ideas behind it, and has the license and the link to the code repository.

!!! note "Development baseline"
    This documentation describes the `mountainash-data` 0.1.0 development baseline, which requires Python 3.12 or later and Ibis 12 or later. Until its dependencies are published, install it from development checkouts alongside `mountainash-settings` and `mountainash-auth-client`, as described in [Installing and connecting](chapters/02-installing-and-connecting/index.md).
