---
title: Supported databases and limits
description: What each database supports and how unsupported operations fail.
date: 2026-10-29
---

# Supported databases and limits

`IbisBackend` registers 20 dialects. Connecting, inspecting, creating and dropping tables, inserting, reading and running queries delegate to Ibis, so they work wherever the Ibis backend supports them; upsert, indexes and transaction scopes depend on what each dialect declares, and an operation a dialect does not declare fails with a clear exception before any SQL is sent. These limits also bound what the ecosystem can push to each database: a mountainash relation over a table compiles through the same Ibis backend ([Running mountainash expressions against databases](../07-running-mountainash-expressions-against-databases/index.md)), and writing its results back uses the operations listed here.

A registered dialect or an installable extra does not guarantee every operation. The capability depends on the engine and, for services such as Trino, on the configured connector. The package's own recipes exercise local SQLite and DuckDB; they do not qualify hosted services, every operation on every dialect, or a public-release Python matrix.

## Support matrix

<!-- concept:96 -->
The matrix below is generated from the dialect registry (`DIALECTS` in `src/mountainash_data/backends/ibis/dialects/_registry.py`) at source revision `2d0782b`, and checked against it row by row. Each dialect name is also the name of its package extra: `pip install 'mountainash-data[snowflake]'` installs what the `snowflake` dialect needs.

| Dialect | Upsert style | Transactions | Indexes | Raw adoption | Dialect-specific hooks |
|---|---|---|---|---|---|
| `sqlite` | `on_conflict` | full, joins caller | yes | no | truncate, state probe |
| `duckdb` | `on_conflict` | full, joins caller | yes | yes | index listing, state probe |
| `motherduck` | `on_conflict` | full | yes | yes | index listing |
| `postgres` | `on_conflict` | full, joins caller | yes | no | begin, state probe, commit probe |
| `mysql` | `on_duplicate_key` | full | yes | no | none |
| `mssql` | `merge` | full | yes | no | none |
| `oracle` | `merge` | full | yes | no | none |
| `snowflake` | `merge` | full | no | no | none |
| `bigquery` | `merge` | none | no | no | none |
| `redshift` | `merge` | full | no | no | none |
| `trino` | `merge` | limited | no | no | none |
| `clickhouse` | no | none | no | no | none |
| `databricks` | `merge` | none | no | no | none |
| `singlestoredb` | `on_duplicate_key` | limited | yes | no | none |
| `exasol` | `merge` | full | no | no | connection adapter |
| `impala` | no | none | no | no | none |
| `materialize` | no | full | no | no | none |
| `risingwave` | `on_conflict` | limited | no | no | none |
| `druid` | no | none | no | no | none |
| `pyspark` | no | none | no | no | none |

The columns read as follows.

Upsert style is the SQL family [`upsert()`](../08-creating-and-changing-tables/index.md) renders: `INSERT ... ON CONFLICT`, `MERGE`, or MySQL-style `INSERT ... ON DUPLICATE KEY UPDATE`. "no" means `upsert()` raises `NotImplementedError`. On the `on_duplicate_key` family (MySQL, MariaDB and SingleStore), the package first proves the statement is safe: the conflict columns must exactly match the table's only unique index and must not be nullable. A table that fails the check, or an `update_condition` argument, raises `ValueError`.

Transactions is the dialect's declared support for [`transaction()`](../10-transactions/index.md). "full" and "limited" behave the same way in the package: the outermost scope begins a transaction and commits or rolls it back. "limited" records that the engine's own transaction semantics are partial or connector-dependent. "joins caller" marks the three dialects (SQLite, DuckDB, PostgreSQL) where the scope first probes the driver's native state and joins a transaction the caller already opened instead of starting its own. "none" means `transaction()` raises `TransactionUnsupportedError`, or runs as a no-op with a one-time warning when called with `required=False`. Oracle and Exasol issue no `BEGIN` statement, because their sessions open transactions implicitly.

Indexes is whether `create_index()`, `drop_index()`, `index_exists()` and `list_indexes()` are supported; the next section breaks it down.

Raw adoption is whether [`IbisBackend.from_raw_connection()`](../11-adopting-existing-connections/index.md) accepts a raw driver connection for the dialect. DuckDB and MotherDuck are the only verified dialects, and adoption on them can preserve the `python_enable_replacements` and `timezone` session options that Ibis changes when it adopts a connection. Every other dialect can still adopt a connection you build with Ibis, through `from_ibis_connection()`.

Dialect-specific hooks are the places where a dialect replaces or extends the generic behaviour. SQLite truncates with its own statement, because it has no `TRUNCATE`. DuckDB and MotherDuck list indexes through a dedicated query. PostgreSQL starts owned transactions its own way and checks, before committing, that the transaction has not failed. The state probes read the driver's native transaction state. Exasol's connection is adapted so the generic rename, add-columns and `MERGE` paths can execute SQL through it. Everything else, including `add_columns()` and `rename_table()` on every dialect, uses the generic implementation, rendered for each dialect through SQLGlot. How hooks and generic defaults fit together is the subject of [How one backend serves 20 dialects](../14-how-one-backend-serves-20-dialects/index.md).

Oracle connections are also patched when they are built, so that `drop_table(force=True)`, `create_table(overwrite=True)` and schema inspection produce SQL that Oracle releases before 23ai accept.

## Index support by dialect

<!-- concept:97 -->
Eight dialects declare conventional secondary-index support. The table shows how each one's index SQL is shaped, again generated from the registry:

| Dialect | Drop scope | Partial (`where=`) | Native `IF NOT EXISTS` | Native `IF EXISTS` | `index_type` values |
|---|---|---|---|---|---|
| `sqlite` | `schema_global` | yes | yes | yes | none |
| `duckdb` | `schema_global` | no | yes | yes | none |
| `motherduck` | `schema_global` | no | yes | yes | none |
| `postgres` | `schema_global` | yes | yes | yes | `brin`, `btree`, `gin`, `gist`, `hash`, `spgist` |
| `mysql` | `table_scoped` | no | no | no | `btree` |
| `mssql` | `table_scoped` | yes | no | yes | none |
| `oracle` | `schema_global` | no | no | no | none |
| `singlestoredb` | `table_scoped` | no | no | no | `btree`, `hash` |

A `table_scoped` dialect names the table when dropping an index (`DROP INDEX name ON table`), so `drop_index()` needs `table_name=` there; `schema_global` dialects drop by index name alone. Where a dialect lacks native `IF NOT EXISTS` or `IF EXISTS`, the package checks for the index first, so `if_not_exists=True` and `if_exists=True` still work. A `where=` filter on a dialect without partial indexes, or an `index_type` outside the listed values, is rejected with `ValueError`. "none" in the last column means the dialect takes no `USING` clause, so `index_type` must be left unset. The full API is in [Indexes](../09-indexes/index.md).

Three index limits are not visible in the registry. Oracle declares index capability, but its index operations have unresolved identifier and owner-resolution issues: do not treat them as supported workflows. Exasol manages its indexes itself, and non-B-tree families such as ClickHouse data-skipping indexes are outside the index API; both dialects raise `NotImplementedError` for index operations.

## NotImplementedError and other failure modes

<!-- concept:100 -->
An operation a dialect does not support raises `NotImplementedError` naming the dialect and the operation. The check runs before any SQL is sent, and for `index_exists()` and `from_raw_connection()` it runs before a connection is needed at all, so these calls fail the same way on a laptop with no database server. The other errors in this example come from the same up-front checks:

```python
import sqlite3

from mountainash_data import IbisBackend
from mountainash_data.core.errors import TransactionUnsupportedError

try:
    IbisBackend(dialect="clickhouse").index_exists("ix_sales_region")
except NotImplementedError as exc:
    print(f"NotImplementedError: {exc}")

try:
    IbisBackend.from_raw_connection(sqlite3.connect(":memory:"), dialect="sqlite")
except NotImplementedError as exc:
    print(f"NotImplementedError: {exc}")

try:
    with IbisBackend(dialect="bigquery").transaction():
        pass
except TransactionUnsupportedError as exc:
    print(f"TransactionUnsupportedError: {exc}")

try:
    IbisBackend(dialect="postgres", host="db.example", database="app").connect()
except ImportError as exc:
    print(f"ImportError: {exc}")
```

```text
NotImplementedError: Dialect 'clickhouse' does not support index_exists
NotImplementedError: raw adoption not yet verified for 'sqlite'; construct the ibis connection yourself and use IbisBackend.from_ibis_connection(...).
TransactionUnsupportedError: 'bigquery' has no transaction concept; call transaction(required=False) to run as a best-effort no-op.
ImportError: No module named 'psycopg'

Install this backend with: pip install 'mountainash-data[postgres]'
```

The last case ran without the `postgres` extra installed. A missing driver surfaces at `connect()` as `ImportError`, with the extra to install appended; failures after the driver loads (network, authentication, native libraries) propagate unchanged.

The failure modes across the user chapters:

| Exception | Raised when | Covered in |
|---|---|---|
| `NotImplementedError` | The dialect has no upsert style, no index capability, or no verified raw adoption. | This section |
| `ValueError` | An argument is invalid for the dialect: a namespace shape it cannot address, an unsupported index option, a missing table or column, an unsafe identifier. | [Namespace limits](#namespace-limits) |
| `ImportError` | The dialect's driver is not installed. The message names the extra. | [Installing and connecting](../02-installing-and-connecting/index.md) |
| `RuntimeError` | A method that needs a connection runs before `connect()`. | [Inspecting databases](../05-inspecting-databases/index.md) |
| `TransactionUnsupportedError`, `TransactionPoisonedError`, `TransactionIntegrityError` | A scope on a dialect without transactions, a scope that a caught failure poisoned, or native transaction state that cannot be trusted. All subclass `TransactionError`. | [Transactions](../10-transactions/index.md) |
| `BackendConnectionError` | A connection failure whose message carried credentials. | [BackendConnectionError](#backendconnectionerror) |

The exception classes are importable from `mountainash_data.core.errors`.

## Namespace limits

<!-- concept:112 -->
[`Namespace`](../05-inspecting-databases/index.md#namespace-and-namespacelike) can describe any catalog and path, but each operation accepts only the shapes it can address:

| Operations | One path level | Catalog plus one level | Two or more levels |
|---|---|---|---|
| Delegated to Ibis: `table()`, `list_tables()`, `inspect_table()`, `create_table()`, `insert()`, `drop_table()`, `create_view()`, `drop_view()`, `truncate()`, `table_exists()` | yes | yes | `ValueError` |
| Rendered by the package: `upsert()`, `add_columns()`, `create_index()`, `create_unique_index()`, `drop_index()`, `index_exists()`, `list_indexes()` | yes | `ValueError` | `ValueError` |

Ibis models exactly catalog, database and table, so a deeper path is unrepresentable. The package-rendered operations build engine-native SQL or index-catalog queries that take a single namespace literal, and cannot reach a foreign catalog (PostgreSQL, for one, has no cross-database SQL). They reject a catalog-qualified namespace with a `ValueError` that says so, rather than emit broken three-part SQL:

```python
from mountainash_data import IbisBackend, Namespace

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("sales", [{"id": 1, "total": 120}])
    location = Namespace(catalog="memory", path=("main",))
    print(backend.list_tables(namespace=location))
    try:
        backend.upsert("sales", [{"id": 1, "total": 130}],
                       conflict_columns=["id"], namespace=location)
    except ValueError as exc:
        print(f"ValueError: {exc}")
```

```text
['sales']
ValueError: upsert does not support catalog-qualified namespaces (catalog='memory'): it builds engine-native SQL that cannot address a foreign catalog. Use a native-delegating op, or omit the catalog.
```

To upsert or manage indexes in another catalog, connect to that catalog and pass a single-level namespace.

## BackendConnectionError

<!-- concept:144 -->
`BackendConnectionError` (a `RuntimeError` subclass in `mountainash_data.core.errors`) is raised when a connection attempt fails and the driver's error message contained credentials. Drivers and Ibis can echo the whole connection URL, password included, in their exceptions. The package checks the message for URL user-info, credential query parameters (`password=`, `token=`, `motherduck_token=` and similar) and the secret values you supplied, and replaces each with `***`.

What you catch depends on the original exception. A failure whose message carried no credentials propagates unchanged, with its original type. A built-in exception such as `ValueError` that did carry credentials is re-raised as the same type with a redacted message, so existing `except ValueError` handlers keep working. A driver-specific exception that carried credentials becomes `BackendConnectionError`, whose message starts with the original exception's class name. The credential-bearing original is not chained, so it does not reappear in a traceback.

Catching connection failures portably therefore means catching `BackendConnectionError` alongside the driver's own errors and `ValueError`. The redaction mechanism itself is described in [From settings to driver arguments](../15-from-settings-to-driver-arguments/index.md).
