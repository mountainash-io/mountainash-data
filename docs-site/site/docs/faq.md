---
title: FAQ
description: Short answers to common questions about installing, connecting, inspecting, writing, transactions, dialect limits and extending mountainash-data.
---

# Frequently asked questions

## About the package

### What is mountainash-data?

`mountainash-data` gives Python code one typed handle, `IbisBackend`, for connecting to relational databases, inspecting their physical metadata and managing their tables. The same class serves 20 dialects, from in-process SQLite and DuckDB to PostgreSQL, Snowflake, BigQuery and Trino. Ibis supplies the table expressions; the package adds connection profiles, namespace handling, dialect-specific writes, index management and explicit transaction scopes behind a backend-neutral `Backend` protocol. See [What the package does](chapters/01-what-mountainash-data-is/index.md#what-the-package-does).

A minimal session:

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="sqlite", database=":memory:") as backend:
    backend.create_table("sales", [{"id": 1, "region": "north", "total": 120}])
    print(backend.list_tables())
    print(backend.inspect_table("sales").column_names)
    sales = backend.table("sales")
    print(backend.run_expr(sales.total.sum()))
```

```text
['sales']
['id', 'region', 'total']
120
```

### How is it different from using the database drivers directly?

Each driver has its own connection arguments, metadata queries, upsert syntax, index DDL and transaction behaviour. `mountainash-data` puts one set of methods over all of them: the same `list_tables()`, `inspect_table()`, `upsert()`, `create_index()` and `transaction()` calls work whatever engine is underneath, and changing `dialect=` (with that engine's connection arguments) leaves the rest of the code unchanged. The native driver handle is still available through `raw_driver_connection()` when you need it. See [What the package does](chapters/01-what-mountainash-data-is/index.md#what-the-package-does).

### Which databases does it support?

`IbisBackend` registers 20 dialects: `sqlite`, `duckdb`, `motherduck`, `postgres`, `mysql`, `mssql`, `oracle`, `snowflake`, `bigquery`, `redshift`, `trino`, `clickhouse`, `databricks`, `singlestoredb`, `exasol`, `impala`, `materialize`, `risingwave`, `druid` and `pyspark`. Connecting, inspecting, creating tables, inserting and querying delegate to Ibis on all of them. Upsert, indexes and transaction scopes depend on what each dialect declares, and a registered dialect does not guarantee every operation. See [Support matrix](chapters/10-supported-databases-and-limits/index.md#support-matrix).

### What does mountainash-data not do?

It does not build query expressions (that is Ibis), define relation semantics across engines (the `mountainash` package), load configuration files or resolve secrets (`mountainash-settings`), or run auth flows such as OAuth token exchange (`mountainash-auth-client`). It also does not plan automatic joins between tables on different database connections. See [What the package does](chapters/01-what-mountainash-data-is/index.md#what-the-package-does).

## Installing

### Which extra do I install for my database?

Each dialect has an extra with the same name as its dialect key, for example `mountainash-data[postgres]` or `mountainash-data[snowflake]`. The base package installs no database driver. `sqlite` and `duckdb` cover every local recipe, `polars` adds Polars frames as table input, and `all` installs every backend extra. Some extras need system software as well: SQL Server needs an ODBC driver, and local PySpark needs Java. See [Choosing extras](chapters/02-installing-and-connecting/index.md#choosing-extras).

### Why do I get an ImportError when I connect?

The dialect's driver is not installed. Constructing `IbisBackend` imports no driver, so the error appears at `connect()`, and its message ends with the extra to install, such as `pip install 'mountainash-data[postgres]'`. Only a missing driver module is translated this way; a missing native library or bad credentials propagate as the driver reports them. See [Missing-driver errors](chapters/02-installing-and-connecting/index.md#missing-driver-errors).

## Connecting

### What are the ways to construct an IbisBackend?

There are three: a dialect name with driver keyword arguments (`IbisBackend(dialect="duckdb", database=":memory:")`), a connection URL (`IbisBackend("postgresql://db.example:5432/app")`), or a `SettingsParameters` object naming a typed backend profile. Pass exactly one. Construction only records configuration; `connect()` opens the connection. See [IbisBackend by dialect and keyword arguments](chapters/02-installing-and-connecting/index.md#ibisbackend-by-dialect-and-keyword-arguments).

### Why is my dialect name or URL scheme rejected?

Dialect keys are Ibis backend names, so PostgreSQL is `dialect="postgres"`, not `"postgresql"`; an unknown key raises `KeyError` listing the valid ones. URLs accept both `postgres://` and `postgresql://`, and an unrecognised scheme raises `ValueError: Cannot detect ibis dialect from URL scheme`. Redshift has no URL scheme of its own, so connect with `dialect="redshift"` or a settings profile, and Databricks has no URL form at all. `duckdb://md:<database>` resolves to `motherduck`. See [IbisBackend from a URL](chapters/02-installing-and-connecting/index.md#ibisbackend-from-a-url).

### How do I put a password with special characters in a URL?

Percent-encode it with `urllib.parse.quote(value, safe="")`. An unencoded `/`, `:`, `@`, `?`, `#` or `%` breaks the URL, and the backend reports a `ValueError` that does not echo the URL. Use `postgresql://` rather than `postgres://` for a PostgreSQL URL that carries credentials. Passing an auth profile to `connect()` avoids encoding entirely. See [Credentials in connection URLs](chapters/02-installing-and-connecting/index.md#credentials-in-connection-urls).

### Why does every method raise "IbisBackend is not connected"?

Operations that need a live connection raise `RuntimeError` until `connect()` has run. Call `connect()` (it returns the backend, and calling it twice opens nothing new) or use the backend as a context manager. A `with` block connects without an auth profile, so when credentials come from one, call `connect(auth_profile=...)` yourself and close in a `finally` block. After `close()` the backend is unconnected again, and an in-memory database reconnects empty. See [connect and close](chapters/02-installing-and-connecting/index.md#connect-and-close).

### How do I get the underlying Ibis or driver connection?

`ibis_connection()` returns the Ibis backend object, `raw_driver_connection()` returns the native driver handle (a `sqlite3.Connection`, a `DuckDBPyConnection`, a psycopg connection and so on), and `get_connection()` returns the package's `IbisConnection` wrapper. All three raise `RuntimeError` on an unconnected backend. The raw handle is not guaranteed to follow DB-API on every dialect, and you should close through the backend rather than through the handle. See [The live IbisConnection](chapters/02-installing-and-connecting/index.md#the-live-ibisconnection).

### How do I connect to MotherDuck?

Use `IbisBackend(dialect="motherduck", database=..., token=...)` or a `duckdb://md:<database>` URL, which use the MotherDuck connection builder. Building the backend from `MotherDuckBackendProfile` settings currently resolves to the plain DuckDB dialect, and connecting fails because DuckDB does not recognise the `token` option. See [The adapter pipeline and build_driver_kwargs](chapters/12-from-settings-to-driver-arguments/index.md#the-adapter-pipeline-and-build_driver_kwargs).

## Settings profiles and authentication

### What is a backend profile?

A `*BackendProfile` (such as `PostgreSQLBackendProfile`) is a pydantic-settings model holding one database's connection configuration, with upper-case fields like `HOST`, `PORT`, `DATABASE` and `SSL_MODE`. It holds no credentials: those live in separate auth profiles passed to `connect(auth_profile=...)`, so one configuration can be used with different credentials and a constructed backend holds no secrets. You pass it to `IbisBackend` as `SettingsParameters.create(settings_class=..., **values)`, and values are validated when the backend is constructed. See [BackendProfile and SettingsParameters](chapters/03-connection-profiles-and-authentication/index.md#backendprofile-and-settingsparameters).

### Which auth types can I use with which database?

Each profile's spec lists the auth types it accepts in `supported_auth`:

```python
from mountainash_data.core.settings import get_descriptor

for name in ("postgresql", "snowflake", "sqlite"):
    spec = get_descriptor(name)
    print(name, [auth.__name__ for auth in spec.supported_auth])
```

```text
postgresql ['PasswordAuthProfile', 'NoAuthProfile']
snowflake ['PasswordAuthProfile', 'OAuth2AuthProfile', 'CertificateAuthProfile', 'TokenAuthProfile']
sqlite ['NoAuthProfile']
```

The full table of eleven auth types and the backends that accept each is in [Auth profiles](chapters/03-connection-profiles-and-authentication/index.md#auth-profiles).

### Why do I get "does not support auth" or "no auth adapter" errors?

The first comes from the settings path: the auth profile's type is not in the profile's `supported_auth`. Omitting `auth_profile` counts as `NoAuthProfile`, so MySQL, Oracle, SQL Server, Snowflake, Redshift, Exasol and MotherDuck profiles fail without credentials. The second comes from the dialect-keyword and URL paths, which have no profile: no adapter is registered for that dialect and auth type. See [Connect-time composition](chapters/03-connection-profiles-and-authentication/index.md#connect-time-composition).

### How can I see the driver arguments a profile and auth profile produce?

Call `build_driver_kwargs(profile, auth_profile)` from `mountainash_data.core.factories.connection_factory`. It runs the same pipeline `connect()` uses and returns the plain keyword dictionary the driver receives, secrets included, so don't log it. `build_connection_string()` produces a URL from the same two objects. See [The adapter pipeline and build_driver_kwargs](chapters/12-from-settings-to-driver-arguments/index.md#the-adapter-pipeline-and-build_driver_kwargs).

### What are BackendSpec and ParameterSpec?

Every profile class carries a `BackendSpec` in `__spec__`, retrievable with `get_descriptor(name)`. It records the backend's registry name, Ibis dialect, default port, URL scheme, supported auth types and an ordered list of `ParameterSpec` entries. Each `ParameterSpec` describes one field: its type, default, tier, the driver keyword it is emitted under, whether it is secret, and optional transform and validator callables. See [BackendSpec, ParameterSpec and parameter tiers](chapters/03-connection-profiles-and-authentication/index.md#backendspec-parameterspec-and-parameter-tiers).

### Does the package refresh OAuth tokens?

No. On Snowflake, both `TokenAuthProfile` and `OAuth2AuthProfile` become `authenticator="oauth"` plus the access token, and the OAuth2 adapter ignores the client ID, client secret, scope and refresh token. Obtaining and refreshing tokens is the job of `mountainash-auth-client`. See [Adapter families](chapters/12-from-settings-to-driver-arguments/index.md#adapter-families).

### Do Redshift IAM and BigQuery service-account credentials work?

Not through `IbisBackend` at this revision, and no test connects either one. The Redshift dialect connects through psycopg, which rejects the `iam` and `readonly` keys the Redshift adapter and profile emit. The BigQuery adapter emits `credentials`, but the BigQuery connection builder does not read that key, so it falls back to application default credentials. See [Adapter families](chapters/12-from-settings-to-driver-arguments/index.md#adapter-families).

## Inspecting databases

### Which inspection methods are there?

`list_catalogs()`, `list_namespaces(catalog=None)` and `list_tables(namespace=None)` return lists of names. `inspect_catalog()`, `inspect_namespace()` and `inspect_table()` return `CatalogInfo`, `NamespaceInfo` and `TableInfo`, and `list_indexes()` returns `IndexInfo` objects. None of them reads table rows. They belong to the `Backend` protocol and return types with no reference to Ibis or a driver, so the same inspection code works on every dialect and on non-Ibis backends. See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### What is a namespace, and how do I target one?

A namespace is the schema, database or dataset level that holds tables. Methods take `namespace=` as a string, a tuple of path levels, or a `Namespace(catalog=..., path=(...))` value object. A string or tuple only ever fills `path`; to name a catalog, build a `Namespace` with `catalog=` explicitly. See [Namespace and NamespaceLike](chapters/04-inspecting-databases/index.md#namespace-and-namespacelike).

### Why does a catalog-qualified namespace raise ValueError?

Operations that delegate to Ibis accept a catalog plus one path level, but no deeper path. Operations the package renders itself (`upsert`, `add_columns` and the index methods) accept one path level with no catalog, because their SQL cannot address a foreign catalog. To upsert or manage indexes in another catalog, connect to that catalog and pass a single-level namespace. See [Namespace limits](chapters/10-supported-databases-and-limits/index.md#namespace-limits).

### Why did `list_tables()` return an empty list instead of an error?

Outside a transaction scope, the three `list_*` methods print the engine's error and return an empty list when a listing fails, so a namespace that does not exist usually looks empty. Inside a `transaction()` scope they raise instead. `inspect_table()` always raises `ValueError` for a table it cannot read. See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### Why does `ColumnInfo.type_name` say `int64` rather than the database's type name?

`type_name` is the Ibis data type rendered as a string, not the engine's native type, so the same table reports the same types on DuckDB and PostgreSQL. `nullable` also comes from the Ibis type. See [How driver metadata becomes TableInfo](chapters/04-inspecting-databases/index.md#how-driver-metadata-becomes-tableinfo).

## Reading and querying

### How do I query a table?

`backend.table(name)` returns an Ibis table expression bound to the connection; nothing is read until you ask for results. Build filters, joins and aggregates with the Ibis expression API, then execute with `run_expr()` or with Ibis's own `to_pyarrow()`, `to_polars()` or `execute()`. See [table() and Ibis expressions](chapters/05-reading-tables-and-running-queries/index.md#table-and-ibis-expressions).

### What is the difference between `run_expr()` and `expr.execute()`?

They return the same result. `run_expr()` goes through the backend, so inside a `transaction()` scope on SQLite, DuckDB or PostgreSQL it is a protected package call; an expression executed directly bypasses that protection. To see the SQL without running it, use `backend.to_sql(expr)` (`pretty=True` formats it). See [run_expr and to_sql](chapters/05-reading-tables-and-running-queries/index.md#run_expr-and-to_sql).

### Can I run raw SQL?

`run_sql(query)` wraps a `SELECT` as an Ibis table expression that you can execute, filter or join; it does not return rows itself, and `dialect=` lets Ibis transpile SQL written for another engine. Statements such as DDL or `UPDATE` belong in the package's write methods or on the raw driver handle from `raw_driver_connection()`. See [run_sql](chapters/05-reading-tables-and-running-queries/index.md#run_sql).

### How do I use a table with the mountainash package?

Pass the Ibis table from `backend.table(...)` to `mountainash.relation()`. The bridge is pull-side: there is no conversion method on the backend, and `mountainash-data` does not depend on `mountainash`. Keep the backend open until the relation has produced its result, because the relation runs on the table's connection. See [Bridging to mountainash relations](chapters/05-reading-tables-and-running-queries/index.md#bridging-to-mountainash-relations).

## Writing data

### Which table operations are available?

`create_table`, `drop_table`, `create_view`, `drop_view`, `insert`, `truncate`, `upsert`, `add_columns` and `rename_table`. Creating, inserting and truncating go to Ibis; upsert, `add_columns` and `rename_table` render dialect-specific SQL in the package. `create_table` and `insert` accept a list of dicts, a Polars, pandas or PyArrow frame, or an Ibis table. See [Creating tables and views](chapters/06-creating-and-changing-tables/index.md#creating-tables-and-views).

### Why does my upsert fail on SQLite, DuckDB or PostgreSQL?

These engines use `INSERT ... ON CONFLICT`, which needs a unique index or primary key on exactly the conflict columns. Without one, the engine rejects the statement. Create it with `create_unique_index(table, columns)` and retry. See [A first upsert](chapters/06-creating-and-changing-tables/index.md#a-first-upsert).

### Which columns does upsert update?

By default, the incoming columns that are not part of the key. A table column missing from the input is left alone on a match and gets `NULL` (or its default) on an insert, and an input column missing from the table raises `ValueError`. Pass `update_columns` to update fewer, `conflict_action="NOTHING"` for insert-or-ignore, or `update_condition` for a row-level condition. Upsert never deletes. See [Which columns are written](chapters/06-creating-and-changing-tables/index.md#which-columns-are-written).

### Does upsert behave the same on every database?

The call is the same, but the SQL family differs, and so does what "the key matches" means. `ON CONFLICT` relies on the unique constraint; `MERGE` matches by equality, so a row with a `NULL` key never matches and is inserted; MySQL's `ON DUPLICATE KEY` fires on any unique index, so the package requires the table's only unique index to equal the conflict columns and rejects `update_condition`. See [Match-key semantics by dialect](chapters/06-creating-and-changing-tables/index.md#match-key-semantics-by-dialect).

### Does upsert read the whole table into memory?

No. Incoming rows passed as a frame or list are cast to the target schema, staged on the connection as a temporary table, and merged in one SQL statement that runs on the database. From the target table, only its schema is read (and, on MySQL and SingleStoreDB, its unique indexes). See [Generic-default operations and sqlglot rendering](chapters/11-how-one-backend-serves-20-dialects/index.md#generic-default-operations-and-sqlglot-rendering).

### How do I add new columns to an existing table?

`add_columns(name, source)` adds the columns in `source` that the table does not have yet. `source` is a mapping of column name to type, or a frame whose schema is used (its rows are not inserted). Existing columns are skipped even if the type differs, so the call is safe to repeat. It issues one `ALTER TABLE` per column, so a failure part-way leaves earlier columns added. See [Adding columns](chapters/06-creating-and-changing-tables/index.md#adding-columns).

## Indexes

### Which databases support index management?

SQLite, DuckDB, MotherDuck, PostgreSQL, MySQL, SQL Server, Oracle and SingleStoreDB declare index support. Every other dialect raises `NotImplementedError` from the index methods. Oracle's index operations have unresolved identifier and owner-resolution issues and should not be relied on. See [Index support by dialect](chapters/10-supported-databases-and-limits/index.md#index-support-by-dialect).

### Why is `where=` or `index_type=` rejected?

The dialect's index capability does not allow it. Partial indexes (`where=`) work on SQLite, PostgreSQL and SQL Server. `index_type` is accepted only on PostgreSQL, MySQL and SingleStoreDB, each with its own set of methods. Both are checked before any SQL is sent. See [Creating indexes](chapters/07-indexes/index.md#creating-indexes).

### Why does `drop_index` ask for `table_name`?

On MySQL, SQL Server and SingleStoreDB, index names are unique per table, so the statement is `DROP INDEX name ON table`. Passing `table_name` everywhere keeps code portable, because the other dialects accept it too. For the same reason, pass `table_name` to `index_exists` on SQL Server. See [Dropping indexes](chapters/07-indexes/index.md#dropping-indexes).

### Are `if_not_exists` and `if_exists` safe under concurrency?

Engines with native `IF [NOT] EXISTS` (SQLite, DuckDB, MotherDuck, PostgreSQL) make the check part of the statement. On MySQL, SQL Server (for creation), Oracle and SingleStoreDB the package checks for the index first and then runs the DDL, and a concurrent session can act between the two; the engine's error is then raised, not suppressed. See [Creating indexes](chapters/07-indexes/index.md#creating-indexes).

## Transactions

### How do I run several writes as one transaction?

Wrap them in `with backend.transaction():`. Outside a scope the package leaves the driver's autocommit setting alone, so each write commits the way the driver normally commits it. When the scope owns the unit of work, a clean exit commits and an exception rolls back before propagating. Which dialects support scopes, and how, is listed by dialect. See [Transaction scopes](chapters/08-transactions/index.md#transaction-scopes).

### What happens if I already have a transaction open?

On SQLite, DuckDB and PostgreSQL the scope probes the driver first. If a transaction is open, the scope joins it: no `BEGIN`, and no `COMMIT` or `ROLLBACK` on exit, so the caller decides. On the other transactional dialects there is no probe, and the scope always begins its own transaction, so do not open one yourself first. See [Owned and joined transactions](chapters/08-transactions/index.md#owned-and-joined-transactions).

### Does a nested `transaction()` create a savepoint?

No. Nested scopes are flat: the inner scope joins the outer unit, and the outermost scope completes everything. An exception that escapes an inner scope poisons the whole unit, even if you catch it, and the outer scope then rolls back and raises `TransactionPoisonedError`. See [Nested scopes are flat](chapters/08-transactions/index.md#nested-scopes-are-flat).

### What is the difference between `in_transaction()` and `native_transaction_open()`?

`in_transaction()` reports whether a package `transaction()` scope is registered on this connection, and never raises. `native_transaction_open()` asks the driver whether a transaction is open, and returns `None` on dialects with no native probe. They disagree when the caller has begun a transaction on the raw handle without a package scope. See [Checking scope and native state](chapters/08-transactions/index.md#checking-scope-and-native-state).

### Why did my scope raise TransactionIntegrityError?

The scope could not trust the native transaction state. The usual cause is a `COMMIT` or `ROLLBACK` issued through the raw handle or directly through Ibis inside the scope, which ends the transaction behind its back. The error cannot undo that commit. On PostgreSQL, a raw statement that fails leaves the transaction aborted, which raises the same error at exit. See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### Why did my scope raise TransactionPoisonedError?

A failure was caught inside the unit: a failed package call such as an insert that violates a key, or an exception from a nested scope. Further package calls into the unit are refused before they touch the database, and an owned unit is rolled back at exit. Arguments rejected before any database work, such as an invalid index name, do not poison the unit. See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### What happens with `transaction()` on BigQuery, ClickHouse or Databricks?

These dialects, with Impala, Druid and PySpark, have no transaction concept. `transaction()` raises `TransactionUnsupportedError`; with `required=False` it warns once and runs the body without a transaction. `supports_transactions` tells you in advance. See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### Why does a later scope on PostgreSQL not commit my work?

With autocommit off, psycopg opens a transaction implicitly before the first statement, including a read. A package read outside a scope can leave that transaction open, and a later `transaction()` sees it as the caller's and joins it rather than owning it, so nothing is committed until you commit. Commit or roll back after such reads, or run the connection with autocommit on. See [Owned and joined transactions](chapters/08-transactions/index.md#owned-and-joined-transactions).

### Why does reusing a memtable after a rollback fail?

Ibis stages a memtable as a temporary table and remembers that it did. A rollback drops the temporary table, but Ibis's record remains, so the same expression fails afterwards. Build a fresh memtable, or pass the source data frame again, when retrying. See [The memtable rollback caveat](chapters/08-transactions/index.md#the-memtable-rollback-caveat).

## Adopting existing connections

### Can I use a connection I already opened?

Yes. `IbisBackend.from_ibis_connection(con, dialect=...)` wraps an Ibis backend for any dialect, and `IbisBackend.from_raw_connection(raw, dialect=...)` wraps a native DuckDB or MotherDuck driver handle. Both share the caller's session, including an open transaction, and need no `connect()` call. `dialect=` is required. See [Connection adoption](chapters/09-adopting-existing-connections/index.md#connection-adoption).

### Will closing the adopted backend close my connection?

Not by default. With `owns_connection=False` (the default), `close()` releases only the wrapper. With `owns_connection=True` it disconnects the underlying connection. After `close()` the backend forgets the adopted connection, so adopt again rather than calling `connect()`. See [Connection ownership](chapters/09-adopting-existing-connections/index.md#connection-ownership).

### Why does `from_raw_connection` raise NotImplementedError for my database?

Raw adoption is enabled only for dialects verified end to end, which at this revision are DuckDB and MotherDuck. For any other dialect, build the Ibis connection yourself (for example with `ibis.<backend>.from_connection(raw)` where the backend provides it) and use `from_ibis_connection`. See [from_raw_connection and verified dialects](chapters/09-adopting-existing-connections/index.md#from_raw_connection-and-verified-dialects).

### Why did my DuckDB session's time zone change after adoption?

When Ibis adopts a DuckDB connection, it sets `TimeZone` to UTC and turns off `python_enable_replacements`. Adopt the raw handle with `from_raw_connection(..., preserve_session=True)` to have the original values restored, or pass `apply_session_options={"timezone": ...}` to `from_ibis_connection` to set the values you want afterwards. See [from_ibis_connection](chapters/09-adopting-existing-connections/index.md#from_ibis_connection).

## Limits and errors

### How do I find out which operations a database supports?

The support matrix lists each dialect's upsert family, transaction support, index support, raw adoption and dialect-specific hooks, generated from the dialect registry. An unsupported operation raises `NotImplementedError` naming the dialect and the operation, before any SQL is sent; upsert, for example, is unavailable on ClickHouse, Impala, Materialize, Druid and PySpark. See [Support matrix](chapters/10-supported-databases-and-limits/index.md#support-matrix).

### Which exceptions should I expect?

`NotImplementedError` for unsupported operations, `ValueError` for arguments the dialect cannot honour, `ImportError` for a missing driver, `RuntimeError` for calls before `connect()`, the three `TransactionError` subclasses for scope problems, and `BackendConnectionError` for connection failures whose message carried credentials. The package's own exception classes are in `mountainash_data.core.errors`. See [NotImplementedError and other failure modes](chapters/10-supported-databases-and-limits/index.md#notimplementederror-and-other-failure-modes).

### Why did I get BackendConnectionError instead of the driver's error?

The driver's error message contained credentials. The package replaces URL user-info, credential query parameters and the secrets you supplied with `***`. A built-in exception type such as `ValueError` keeps its type with a redacted message; a driver-specific exception becomes `BackendConnectionError`, without the original chained. Errors that carried no credentials propagate unchanged. See [BackendConnectionError](chapters/10-supported-databases-and-limits/index.md#backendconnectionerror).

### How do I troubleshoot a connection failure?

Work through the layers in order. An `ImportError` means a missing extra. A pydantic `ValidationError` at construction means a bad profile value. A `ValueError` at `connect()` naming auth means the auth type does not fit the backend. To see exactly what the driver will receive, call `build_driver_kwargs(profile, auth_profile)`. Anything after that (network, credentials, native libraries) is the driver's own error, possibly redacted as `BackendConnectionError`. See [Connect-time composition](chapters/03-connection-profiles-and-authentication/index.md#connect-time-composition).

## Extending and maintaining

### What is the Backend protocol, and why is it runtime-checkable?

`Backend` is a `typing.Protocol` listing the members every backend has: lifecycle, listing, inspection, the raw driver handle and the transaction members. A class satisfies it by having those members, without inheriting from anything in the package. `@runtime_checkable` makes `isinstance(obj, Backend)` work, but the check only confirms the members exist, not their signatures or behaviour. Writes, upsert and indexes are deliberately not on the protocol, because their shape depends on the backend family: type parameters as `Backend` for code that must work with any backend, and as `IbisBackend` for code that needs writes. See [The Backend protocol in one page](chapters/01-what-mountainash-data-is/index.md#the-backend-protocol-in-one-page).

### How do I write a backend for a store that Ibis does not support?

Write a class that implements every `Backend` member and returns the shared inspection types. Test it with an `isinstance` check, behavioural assertions for the protocol's contract, and a static mypy check. See [Implementing a Backend](chapters/14-extending-mountainash-data/index.md#implementing-a-backend).

### How does one class serve 20 dialects?

Each dialect is one frozen `DialectSpec` record in the `DIALECTS` registry, declaring its connection builder, upsert family, index capability, transaction support and any hooks. `IbisBackend` resolves the spec at construction, and every engine-specific decision is a field read on it. For a hook-driven operation, a dialect hook wins; otherwise a missing capability raises `NotImplementedError`; otherwise a generic implementation rendered through sqlglot runs. See [Operation hooks and dispatch order](chapters/11-how-one-backend-serves-20-dialects/index.md#operation-hooks-and-dispatch-order).

### How are registries used across the package?

There are two. `DATABASES_REGISTRY` maps settings backend names to their `BackendSpec` and profile class. It is filled by the `@register` decorator as a side effect of import, so importing `mountainash_data` registers all 20 built-in profiles, and registering a name twice raises `ValueError`. The `DIALECTS` dialect registry maps each Ibis dialect key to its `DialectSpec`. Adding a database means adding entries to these, not editing `IbisBackend`. See [Python ideas the package leans on](chapters/01-what-mountainash-data-is/index.md#python-ideas-the-package-leans-on).

### How do I add a new dialect?

Add a `DialectSpec` entry to `DIALECTS` in the repository, set its connection fields, declare the capabilities the engine has, add hooks where the generic renderers cannot express its SQL, add a matching extra in `pyproject.toml`, and add the dialect to the registry test matrices. The engine must already have an Ibis backend. See [Adding a dialect](chapters/14-extending-mountainash-data/index.md#adding-a-dialect).

### How do I add a settings backend?

Define a `BackendSpec` and a `BackendProfile` subclass decorated with `@register`. This works out of tree with `NoAuthProfile`. The auth adapter table has no public registration function, so a backend that needs credentials, or a new auth pairing for an existing backend, is contributed to the repository with its adapter rows. Auth profile classes themselves belong to `mountainash-auth-client`. See [Adding a settings backend](chapters/14-extending-mountainash-data/index.md#adding-a-settings-backend).

### Is `mountainash_data.core.registry` public API?

Treat it as provisional. It maps names to backend factories, starts empty, is not exported from `mountainash_data`, and nothing in the package reads it. Its public status and tests are tracked as backlog item DEBT-25. See [The backend name registry](chapters/14-extending-mountainash-data/index.md#the-backend-name-registry).

### How do I run the tests?

`hatch run test:test-quick` runs the core tier, which needs only SQLite, DuckDB and Polars. Optional-backend tests run one driver per `test_backend.<backend>` environment, and the live tier runs against real servers through the `live-db` harness. See [Test tiers](chapters/15-contributing-and-testing/index.md#test-tiers) and [The live-database harness](chapters/15-contributing-and-testing/index.md#the-live-database-harness).
