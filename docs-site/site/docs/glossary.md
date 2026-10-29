---
title: Glossary
description: Definitions of the package types, functions and database terms used in this book, with links to where each is explained.
---

# Glossary

## A

### Adapter pipeline

The two-stage process that turns a backend profile and an auth profile into driver keyword arguments. The profile's fields are first emitted under their driver keys and reshaped by any configuration adapter, then an auth adapter merges the credentials into the result.

See [The adapter pipeline and build_driver_kwargs](chapters/12-from-settings-to-driver-arguments/index.md#the-adapter-pipeline-and-build_driver_kwargs).

### `add_columns()`

An `IbisBackend` method that adds the columns from a type mapping or a frame that the table does not have yet. It is additive and idempotent: existing columns are skipped, nothing is dropped or altered, and it issues one `ALTER TABLE ... ADD COLUMN` per new column.

See [Adding columns](chapters/06-creating-and-changing-tables/index.md#adding-columns).

### `apply_session_options`

A keyword argument of `IbisBackend.from_ibis_connection()` that sets declared session options to caller-supplied values after the Ibis connection is wrapped. It restores nothing on its own; options left out keep whatever Ibis set.

See [from_ibis_connection](chapters/09-adopting-existing-connections/index.md#from_ibis_connection).

### Auth adapter

A function `(auth_profile, base_kwargs) -> kwargs` that converts one auth profile type into the credential arguments a particular driver accepts, unwrapping secrets only at that point. Adapters are looked up by provider type and auth profile class, including subclasses of a registered auth class.

See [The auth adapter registry](chapters/12-from-settings-to-driver-arguments/index.md#the-auth-adapter-registry).

### Auth profile

A pydantic model holding one kind of credential, such as `PasswordAuthProfile`, `TokenAuthProfile` or `IAMAuthProfile`. Auth profiles are defined in `mountainash-auth-client`, re-exported from `mountainash_data.core.settings`, and passed to `connect(auth_profile=...)` separately from the backend profile.

See [Auth profiles](chapters/03-connection-profiles-and-authentication/index.md#auth-profiles).

### Autocommit

A driver setting under which each statement is committed as soon as it runs. `mountainash-data` never changes it; on PostgreSQL with autocommit off, a package read can leave a transaction open that a later `transaction()` then joins instead of owning.

See [Owned and joined transactions](chapters/08-transactions/index.md#owned-and-joined-transactions).

## B

### `Backend`

The runtime-checkable protocol that every backend satisfies, covering lifecycle, listing, inspection, raw driver access and transaction members. It deliberately excludes writes, upsert, indexes and query execution, which are `IbisBackend` methods.

See [The Backend protocol in one page](chapters/01-what-mountainash-data-is/index.md#the-backend-protocol-in-one-page).

### Backend name registry

The module `mountainash_data.core.registry`: a process-wide map from a name to a factory that returns a `Backend`, with `register()`, `get()` and `names()`. It starts empty, nothing in the package reads it, and it is not exported from `mountainash_data`, so it is provisional.

See [The backend name registry](chapters/14-extending-mountainash-data/index.md#the-backend-name-registry).

### `BackendConnectionError`

A `RuntimeError` subclass in `mountainash_data.core.errors`, raised when a driver-specific connection error carried credentials in its message. The message is redacted and the original exception is not chained.

See [BackendConnectionError](chapters/10-supported-databases-and-limits/index.md#backendconnectionerror).

### `BackendProfile`

The base class of the 20 per-backend settings profiles, such as `PostgreSQLBackendProfile`. It is a pydantic-settings model from `mountainash-settings` whose upper-case fields hold connection configuration but never credentials.

See [BackendProfile and SettingsParameters](chapters/03-connection-profiles-and-authentication/index.md#backendprofile-and-settingsparameters).

### `BackendSpec`

The frozen dataclass stored in a profile class's `__spec__` that declares a settings backend: its registry name, provider type, parameters, Ibis dialect, default port, URL scheme, `rides_on` and `supported_auth`. `get_descriptor(name)` returns it.

See [BackendSpec, ParameterSpec and parameter tiers](chapters/03-connection-profiles-and-authentication/index.md#backendspec-parameterspec-and-parameter-tiers).

### `build_connection_string()`

A function in `mountainash_data.core.factories.connection_factory` that builds a URL from a backend profile and an optional auth profile. Password credentials are percent-encoded into the authority, and combinations with no URL form raise `NotImplementedError`.

See [build_connection_string](chapters/12-from-settings-to-driver-arguments/index.md#build_connection_string).

### `build_driver_kwargs()`

A function in `mountainash_data.core.factories.connection_factory` that runs the adapter pipeline in one call. It checks the auth type against `supported_auth`, emits the profile, applies the auth adapter and returns a plain dictionary containing unwrapped secrets.

See [The adapter pipeline and build_driver_kwargs](chapters/12-from-settings-to-driver-arguments/index.md#the-adapter-pipeline-and-build_driver_kwargs).

## C

### Capability descriptor

A field on a `DialectSpec` that declares what the package may render for a dialect: `upsert_style`, `index_caps` and `transaction_support`. An absent capability makes the matching operation raise `NotImplementedError` before any SQL is sent.

See [Capability descriptors](chapters/11-how-one-backend-serves-20-dialects/index.md#capability-descriptors).

### Catalog

The level above namespaces in a database: an attached database in DuckDB, a top-level database in Snowflake or Databricks, a single pseudo-catalog in SQLite. `Namespace.catalog` names one explicitly and is never inferred from a tuple position.

See [Databases, schemas and catalogs as the package models them](chapters/01-what-mountainash-data-is/index.md#databases-schemas-and-catalogs-as-the-package-models-them).

### Catalog-qualified namespace

A `Namespace` with `catalog` set. Operations delegated to Ibis accept one with exactly one path level; operations the package renders itself (upsert, `add_columns`, index methods) reject it with `ValueError`.

See [Namespace limits](chapters/10-supported-databases-and-limits/index.md#namespace-limits).

### `CatalogInfo`

The frozen inspection result returned by `inspect_catalog()`, holding a catalog `name` and a sequence of `NamespaceInfo` objects. It is the only inspection type that contains other results rather than names.

See [CatalogInfo](chapters/04-inspecting-databases/index.md#cataloginfo).

### `close()`

The `Backend` method that releases the connection and returns the backend; it is idempotent. Inside an open `transaction()` scope it raises `RuntimeError`, and on an adopted connection it leaves the caller's connection open unless `owns_connection=True`.

See [connect and close](chapters/02-installing-and-connecting/index.md#connect-and-close).

### `ColumnInfo`

The frozen inspection result describing one column: `name`, `type_name`, `nullable`, an optional `description` and `metadata`. `type_name` is the Ibis type as a string, so the same table reports the same types on every engine.

See [ColumnInfo](chapters/04-inspecting-databases/index.md#columninfo).

### Compatibility shim

A private module that works around a defect in one Ibis backend, for SQLite (binding pandas `NaT`), Oracle (pre-23ai SQL) or Exasol (a missing public `raw_sql`). Each is a candidate for removal once Ibis changes.

See [Compatibility shims](chapters/11-how-one-backend-serves-20-dialects/index.md#compatibility-shims).

### `conflict_columns`

The match key of an `upsert()` call: one column name or a list. How a match is found depends on the dialect's upsert family; on `ON CONFLICT` dialects a unique index or primary key on exactly these columns is required.

See [Match-key semantics by dialect](chapters/06-creating-and-changing-tables/index.md#match-key-semantics-by-dialect).

### `connect()`

The `Backend` method that opens the live connection and returns the backend. It is idempotent, imports the dialect's driver, and is the only point where an auth profile is applied, through `connect(auth_profile=...)`.

See [connect and close](chapters/02-installing-and-connecting/index.md#connect-and-close).

### Connect-time composition

The rule that a backend profile and an auth profile meet only inside `connect()`, where the pairing is validated and rendered into driver arguments. A constructed but unconnected backend therefore holds no secrets.

See [Connect-time composition](chapters/03-connection-profiles-and-authentication/index.md#connect-time-composition).

### Connection adoption

Wrapping a connection that other code already opened, so the backend shares the caller's session, temporary objects and open transaction instead of opening its own. It is done with `IbisBackend.from_ibis_connection()` or `IbisBackend.from_raw_connection()`.

See [Connection adoption](chapters/09-adopting-existing-connections/index.md#connection-adoption).

### Connection builder

The callable in `DialectSpec.connection_builder` that receives the merged configuration as keyword arguments and returns a live Ibis backend object. URL-style builders keep credentials out of the URL and pass them as keyword arguments.

See [Connection builders](chapters/11-how-one-backend-serves-20-dialects/index.md#connection-builders).

### Connection factory

The module `mountainash_data.core.factories.connection_factory`, not a class. It holds `build_driver_kwargs()`, `build_connection_string()`, `apply_auth_adapter()`, `provider_for_dialect()` and `provider_for_scheme()`.

See [ConnectionFactory](chapters/12-from-settings-to-driver-arguments/index.md#connectionfactory).

### Connection ownership

Whether closing the backend also closes the underlying connection. A backend opened with `connect()` always owns its connection; an adopted one owns it only when constructed with `owns_connection=True`.

See [Connection ownership](chapters/09-adopting-existing-connections/index.md#connection-ownership).

### Connection profile

A typed, validated description of how to reach one database, held in an instance of a `BackendProfile` subclass such as `PostgreSQLBackendProfile`.

See [BackendProfile and SettingsParameters](chapters/03-connection-profiles-and-authentication/index.md#backendprofile-and-settingsparameters).

### Connection URL

A string of the form `scheme://user:password@host:port/database` passed to `IbisBackend`. The dialect is detected from the scheme at construction, and any credentials are removed from the URL and applied as a `PasswordAuthProfile` at connect time.

See [IbisBackend from a URL](chapters/02-installing-and-connecting/index.md#ibisbackend-from-a-url) and [Credentials in connection URLs](chapters/02-installing-and-connecting/index.md#credentials-in-connection-urls).

### Context manager

An object usable in a `with` block. Entering an `IbisBackend` calls `connect()` with no auth profile and leaving it calls `close()`, even when the block raises; `transaction()` is also a context manager.

See [The context manager](chapters/02-installing-and-connecting/index.md#the-context-manager).

### `create_index()`

An `IbisBackend` method that creates a secondary index, with optional `unique`, `index_type`, a partial `where=` predicate and `if_not_exists` (default `True`). `create_unique_index()` is the same call with `unique=True`. Options the dialect cannot honour raise `ValueError` before any SQL is sent.

See [Creating indexes](chapters/07-indexes/index.md#creating-indexes).

### `create_table()`

An `IbisBackend` method that creates a table from data (a list of dicts, a frame or an Ibis expression) or from a schema alone. It delegates to Ibis and accepts `namespace=`, `temp=` and `overwrite=`.

See [Creating tables and views](chapters/06-creating-and-changing-tables/index.md#creating-tables-and-views).

### `create_view()`

An `IbisBackend` method that stores an Ibis expression as a database view, compiled to the engine's SQL once at creation. `drop_view()` removes it.

See [Creating tables and views](chapters/06-creating-and-changing-tables/index.md#creating-tables-and-views).

### Credential redaction

Removing credentials from connection error messages: URL user information, secret-looking query parameters and explicitly supplied secret values are replaced with `***`. A redacted built-in exception keeps its type; a redacted driver exception becomes `BackendConnectionError`.

See [Credential redaction](chapters/12-from-settings-to-driver-arguments/index.md#credential-redaction).

### Credential transformation

The step that turns an auth profile's fields (`USERNAME`, `TOKEN`, `ACCESS_TOKEN`) into the argument names and types a particular driver expects. Auth adapters perform it.

See [The auth adapter registry](chapters/12-from-settings-to-driver-arguments/index.md#the-auth-adapter-registry).

## D

### `DATABASES_REGISTRY`

The settings registry that maps each backend name, such as `"postgresql"`, to its `BackendSpec` and `*BackendProfile` class. It is filled by the `register` decorator when `mountainash_data.core.settings` is imported and read by `get_descriptor()` and `get_settings_class()`.

See [The settings registry and automatic registration](chapters/03-connection-profiles-and-authentication/index.md#the-settings-registry-and-automatic-registration).

### DDL

Data definition language: SQL statements that change structure rather than rows, such as `CREATE TABLE`, `ALTER TABLE` and `CREATE INDEX`. In `IbisBackend` these are `create_table()`, `drop_table()`, `create_view()`, `drop_view()`, `add_columns()`, `rename_table()` and the index methods.

See [Creating tables and views](chapters/06-creating-and-changing-tables/index.md#creating-tables-and-views).

### Decorator

A callable applied to a function or class at definition time with `@name` syntax. The package uses decorators for registration (`@register`) and relies on the standard `@dataclass(frozen=True)` and `@runtime_checkable`.

See [Python ideas the package leans on](chapters/01-what-mountainash-data-is/index.md#python-ideas-the-package-leans-on).

### Dialect

One of the 20 database engines `IbisBackend` serves, named by its Ibis backend key, such as `sqlite`, `duckdb`, `postgres` or `snowflake`. The key is passed as `IbisBackend(dialect=...)` and is also the name of the matching install extra.

See [IbisBackend by dialect and keyword arguments](chapters/02-installing-and-connecting/index.md#ibisbackend-by-dialect-and-keyword-arguments).

### `DIALECTS`

The dialect registry: a `dict[str, DialectSpec]` with one entry per dialect key. It is the only place a dialect exists, and an unknown key raises `KeyError` at construction.

See [DialectSpec and the DIALECTS registry](chapters/11-how-one-backend-serves-20-dialects/index.md#dialectspec-and-the-dialects-registry).

### `DialectSpec`

The frozen dataclass that describes one dialect as data: connection fields, write capabilities, operation hooks, transaction fields and adoption fields. `IbisBackend` reads it for every engine-specific decision instead of using a subclass per database.

See [DialectSpec and the DIALECTS registry](chapters/11-how-one-backend-serves-20-dialects/index.md#dialectspec-and-the-dialects-registry).

### Dispatch order

The sequence every hook-dispatched `IbisBackend` method follows: a dialect hook wins if set; otherwise an absent capability raises `NotImplementedError`; otherwise the generic default runs.

See [Operation hooks and dispatch order](chapters/11-how-one-backend-serves-20-dialects/index.md#operation-hooks-and-dispatch-order).

### DML

Data manipulation language: SQL statements that change rows rather than structure. In `IbisBackend` these are `insert()`, `truncate()` and `upsert()`.

See [Inserting and truncating](chapters/06-creating-and-changing-tables/index.md#inserting-and-truncating).

### `drop_index()`

An `IbisBackend` method that drops an index by name, a no-op for a missing index when `if_exists=True` (the default). Table-scoped dialects (MySQL, SQL Server, SingleStoreDB) require `table_name=`.

See [Dropping indexes](chapters/07-indexes/index.md#dropping-indexes).

### `DropScope`

The enum that says how a dialect names an index in `DROP INDEX`. `SCHEMA_GLOBAL` drops by index name alone; `TABLE_SCOPED` needs `DROP INDEX name ON table`.

See [Capability descriptors](chapters/11-how-one-backend-serves-20-dialects/index.md#capability-descriptors).

## E

### Extra

An optional dependency group installed with `pip install 'mountainash-data[<extra>]'`. Each dialect has an extra with the same name that installs its Ibis backend and driver; the base package installs no database driver.

See [Choosing extras](chapters/02-installing-and-connecting/index.md#choosing-extras).

## F

### Fluent chaining

Calling several methods in one expression because each state-changing `IbisBackend` method returns the backend itself. Methods that return data, such as `table()` or `list_tables()`, end the chain.

See [Fluent chaining](chapters/05-reading-tables-and-running-queries/index.md#fluent-chaining).

### `from_ibis_connection()`

An `IbisBackend` class method that wraps an already-connected Ibis backend object for any registered dialect. It requires `dialect=`, defaults to `owns_connection=False`, and accepts `apply_session_options`.

See [from_ibis_connection](chapters/09-adopting-existing-connections/index.md#from_ibis_connection).

### `from_raw_connection()`

An `IbisBackend` class method that adopts a native driver handle and performs the Ibis wrapping step itself. It is enabled only for dialects with verified raw adoption (DuckDB and MotherDuck) and can preserve session options with `preserve_session=True`.

See [from_raw_connection and verified dialects](chapters/09-adopting-existing-connections/index.md#from_raw_connection-and-verified-dialects).

### Frozen dataclass

A dataclass declared with `frozen=True`, whose fields cannot be assigned after construction; `dataclasses.replace()` derives a modified copy. `Namespace` and every inspection result are frozen.

See [Frozen, backend-agnostic metadata](chapters/04-inspecting-databases/index.md#frozen-backend-agnostic-metadata).

## G

### Generic-default operation

One implementation that serves every dialect declaring the relevant capability, with no per-dialect code: `upsert`, `rename_table`, `add_columns`, the index DDL methods and `list_indexes`. SQL is rendered with sqlglot off the live connection's compiler.

See [Generic-default operations and sqlglot rendering](chapters/11-how-one-backend-serves-20-dialects/index.md#generic-default-operations-and-sqlglot-rendering).

### `get_descriptor()`

A function in `mountainash_data.core.settings` that returns the `BackendSpec` registered under a backend name. `get_settings_class()` returns the profile class; both raise `KeyError` for an unknown name.

See [The settings registry and automatic registration](chapters/03-connection-profiles-and-authentication/index.md#the-settings-registry-and-automatic-registration).

## I

### Ibis

A Python dataframe library that compiles table expressions to each engine's SQL and runs them there. `IbisBackend` connects through an Ibis backend, delegates many operations to it, and returns Ibis tables from `table()`.

See [Where Ibis fits](chapters/01-what-mountainash-data-is/index.md#where-ibis-fits).

### Ibis table expression

The `ibis.Table` returned by `table()`: a lazy expression bound to the backend's connection. Filters, aggregates and joins build a larger expression that runs on the database only when a result is requested.

See [table() and Ibis expressions](chapters/05-reading-tables-and-running-queries/index.md#table-and-ibis-expressions).

### `IbisBackend`

The package's single `Backend` implementation, serving all 20 Ibis dialects from one class by reading each dialect's `DialectSpec`. It is constructed from a dialect name and keyword arguments, a connection URL, or `SettingsParameters`.

See [IbisBackend by dialect and keyword arguments](chapters/02-installing-and-connecting/index.md#ibisbackend-by-dialect-and-keyword-arguments).

### `IbisConnection`

The wrapper `connect()` stores around the live Ibis backend object, recording the dialect spec and whether the package owns the connection. `get_connection()` returns it, `ibis_connection()` returns the Ibis object inside, and `raw_driver_connection()` the native handle.

See [The live IbisConnection](chapters/02-installing-and-connecting/index.md#the-live-ibisconnection).

### `in_transaction()`

A `Backend` predicate that is `True` while a `transaction()` scope is registered on the backend's native connection, whether that scope owns the unit or joined the caller's. It never raises and returns `False` on dialects without transactions or on an unconnected backend.

See [Checking scope and native state](chapters/08-transactions/index.md#checking-scope-and-native-state).

### `index_exists()`

An `IbisBackend` method that returns `True` or `False` by querying the engine's catalog for an index name, optionally limited to `table_name=`. It is the same check the emulated `IF [NOT] EXISTS` paths use.

See [index_exists](chapters/07-indexes/index.md#index_exists).

### `IndexCapability`

The frozen dataclass in `DialectSpec.index_caps` that declares a dialect's secondary-index support: `drop_scope`, `partial`, `native_if_not_exists`, `native_if_exists` and permitted `index_types`. `None` means index operations raise `NotImplementedError`.

See [Capability descriptors](chapters/11-how-one-backend-serves-20-dialects/index.md#capability-descriptors).

### `IndexInfo`

The frozen inspection result describing one index: `name`, `unique`, `is_primary`, `columns`, `index_type`, `included_columns`, `is_valid`, `definition` and `metadata`. `list_indexes()` returns a list of them.

See [IndexInfo](chapters/04-inspecting-databases/index.md#indexinfo) and [Listing indexes and IndexInfo](chapters/07-indexes/index.md#listing-indexes-and-indexinfo).

### `insert()`

An `IbisBackend` method that appends rows from a list of dicts, frame or Ibis expression, delegating to Ibis. `overwrite=True` replaces the table's rows while keeping its definition.

See [Inserting and truncating](chapters/06-creating-and-changing-tables/index.md#inserting-and-truncating).

### `inspect_catalog()`

A `Backend` method that returns a `CatalogInfo` holding every namespace in a catalog with its table names. On a server database it issues one table listing per namespace.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### `inspect_namespace()`

A `Backend` method that returns a `NamespaceInfo` for one single-level namespace: its location and the names of its tables, without reading column metadata.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### `inspect_table()`

A `Backend` method that returns a `TableInfo` for one table, with its columns and the location passed in. On `IbisBackend` any failure is re-raised as `ValueError` naming the table.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

## J

### Joined transaction

A `transaction()` scope that enters while the caller already has a transaction open on the native connection. It sends no `BEGIN` on entry and no `COMMIT` or `ROLLBACK` on exit, leaving completion to the caller; only SQLite, DuckDB and PostgreSQL can detect and join a caller's transaction.

See [Owned and joined transactions](chapters/08-transactions/index.md#owned-and-joined-transactions).

### JWT adapter

The Trino auth adapter that turns a `JWTAuthProfile` into `auth=trino.auth.JWTAuthentication(token)`. Trino's password and Kerberos adapters likewise build `trino.auth` objects, so they need the `trino` extra.

See [Adapter families](chapters/12-from-settings-to-driver-arguments/index.md#adapter-families).

## L

### `list_catalogs()`

A `Backend` method that returns the catalog names a connection can see, falling back to the current catalog or the dialect name so a live connection always returns at least one entry.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### `list_indexes()`

An `IbisBackend` method that returns one `IndexInfo` per index on a table, from a per-dialect catalog query. Dialects without index support raise `NotImplementedError`.

See [Listing indexes and IndexInfo](chapters/07-indexes/index.md#listing-indexes-and-indexinfo).

### `list_namespaces()`

A `Backend` method that returns namespace names, optionally within one catalog. Outside a transaction scope a failed listing prints the error and returns an empty list; inside one it raises.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### `list_tables()`

A `Backend` method that returns the table names in the default namespace or a named one. Like the other `list_*` methods, it degrades to an empty list outside a transaction scope and raises inside one.

See [The inspection methods](chapters/04-inspecting-databases/index.md#the-inspection-methods).

### Live-database harness

The runner `python -m scripts.live_db` (Hatch script `live-db`) that runs the live test tier against a named target, starting Compose services where configured. It refuses destructive tests on an external target unless that target opts in.

See [The live-database harness](chapters/15-contributing-and-testing/index.md#the-live-database-harness).

## M

### Memtable

An Ibis in-memory table (`ibis.memtable(...)`), which package writes create from data frames and stage as a temporary table on the connection. After a scope rolls back, reusing the same memtable expression fails, so retries need a fresh one.

See [The memtable rollback caveat](chapters/08-transactions/index.md#the-memtable-rollback-caveat).

### Missing-driver `ImportError`

The error `connect()` raises when a dialect's driver module is not installed, with the extra to install appended to the message. Constructing `IbisBackend` imports no driver, so it appears only at connect time.

See [Missing-driver errors](chapters/02-installing-and-connecting/index.md#missing-driver-errors).

## N

### Namespace

The package's neutral word for the container that holds tables: a schema in PostgreSQL, a database in MySQL, a dataset in BigQuery. Methods that take a table location accept it as a `namespace=` argument.

See [Databases, schemas and catalogs as the package models them](chapters/01-what-mountainash-data-is/index.md#databases-schemas-and-catalogs-as-the-package-models-them).

### `Namespace`

The frozen, hashable value object that locates tables, with a `path` tuple of levels and an explicit `catalog`. Empty fields mean the connection's defaults; it has `dotted` and `is_default` properties and does not render itself into SQL.

See [Namespace and NamespaceLike](chapters/04-inspecting-databases/index.md#namespace-and-namespacelike).

### `NamespaceInfo`

The frozen inspection result describing one namespace: a `location` (a `Namespace`) and a sequence of table names. Its `name` property is the last path segment.

See [NamespaceInfo](chapters/04-inspecting-databases/index.md#namespaceinfo).

### `NamespaceLike`

The type alias for anything a `namespace=` argument accepts: a `Namespace`, a string, a tuple of strings or `None`. `Namespace.coerce()` converts it, and strings and tuples only ever fill `path`.

See [Namespace and NamespaceLike](chapters/04-inspecting-databases/index.md#namespace-and-namespacelike).

### `native_transaction_open()`

A `Backend` method that asks the driver whether a transaction is open. It returns `True` or `False` on SQLite, DuckDB and PostgreSQL, `False` on dialects without transactions or an unconnected backend, and `None` where the dialect has no native probe.

See [Checking scope and native state](chapters/08-transactions/index.md#checking-scope-and-native-state).

### Nested scope

A `transaction()` opened inside another scope on the same native connection. Nesting is flat: there are no savepoints, the inner scope joins the outer unit, and only the outermost scope completes it.

See [Nested scopes are flat](chapters/08-transactions/index.md#nested-scopes-are-flat).

### `NoAuthProfile`

The auth profile that carries no credentials and means "connect without package-supplied credentials". Omitting `auth_profile` is the same as passing it, so backends whose `supported_auth` omits it require credentials.

See [Auth profiles](chapters/03-connection-profiles-and-authentication/index.md#auth-profiles).

### `NotImplementedError`

The exception an `IbisBackend` operation raises when the dialect does not declare the capability it needs: no upsert style, no index capability, or no verified raw adoption. The check runs before any SQL is sent.

See [NotImplementedError and other failure modes](chapters/10-supported-databases-and-limits/index.md#notimplementederror-and-other-failure-modes).

## O

### OAuth adapter

The Snowflake auth adapter that maps `TokenAuthProfile` or `OAuth2AuthProfile` to `authenticator="oauth"` and `token=<access token>`. It reads only the access token and performs no token exchange or refresh.

See [Adapter families](chapters/12-from-settings-to-driver-arguments/index.md#adapter-families).

### Operation hook

A callable on `DialectSpec` that replaces the generic implementation of one operation for one dialect, such as `truncate_table_hook` on SQLite or `list_indexes_hook` on DuckDB. A set hook takes precedence over the capability check and the generic default.

See [Operation hooks and dispatch order](chapters/11-how-one-backend-serves-20-dialects/index.md#operation-hooks-and-dispatch-order).

### Owned transaction

A transaction that the outermost `transaction()` scope began itself on an idle connection. The scope commits it on a clean exit and rolls it back on an exception or a poisoned unit.

See [Owned and joined transactions](chapters/08-transactions/index.md#owned-and-joined-transactions).

### Ownership dialect

One of the dialects (SQLite, DuckDB and PostgreSQL) whose `transaction()` probes native state on entry, so it can own or join, and whose package calls are protected inside a scope. Other transactional dialects always begin and complete their own transaction.

See [Transaction scopes](chapters/08-transactions/index.md#transaction-scopes).

## P

### Parameter tier

The `"core"` or `"advanced"` label on each `ParameterSpec`. It is documentation metadata only; both tiers are ordinary profile fields emitted to the driver in the same way.

See [BackendSpec, ParameterSpec and parameter tiers](chapters/03-connection-profiles-and-authentication/index.md#backendspec-parameterspec-and-parameter-tiers).

### `ParameterSpec`

The description of one profile field: upper-case `name`, pydantic `type`, `tier`, `default`, the `driver_key` it is emitted under, a `secret` flag and optional `transform` and `validator`. It is defined in `mountainash-settings` and re-exported from `mountainash_data.core.settings`.

See [BackendSpec, ParameterSpec and parameter tiers](chapters/03-connection-profiles-and-authentication/index.md#backendspec-parameterspec-and-parameter-tiers).

### `PasswordAuthProfile`

The auth profile holding `USERNAME` and `PASSWORD`, the common case for SQL servers. Credentials in a connection URL are converted into one at connect time.

See [Auth profiles](chapters/03-connection-profiles-and-authentication/index.md#auth-profiles).

### Poisoned unit of work

A transaction unit that can no longer commit because an exception escaped a nested scope or a package call failed inside it, even if the caller caught the error. Later package calls into it are refused, and the outermost exit raises `TransactionPoisonedError`, rolling back an owned unit.

See [Nested scopes are flat](chapters/08-transactions/index.md#nested-scopes-are-flat).

### `preserve_session`

A keyword argument of `IbisBackend.from_raw_connection()`. When `True`, the declared session options are read before Ibis adopts the handle and restored afterwards, even if adoption fails.

See [from_raw_connection and verified dialects](chapters/09-adopting-existing-connections/index.md#from_raw_connection-and-verified-dialects).

### Protected package call

A package operation run inside a `transaction()` scope on SQLite, DuckDB or PostgreSQL, during which Ibis's internal `commit()` and `rollback()` are absorbed so they cannot end the unit early. A failure inside it poisons the unit; direct Ibis or raw-handle calls are not protected.

See [Protected package calls](chapters/13-transaction-and-adoption-internals/index.md#protected-package-calls).

### Protocol

A `typing.Protocol` class (PEP 544) that describes a type by the members it has rather than what it inherits from. Any class with matching members satisfies it; this is structural typing.

See [The Backend protocol in one page](chapters/01-what-mountainash-data-is/index.md#the-backend-protocol-in-one-page).

### Protocol conformance testing

Testing that a class satisfies `Backend` in three parts: the shallow `isinstance(obj, Backend)` check, behavioural assertions for the protocol's stated contract, and a static type check of an annotated assignment.

See [Protocol conformance testing](chapters/14-extending-mountainash-data/index.md#protocol-conformance-testing).

### Provider type

The settings-layer identifier of a backend (a `CONST_DB_PROVIDER_TYPE` member for the built-in backends) stored in `BackendSpec.provider_type`. Auth adapters are keyed by it, and `provider_for_dialect()` and `provider_for_scheme()` map dialect keys and URL schemes to it.

See [ConnectionFactory](chapters/12-from-settings-to-driver-arguments/index.md#connectionfactory).

### Pull-side bridge

The way a table reaches the separate `mountainash` package: pass the Ibis table from `table()` to `mountainash.relation()`. The backend has no conversion method, and the relation runs on the same connection.

See [Bridging to mountainash relations](chapters/05-reading-tables-and-running-queries/index.md#bridging-to-mountainash-relations).

### Pydantic

A Python library for validated data models. Backend profiles and auth profiles are pydantic models, so values are converted and checked when a profile is built, before any connection is attempted.

See [Python ideas the package leans on](chapters/01-what-mountainash-data-is/index.md#python-ideas-the-package-leans-on).

## R

### Raw adoption

Adopting a native driver handle with `from_raw_connection()`. It is allowed only for dialects whose `DialectSpec.raw_adoption_verified` flag is set (DuckDB and MotherDuck); others raise `NotImplementedError` and point to `from_ibis_connection()`.

See [from_raw_connection and verified dialects](chapters/09-adopting-existing-connections/index.md#from_raw_connection-and-verified-dialects).

### `raw_driver_connection()`

A `Backend` method that returns the native driver handle behind the connection, such as a `sqlite3.Connection` or `DuckDBPyConnection`. It raises rather than returning `None` when there is no handle, and statements run on it are outside the package's transaction protection.

See [The raw driver connection](chapters/08-transactions/index.md#the-raw-driver-connection).

### `register`

The decorator, `DATABASES_REGISTRY.decorator()`, that records a `BackendProfile` subclass under its spec's name. It raises `TypeError` for a class without a class-body `__spec__` and `ValueError` for a name already taken.

See [The settings registry and automatic registration](chapters/03-connection-profiles-and-authentication/index.md#the-settings-registry-and-automatic-registration).

### Registry

A lookup table mapping a name to a definition, filled as definitions are declared so dispatching code needs no hard-coded list. The package keeps a settings registry (`DATABASES_REGISTRY`) and a dialect registry (`DIALECTS`).

See [Python ideas the package leans on](chapters/01-what-mountainash-data-is/index.md#python-ideas-the-package-leans-on).

### `rename_table()`

An `IbisBackend` method that renames a table in the connection's current schema, rendering each engine's syntax with sqlglot. It has no `namespace=` argument and requires simple identifiers.

See [Renaming tables](chapters/06-creating-and-changing-tables/index.md#renaming-tables).

### Rides-on backend

A settings backend with no Ibis backend of its own that connects through another: MotherDuck rides on DuckDB and Redshift on PostgreSQL. `BackendSpec.rides_on` records this as metadata, and `ibis_dialect` holds the underlying dialect.

See [Rides-on backends](chapters/03-connection-profiles-and-authentication/index.md#rides-on-backends).

### `run_expr()`

An `IbisBackend` method that executes an Ibis expression on the backend's connection and returns a scalar or a pandas `DataFrame`. Unlike `expr.execute()`, it is a protected package call inside a transaction scope.

See [run_expr and to_sql](chapters/05-reading-tables-and-running-queries/index.md#run_expr-and-to_sql).

### `run_sql()`

An `IbisBackend` method that wraps a SQL `SELECT` string as an Ibis table expression rather than returning rows. It is for row-producing queries, not DDL or `UPDATE`.

See [run_sql](chapters/05-reading-tables-and-running-queries/index.md#run_sql).

### Runtime-checkable protocol

A protocol decorated with `@typing.runtime_checkable`, which lets `isinstance()` test conformance. The check is shallow: it confirms each member exists, not that signatures or return types match.

See [The Backend protocol in one page](chapters/01-what-mountainash-data-is/index.md#the-backend-protocol-in-one-page).

## S

### Schema

A named container for tables inside a database. The package calls this level a namespace, since engines name it differently.

See [Databases, schemas and catalogs as the package models them](chapters/01-what-mountainash-data-is/index.md#databases-schemas-and-catalogs-as-the-package-models-them).

### Scope registry

The process-wide record of open `transaction()` scopes, keyed by the identity of the native driver connection rather than the `IbisBackend` object. Two backends wrapping one driver connection therefore share one unit of work.

See [The per-raw-handle scope registry](chapters/13-transaction-and-adoption-internals/index.md#the-per-raw-handle-scope-registry).

### `SecretStr`

The pydantic type used for secret fields in auth profiles; it prints as asterisks. Auth adapters unwrap it with `get_secret_value()` only when building driver arguments.

See [Auth profiles](chapters/03-connection-profiles-and-authentication/index.md#auth-profiles).

### Session-option preservation

Keeping a caller's session settings intact across connection adoption. Ibis changes DuckDB's `TimeZone` and `python_enable_replacements` when it wraps a handle; the package can snapshot and restore them (`preserve_session`) or set declared end values (`apply_session_options`).

See [Session-option snapshot and restore](chapters/13-transaction-and-adoption-internals/index.md#session-option-snapshot-and-restore).

### `SessionOption`

A record in `DialectSpec.adoption_mutations` naming one session setting Ibis changes on adoption, with the SQL to read its value and a function that renders the statement to set it. Only DuckDB and MotherDuck declare any.

See [Session-option snapshot and restore](chapters/13-transaction-and-adoption-internals/index.md#session-option-snapshot-and-restore).

### Settings backend

A `BackendSpec` plus a `BackendProfile` subclass decorated with `@register`, which makes a new way of configuring a connection available to `IbisBackend` through `SettingsParameters`.

See [Adding a settings backend](chapters/14-extending-mountainash-data/index.md#adding-a-settings-backend).

### `SettingsParameters`

The `mountainash-settings` object that names a profile class and supplies field values, configuration files, an environment prefix or a secret store. Passing it to `IbisBackend` validates the profile and selects the dialect from its spec.

See [BackendProfile and SettingsParameters](chapters/03-connection-profiles-and-authentication/index.md#backendprofile-and-settingsparameters).

### Simple identifier

A name made of letters, digits, `_` and `$` that does not start with a digit. Operations the package renders itself (upsert, `add_columns`, `rename_table`, index methods) require table, column, index and namespace names of this form and raise `ValueError` for dotted or quoted names.

See [Creating indexes](chapters/07-indexes/index.md#creating-indexes).

### sqlglot

A Python SQL parser and transpiler. The package uses it, with the live Ibis connection's dialect and type mapper, to render the SQL for generic-default operations such as upsert, `rename_table` and index DDL.

See [Generic-default operations and sqlglot rendering](chapters/11-how-one-backend-serves-20-dialects/index.md#generic-default-operations-and-sqlglot-rendering).

### SSL bundle adapter

MySQL's configuration adapter, which emits `ssl_mode` and collects the separate SSL file and cipher fields into one `ssl` dictionary with the driver's key names. It runs during profile emission, before any auth adapter.

See [Adapter families](chapters/12-from-settings-to-driver-arguments/index.md#adapter-families).

### Support matrix

The per-dialect table of upsert style, transaction support, index support, raw adoption and dialect-specific hooks, generated from the dialect registry.

See [Support matrix](chapters/10-supported-databases-and-limits/index.md#support-matrix).

### `supported_auth`

The non-empty tuple of auth profile types a `BackendSpec` accepts. On the settings path an auth profile outside it raises `ValueError` before any adapter runs.

See [Supported-auth validation](chapters/12-from-settings-to-driver-arguments/index.md#supported-auth-validation).

### `supports_transactions`

A `Backend` property that is `True` when the dialect has a transaction concept. On dialects where it is `False`, `transaction()` raises `TransactionUnsupportedError` unless called with `required=False`.

See [Transaction scopes](chapters/08-transactions/index.md#transaction-scopes).

## T

### `table()`

An `IbisBackend` method that looks a table up on the live connection and returns an `ibis.Table` expression without reading any rows.

See [table() and Ibis expressions](chapters/05-reading-tables-and-running-queries/index.md#table-and-ibis-expressions).

### `table_to_info()`

A public helper in `mountainash_data.backends.ibis.inspect` that builds a `TableInfo` from an Ibis table's schema, recording each column's name, Ibis type string and nullability. It issues no queries of its own.

See [How driver metadata becomes TableInfo](chapters/04-inspecting-databases/index.md#how-driver-metadata-becomes-tableinfo).

### `TableInfo`

The frozen inspection result describing one table or view: `name`, `columns` (a sequence of `ColumnInfo`), `location`, `description` and `metadata`, plus `column_names` and `qualified_name` properties.

See [TableInfo](chapters/04-inspecting-databases/index.md#tableinfo).

### Test tiers

The division of the test suite by what a test needs to run: the core tier (SQLite and DuckDB in memory, run by default), the optional-backend tier (one extra driver, no server) and the live tier (a running server, through the live-database harness).

See [Test tiers](chapters/15-contributing-and-testing/index.md#test-tiers).

### `to_sql()`

An `IbisBackend` method that compiles an Ibis expression to the SQL string the connected dialect would run, without executing it.

See [run_expr and to_sql](chapters/05-reading-tables-and-running-queries/index.md#run_expr-and-to_sql).

### `transaction()`

A `Backend` context manager that groups package operations into one unit of work on the native driver connection. It takes one keyword, `required` (default `True`); what it does on entry and exit depends on the dialect's transaction support and whether a caller transaction is already open.

See [Transaction scopes](chapters/08-transactions/index.md#transaction-scopes).

### `TransactionError`

The base class of the transaction errors in `mountainash_data.core.errors`, a `RuntimeError` subclass. Its subclasses are `TransactionUnsupportedError`, `TransactionPoisonedError` and `TransactionIntegrityError`.

See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### `TransactionIntegrityError`

Raised when a scope cannot trust the native transaction state: unknown on entry, or already ended, aborted or unknown at exit. The scope commits nothing and rolls back an owned transaction that is still open.

See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### `TransactionPoisonedError`

Raised when a caught failure poisoned the unit of work, at the outermost scope exit or by any later package call into the unit. An owned unit is rolled back; a joined unit is left for the caller.

See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### `TransactionSupport`

The enum on `DialectSpec` with values `NONE`, `FULL` and `LIMITED`. `NONE` makes `transaction()` raise `TransactionUnsupportedError`; `FULL` and `LIMITED` are handled identically, with `LIMITED` recording narrower engine guarantees.

See [Capability descriptors](chapters/11-how-one-backend-serves-20-dialects/index.md#capability-descriptors).

### `TransactionUnsupportedError`

Raised by `transaction()` with `required=True` on a dialect with no transaction concept, such as BigQuery or ClickHouse. With `required=False` the scope warns once and runs as a no-op instead.

See [Transaction errors](chapters/08-transactions/index.md#transaction-errors).

### `truncate()`

An `IbisBackend` method that deletes every row and keeps the table. Most dialects emit `TRUNCATE TABLE` through Ibis; SQLite, which has no `TRUNCATE`, uses a hook that deletes all rows.

See [Inserting and truncating](chapters/06-creating-and-changing-tables/index.md#inserting-and-truncating).

## U

### Unit of work

The set of operations that commit or roll back together inside a `transaction()` scope and any scopes nested in it on the same native connection.

See [Transaction scopes](chapters/08-transactions/index.md#transaction-scopes).

### `update_condition`

An `upsert()` argument: a function `(incoming, existing) -> bool expression` that limits which matched rows are updated. It must be a scalar row-level predicate, and the MySQL family rejects it.

See [Conditional updates](chapters/06-creating-and-changing-tables/index.md#conditional-updates).

### Upsert

An operation that inserts rows whose key is new and updates rows whose key already exists, in one SQL statement. `IbisBackend.upsert()` renders the dialect's family, validates the request against the target table first, and never deletes rows.

See [Upsert](chapters/06-creating-and-changing-tables/index.md#upsert).

### `UpsertStyle`

The enum on `DialectSpec` that selects which SQL family the generic upsert renders: `ON_CONFLICT`, `MERGE` or `ON_DUPLICATE_KEY`. A `None` style makes `upsert()` raise `NotImplementedError`.

See [Capability descriptors](chapters/11-how-one-backend-serves-20-dialects/index.md#capability-descriptors).
