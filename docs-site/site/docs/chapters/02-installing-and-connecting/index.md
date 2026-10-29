---
title: Installing and connecting
description: Install the right extra and open a connection from a dialect name or a URL.
date: 2026-10-29
---

# Installing and connecting

`IbisBackend` opens a connection to any of the package's 20 dialects from a dialect name plus driver keyword arguments, or from a connection URL. The base package installs no database driver; each dialect has a matching extra that pulls in the Ibis backend and its driver. Once constructed, a backend connects with `connect()`, releases its connection with `close()`, and can do both as a context manager. Typed settings profiles are a third way to construct a backend, covered in [Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md).

## Choosing extras

<!-- concept:101 -->
`mountainash-data` requires Python 3.12 or later and Ibis 12 or later. Its base dependencies are `mountainash-settings`, `mountainash-auth-client`, Ibis and sqlglot; it installs no database driver or dataframe library. Each dialect has an extra with the same name as its dialect key, which installs the matching Ibis backend dependencies:

| Extra | Backend or input |
|---|---|
| `sqlite`, `duckdb` | Local databases; enough for every recipe in `examples/` |
| `motherduck` | MotherDuck through DuckDB |
| `postgres`, `redshift`, `materialize` | PostgreSQL-family backends; these include `psycopg[binary]`, which bundles libpq |
| `mysql`, `singlestoredb`, `mssql`, `oracle` | MySQL/MariaDB, SingleStore, SQL Server and Oracle |
| `snowflake`, `bigquery`, `databricks` | Hosted analytics services |
| `trino`, `clickhouse`, `exasol`, `impala`, `risingwave`, `druid`, `pyspark` | Other Ibis backends |
| `polars` | Polars dataframes as table input |
| `all` | Every backend extra and Polars |

Some extras also need system software. SQL Server needs an ODBC driver manager and the Microsoft ODBC driver; MySQL may need client development libraries when no wheel is available; local PySpark needs Java; and a source build of RisingWave's `psycopg2` needs `pg_config`.

At the 0.1.0 development baseline, `mountainash-settings` and `mountainash-auth-client` are installed from sibling checkouts alongside this package:

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

Add further extras to the list in the last line, for example `-e '.[sqlite,duckdb,postgres,snowflake]'`.

## Missing-driver errors

<!-- concept:102 -->
A dialect can be named without its extra installed: constructing `IbisBackend` imports no driver. The driver is imported when `connect()` runs, and if it is missing the backend raises `ImportError` with the original message followed by the extra to install:

```python
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="postgres", host="db.example", database="app")
try:
    backend.connect()
except ImportError as exc:
    print(exc)
```

```text
No module named 'psycopg'

Install this backend with: pip install 'mountainash-data[postgres]'
```

Only a missing driver module is translated this way. Failures while connecting (a missing native library, an optional sub-dependency such as Spark Connect's grpc, bad credentials, an unreachable host) propagate unchanged, or as `BackendConnectionError` with credentials removed from the message; see [Supported databases and limits](../10-supported-databases-and-limits/index.md). The driver import happens before credentials are applied, so a missing extra is reported first even when the auth profile is also wrong.

<!-- concept:19 -->
## IbisBackend by dialect and keyword arguments

`IbisBackend` is the package's single entry point for every Ibis-supported database. One class serves all 20 dialects: it looks up the dialect's `DialectSpec` record when it is constructed and dispatches through it, with no subclass per database. Its constructor accepts exactly one of three inputs:

| Input | Form | Typical use |
|---|---|---|
| Dialect name and keyword arguments | `IbisBackend(dialect="duckdb", database=":memory:")` | Tests, scripts, local databases |
| Connection URL | `IbisBackend("postgresql://db.example:5432/app")` | Configuration held as a single string |
| `SettingsParameters` | `IbisBackend(params)` | Typed, validated, environment-driven configuration ([chapter 3](../03-connection-profiles-and-authentication/index.md)) |

With the dialect form, the keyword arguments go to the dialect's connection builder at connect time. For the server dialects the builder reads the common keys (`host`, `port`, `user`, `password`, `database`), assembles a credential-free URL from them, and passes credentials and every other keyword to Ibis as connection arguments. Local dialects read fewer keys: SQLite uses only `database` (default `":memory:"`), and DuckDB uses `database` and `read_only` and passes the rest through. A bare string without `://` is also read as a dialect name:

```python
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="duckdb", database=":memory:", read_only=False)
print(backend.dialect)

backend = IbisBackend("sqlite", database=":memory:")  # a bare name is a dialect
print(backend.dialect)

try:
    IbisBackend(dialect="postgresql")
except KeyError as exc:
    print(exc)
```

```text
duckdb
sqlite
"Unknown ibis dialect 'postgresql'. Available: ['bigquery', 'clickhouse', 'databricks', 'druid', 'duckdb', 'exasol', 'impala', 'materialize', 'motherduck', 'mssql', 'mysql', 'oracle', 'postgres', 'pyspark', 'redshift', 'risingwave', 'singlestoredb', 'snowflake', 'sqlite', 'trino']"
```

The dialect keys are Ibis's backend names, so PostgreSQL is `postgres`, not `postgresql`. The `dialect` attribute holds the resolved key. Passing both a positional argument and `dialect=` raises `ValueError`, as does passing neither. Apart from the dialect name, nothing is validated at construction: keyword arguments are checked by Ibis and the driver when `connect()` runs. The extra arguments each dialect accepts are listed in the [Ibis backend documentation](https://ibis-project.org/backends/); the builders themselves are described in [How one backend serves 20 dialects](../11-how-one-backend-serves-20-dialects/index.md).

## IbisBackend from a URL

A string containing `://` is treated as a connection URL. The backend detects the dialect from the URL scheme when it is constructed, and at connect time passes the URL to `ibis.connect()`:

```python
from urllib.parse import quote

from mountainash_data import IbisBackend

for url in ("sqlite://", "duckdb://", "postgresql://db.example/app", "duckdb://md:analytics"):
    print(url, "->", IbisBackend(url).dialect)

password = quote("pa/ss:word", safe="")
print(IbisBackend(f"postgresql://app:{password}@db.example:5432/app").dialect)

for url in ("postgresql://app:pa/ss@db.example/app", "redshift://cluster.example:5439/dev"):
    try:
        IbisBackend(url)
    except ValueError as exc:
        print(exc)
```

```text
sqlite:// -> sqlite
duckdb:// -> duckdb
postgresql://db.example/app -> postgres
duckdb://md:analytics -> motherduck
postgres
Invalid connection URL (value redacted): percent-encode credentials with urllib.parse.quote(value, safe='') or pass an auth profile
Cannot detect ibis dialect from URL scheme: 'redshift'
```

Scheme detection follows a few rules. `postgres://` and `postgresql://` both resolve to `postgres`. `duckdb://md:<database>` resolves to `motherduck`, and any other `duckdb://` URL to `duckdb`; `sqlite://` and `duckdb://` with no path open in-memory databases. Redshift has no URL scheme of its own (its dialect uses `postgres://`, which resolves to `postgres`), so connect to Redshift with `dialect="redshift"` or a settings profile. Databricks has no URL form at all. An unrecognised scheme raises `ValueError` at construction.

Keyword arguments passed alongside a URL, as in `IbisBackend(url, connect_timeout=5)`, are applied on top of anything derived from the URL when the backend connects.

<!-- concept:12 -->
## connect and close

`connect()` builds the live connection and returns the backend itself, so calls can be chained. It is idempotent: calling it on a connected backend returns immediately without opening a second connection. It is also where credentials are applied. `connect(auth_profile=...)` takes an auth profile and composes it with the backend's configuration at that moment, so a constructed but unconnected backend holds no secrets ([Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md) covers auth profiles). Every operation that needs a connection raises `RuntimeError` until `connect()` has run:

```python
from mountainash_data import IbisBackend

backend = IbisBackend(dialect="duckdb", database=":memory:")
try:
    backend.list_tables()
except RuntimeError as exc:
    print(exc)

print(backend.connect() is backend)   # returns self
print(backend.connect() is backend)   # already connected: no new connection
print(backend.list_tables())
backend.close()
backend.close()                       # idempotent
```

```text
IbisBackend is not connected. Call connect() first.
True
True
[]
```

<!-- concept:13 -->
`close()` releases the connection, returns the backend, and is idempotent. After `close()` the backend is unconnected again and `connect()` opens a fresh connection. For an in-memory database that means a new, empty database. Two cases change what `close()` does. Inside an open `transaction()` scope it raises `RuntimeError` instead of closing, because the open unit of work depends on the handle ([Transactions](../08-transactions/index.md)). For a connection adopted with `from_ibis_connection()` or `from_raw_connection()`, it marks the wrapper closed but leaves the caller's connection open ([Adopting existing connections](../09-adopting-existing-connections/index.md)).

<!-- concept:20 -->
## The live IbisConnection

`connect()` stores the live connection as an `IbisConnection`, a wrapper around the Ibis backend object that records the dialect spec and whether the package owns the connection. `IbisBackend` delegates its listing and inspection calls to it, and three accessors expose the layers underneath:

| Accessor | Returns |
|---|---|
| `get_connection()` | The `IbisConnection` wrapper |
| `ibis_connection()` | The Ibis backend object, for Ibis APIs the package does not wrap |
| `raw_driver_connection()` | The native driver handle (`sqlite3.Connection`, `duckdb.DuckDBPyConnection`, a psycopg connection, and so on) |

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    conn = backend.get_connection()
    print(type(conn).__name__, conn.list_catalogs())
    print(type(backend.ibis_connection()))
    print(type(backend.raw_driver_connection()))
```

```text
IbisConnection ['memory', 'system', 'temp']
<class 'ibis.backends.duckdb.Backend'>
<class 'duckdb.duckdb.DuckDBPyConnection'>
```

All three raise `RuntimeError` on an unconnected backend. The handle returned by `raw_driver_connection()` is an escape hatch for driver-specific SQL and DDL. Its kind varies by dialect and it is not guaranteed to follow DBAPI, so check the dialect before relying on DBAPI methods. Close the connection through the backend's `close()` rather than through the wrapper or the Ibis object, so the backend's own state stays consistent.

<!-- concept:34 -->
## The context manager

`IbisBackend` is a context manager. Entering the `with` block calls `connect()` with no auth profile and binds the backend; leaving it calls `close()`, including when the block raises:

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="sqlite", database=":memory:") as backend:
    print(backend.list_tables())
```

```text
[]
```

Because `__enter__` passes no auth profile, use it for backends whose credentials travel in the URL or keyword arguments, or that need none. When credentials come from an auth profile, call `connect(auth_profile=...)` explicitly and close in a `finally` block.

<!-- concept:103 -->
## Credentials in connection URLs

A URL may carry a username and password in its authority (`scheme://user:password@host:port/database`). The backend never passes them to Ibis inside the URL. At connect time it removes the user information from the URL, turns it into a `PasswordAuthProfile`, and applies that through the same auth adapter an explicit `auth_profile` would use. Because of this a URL can carry credentials or an auth profile can be passed, but not both:

```python
from mountainash_data import IbisBackend
from mountainash_data.core.settings import PasswordAuthProfile

backend = IbisBackend("sqlite://app:secret@/orders.db")
try:
    backend.connect(auth_profile=PasswordAuthProfile(USERNAME="app", PASSWORD="secret"))
except ValueError as exc:
    print(exc)
```

```text
both URL credentials and an explicit auth_profile given
```

Credentials are percent-decoded before use, so encode any reserved character (`/`, `:`, `@`, `?`, `#`, `%`) with `urllib.parse.quote(value, safe="")` when building the URL, as in the `pa/ss:word` example above. An unencoded `/` in a password ends the URL authority early. The backend reports that as a `ValueError` whose message does not echo any part of the URL.

The auth adapter for URL credentials is found by matching the URL scheme against the settings profiles' schemes. Use `postgresql://`, not `postgres://`, for a PostgreSQL URL that carries credentials: in the source, `postgres://` with credentials raises `KeyError("no provider_type for URL scheme 'postgres'")` at connect time, because no profile declares that scheme (`src/mountainash_data/core/factories/connection_factory.py`, `provider_for_scheme`). A URL without credentials is unaffected.

Keeping credentials out of the URL altogether, in an auth profile passed to `connect()`, avoids encoding problems and keeps secrets out of configuration strings and logs. [Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md) covers that route.
