---
title: Adopting existing connections
description: Wrap a connection you already have while keeping control of its lifecycle.
date: 2026-10-29
---

# Adopting existing connections

`IbisBackend` can wrap a connection that some other code already opened, instead of opening its own. Two class methods do this: `from_ibis_connection` adopts an Ibis backend object, and `from_raw_connection` adopts a native driver handle such as a `duckdb.DuckDBPyConnection`. The adopted backend shares the caller's session, so tables, temporary objects and an open transaction are visible to both sides. By default the caller keeps ownership: closing the wrapper leaves the connection open. This is the usual route when a framework, a test fixture or an application pool hands you a live connection and you want the package's operations on it.

The anchor recipe is [`examples/connection_adoption`](https://github.com/mountainash-io/mountainash-data/tree/main/examples/connection_adoption). The examples below run on in-memory SQLite and DuckDB, and each block continues from the one before it.

## Connection ownership

<!-- concept:138 -->
Both adoption constructors take `owns_connection`, which defaults to `False`. It decides what `close()` does to the underlying connection:

| `owns_connection` | `close()` on the backend | Use when |
|---|---|---|
| `False` (default) | Releases the wrapper only; the connection stays open for the caller | The caller, a fixture or a pool manages the connection's lifetime |
| `True` | Disconnects the underlying connection | You are handing the connection over and will not use it again |

A backend opened with `connect()` always owns its connection. Ownership changes only what happens at `close()`; every other method behaves the same.

```python
import ibis
from mountainash_data import IbisBackend

con = ibis.sqlite.connect(":memory:")

borrowed = IbisBackend.from_ibis_connection(con, dialect="sqlite")
borrowed.create_table("sales", [{"id": 1, "total": 120}])
borrowed.close()
print(con.list_tables())
# → ['sales']

owned = IbisBackend.from_ibis_connection(con, dialect="sqlite", owns_connection=True)
owned.close()
try:
    con.list_tables()
except Exception as exc:
    print(type(exc).__name__, exc)
# → ProgrammingError Cannot operate on a closed database.
```

An adopted backend is connected as soon as it is constructed, so it needs no `connect()` call; calling `connect()` anyway returns the backend unchanged, which also makes `with IbisBackend.from_...(...) as backend:` work. After `close()`, though, the backend has forgotten the adopted connection. A later `connect()` builds a new connection from the dialect's own configuration, which for a DuckDB backend constructed this way is a fresh, empty in-memory database:

```python
import duckdb

raw = duckdb.connect(":memory:")
raw.execute("CREATE TABLE sales (id INTEGER)")

backend = IbisBackend.from_raw_connection(raw, dialect="duckdb")
print(backend.list_tables())
# → ['sales']
backend.close()
backend.connect()
print(backend.list_tables())
# → []
backend.close()
```

Adopt again with the same constructor rather than reconnecting. `close()` also raises `RuntimeError` while a `transaction()` scope is open on the connection, whichever side owns it.

## Connection adoption

<!-- concept:134 -->
Adopting a connection binds the backend to the caller's session rather than to a copy of it. Everything the package does runs on that session, and everything the caller does on the session is visible to the package. That includes transactions. On SQLite, DuckDB and PostgreSQL, if the caller has begun a transaction on the native handle, `backend.transaction()` detects it and joins it: the package's writes become part of the caller's unit of work, and the scope neither commits nor rolls it back when it exits.

```python
raw = duckdb.connect(":memory:")
raw.execute("CREATE TABLE sales (id INTEGER, total INTEGER)")

backend = IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=True)
raw.execute("BEGIN")
with backend.transaction():
    backend.insert("sales", [{"id": 1, "total": 120}])
print(backend.native_transaction_open())
# → True

raw.rollback()
print(raw.execute("SELECT count(*) FROM sales").fetchone())
# → (0,)
backend.close()
```

The caller's rollback removed the row the package inserted. Owned and joined scopes are covered in [Transactions](../08-transactions/index.md).

Which constructor to use depends on what you hold:

| You have | Use |
|---|---|
| An Ibis backend, for any registered dialect | `from_ibis_connection(con, dialect=...)` |
| A raw DuckDB or MotherDuck driver connection | `from_raw_connection(raw, dialect=...)` |
| A raw driver connection for any other dialect | Wrap it with Ibis yourself (for example `ibis.<backend>.from_connection(raw)` where that backend provides it), then call `from_ibis_connection` |

Both constructors require `dialect=`, naming the package dialect (`"duckdb"`, `"postgres"`, and so on) whose capabilities apply. The package does not infer it from the connection object.

## from_ibis_connection

<!-- concept:135 -->
`from_ibis_connection(ibis_conn, *, dialect, owns_connection=False, apply_session_options=None)` wraps an Ibis backend that is already connected. It works for every registered dialect, because the Ibis connection already exists and the package only has to wrap it.

Building the Ibis connection is the caller's step, and for some backends Ibis changes session settings while it adopts a native connection. DuckDB is the case the package records: Ibis sets `TimeZone` to UTC and turns off `python_enable_replacements`. By the time `from_ibis_connection` runs, the original values are gone. `apply_session_options` lets you declare the values the session should end with, and the package sets them after wrapping:

```python
def session(conn):
    return conn.execute(
        "SELECT current_setting('TimeZone'), current_setting('python_enable_replacements')"
    ).fetchone()

raw = duckdb.connect(":memory:")
raw.execute("SET TimeZone = 'Australia/Sydney'")
print(session(raw))
# → ('Australia/Sydney', True)

ibis_con = ibis.duckdb.from_connection(raw)
print(session(raw))
# → ('UTC', False)

backend = IbisBackend.from_ibis_connection(
    ibis_con, dialect="duckdb", apply_session_options={"timezone": "Australia/Sydney"}
)
print(session(raw))
# → ('Australia/Sydney', False)
backend.close()
```

The keys are the option names the dialect declares: `timezone` and `python_enable_replacements` for DuckDB and MotherDuck. Options you leave out keep whatever Ibis set. A name the dialect does not declare is ignored with a warning; for a dialect that declares no options, that is every name. To get the caller's original values back without having to know them, adopt the raw handle with `from_raw_connection(..., preserve_session=True)` instead.

## from_raw_connection and verified dialects

<!-- concept:136 -->
`from_raw_connection(raw_conn, *, dialect, owns_connection=False, preserve_session=False)` adopts a native driver handle and performs the Ibis wrapping step itself. Because the package owns that step, it can read the session options Ibis is about to change before Ibis touches them. With `preserve_session=True` it snapshots them, lets Ibis adopt the connection, and restores the snapshot afterwards. The restore runs even when Ibis's adoption raises, so a failed adoption does not leave the caller's session altered. An option that cannot be read is reported with a warning and is not restored. With the default `preserve_session=False` the result matches plain Ibis adoption, with the changes shown in the previous section.

```python
raw = duckdb.connect(":memory:")
raw.execute("SET TimeZone = 'Australia/Sydney'")

with IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=True) as backend:
    print(session(raw))
    # → ('Australia/Sydney', True)
    backend.create_table("sales", [{"id": 1, "total": 120}])

print(raw.execute("SELECT * FROM sales").fetchall())
# → [(1, 120)]
```

The `with` block closed the wrapper, and the borrowed handle was still usable afterwards.

Raw adoption is enabled only for dialects whose raw path has been verified end to end: at this revision, `duckdb` and `motherduck` (the `raw_adoption_verified` flag in `src/mountainash_data/backends/ibis/dialects/_registry.py`, checked in `IbisBackend.from_raw_connection` in `src/mountainash_data/backends/ibis/backend.py`). Every other dialect raises `NotImplementedError` before the handle is used, pointing to `from_ibis_connection`:

```python
import sqlite3

try:
    IbisBackend.from_raw_connection(sqlite3.connect(":memory:"), dialect="sqlite")
except NotImplementedError as exc:
    print(exc)
# → raw adoption not yet verified for 'sqlite'; construct the ibis connection yourself
#   and use IbisBackend.from_ibis_connection(...).
```

The adoption status of each dialect is listed in [Supported databases and limits](../10-supported-databases-and-limits/index.md). How the snapshot and restore work, and why they run in a `finally`, is described in [Transaction and adoption internals](../13-transaction-and-adoption-internals/index.md).
