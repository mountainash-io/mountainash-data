# Adopting a caller-owned connection

## Prerequisites

Follow the [source installation](../../README.md#installation) with the `duckdb` extra.

## Run

From the repository root:

```bash
python examples/connection_adoption/example.py
```

## Expected output

```text
raw usable after wrapper close: [(1, 'north', 120), (2, 'south', 95)]
scope joined caller transaction: True
caller rollback rows: [(1, 'north', 120), (2, 'south', 95)]
```

## What it demonstrates

`from_raw_connection(..., dialect="duckdb", preserve_session=True)` borrows a
DuckDB handle and preserves session options that Ibis changes during adoption.
Closing the wrapper leaves the caller's connection usable. In the second part,
the package scope joins a caller-started transaction without committing it.
The caller rolls back both inserted rows and closes the native connection.

Raw adoption is restricted to verified dialects; SQLite raw adoption is not
enabled. To adopt an existing Ibis backend, use `from_ibis_connection()` instead.
Both constructors borrow by default; `owns_connection=True` transfers closure
responsibility to the wrapper. An absolute script path also works from another
working directory.
