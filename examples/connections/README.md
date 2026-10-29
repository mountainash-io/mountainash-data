# Connection inputs and authentication

## Prerequisites

Follow the [source installation](../../README.md#installation) with the `sqlite`
extra. The driver is in Python's standard library, but Ibis's SQLite dependencies
still come from the extra.

## Run

From the repository root:

```bash
python examples/connections/example.py
```

## Expected output

```text
dialect: [(1, 'north', 120), (2, 'south', 95)]
url: [(1, 'north', 120), (2, 'south', 95)]
settings: [(1, 'north', 120), (2, 'south', 95)]
auth: supplied separately to connect()
```

## What it demonstrates

`IbisBackend` accepts a dialect with driver keyword arguments, a URL, or `SettingsParameters` created with `SQLiteBackendProfile`. The backend profile describes connection parameters; authentication is a separate `connect(auth_profile=...)` input. Each path opens its own in-memory SQLite database, populates the same small sales table, checks the result, and closes its connection.

Use `sqlite://` for an in-memory URL. An absolute script path also works from
another working directory.

## Settings and credential lifetime

Backend profiles and auth profiles are distinct. Choose the database profile
before materializing values, then pass compatible credentials to `connect()`.
The package's `get_descriptor(name)` and read-only `REGISTRY` delegate to the
settings registry.

If settings contain `secret:` references, supply an explicitly selected
`mountainash_settings.secrets.FilesystemBackend` through
`SettingsParameters.create(secret_store=store, ...)`. The application owns the
store and must keep it open until settings materialization finishes, then close
it even on failure. Child processes reconstruct settings from configuration
paths and profile identities instead of receiving live stores or credentials.
Literal resolved strings beginning with `secret:` remain a known limitation.

SQLite and DuckDB need no secret store. OAuth flows and their dependencies are
separate from these local connection examples.
