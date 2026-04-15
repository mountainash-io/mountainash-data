# Settings Audit — 2026-04-15

Audit of backend settings classes in `src/mountainash_data/core/settings/` against their authoritative source specs.

## Goal

For each backend settings class, compare the object against its source spec(s) to surface:

- **Completeness** — parameters in the source spec missing from our class
- **Correctness** — field names, types, defaults, validation that don't match the spec
- **Currency** — spec URLs that are stale, redirected, or 404

Deliverable is report-only. Fixes are handled in separate per-backend writing-plans cycles.

## Scope

11 backend settings classes:

`sqlite`, `duckdb`, `motherduck`, `postgresql`, `mysql`, `mssql`, `snowflake`, `bigquery`, `redshift`, `pyspark`, `trino`, `pyiceberg_rest`.

Out of scope: `base.py`, `templates.py`, `exceptions.py`, and any code changes to settings classes.

## Source precedence

When specs disagree, resolve in this order:

1. **Driver/client spec** — authoritative for parameter names, types, defaults, validation semantics (e.g. libpq, mysqlclient, snowflake-connector-python, PyIceberg REST catalog).
2. **Ibis backend** — authoritative for what can actually be passed through from our settings to the underlying driver. Ground truth: `do_connect()` signature in `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/<backend>/__init__.py`.
   - `motherduck` → ibis `duckdb` backend
   - `redshift` → ibis `postgres` backend
   - `pyiceberg_rest` → no Ibis backend; passthrough column records PyIceberg REST catalog kwargs instead
3. **Vendor docs** — context for semantics only (Databricks Spark conf, gcloud auth guide, Snowflake OAuth guide, etc.).

## Parameter tiering

Every parameter audited is tagged `core` or `advanced`.

- **core** — affects establishing a connection or core session behavior: auth, host/endpoint, TLS, timeouts, database/schema/catalog selection.
- **advanced** — tuning knobs, rarely-used flags, deprecated options, driver-specific esoterica.

Tiering drives fix prioritization in downstream plans.

## Per-backend report structure

Each report lives at `./<backend>.md` and contains:

1. **Header** — spec URLs with precedence labels; date checked; spec version if versioned; link to our settings class file; link to Ibis backend file used.
2. **Stale-link check** — result of fetching each URL: `OK`, `redirect → <final URL>`, or `404`.
3. **Summary counts** — core missing, core mismatch, advanced missing, advanced mismatch, extra, total audited.
4. **Parameter table** with columns:

   | Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |

   - **Status**: `present` / `missing` / `mismatch` / `extra` (we have it, spec doesn't)
   - **Type ✓** / **Default ✓**: ✓ / ✗ / N/A
   - **Ibis passthrough**: ✓ / ✗ / unknown (or `via duckdb` / `via postgres` for motherduck/redshift; `N/A` for pyiceberg_rest)

5. **Findings narrative** — prioritized issue list: core gaps → core mismatches → advanced gaps → advanced mismatches → stale links.
6. **Recommended follow-ups** — concrete, per-backend bullets a downstream plan can pick up directly.

## Process per backend

1. Read our settings class in `src/mountainash_data/core/settings/<backend>.py`.
2. Fetch each linked spec URL (WebFetch); record stale/redirects.
3. Read the corresponding Ibis `do_connect()` signature (or PyIceberg REST catalog for `pyiceberg_rest`).
4. Build the parameter union across driver spec + our class + Ibis passthrough; classify each row.
5. Write the report.

## Index

| Backend | Report | Core missing | Core mismatch | Advanced missing | Advanced mismatch | Extra | Stale links |
|---|---|---|---|---|---|---|---|
| sqlite | [sqlite.md](./sqlite.md) | 0 | 0 | 8 | 0 | 7 | 0 |
| duckdb | [duckdb.md](./duckdb.md) | 0 | 1 | 12 | 2 | 7 | 0 |
| motherduck | [motherduck.md](./motherduck.md) | 0 | 2 | many (via duckdb) | 1 | 6 | 0 |
| postgresql | [postgresql.md](./postgresql.md) | 0 | 5 | ~20 | ~12 | 1 | 0 |
| mysql | [mysql.md](./mysql.md) | 0 | 3 | ~8 | 2 | 0 | 0 |
| mssql | [mssql.md](./mssql.md) | 1 | 4 | ~15 | 1 | 1 | 0 |
| snowflake | [snowflake.md](./snowflake.md) | 0 | 4 | ~15 | 1 | 1 | 0 |
| bigquery | [bigquery.md](./bigquery.md) | 4 | 3 | 3 | 0 | 8 | 0 |
| redshift | [redshift.md](./redshift.md) | 1 | 5 | ~15 | 2 | 3 | 0 |
| pyspark | [pyspark.md](./pyspark.md) | 1 | 3 | out-of-scope | 3 | 8 | 0 |
| trino | [trino.md](./trino.md) | 0 | 5 | 1 | 8 | 1 | 0 |
| pyiceberg_rest | [pyiceberg_rest.md](./pyiceberg_rest.md) | 2 | 3 | 9 | 0 | 6 | 0 |

## Cross-cutting findings

Patterns that emerged across multiple backends:

- **`db_provider_type` copy-paste bugs**: `postgresql.py` and `mysql.py` both return `CONST_DB_PROVIDER_TYPE.BIGQUERY` instead of their own provider. Real defects.
- **Plumbing gap**: Nearly every backend declares Fields that `get_connection_kwargs()` then silently drops. postgres, trino, snowflake, mssql, bigquery all have orphan fields.
- **SecretStr not unwrapped**: Passwords and tokens are passed as `SecretStr` objects rather than via `.get_secret_value()` in most cloud backends (snowflake, mssql, redshift, pyiceberg_rest).
- **Enums defined but unused**: postgresql (4), snowflake (1), mssql (2), pyspark (1) all have constant classes that aren't enforced as field types.
- **Base-class composition**: `HOST`/`PORT`/`USERNAME`/`PASSWORD`/`TOKEN` leak into every class via `BaseDBAuthSettings`, including file/cloud/catalog classes where they're meaningless.
- **Docstring drift**: pyspark docstring says "SQLite authentication settings"; pyiceberg_rest says "Cloudflare R2"; several classes have stale `#path:` comments pointing at `mountainash_settings/...` paths that don't match the current layout.
- **Dead validators**: redshift's `_init_provider_specific` is never called (base class hook is `_post_init`); its serverless/cluster validation never runs.

Counts are filled in as each per-backend audit completes.

Counts are filled in as each per-backend audit completes.
