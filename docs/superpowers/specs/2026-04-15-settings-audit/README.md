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
| sqlite | [sqlite.md](./sqlite.md) | — | — | — | — | — | — |
| duckdb | [duckdb.md](./duckdb.md) | — | — | — | — | — | — |
| motherduck | [motherduck.md](./motherduck.md) | — | — | — | — | — | — |
| postgresql | [postgresql.md](./postgresql.md) | — | — | — | — | — | — |
| mysql | [mysql.md](./mysql.md) | — | — | — | — | — | — |
| mssql | [mssql.md](./mssql.md) | — | — | — | — | — | — |
| snowflake | [snowflake.md](./snowflake.md) | — | — | — | — | — | — |
| bigquery | [bigquery.md](./bigquery.md) | — | — | — | — | — | — |
| redshift | [redshift.md](./redshift.md) | — | — | — | — | — | — |
| pyspark | [pyspark.md](./pyspark.md) | — | — | — | — | — | — |
| trino | [trino.md](./trino.md) | — | — | — | — | — | — |
| pyiceberg_rest | [pyiceberg_rest.md](./pyiceberg_rest.md) | — | — | — | — | — | — |

Counts are filled in as each per-backend audit completes.
