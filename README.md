# mountainash-data

![Python](https://img.shields.io/badge/python-3.12%2B-blue) ![Category](https://img.shields.io/badge/category-core-purple) ![Tests](https://img.shields.io/badge/tests-✓-green) ![Docs](https://img.shields.io/badge/docs-✓-blue)


Mountain Ash - Data

Physical access to backend data services — relational databases via Ibis,
behind a backend-agnostic `Backend` protocol ready for additional backends.



## Installation

The base install carries no database driver or dataframe library. Install the
extra for each backend you use; the extra is named exactly as the dialect:

```bash
pip install 'mountainash-data[duckdb]'           # one backend
pip install 'mountainash-data[sqlite,polars]'    # SQLite plus Polars dataframe input
pip install 'mountainash-data[all]'              # every backend
```

Extras: `sqlite`, `duckdb`, `motherduck`, `postgres`, `redshift`, `mysql`,
`mssql`, `oracle`, `snowflake`, `bigquery`, `trino`, `clickhouse`,
`databricks`, `singlestoredb`, `exasol`, `impala`, `materialize`,
`risingwave`, `druid`, `pyspark`, plus `polars` for Polars dataframe input.
Each installs the matching `ibis-framework[...]` extra. Connecting without the
extra raises an `ImportError` naming the command to run.

System prerequisites that pip cannot install:

- `mssql`: an ODBC driver manager and the Microsoft ODBC Driver for SQL Server.
- `mysql`: MySQL/MariaDB client libraries (e.g. `libmariadb-dev`) where no wheel exists.
- `pyspark`: a Java runtime.
- `risingwave`: `psycopg2` may build from source, which needs `pg_config` (libpq development files).

`postgres`, `redshift` and `materialize` use `psycopg[binary]`, which bundles libpq.

### Development Installation

```bash
# Clone and install in development mode
git clone <repository-url>
cd mountainash-data
pip install -e '.[sqlite,duckdb,polars]'
```

### Using Hatch

```bash
# Create development environment
hatch env create

# Run commands in the environment
hatch run <command>
```



## Quick Start

```python
from mountainash_data import IbisBackend, DatabaseUtils

# Direct backend usage (new-style)
backend = IbisBackend(dialect="sqlite", database=":memory:")
conn = backend.connect()
try:
    tables = conn.list_tables()
    info = conn.inspect_table("my_table")
    ibis_table = conn.table("my_table")  # backend-native ibis Table
    # Bridge to the unified mountainash package (pull-side):
    #   import mountainash as ma
    #   rel = ma.relation(ibis_table)  # compiles against Ibis automatically
finally:
    conn.close()

# High-level facade (settings-driven)
from mountainash_data import DatabaseUtils
from mountainash_data.core.settings import SQLiteAuthSettings
from mountainash_settings import SettingsParameters

settings_params = SettingsParameters.create(
    settings_class=SQLiteAuthSettings,
    kwargs={"DATABASE": ":memory:"}
)
connection = DatabaseUtils.create_connection(settings_params)
ibis_backend = connection.connect()
```

## Transactions

`backend.transaction()` groups package operations into one unit of work. On
SQLite, DuckDB and PostgreSQL it checks the driver's own state first:

- **Idle connection:** it begins a transaction and owns it. A clean exit
  commits; an exception, or a failure you caught inside the block, rolls back.
- **Your transaction is already open:** it joins it and never commits or rolls
  it back. Completion stays with you.

```python
backend = IbisBackend(dialect="postgres", ...).connect()

with backend.transaction():           # owned: commits on success
    backend.upsert("orders", frame, conflict_columns=["id"])
    backend.add_columns("orders", {"note": "string"})

raw = backend.raw_driver_connection()
raw.execute("BEGIN")                   # your transaction
with backend.transaction():           # joined: no BEGIN, no COMMIT
    backend.insert("audit", rows)
raw.execute("COMMIT")                  # you decide
```

Inside a scope, package operations cannot commit or roll back behind it, and
metadata queries that fail raise instead of returning empty results. Nested
scopes are flat (no savepoints).

- `backend.native_transaction_open()` reports whether the driver has a
  transaction open (`True`/`False`, or `None` where it cannot tell).
- `backend.in_transaction()` reports whether a `transaction()` scope is active,
  including one that joined your transaction.

Notes:

- PostgreSQL with `autocommit` off starts a transaction implicitly on any
  statement, including a read. A later `transaction()` therefore joins it
  rather than owning it. The autocommit setting is never changed.
- Statements you run on the native handle or on returned Ibis expressions are
  not intercepted. A `COMMIT` issued that way just before the scope ends is
  reported with `TransactionIntegrityError`; it cannot be undone.
- Other dialects keep their earlier behaviour: the outermost scope begins and
  commits.


## Settings 0.1 migration (candidate)

Data retains its own `get_descriptor(name)` and read-only `REGISTRY` APIs,
including membership, indexing, iteration and views. They delegate to the
canonical settings registry. Backend profiles and auth-client profiles remain
distinct: select a backend and a compatible auth profile before resolving values.

The live-DB harness owns the selected filesystem store. `secret_providers` and
`targets.*.secrets_provider` remain local configuration labels; they do not
register process-global providers. Only the selected backend's references are
resolved, so an unselected target's missing record cannot block local work.
SQLite/DuckDB and ordinary Compose workflows need no store.

The ownership boundary for selected resolution is a context manager:

```python
from mountainash_settings import SettingsParameters
from mountainash_settings.secrets import FilesystemBackend

# root must already be securely created and owned by the harness operator.
# selected_settings_class / selected_values describe only the selected profile.
with FilesystemBackend(root) as store:
    selected = SettingsParameters.create(
        settings_class=selected_settings_class,
        secret_store=store,
        **selected_values,
    ).get_settings()
# The harness closes the store after materialization, including on failure.
```

Child processes reconstruct the selection from configuration paths and selected
identities, rather than receiving stores or resolved credentials. Literal
resolved strings beginning with `secret:` remain a deferred limitation.

The runtime bounds are `mountainash-settings>=0.1.0,<0.2` and
`mountainash-auth-client>=0.1.0,<0.2`, matching the coordinated **0.1.0**
development baseline. Hatch selects sibling sources for development and CI;
installed-artifact checks record exact hashes separately. Python 3.14 release
qualification and publication remain separate gates.

## Architecture

mountainash-data uses a layered architecture:

```
src/mountainash_data/
├── __init__.py                  # Public API surface
├── __version__.py               # Version information
├── core/                        # Protocol, inspection, settings, factories
│   ├── protocol.py              # Backend / Connection protocols
│   ├── inspection.py            # CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo
│   ├── utils.py                 # DatabaseUtils high-level facade
│   ├── constants.py             # CONST_DB_PROVIDER_TYPE and friends
│   ├── settings/                # Per-dialect auth settings (pydantic)
│   └── factories/               # ConnectionFactory, OperationsFactory, SettingsFactory
└── backends/
    └── ibis/                    # IbisBackend — 12-dialect registry
        ├── backend.py           # IbisBackend + IbisConnection
        ├── connection.py        # BaseIbisConnection + per-dialect subclasses
        ├── operations.py        # BaseIbisOperations + per-dialect subclasses
        ├── inspect.py           # Ibis-specific inspection helpers
        └── dialects/            # DialectSpec registry (data-driven)
```

### Public API

```python
from mountainash_data import (
    Backend,            # Protocol: what every backend must implement
    Connection,         # Protocol: what every connection must implement
    IbisBackend,        # Ibis-style relational backends (sqlite, duckdb, postgres, …)
    CatalogInfo,        # Physical catalog metadata
    NamespaceInfo,      # Physical namespace/schema metadata
    TableInfo,          # Physical table metadata
    ColumnInfo,         # Physical column metadata
    DatabaseUtils,      # High-level facade (settings-driven)
    ConnectionFactory,  # Factory: settings → connection
    OperationsFactory,  # Factory: settings → operations
    SettingsFactory,    # Factory: URL / backend-type → settings
)
```

### Optional Dependencies

One extra per dialect plus `polars` and `all`; see [Installation](#installation).



## Features

- **12-dialect ibis registry** — SQLite, DuckDB, MotherDuck, PostgreSQL, MySQL, MSSQL, Oracle, Snowflake, BigQuery, Redshift, Trino, PySpark
- **Protocol-first design** — `Backend` and `Connection` protocols enable type-safe composition
- **mountainash seam (pull-side)** — `table()` returns a backend-native ibis Table; `ma.relation(table)` in the unified `mountainash` package compiles against it directly. There is deliberately no push-side `to_relation()` bridge in this package.
- **Settings-driven** — pydantic settings for every dialect, factory auto-detection from URLs
- **Comprehensive test suite** ensuring reliability



## Documentation

- **[CLAUDE.md](CLAUDE.md)** - Technical documentation and development guide
- **[Examples](docs/examples/)** - Usage examples and tutorials
- **[Mountain Ash Documentation](https://mountainash-io.github.io/mountainash-docs/)** - Complete ecosystem documentation



## Textbook

The [production textbook](https://docs.mountainash.io/mountainash-data/) is
built from `main`; the [development textbook](https://docs.mountainash.io/mountainash-data/dev/)
is built from `develop`. Each push to either branch builds both snapshots and
publishes them together. A failed build leaves the previous paired site live.

For initial activation, merge the textbook changes into both branches and
configure Pages, its environment, and the shared custom domain first. Then set
the repository Actions variable `TEXTBOOK_PUBLISHING_ENABLED` to `true` and
manually dispatch `deploy-textbook.yml`. Until enabled, publishing runs are
skipped; a one-sided bootstrap cannot deploy an incomplete site.

The source artifacts live together in this repository:

- `docs-site/profile/`: package profile and source provenance.
- `docs-site/learning-graph/`: canonical graph and FAQ artifacts.
- `docs-site/site/`: MkDocs configuration, textbook Markdown, and refresh state.

Preview locally without installing the source package or sibling repositories:

```bash
uv run --no-project --with-requirements docs-site/requirements.txt \
  python -m mkdocs serve --config-file docs-site/site/mkdocs.yml
```

Refreshes are manual. Load `textbook-refresh` from the central
`hiivmind-documentation-profile` tooling project and supply this repository's
absolute root as `source_repo`, starting with `mode: check`. For a separate
profile update, supply `docs-site/profile/` as the profiler's explicit output.
Do not regenerate content merely to publish it or advance source baselines on
a directory move. Preserve the existing FAQ format; the marker-only FAQ
exporter does not support it and must not overwrite its JSON.

A strict build of the current content surfaces two pre-existing gaps, neither
caused by this relocation: `learning-graph/index.md` links to
`learning-graph/course-description.md`, which does not exist, and five
learning-graph pages (`concept-list.md`, `concept-taxonomy.md`, `faq.md`,
`quality-metrics.md`, `taxonomy-distribution.md`) exist on disk but are not
wired into the site nav. Both are inherited from the source content and are
left as-is here.

## Development

### Testing

```bash
# Run full test suite with coverage
hatch run test:test

# Quick run (no coverage)
hatch run test:test-quick

# Lint
hatch run ruff:check

# Type check
hatch run mypy:check

# Separate source and test checks
hatch run mypy:check-src
hatch run mypy:check-tests

# Also check bodies of unannotated functions
hatch run mypy:check-src-untyped
hatch run mypy:check-tests-untyped
```

The four targeted commands accept extra mypy flags without replacing their
targets. Test checks still follow source imports and can also report source
errors. The `-untyped` variants include `--check-untyped-defs`.
Test shortcuts use `--explicit-package-bases` to distinguish nested `conftest`
modules in the test tree.

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request



## License

See LICENSE file for details.

## Mountain Ash Ecosystem

This package is part of the [Mountain Ash](https://github.com/mountainash-io) ecosystem of Python packages.

---
*README.md updated 2026-04-09 to reflect the core+backends refactor*
