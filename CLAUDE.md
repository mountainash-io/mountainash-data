# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mountainash-data** provides physical access to backend data services — relational databases via Ibis behind a backend-agnostic `Backend` protocol ready for additional backends. A single `IbisBackend` serves 20 dialects, with per-dialect behaviour expressed as data in the `DialectSpec` registry (capabilities, hooks, transaction probes, session options) rather than subclasses. Typed settings (`*BackendProfile`) carry connection config; auth (`*AuthProfile`) is supplied separately and composed by `ConnectionFactory` at connect time.

## Planning, Specs & Principles (live in mountainash-central)

This repo holds **code only**. All design specs, implementation plans, the technical-debt
backlog, and the architecture principles live in the ecosystem repo **`mountainash-central`**
(sibling checkout: `../mountainash-central`):

| Artifact | Location in `mountainash-central` |
|----------|-----------------------------------|
| Design specs | `04.planning/mountainash-data/superpowers/specs/` |
| Implementation plans | `04.planning/mountainash-data/superpowers/plans/` |
| Technical-debt backlog (+ `archive/`) | `04.planning/mountainash-data/a.backlog/` |
| ENFORCED architecture principles | `01.principles/mountainash-data/` |

**When running the superpowers flow (brainstorming → writing-plans → SDD) for this repo,
write the spec/plan to `mountainash-central/04.planning/mountainash-data/superpowers/{specs,plans}/`,
not to a local `docs/superpowers/` directory.** (The superpowers skills default to a local
`docs/superpowers/` path — override that default to the central location above.) Only source,
tests, and this `CLAUDE.md` live in this repo's tree.

## Central Repository — Principles & Backlog

Durable architecture principles and the implementation backlog for this project live in the
**mountainash-central** repo, not here.

- **Principles:** [`../mountainash-central/01.principles/mountainash-data/README.md`](../mountainash-central/01.principles/mountainash-data/README.md) — see [`PRINCIPLES.md`](../mountainash-central/01.principles/mountainash-data/PRINCIPLES.md) for governance (statuses, category precedence).
- **Backlog:** [`../mountainash-central/04.planning/mountainash-data/a.backlog/INDEX.md`](../mountainash-central/04.planning/mountainash-data/a.backlog/INDEX.md) — prioritized items; see that directory's `CHANGELOG.md` for update history.
- **Specs & plans:** save new design specs/plans to `../mountainash-central/04.planning/mountainash-data/superpowers/{specs,plans}/`, not under `docs/superpowers/` in this repo.

## Architecture

### Core Components

1. **Protocol layer** (`src/mountainash_data/core/protocol.py`)
   - `Backend` — runtime-checkable protocol, the single handle every backend must implement (structurally, no inheritance): `name`, `connect()`/`close()`/context-manager lifecycle, `list_tables()`/`list_namespaces()`/`list_catalogs()`, `inspect_table()`/`inspect_namespace()`/`inspect_catalog()`, and the transaction surface `raw_driver_connection()`, `supports_transactions`, `transaction(required=...)`, `native_transaction_open()`, `in_transaction()`
   - Every new protocol member is a breaking change for third-party implementations; `tests/test_unit/core/test_protocol.py` keeps it backend-agnostic with a non-Ibis `_StubBackend`
   - There is deliberately no `to_relation()` on the protocol — bridging to the unified `mountainash` package is pull-side: `table()` returns a backend-native ibis Table and `ma.relation(ibis_table)` compiles against it directly

2. **Inspection model** (`core/inspection.py`, `core/namespace.py`)
   - `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo`, `IndexInfo` — frozen physical-metadata dataclasses
   - `Namespace` value object (catalog + namespace path) and the `NamespaceLike` alias accepted by every namespace argument

3. **Errors** (`core/errors.py`)
   - `TransactionError` and subclasses (`TransactionUnsupportedError`, `TransactionPoisonedError`, `TransactionIntegrityError`), `BackendConnectionError` — not re-exported from the package root

4. **Settings** (`src/mountainash_data/core/settings/`)
   - Database-flavored layer over `mountainash_settings.profiles`.
   - `BackendSpec` (subclass of `ProfileSpec`) adds `default_port`, `connection_string_scheme`, `ibis_dialect`, `rides_on`, `supported_auth` and `resource_read_*` metadata. `BackendDescriptor` survives only as a deprecated alias.
   - `BackendProfile` (subclass of `Profile`) is a pure config emitter with `to_url_parts()` → `UrlParts`; it has **no auth methods**.
   - 20 per-backend modules (`sqlite.py` … `trino.py`), each a `<NAME>_SPEC` plus a `@register`-decorated `*BackendProfile`. Importing `core.settings` imports all of them, which registers every backend.
   - `DATABASES_REGISTRY` with `get_descriptor()` / `get_settings_class()` / `register` in `registry.py`.
   - 12 `*AuthProfile` types (`NoAuthProfile`, `PasswordAuthProfile`, `OAuth2AuthProfile`, …) come from `mountainash_auth_client` and are re-exported from `mountainash_data.core.settings`.
   - Auth adapters live in `settings/adapters/<backend>.py`, dispatched by `(provider_type, auth class)` in `adapters/registry.py`. Each spec's `supported_auth` must agree with that table (enforced by `test_supported_auth_consistency.py`).

5. **Connection factory** (`core/factories/connection_factory.py`)
   - `build_driver_kwargs(profile, auth_profile)` — config + auth → driver kwargs (validates `supported_auth`, fails closed when no adapter exists)
   - `build_connection_string(profile, auth_profile)` — credentialed URL for no-auth, password and per-provider appliers
   - `provider_for_dialect()` / `provider_for_scheme()` / `apply_auth_adapter()` for the dialect and URL construction paths

6. **Ibis backend** (`src/mountainash_data/backends/ibis/`)
   - `IbisBackend` — the entry point; constructed from `SettingsParameters`, a URL, or `dialect=` + driver kwargs. `connect(auth_profile=...)` composes auth at connect time. Also `from_ibis_connection()` / `from_raw_connection()` for caller-owned connections (raw adoption is gated on `DialectSpec.raw_adoption_verified`, currently duckdb/motherduck).
   - `IbisConnection` — live-connection wrapper returned by `get_connection()`
   - `DialectSpec` / `DIALECTS` registry in `dialects/_registry.py` — connection builders, `UpsertStyle`, `TransactionSupport`, `DropScope`, `IndexCapability`, `SessionOption`, and optional per-dialect hooks (`upsert_hook`, `add_columns_hook`, `truncate_table_hook`, `list_indexes_hook`, …)
   - `operations.py` — dialect-specific operation functions referenced by `DialectSpec` hooks (imported back into `_registry.py` at module bottom; mind the cycle)
   - Generic-default implementations rendered with sqlglot when no hook is set: `_render.py` (upsert, rename, add-columns), `_index.py` / `_index_inspection.py` (index DDL and listing)
   - Transactions: `_transaction.py` (ownership scopes, per-raw-handle registry), `_protected.py` (guards package calls inside a scope), `_raw.py` / `_adoption.py` (raw driver access, session-option snapshot/restore)
   - Compat shims: `_exasol_compat.py`, `_oracle_compat.py`, `_sqlite_compat.py` (rely on Ibis private APIs — recheck on Ibis upgrades)

7. **Backend name registry** (`core/registry.py`) — `register` / `get` / `names`; currently unwired (no importer, `IbisBackend` does not register itself)

### Package Structure

```
src/mountainash_data/
├── __init__.py                  # Public API: Backend, IbisBackend, *Info, Namespace, NamespaceLike
├── __version__.py               # Version information
├── core/
│   ├── protocol.py              # Backend protocol
│   ├── inspection.py            # CatalogInfo / NamespaceInfo / TableInfo / ColumnInfo / IndexInfo
│   ├── namespace.py             # Namespace value object
│   ├── errors.py                # Transaction / connection exceptions
│   ├── registry.py              # Backend name registry (unwired)
│   ├── constants.py             # CONST_DB_PROVIDER_TYPE and friends
│   ├── _redact.py               # Credential redaction for URLs / driver errors
│   ├── _warn.py                 # warn_once helper
│   ├── factories/
│   │   └── connection_factory.py  # BackendProfile + AuthProfile → driver kwargs / URL
│   └── settings/
│       ├── descriptor.py        # BackendSpec
│       ├── profile.py           # BackendProfile, UrlParts
│       ├── registry.py          # DATABASES_REGISTRY, register
│       ├── adapters/            # Per-backend auth adapters + dispatch table
│       └── <backend>.py         # 20 per-backend specs + *BackendProfile classes
└── backends/
    └── ibis/
        ├── backend.py           # IbisBackend + IbisConnection
        ├── dialects/_registry.py  # DialectSpec + DIALECTS (data-driven)
        ├── operations.py        # Per-dialect operation hooks
        ├── inspect.py           # Ibis schema → inspection model
        ├── _render.py           # Generic-default SQL rendering (sqlglot)
        ├── _index.py            # Generic-default index DDL
        ├── _index_inspection.py # Index listing per dialect family
        ├── _transaction.py      # Transaction ownership scopes
        ├── _protected.py        # Protected package calls inside scopes
        ├── _raw.py              # Raw driver connection access
        ├── _adoption.py         # Session-option snapshot/restore on adoption
        └── _{exasol,oracle,sqlite}_compat.py  # Dialect compatibility shims
```

### Optional Dependencies (extras)
One extra per registered dialect, named as the dialect key (`sqlite`, `duckdb`, `motherduck`, `postgres`, `redshift`, …), plus `polars` (dataframe input) and `all`. Each delegates to the matching `ibis-framework[...]` extra; `postgres`/`redshift`/`materialize` add `psycopg[binary]`. `IbisBackend.connect()` / `from_raw_connection()` translate a missing driver module into an `ImportError` naming `mountainash-data[<dialect>]`. Hatch test environments install `sqlite`, `duckdb`, `polars` via `features`.

## Build/Test/Lint Commands

### Core Commands
- **Build**: `hatch build`
- **Lint**: `hatch run ruff:check` or `hatch run ruff:fix` to auto-fix
- **Type check**: `hatch run mypy:check`

### Testing Commands
- **Full test suite with coverage**: `hatch run test:test`
- **Quick tests (no coverage)**: `hatch run test:test-quick`
- **Target specific tests**: `hatch run test:test-target tests/path/to/test_file.py::TestClass::test_function`
- **Test only changed files**: `hatch run test:test-changed`
- **Performance benchmarks**: `hatch run test:test-perf`
- **Test by markers**: `hatch run test:test-unit`, `hatch run test:test-integration`, `hatch run test:test-performance`
- **Optional-backend tests** (oracle, trino, bigquery matrix): `hatch run test_optional:test`
- **Live-database tests**: `hatch run test:live-db ...` (harness in `scripts/live_db/`, config in `tests/config/live-db.toml`)

Local envs resolve sibling packages (`mountainash-settings`, `mountainash`, `mountainash-auth-client`) from `../`; the `mypy` env uses `HATCH_SIBLINGS`. If an env fails to resolve a sibling's transitive dependency, a sibling checkout is stale or missing.

### Code Quality Analysis
- **Complexity analysis**: `hatch run radon:radon-cc` (cyclomatic complexity)
- **Maintainability index**: `hatch run radon:radon-mi`

## Dependencies

### Core Dependencies
- **mountainash-settings** >=0.1.0,<0.2 and **mountainash-auth-client** >=0.1.0,<0.2
- **ibis-framework** >=12.0.0 (core only, no backend extras)
- **sqlglot** >=29.0.1,<30.18.0 — floor is CI's pinned version (`requirements-hatch.txt`); cap is a verified DuckDB memtable regression

No driver, dataframe library, NumPy or Arrow is a base requirement; they arrive through extras.

### Development Dependencies
- **pytest==8.3.5** with extended plugins:
  - pytest-asyncio, pytest-check, pytest-cov, pytest-mock
  - pytest-benchmark, pytest-clarity, pytest-timeout, pytest-picked
  - pytest-json-report, pytest-metadata for enhanced reporting
- **ruff==0.3.7** - Fast Python linter and formatter
- **mypy==1.10.1** - Static type checker
- **radon==6.0.1** - Code complexity analysis

## GitHub Actions Workflows

### Testing Workflows
- **python-run-pytest.yml**: mypy gate (`hatch run mypy_github:check`) then pytest on pull requests, Python 3.12/Ubuntu 24.04
- **python-run-ruff.yml**: Code linting checks
- **python-run-radon.yml**: Complexity analysis and code quality metrics
- **python-verify-live-db.yml**: Live-database runs (postgres, mysql, …) via Docker targets

### Release Process
- **build-and-release-package.yml**: Shared release-chain workflow. PRs build and verify a candidate; merging a PR performs no release action (no tag, no GitHub release, no wheels-repository PR). PyPI publication is manual only: `workflow_dispatch` with `publish=true` on `main`, approved in the protected `pypi` environment (Trusted Publishing)
  - Supports production, RC, and beta releases
  - Generates SBOMs (Software Bill of Materials)
- **main-release-branch-validation.yml** / **main-release-build-dependencies.yml**: PRs to `main` must come from `release/*` or `hotfix/*` and pass a build-dependency check

### Docs
- **deploy-textbook.yml**: Builds and deploys the textbook from `docs-site/` on `main` and `develop`
- **changelog-generation.yml**: Disabled (`if: false`)

### Branch Strategy
- `main`: Production releases
- `develop`: Development and RC releases
- `feature/*`, `bugfix/*`, `hotfix/*`: Feature branches

## Code Style Guidelines
- Formatting: Uses ruff for formatting and linting
- Imports: Standard lib first, third-party next, project imports last
- Types: Use typing annotations (e.g., `import typing as t`) for all functions
- Naming: CamelCase for classes, snake_case for functions/variables, UPPER_CASE for constants
- Error handling: Use ValueError for validation errors, custom exceptions for specific cases
- Documentation: Use Google-style docstrings for classes and methods
- Organization: Follow modular design with clear separation of concerns
- Testing: Create unit tests with appropriate markers (unit, integration, performance)

## Testing Structure

### Test Organization
```
tests/
├── conftest.py                    # Shared fixtures + extra markers
├── config/live-db.toml            # Live-database targets
├── fixtures/                      # settings / database / dataframe / live_db fixtures
├── helpers/                       # oracle_compat, transaction_cases
├── test_unit/
│   ├── core/                      # protocol, inspection, namespace, _warn
│   │   ├── factories/             # connection factory, URL consistency
│   │   └── settings/              # specs, profiles, registry, adapters, per-backend
│   ├── backends/ibis/             # IbisBackend, dialect spec, render, index, transaction, adoption, compat
│   ├── databases/settings/        # parametrized per-backend settings tests
│   ├── live_db/                   # live-db harness unit tests
│   ├── test_installed_contract.py # installed-package contract
│   └── test_mountainash_data.py   # package-level import tests
├── test_integration/
│   └── test_end_to_end_workflows.py
├── test_optional_backends/        # oracle, trino, bigquery, mountainash (need extras)
└── test_live_backends/            # real databases via the live-db harness
```

`pytest.ini` sets `testpaths = tests/test_unit tests/test_integration` and `--strict-markers`.

### Test Markers
Declared in `pytest.ini`: `unit`, `integration`, `slow`, `performance`, `smoke`, `regression`, `api`, `data`, `parametrize`. `tests/conftest.py` adds `requires_postgres`, `requires_duckdb`, `requires_snowflake`.

### Test Backends
- SQLite and DuckDB for local testing (in-memory where possible)
- Optional-backend suites run only with the matching extra installed
- Live suites run against real databases (Docker or configured services)

## Usage Patterns

```python
import pyarrow as pa
from mountainash_settings import SettingsParameters

from mountainash_data import IbisBackend
from mountainash_data.core.factories.connection_factory import (
    build_connection_string,
    build_driver_kwargs,
)
from mountainash_data.core.settings import (
    NoAuthProfile,
    PasswordAuthProfile,
    PostgreSQLBackendProfile,
    SQLiteBackendProfile,
)

# Settings are pure config (BackendProfile); auth is a separate AuthProfile
pg = PostgreSQLBackendProfile(HOST="db.example", DATABASE="app")
auth = PasswordAuthProfile(USERNAME="app", PASSWORD="s3cret")
kwargs = build_driver_kwargs(pg, auth)    # → dict ready for ibis.postgres.connect
url = build_connection_string(pg, auth)   # → "postgresql://app:s3cret@db.example:5432/app"

# IbisBackend accepts SettingsParameters, a URL ("sqlite://"), or dialect= + kwargs
params = SettingsParameters.create(settings_class=SQLiteBackendProfile, DATABASE=":memory:")
with IbisBackend(params).connect(auth_profile=NoAuthProfile()) as backend:
    backend.create_table("users", pa.table({"id": [1], "name": ["ada"]}))
    with backend.transaction():           # explicit transaction scope
        backend.insert("users", pa.table({"id": [2], "name": ["bob"]}))
    info = backend.inspect_table("users")  # → TableInfo
    ibis_table = backend.table("users")    # backend-native ibis Table
    # Bridge to the unified mountainash package (pull-side):
    #   import mountainash as ma
    #   rel = ma.relation(ibis_table)  # compiles against Ibis automatically
```

Runnable recipes per capability live in `examples/` (connections, inspection, mutations, indexes, transactions, connection_adoption, local_tables).

## Development Environments

### Hatch Environments
- `default`: Basic local development environment
- `dev`: Local development (`.venv`) with sibling mountainash packages and `sqlite`/`duckdb`/`polars` extras
- `test`: Local testing with extended pytest plugins and coverage
- `test_backend`: Per-dialect test matrix (postgres, mysql, oracle, singlestoredb, mssql, trino, exasol, pyspark, bigquery)
- `test_optional`: Optional-backend test matrix (oracle, trino, bigquery)
- `test_github` / `test_github_live`: GitHub Actions unit and live-database environments
- `build_github`: GitHub Actions building and SBOM generation
- `tower`: Minimal production-like environment
- `ruff`: Linting
- `radon`: Complexity analysis
- `mypy` / `mypy_github`: Type checking (`check`, `check-src`, `check-tests`)

## Versioning Strategy

Release tags use CalVer `vYY.MM.MICRO` (e.g. `v25.10.0`, `v26.4.0`). The release workflow reads the base version from `hatch version` (`src/mountainash_data/__version__.py`, must be `X.Y.Z`) and supports production, RC and beta releases.

## Configuration and Settings

### Environment Settings
- **settings/**: Environment-specific configuration files
  - **mountainash_acrds_settings.env**: Mountain Ash ACRDS settings

### Configuration Files
- **hatch.toml**: Hatch build tool configuration
- **sonar-project.properties**: SonarCloud analysis configuration
- **pyproject.toml**: Python project configuration and dependencies

## License
Apache License 2.0
