# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mountainash-data** provides physical access to backend data services — relational databases via Ibis, and Iceberg table-format catalogs via PyIceberg. It collapses what was previously 13 per-dialect connection classes into a data-driven `DialectSpec` registry, exposes clean `Backend` / `Connection` protocols, and provides factories and a high-level facade (`DatabaseUtils`).

## Architecture

### Core Components

1. **Protocol layer** (`src/mountainash_data/core/protocol.py`)
   - `Backend` protocol — what every backend must implement (`connect()`)
   - `Connection` protocol — what every connection must implement (`list_tables()`, `inspect_table()`, `to_relation()`, `close()`)

2. **Inspection model** (`src/mountainash_data/core/inspection.py`)
   - `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo` — shared physical metadata dataclasses

3. **Settings** (`src/mountainash_data/core/settings/`)
   - Declarative per-backend descriptors (`BackendDescriptor` + `ParameterSpec` list).
   - Typed discriminated-union auth via `AuthSpec` subclasses (`PasswordAuth`, `OAuth2Auth`, `IAMAuth`, …).
   - Backend shell classes register themselves via `@register` and expose `to_driver_kwargs()` + `to_connection_string()`.
   - Composite driver mappings live in `settings/adapters/<backend>.py`.

4. **Factories** (`src/mountainash_data/core/factories/`)
   - `ConnectionFactory` — settings → connection
   - `OperationsFactory` — settings → operations
   - `SettingsFactory` — URL / backend-type → settings

5. **High-level facade** (`src/mountainash_data/core/utils.py`)
   - `DatabaseUtils` — one-stop methods for the most common workflows

6. **Ibis backend** (`src/mountainash_data/backends/ibis/`)
   - `IbisBackend` — new-style entry point (dialect string + raw kwargs)
   - `BaseIbisConnection` + 12 per-dialect subclasses in `connection.py`
   - `BaseIbisOperations` + per-dialect subclasses in `operations.py`
   - `DialectSpec` registry in `dialects/`

7. **Iceberg backend** (`src/mountainash_data/backends/iceberg/`)
   - `IcebergBackend` — catalog-type registry (currently: `"rest"`)
   - Connection, operations, and inspection classes
   - Requires optional `pyiceberg` dependency

### Package Structure

```
src/mountainash_data/
├── __init__.py                  # Public API surface
├── __version__.py               # Version information
├── core/
│   ├── protocol.py              # Backend / Connection protocols
│   ├── inspection.py            # Shared physical metadata model
│   ├── connection.py            # BaseDBConnection abstract class
│   ├── utils.py                 # DatabaseUtils high-level facade
│   ├── constants.py             # CONST_DB_PROVIDER_TYPE and friends
│   ├── settings/                # Per-dialect auth settings (pydantic)
│   └── factories/               # ConnectionFactory, OperationsFactory, SettingsFactory
└── backends/
    ├── ibis/
    │   ├── backend.py           # IbisBackend + IbisConnection (new-style)
    │   ├── connection.py        # BaseIbisConnection + dialect subclasses
    │   ├── operations.py        # BaseIbisOperations + dialect subclasses
    │   ├── inspect.py           # Ibis-specific inspection helpers
    │   └── dialects/            # DialectSpec registry (data-driven)
    └── iceberg/
        ├── backend.py           # IcebergBackend + catalog registry
        ├── connection.py        # IcebergConnectionBase
        ├── operations.py        # IcebergOperationsBase
        ├── inspect.py           # Iceberg inspection helpers
        └── catalogs/            # Per-catalog implementations
```

### Optional Dependencies (extras)
- **pyiceberg**: Required for `IcebergBackend`
- **postgres**: PostgreSQL support (psycopg2-binary, ibis-framework[postgres])
- **mssql**: SQL Server support (pyodbc, ibis-framework[mssql])
- **snowflake**: Snowflake support (snowflake-connector-python, ibis-framework[snowflake])
- **bigquery**: Google BigQuery support (ibis-framework[bigquery])
- **pyspark**: Apache Spark support (ibis-framework[pyspark])
- **trino**: Trino support (ibis-framework[trino])

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

### Code Quality Analysis
- **Complexity analysis**: `hatch run radon:radon-cc` (cyclomatic complexity)
- **Maintainability index**: `hatch run radon:radon-mi`

## Dependencies

### Core Dependencies
- **ibis-framework[polars,pandas,sqlite,duckdb]** == 10.4.0 - Core data processing framework
- **numpy** >=1.23.2,<3 - Numerical computing
- **pandas** >=2.2.0 - Data manipulation and analysis
- **polars** ==1.16.0 - Fast DataFrame library
- **pyarrow** ==17.0.0 - Columnar in-memory analytics
- **xarray** ==2024.11.0 - N-dimensional arrays
- **rich** >=12.4.4,<14 - Rich text and beautiful formatting
- **universal_pathlib** ==0.2.2 - Universal pathlib interface
- **sqlalchemy** - SQL toolkit and ORM
- **duckdb** - In-process SQL OLAP database

### Internal Mountain Ash Dependencies
- **mountainash-settings** - Settings management and configuration framework
- **mountainash-constants** - Shared constants and configuration
- **mountainash-dataframes** - DataFrame abstractions and utilities
- **mountainash-utils-files** - File system utilities
- **mountainash-utils-os** - Operating system utilities
- **mountainash-utils-ssh** - SSH connection utilities

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
- **python-run-pytest.yml**: Runs pytest on pull requests targeting Python 3.12/Ubuntu 24.04
- **python-run-ruff.yml**: Code linting and formatting checks
- **python-run-radon.yml**: Complexity analysis and code quality metrics

### Release Process
- **build-and-release-package.yml**: Automated release workflow
  - Supports production, RC, and beta releases
  - Generates SBOMs (Software Bill of Materials)

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
├── conftest.py                    # Shared fixtures
├── fixtures/
│   ├── settings_fixtures.py       # Auth settings fixtures
│   ├── database_fixtures.py       # Database/backend fixtures
│   └── dataframe_fixtures.py      # DataFrame fixtures
├── test_unit/
│   ├── core/                      # Protocol, inspection tests
│   ├── backends/ibis/             # IbisBackend tests
│   ├── backends/iceberg/          # IcebergBackend tests
│   ├── factories/                 # Factory tests
│   ├── databases/                 # Legacy-path tests (updated to new paths)
│   ├── test_database_utils.py     # DatabaseUtils tests
│   └── test_mountainash_data.py   # Package-level import tests
└── test_integration/
    └── test_end_to_end_workflows.py
```

### Test Markers
- `unit`: Unit tests (fast, isolated)
- `integration`: Integration tests (slower, external dependencies)
- `performance`: Performance and benchmark tests
- `slow`: Tests that take longer to run
- `smoke`: Quick tests to verify basic functionality

### Test Backends
- SQLite and DuckDB for local testing (in-memory where possible)
- Mock connections for optional backends

## Usage Patterns

```python
from mountainash_data import IbisBackend, IcebergBackend
from mountainash_data.core.settings import (
    SQLiteAuthSettings,
    NoAuth,
    PostgreSQLAuthSettings,
    PasswordAuth,
)

sqlite = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
pg = PostgreSQLAuthSettings(
    HOST="db.example",
    DATABASE="app",
    auth=PasswordAuth(username="app", password="s3cret"),
)

kwargs = pg.to_driver_kwargs()           # → dict ready for Ibis
url = pg.to_connection_string()          # → "postgresql://app:s3cret@db.example:5432/app"
print(pg.provider_type, pg.backend)

# Ibis backend (direct)
backend = IbisBackend(dialect="sqlite", database=":memory:")
conn = backend.connect()
try:
    tables = conn.list_tables()
    info = conn.inspect_table("users")
    relation = conn.to_relation("users")  # → mountainash-expressions Relation
finally:
    conn.close()

# Iceberg backend (requires pyiceberg)
ice = IcebergBackend(catalog="rest", uri="http://localhost:8181")
ice_conn = ice.connect()
try:
    namespaces = ice_conn.list_namespaces()
finally:
    ice_conn.close()
```

## Development Environments

### Hatch Environments
- `default`: Basic local development environment
- `dev`: Full local development with all Mountain Ash dependencies
- `test`: Local testing with extended pytest plugins and coverage
- `test_github`: GitHub Actions testing environment
- `build_github`: GitHub Actions building and SBOM generation
- `tower`: Minimal production-like environment
- `ruff`: Linting and formatting
- `radon`: Complexity analysis
- `mypy`: Type checking

## Versioning Strategy

Uses CalVer (Calendar Versioning) with semantic versioning:
- Format: `YYYY.MM.MICRO`
- Release candidate: `YYYY.MM.0`
- Production: `YYYY.MM.1`
- Patches: `YYYY.MM.X`

## Configuration and Settings

### Environment Settings
- **settings/**: Environment-specific configuration files
  - **local_auth_settings.env**: Local authentication settings
  - **local_postgres_auth_settings.env**: PostgreSQL authentication
  - **mountainash_acrds_settings.env**: Mountain Ash ACRDS settings

### Configuration Files
- **hatch.toml**: Hatch build tool configuration
- **sonar-project.properties**: SonarCloud analysis configuration
- **pyproject.toml**: Python project configuration and dependencies

## License
MIT License
