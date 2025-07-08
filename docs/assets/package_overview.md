## Project Overview

**mountainash-data** is a Python package that provides unified database connections and dataframe abstractions for multiple backends via Ibis. It implements dataframe wrappers for native pandas, polars, and Ibis backends, enabling seamless cross-platform data operations.

## Architecture

### Core Components

1. **Database Connections Layer** (`src/mountainash_data/databases/`)
   - Base database connection abstraction (`BaseDBConnection`)
   - Ibis-based connections supporting multiple backends (SQLite, DuckDB, PostgreSQL, SQL Server, etc.)
   - PyIceberg support for data lake operations
   - Connection factory pattern for backend instantiation

2. **DataFrame Abstraction Layer** (`src/mountainash_data/dataframes/`)
   - Unified dataframe interface (`BaseDataFrame`, `IbisDataFrame`)
   - Cross-backend dataframe operations (filter, join, aggregate, etc.)
   - Materialization to pandas, polars, and pyarrow formats
   - Data type conversion and optimization utilities

3. **Utilities and Helpers** (`src/mountainash_data/dataframes/utils/`)
   - Column mapping and transformation (`column_mapper/`)
   - Dataframe filtering strategies (`dataframe_filters/`)
   - Backend-specific handlers (`dataframe_handlers/`)
   - Python data structure converters (`pydata_converter/`)

4. **Lineage and Metadata** (`src/mountainash_data/lineage/`)
   - OpenLineage integration for data lineage tracking

### Package Structure

```
src/mountainash_data/
├── __init__.py                    # Main package exports
├── __version__.py                 # Version information
├── databases/                     # Database connection layer
│   ├── base_db_connection.py     # Abstract base connection
│   ├── ibis/                     # Ibis-based connections
│   │   ├── base_ibis_connection.py
│   │   ├── connections/          # Specific backend implementations
│   │   │   ├── sqlite_ibis_connection.py
│   │   │   ├── duckdb_ibis_connection.py
│   │   │   ├── postgres_ibis_connection.py
│   │   │   └── [other backends...]
│   │   └── ibis_connection_factory.py
│   └── pyiceberg/               # PyIceberg support
├── dataframes/                   # DataFrame abstraction layer
│   ├── base_dataframe.py        # Abstract dataframe interface
│   ├── ibis_dataframe.py        # Ibis dataframe implementation
│   └── utils/                   # DataFrame utilities
│       ├── column_mapper/       # Column mapping utilities
│       ├── dataframe_filters/   # Filtering strategies
│       ├── dataframe_handlers/  # Backend-specific handlers
│       └── pydata_converter/    # Python data structure converters
└── lineage/                     # Data lineage tracking
    └── openlineage_helper.py
```


## Build/Test/Lint Commands
- Build: `hatch build`
- Lint: `hatch run ruff:check` or `hatch run ruff:fix` to auto-fix
- Tests: `hatch run test:test` or `hatch run test:cov` for coverage
- Single test: `pytest tests/path/to/test_file.py::TestClass::test_function -v`
- Type check: `hatch run mypy:check`

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
- **openlineage-python** ==1.17.1 - Data lineage tracking
- **sqlalchemy** - SQL toolkit and ORM
- **duckdb** - In-process SQL OLAP database

### Optional Dependencies (extras)
- **all**: Complete set of database backends
- **mssql**: SQL Server support (pyodbc, ibis-framework[mssql])
- **snowflake**: Snowflake support (snowflake-connector-python, ibis-framework[snowflake])
- **postgres**: PostgreSQL support (psycopg2-binary, ibis-framework[postgres])
- **bigquery**: Google BigQuery support (ibis-framework[bigquery])
- **pyspark**: Apache Spark support (ibis-framework[pyspark])
- **trino**: Trino support (ibis-framework[trino])

### Internal Mountain Ash Dependencies
- **mountainash-settings** - Settings management and configuration framework

### Development Dependencies
- pytest==8.3.5
- pytest-check, pytest-cov, pytest-mock
- ruff==0.3.7
- mypy==1.10.1
- radon==6.0.1

## GitHub Actions Workflows

### Testing Workflows
- **python-run-pytest.yml**: Runs pytest on pull requests targeting Python 3.12/Ubuntu 24.04
  - Includes coverage reporting to Codecov
  - Integrates with Mountain Ash dependency management
  - Supports workflow dispatch with fallback branches
- **python-run-ruff.yml**: Code linting and formatting checks
- **python-run-radon.yml**: Complexity analysis and code quality metrics

### Release Process
- **build-and-release-package.yml**: Automated release workflow
  - Supports production, RC, and beta releases
  - Generates SBOMs (Software Bill of Materials)
  - Creates releases in GitHub and mountainash-wheels repository
- **main-release-branch-validation.yml**: Validates release branches
- **main-release-build-dependencies.yml**: Manages build dependencies

### Branch Strategy
- `main`: Production releases (only release/* and hotfix/* branches)
- `develop`: Development and RC releases
- `feature/*`, `bugfix/*`, `hotfix/*`: Feature branches
- Protected branches require code owner approval

### Quality Gates
- **SonarCloud**: Code quality analysis
- **Codecov**: Code coverage reporting
- **GitHub Actions**: Automated testing and validation

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
- **tests/**: Main test directory
  - **test_ibis_backends.py**: Database backend connection tests
  - **test_dataframe_utils.py**: DataFrame utility function tests
  - **test_dataframe_filters.py**: DataFrame filtering tests
  - **test_polars_dataframe.py**: Polars-specific DataFrame tests
  - **column_mapper/**: Column mapping functionality tests
  - **pydata_converter/**: Python data structure converter tests

### Test Configuration
- **pytest.ini**: Pytest configuration
- **codecov.yml**: Code coverage configuration
- **coverage.toml**: Coverage reporting settings

### Test Backends
- SQLite and DuckDB for local testing
- Mock connections for other backends
- Integration with Mountain Ash settings framework

## Documentation

### Core Documentation
- **README.md**: Project overview and basic usage
- **CONTRIBUTING.md**: Contribution guidelines
- **TESTING.md**: Testing procedures and guidelines
- **RELEASE.md**: Release process documentation

### Requirements Documentation
- **docs/adapter_requirements.md**: Data structure adapter requirements
- **docs/hierarchy_builder_requirements.md**: Hierarchy builder specifications
- **docs/hierachy_flattener_requirements.md**: Hierarchy flattener requirements

### Notebooks
- **notebooks/**: Jupyter notebooks for exploration and examples
  - **db.ipynb**: Database connection examples
  - **ibis.ipynb**: Ibis framework usage
  - **pyarrow.ipynb**: PyArrow integration examples

## Development Environments

### Hatch Environments
- `default`: Local development
- `test`: Local testing with extended pytest plugins
- `test_github`: GitHub Actions testing
- `build_github`: GitHub Actions building
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

## Usage Patterns

### Database Connections
```python
from mountainash_data import SQLite_IbisConnection, DuckDB_IbisConnection
from mountainash_settings import SettingsParameters

# Create connection
settings = SettingsParameters.create(settings_class=SQLiteAuthSettings)
conn = SQLite_IbisConnection(db_auth_settings_parameters=settings)
backend = conn.connect()
```

### DataFrame Operations
```python
from mountainash_data import IbisDataFrame, DataFrameFactory

# Create dataframe
df = IbisDataFrame(ibis_table)

# Cross-backend operations
result = df.filter(expression).materialise('polars')
```

### Data Conversion
```python
from mountainash_data.dataframes.utils.pydata_converter import DataFrameFactory

# Convert Python data structures
df = DataFrameFactory.create_from_list_of_dicts(data)
```

## License
MIT License
