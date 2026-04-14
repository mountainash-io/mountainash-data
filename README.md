# mountainash-data

![Python](https://img.shields.io/badge/python-3.10%2B-blue) ![Category](https://img.shields.io/badge/category-core-purple) ![Tests](https://img.shields.io/badge/tests-✓-green) ![Docs](https://img.shields.io/badge/docs-✓-blue)


Mountain Ash - Data

Physical access to backend data services — relational databases via Ibis,
and Iceberg table-format catalogs via PyIceberg.



## Installation

### Development Installation

```bash
# Clone and install in development mode
git clone <repository-url>
cd mountainash-data
pip install -e .
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
    relation = conn.to_relation("my_table")  # → mountainash-expressions Relation
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



## Architecture

mountainash-data uses a layered architecture:

```
src/mountainash_data/
├── __init__.py                  # Public API surface
├── __version__.py               # Version information
├── core/                        # Protocol, inspection, settings, factories
│   ├── protocol.py              # Backend / Connection protocols
│   ├── inspection.py            # CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo
│   ├── connection.py            # BaseDBConnection abstract class
│   ├── utils.py                 # DatabaseUtils high-level facade
│   ├── constants.py             # CONST_DB_PROVIDER_TYPE and friends
│   ├── settings/                # Per-dialect auth settings (pydantic)
│   └── factories/               # ConnectionFactory, OperationsFactory, SettingsFactory
└── backends/
    ├── ibis/                    # IbisBackend — 12-dialect registry
    │   ├── backend.py           # IbisBackend + IbisConnection
    │   ├── connection.py        # BaseIbisConnection + per-dialect subclasses
    │   ├── operations.py        # BaseIbisOperations + per-dialect subclasses
    │   ├── inspect.py           # Ibis-specific inspection helpers
    │   └── dialects/            # DialectSpec registry (data-driven)
    └── iceberg/                 # IcebergBackend — PyIceberg catalogs
        ├── backend.py           # IcebergBackend + catalog registry
        ├── connection.py        # IcebergConnectionBase
        ├── operations.py        # IcebergOperationsBase
        ├── inspect.py           # Iceberg inspection helpers
        └── catalogs/            # Per-catalog implementations (rest, …)
```

### Public API

```python
from mountainash_data import (
    Backend,            # Protocol: what every backend must implement
    Connection,         # Protocol: what every connection must implement
    IbisBackend,        # Ibis-style relational backends (sqlite, duckdb, postgres, …)
    IcebergBackend,     # Iceberg-style table-format catalogs (pyiceberg required)
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

- **pyiceberg**: Required for `IcebergBackend`. Not installed by default.
- **postgres**: `psycopg2-binary` + `ibis-framework[postgres]`
- **mssql**: `pyodbc` + `ibis-framework[mssql]`
- **snowflake**: `snowflake-connector-python` + `ibis-framework[snowflake]`
- **bigquery**: `ibis-framework[bigquery]`
- **pyspark**: `ibis-framework[pyspark]`
- **trino**: `ibis-framework[trino]`



## Features

- **12-dialect ibis registry** — SQLite, DuckDB, MotherDuck, PostgreSQL, MySQL, MSSQL, Oracle, Snowflake, BigQuery, Redshift, Trino, PySpark
- **Protocol-first design** — `Backend` and `Connection` protocols enable type-safe composition
- **Expressions seam** — `Connection.to_relation()` bridges to `mountainash-expressions`
- **Settings-driven** — pydantic settings for every dialect, factory auto-detection from URLs
- **Comprehensive test suite** ensuring reliability



## Documentation

- **[CLAUDE.md](CLAUDE.md)** - Technical documentation and development guide
- **[Examples](docs/examples/)** - Usage examples and tutorials
- **[Mountain Ash Documentation](https://mountainash-io.github.io/mountainash-docs/)** - Complete ecosystem documentation



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
```

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
