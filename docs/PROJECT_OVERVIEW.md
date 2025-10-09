# mountainash-data Package Overview

## Purpose
Provides unified database connections and dataframe abstractions for multiple backends via Ibis. It implements dataframe wrappers for native pandas, polars, and Ibis backends, enabling seamless cross-platform data operations.

## Architecture
The package is built on a layered architecture with three main components:
1. **Database Connections Layer** - Abstracts database connections using Ibis framework and PyIceberg
2. **DataFrame Abstraction Layer** - Provides unified dataframe interface across multiple backends
3. **Utilities Layer** - Supporting utilities for data transformation, mapping, and conversion

## Directory + File Structure
```
src/mountainash_data/
├── __init__.py                    # Main package exports
├── __version__.py                 # Version information
├── databases/                     # Database connection layer
│   ├── __init__.py
│   ├── base_db_connection.py     # Abstract base connection
│   ├── ibis/                     # Ibis-based connections
│   │   ├── __init__.py
│   │   ├── base_ibis_connection.py
│   │   ├── constants.py
│   │   ├── ibis_connection_factory.py
│   │   └── connections/          # Specific backend implementations
│   │       ├── __init__.py
│   │       ├── bigquery_ibis_connection.py
│   │       ├── duckdb_ibis_connection.py
│   │       ├── motherduck_ibis_connection.py
│   │       ├── mssql_ibis_connection.py
│   │       ├── mysql_ibis_connection.py
│   │       ├── oracle_ibis_connection.py
│   │       ├── postgres_ibis_connection.py
│   │       ├── pyspark_ibis_connection.py
│   │       ├── redshift_ibis_connection.py
│   │       ├── snowflake_ibis_connection.py
│   │       ├── sqlite_ibis_connection.py
│   │       └── trino_ibis_connection.py
│   └── pyiceberg/               # PyIceberg support
│       ├── __init__.py
│       ├── base_pyiceberg_connection.py
│       └── connections/
│           ├── __init__.py
│           └── pyiceberg_rest_connection.py
├── dataframes/                   # DataFrame abstraction layer
│   ├── __init__.py
│   ├── base_dataframe.py        # Abstract dataframe interface
│   ├── ibis_dataframe.py        # Ibis dataframe implementation
│   └── utils/                   # DataFrame utilities
│       ├── __init__.py
│       ├── dataframe_factory.py
│       ├── dataframe_utils.py
│       ├── ibis_utils.py
│       ├── column_mapper/       # Column mapping utilities
│       │   ├── __init__.py
│       │   ├── column_config.py
│       │   ├── column_config_builder.py
│       │   ├── column_mapper.py
│       │   └── constants.py
│       ├── dataframe_filters/   # Filtering strategies
│       │   ├── __init__.py
│       │   ├── dataframe_filter.py
│       │   ├── dataframe_filter_ibis.py
│       │   ├── dataframe_filter_pandas.py
│       │   ├── dataframe_filter_polars.py
│       │   └── dataframe_filter_pyarrow.py
│       ├── dataframe_handlers/  # Backend-specific handlers
│       │   ├── __init__.py
│       │   ├── base_dataframe_strategy.py
│       │   ├── dataframe_strategy_factory.py
│       │   ├── dataframe_strategy_ibis.py
│       │   ├── dataframe_strategy_pandas.py
│       │   ├── dataframe_strategy_polars.py
│       │   ├── dataframe_strategy_polars_lazyframe.py
│       │   ├── dataframe_strategy_pyarrow_recordbatch.py
│       │   ├── dataframe_strategy_pyarrow_table.py
│       │   └── dataframe_strategy_xarray.py
│       └── pydata_converter/    # Python data structure converters
│           ├── __init__.py
│           ├── base_pydata_converter.py
│           ├── pydata_converter_dataclass.py
│           ├── pydata_converter_factory.py
│           ├── pydata_converter_pydantic.py
│           ├── pydata_converter_pydict.py
│           └── pydata_converter_pylist.py
└── lineage/                     # Data lineage tracking
    └── openlineage_helper.py
```

## Key Components

### mountainash_data
Core package providing unified data access layer with key classes:
- `BaseDataFrame` / `IbisDataFrame` - Unified dataframe interface
- `BaseIbisConnection` - Abstract database connection base
- `SQLite_IbisConnection` / `DuckDB_IbisConnection` / `MotherDuck_IbisConnection` - Core database backends
- `DataFrameFactory` - Creates dataframes from various data sources
- `DataFrameUtils` - Utility functions for dataframe operations

### databases/
Database connection abstraction supporting multiple backends:
- **Base Layer**: `BaseDBConnection` abstract interface
- **Ibis Layer**: Connection implementations for 12+ database backends
- **PyIceberg Layer**: Data lake connectivity via Apache Iceberg

### dataframes/
DataFrame abstraction and utilities:
- **Core**: `BaseDataFrame` and `IbisDataFrame` unified interface
- **Utilities**: Column mapping, filtering strategies, backend handlers, data converters
- **Conversion**: Python data structure to dataframe conversion utilities

### lineage/
Data lineage tracking via OpenLineage standard for data governance and observability.

## Usage Patterns

### Database Connection
```python
from mountainash_data import SQLite_IbisConnection
conn = SQLite_IbisConnection(db_auth_settings_parameters=settings)
backend = conn.connect()
```

### DataFrame Operations
```python
from mountainash_data import IbisDataFrame
df = IbisDataFrame(ibis_table)
result = df.filter(expression).materialise('polars')
```

### Data Factory
```python
from mountainash_data import DataFrameFactory
df = DataFrameFactory.create_from_list_of_dicts(data)
```

## Dependencies
**Runtime**: 12 core packages + optional backend dependencies

### Core Dependencies
- `ibis-framework[polars,pandas,sqlite,duckdb] == 10.4.0` - Core data processing framework
- `numpy >=1.23.2,<3` - Numerical computing
- `pandas >=2.2.0` - Data manipulation
- `polars == 1.16.0` - Fast dataframe library
- `pyarrow == 17.0.0` - Columnar analytics
- `xarray == 2024.11.0` - N-dimensional arrays
- `rich >=12.4.4,<14` - Rich formatting
- `universal_pathlib == 0.2.2` - Universal pathlib interface
- `openlineage-python == 1.17.1` - Data lineage tracking
- `sqlalchemy` - SQL toolkit
- `duckdb` - In-process OLAP database
- `pyarrow-hotfix >=0.4,<1` - PyArrow security fixes

### Optional Dependencies (extras)
- `all` - Complete database backend set
- `mssql` - SQL Server support
- `snowflake` - Snowflake support
- `postgres` - PostgreSQL support
- `bigquery` - Google BigQuery support
- `pyspark` - Apache Spark support
- `trino` - Trino support

## Integration
Integrates with the Mountain Ash ecosystem as a foundational data access layer. Designed to work with `mountainash-settings` for configuration management and supports the broader Mountain Ash data platform architecture for unified analytics and data operations.