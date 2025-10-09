# from typing import TYPE_CHECKING
import lazy_loader
from .base_ibis_operations import BaseIbisOperations

# Base operations class (always available)

# Type hints for optional operations (zero runtime cost)
# if TYPE_CHECKING:
#     from .postgres_ibis_operations import Postgres_IbisOperations
#     from .snowflake_ibis_operations import Snowflake_IbisOperations
#     from .bigquery_ibis_operations import BigQuery_IbisConnection
#     from .pyspark_ibis_operations import PySpark_IbisOperations
#     from .trino_ibis_operations import Trino_IbisOperations
#     from .mssql_ibis_operations import MSSQL_IbisOperations
#     from .mysql_ibis_operations import MySQL_IbisOperations
#     from .redshift_ibis_operations import Redshift_IbisOperations
#     from .oracle_ibis_operations import Oracle_IbisOperations
#     from .duckdb_ibis_operations import DuckDB_IbisOperations
#     from .motherduck_ibis_operations import MotherDuck_IbisOperations
#     from .sqlite_ibis_operations import SQLite_IbisOperations

# Lazy loading for operations (imported only when used)
__getattr__, __dir__, __all__ = lazy_loader.attach(
    __name__,
    submodules=[],
    submod_attrs={
        'postgres_ibis_operations': ['Postgres_IbisOperations'],
        'snowflake_ibis_operations': ['Snowflake_IbisOperations'],
        'bigquery_ibis_operations': ['BigQuery_IbisConnection'],
        'pyspark_ibis_operations': ['PySpark_IbisOperations'],
        'trino_ibis_operations': ['Trino_IbisOperations'],
        'mssql_ibis_operations': ['MSSQL_IbisOperations'],
        'mysql_ibis_operations': ['MySQL_IbisOperations'],
        'redshift_ibis_operations': ['Redshift_IbisOperations'],
        'oracle_ibis_operations': ['Oracle_IbisOperations'],
        'duckdb_ibis_operations': ['DuckDB_IbisOperations'],
        'motherduck_ibis_operations': ['MotherDuck_IbisOperations'],
        'sqlite_ibis_operations': ['SQLite_IbisOperations'],
    }
)


# Manually extend __all__ to include base export
