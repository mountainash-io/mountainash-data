from typing import TYPE_CHECKING
import lazy_loader

# Base operations class (always available)
from .base_ibis_operations import BaseIbisOperations

# Type hints for optional operations (zero runtime cost)
if TYPE_CHECKING:
    from .postgres_ibis_operations import PostgresIbisOperations
    from .snowflake_ibis_operations import SnowflakeIbisOperations
    from .bigquery_ibis_operations import BigQueryIbisOperations
    from .pyspark_ibis_operations import PySparkIbisOperations
    from .trino_ibis_operations import TrinoIbisOperations
    from .mssql_ibis_operations import MSSQLIbisOperations
    from .mysql_ibis_operations import MySQLIbisOperations
    from .redshift_ibis_operations import RedshiftIbisOperations
    from .oracle_ibis_operations import OracleIbisOperations
    from .duckdb_ibis_operations import DuckDBIbisOperations
    from .motherduck_ibis_operations import MotherDuckIbisOperations
    from .sqlite_ibis_operations import SQLiteIbisOperations

# Lazy loading for operations (imported only when used)
__getattr__, __dir__, __all__ = lazy_loader.attach(
    __name__,
    submodules=[],
    submod_attrs={
        'postgres_ibis_operations': ['PostgresIbisOperations'],
        'snowflake_ibis_operations': ['SnowflakeIbisOperations'],
        'bigquery_ibis_operations': ['BigQueryIbisOperations'],
        'pyspark_ibis_operations': ['PySparkIbisOperations'],
        'trino_ibis_operations': ['TrinoIbisOperations'],
        'mssql_ibis_operations': ['MSSQLIbisOperations'],
        'mysql_ibis_operations': ['MySQLIbisOperations'],
        'redshift_ibis_operations': ['RedshiftIbisOperations'],
        'oracle_ibis_operations': ['OracleIbisOperations'],
        'duckdb_ibis_operations': ['DuckDBIbisOperations'],
        'motherduck_ibis_operations': ['MotherDuckIbisOperations'],
        'sqlite_ibis_operations': ['SQLiteIbisOperations'],
    }
)

# Manually extend __all__ to include base export
__all__ = [
    "BaseIbisOperations",
] + list(__all__)
