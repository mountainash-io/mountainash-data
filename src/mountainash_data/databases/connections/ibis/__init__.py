# from typing import TYPE_CHECKING
import lazy_loader

# from .base_ibis_connection import BaseIbisConnection

# Core connections that are always available (eager loading)
# from .duckdb_ibis_connection import DuckDB_IbisConnection
# from .motherduck_ibis_connection import MotherDuck_IbisConnection

# Type hints for optional backends (zero runtime cost)
# if TYPE_CHECKING:
#     from .sqlite_ibis_connection import SQLite_IbisConnection
#     from .duckdb_ibis_connection import DuckDB_IbisConnection
#     from .motherduck_ibis_connection import MotherDuck_IbisConnection
#     from .postgres_ibis_connection import Postgres_IbisConnection
#     from .snowflake_ibis_connection import Snowflake_IbisConnection
#     from .bigquery_ibis_connection import BigQuery_IbisConnection
#     from .pyspark_ibis_connection import PySpark_IbisConnection
#     from .trino_ibis_connection import Trino_IbisConnection
#     from .mssql_ibis_connection import MSSQL_IbisConnection
#     from .mysql_ibis_connection import MySQL_IbisConnection
#     from .redshift_ibis_connection import Redshift_IbisConnection
#     from .oracle_ibis_connection import Oracle_IbisConnection

# Lazy loading for optional backends (imported only when used)
__getattr__, __dir__, __all__ = lazy_loader.attach(
    __name__,
    submodules=[],
    submod_attrs={
        'base_ibis_connection': ['BaseIbisConnection'],
        'sqlite_ibis_connection': ['SQLite_IbisConnection'],
        'duckdb_ibis_connection': ['DuckDB_IbisConnection'],
        'motherduck_ibis_connection': ['MotherDuck_IbisConnection'],
        'postgres_ibis_connection': ['Postgres_IbisConnection'],
        'snowflake_ibis_connection': ['Snowflake_IbisConnection'],
        'bigquery_ibis_connection': ['BigQuery_IbisConnection'],
        'pyspark_ibis_connection': ['PySpark_IbisConnection'],
        'trino_ibis_connection': ['Trino_IbisConnection'],
        'mssql_ibis_connection': ['MSSQL_IbisConnection'],
        'mysql_ibis_connection': ['MySQL_IbisConnection'],
        'redshift_ibis_connection': ['Redshift_IbisConnection'],
        'oracle_ibis_connection': ['Oracle_IbisConnection'],
    }
)



# Manually extend __all__ to include core exports
# __all__ = [
#     "BaseIbisConnection",
#     "SQLite_IbisConnection",
#     "DuckDB_IbisConnection",
#     "MotherDuck_IbisConnection",
# ] + list(__all__)
