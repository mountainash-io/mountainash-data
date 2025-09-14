from .base_ibis_connection import BaseIbisConnection

# Core connections that are always available
from .sqlite_ibis_connection import SQLite_IbisConnection
from .duckdb_ibis_connection import DuckDB_IbisConnection
from .motherduck_ibis_connection import MotherDuck_IbisConnection

# Define base exports that are always available
__all__ = [
    "BaseIbisConnection",
    "SQLite_IbisConnection",
    "DuckDB_IbisConnection",
    "MotherDuck_IbisConnection"
]

# # Import optional connections only if they're available
# try:
#     from .connections import Snowflake_IbisConnection
#     __all__.append("Snowflake_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .connections import PySpark_IbisConnection
#     __all__.append("PySpark_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .connections import Postgres_IbisConnection
#     __all__.append("Postgres_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .connections import MSSQL_IbisConnection
#     __all__.append("MSSQL_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .connections import BigQuery_IbisConnection
#     __all__.append("BigQuery_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .connections import Trino_IbisConnection
#     __all__.append("Trino_IbisConnection")
# except ImportError:
#     pass
