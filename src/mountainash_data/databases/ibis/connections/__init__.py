# Core connections that are always available
from .sqlite_ibis_connection import SQLite_IbisConnection
from .duckdb_ibis_connection import DuckDB_IbisConnection
from .motherduck_ibis_connection import MotherDuck_IbisConnection

# Define available connections and import them conditionally
__all__ = [
    "SQLite_IbisConnection",
    "DuckDB_IbisConnection",
    "MotherDuck_IbisConnection"
]

# # Optional connections - import only if available
# try:
#     from .snowflake_ibis_connection import Snowflake_IbisConnection
#     __all__.append("Snowflake_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .pyspark_ibis_connection import PySpark_IbisConnection
#     __all__.append("PySpark_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .postgres_ibis_connection import Postgres_IbisConnection
#     __all__.append("Postgres_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .mssql_ibis_connection import MSSQL_IbisConnection
#     __all__.append("MSSQL_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .bigquery_ibis_connection import BigQuery_IbisConnection
#     __all__.append("BigQuery_IbisConnection")
# except ImportError:
#     pass

# try:
#     from .trino_ibis_connection import Trino_IbisConnection
#     __all__.append("Trino_IbisConnection")
# except ImportError:
#     pass

"""
Note: In future versions, we may add more Ibis connections to this package.
Examples:
- MySQL_IbisConnection
- Oracle_IbisConnection
"""