from .__version__ import __version__
# import importlib.metadata
# import sys


# Import base and core ibis connections
from .databases.ibis import (
    BaseIbisConnection,
    SQLite_IbisConnection,
    DuckDB_IbisConnection,
    MotherDuck_IbisConnection
)

# Start with the core exports
__all__ = [
    "__version__",
    "BaseIbisConnection",
    "SQLite_IbisConnection",
    "DuckDB_IbisConnection",
    "MotherDuck_IbisConnection",
]

# Track which optional extras are installed
# This will be used by other modules to conditionally import/export their components
# INSTALLED_EXTRAS = set()

# try:
#     # Get the distribution metadata for this package
#     dist = importlib.metadata.distribution("mountainash_data")

#     # Check which optional extras are installed
#     if "pyspark" in sys.modules:
#         INSTALLED_EXTRAS.add("pyspark")

#     if any("snowflake" in req for req in dist.requires or []):
#         INSTALLED_EXTRAS.add("snowflake")

#     if any("psycopg2" in req for req in dist.requires or []):
#         INSTALLED_EXTRAS.add("postgres")

#     if any("pyodbc" in req for req in dist.requires or []):
#         INSTALLED_EXTRAS.add("mssql")

#     if any("bigquery" in req for req in dist.requires or []):
#         INSTALLED_EXTRAS.add("bigquery")

#     if any("trino" in req for req in dist.requires or []):
#         INSTALLED_EXTRAS.add("trino")

#     # If the "all" extra was installed, include all extras
#     if any(req.startswith("mountainash_data[all]") for req in dist.requires or []):
#         INSTALLED_EXTRAS.update(["pyspark", "snowflake", "postgres", "mssql", "bigquery", "trino"])
# except Exception:
#     # If we can't determine extras, continue with core functionality
#     pass

# # Now conditionally import the optional connections based on installed extras
# if "pyspark" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import PySpark_IbisConnection
#         __all__.append("PySpark_IbisConnection")
#     except ImportError:
#         pass

# if "snowflake" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import Snowflake_IbisConnection
#         __all__.append("Snowflake_IbisConnection")
#     except ImportError:
#         pass

# if "postgres" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import Postgres_IbisConnection
#         __all__.append("Postgres_IbisConnection")
#     except ImportError:
#         pass

# if "mssql" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import MSSQL_IbisConnection
#         __all__.append("MSSQL_IbisConnection")
#     except ImportError:
#         pass

# if "bigquery" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import BigQuery_IbisConnection
#         __all__.append("BigQuery_IbisConnection")
#     except ImportError:
#         pass

# if "trino" in INSTALLED_EXTRAS:
#     try:
#         from .databases.ibis import Trino_IbisConnection
#         __all__.append("Trino_IbisConnection")
#     except ImportError:
#         pass
