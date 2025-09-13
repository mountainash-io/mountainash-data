#path: mountainash_data/databases/constants.py

from enum import Enum,StrEnum, auto
from mountainash_constants import identity_enum_helpers, value_enum_helpers


class IBIS_DB_CONNECTION_MODE(StrEnum):
    CONNECTION_STRING = "connection_string"
    KWARGS = "kwargs"
    HYBRID = "hybrid"


class CONST_DB_PROVIDER_TYPE(Enum):
    """Database provider types"""
    MYSQL = auto()
    POSTGRESQL = auto()
    MSSQL = auto()
    SNOWFLAKE = auto()
    BIGQUERY = auto()
    REDSHIFT = auto()
    SQLITE =auto()
    DUCKDB =auto()
    MOTHERDUCK = auto()
    TRINO = auto()
    PYICEBERG_REST = auto()
    ORACLE = auto()
    PYSPARK = auto()


class CONST_DB_AUTH_METHOD(StrEnum):
    """Authentication methods"""
    PASSWORD = "password"
    OAUTH = "oauth"
    IAM = "iam"
    TOKEN = "token"
    CERTIFICATE = "certificate"
    WINDOWS = "windows"
    MANAGED_IDENTITY = "managed_identity"
    NONE = "none"

class CONST_DB_SSL_MODE_MYSQL(StrEnum):
    """SSL modes for database connections"""
    DISABLED = "disabled"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"

class CONST_DB_SSL_MODE_POSTGRES(StrEnum):
    """SSL modes for database connections"""
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"

class CONST_DB_CONNECTION_STATUS(StrEnum):
    """Database connection status"""
    UNTESTED = "untested"
    VALID = "valid"
    INVALID = "invalid"
    ERROR = "error"

class CONST_DB_POOL_MODE(StrEnum):
    """Connection pool modes"""
    FIXED = "fixed"
    DYNAMIC = "dynamic"
    NONE = "none"


class CONST_DB_ABSTRACTION_LAYER(Enum):
    """
    Enumeration for different database abstraction layers.

    Attributes:
        - IBIS (str): Ibis database abstraction layer.
        - FUGUE (str): Fugue database abstraction layer.
    """
    IBIS =     auto()
    PYICEBERG = auto()

class CONST_DB_BACKEND(StrEnum):
    """
    Enumeration for different database backends.

    Attributes:
        - DUCKDB (str): DuckDB backend.
        - SQLITE (str): SQLite backend.
        - POSTGRES (str): PostgreSQL backend.
        - TRINO (str): Trino backend.
        - DATABRICKS (str): Databricks backend.
        - SNOWFLAKE (str): Snowflake backend.
        - ORACLE (str): Oracle backend.
        - REDSHIFT (str): Redshift backend.
        - MSSQL (str): Microsoft SQL Server backend.
        - MYSQL (str): MySQL backend.
    """

    BIGQUERY =     "BIGQUERY"
    DUCKDB =       "DUCKDB"
    SQLITE =       "SQLITE"
    POSTGRES =     "POSTGRES"
    TRINO =        "TRINO"
    DATABRICKS =   "DATABRICKS"
    PYSPARK =      "PYSPARK"
    SNOWFLAKE =    "SNOWFLAKE"
    ORACLE =       "ORACLE"
    REDSHIFT =     "REDSHIFT"
    MSSQL =        "MSSQL"
    MYSQL =        "MYSQL"
    MOTHERDUCK =   "MOTHERDUCK"
    PYICEBERG =    "PYICEBERG"
    # POLARS =       "POLARS"
    # PANDAS =       "PANDAS"


class CONST_DB_BACKEND_IBIS_PREFIX(StrEnum):
    """
    Enumeration for different database backends.

    Attributes:
        - DUCKDB (str): DuckDB backend.
        - SQLITE (str): SQLite backend.
        - POSTGRES (str): PostgreSQL backend.
        - TRINO (str): Trino backend.
        - DATABRICKS (str): Databricks backend.
        - SNOWFLAKE (str): Snowflake backend.
        - ORACLE (str): Oracle backend.
        - REDSHIFT (str): Redshift backend.
        - MSSQL (str): Microsoft SQL Server backend.
        - MYSQL (str): MySQL backend.
    """

    BIGQUERY =     "bigquery:"
    DUCKDB =       "duckdb:"
    MOTHERDUCK =   "duckdb://md:"
    SQLITE =       "sqlite:"
    POSTGRES =     "postgres:"
    TRINO =        "trino:"
    DATABRICKS =   "databricks:"
    SNOWFLAKE =    "snowflake:"
    ORACLE =       "oracle:"
    REDSHIFT =     "redshift:"
    MSSQL =        "mssql:"
    MYSQL =        "mysql:"

class CONST_DB_BACKEND_CAPABILITIES(Enum):
    """
    Enumeration for different database backend capabilities.

    Attributes:
        - READ_PARQUET (str): Capability to read Parquet files.
        - READ_CSV (str): Capability to read CSV files.
        - READ_JSON (str): Capability to read JSON files.
        - READ_DELTA (str): Capability to read Delta files.
        - WRITE_PARQUET (str): Capability to write Parquet files.
        - WRITE_CSV (str): Capability to write CSV files.
        - WRITE_JSON (str): Capability to write JSON files.
        - WRITE_DELTA (str): Capability to write Delta files.
    """
    READ_PARQUET =     "READ_PARQUET"
    READ_CSV =         "READ_CSV"
    READ_JSON =        "READ_JSON"
    READ_DELTA =       "READ_DELTA"
    WRITE_PARQUET =    "WRITE_PARQUET"
    WRITE_CSV =        "WRITE_CSV"
    WRITE_JSON =       "WRITE_JSON"
    WRITE_DELTA =      "WRITE_DELTA"
