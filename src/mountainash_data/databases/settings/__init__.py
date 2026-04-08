"""DEPRECATED: import from mountainash_data.core.settings instead."""
from mountainash_data.core.settings import *  # noqa: F401,F403
from mountainash_data.core.settings import (  # noqa: F401
    BaseDBAuthSettings,
    DBAuthConfigError,
    DBAuthConnectionError,
    DBAuthValidationError,
    DBAuthSecurityError,
    DBAuthTemplates,
    BigQueryAuthSettings,
    RedshiftAuthSettings,
    SnowflakeAuthSettings,
    DuckDBAuthSettings,
    SQLiteAuthSettings,
    MSSQLAuthSettings,
    MySQLAuthSettings,
    PostgreSQLAuthSettings,
    MotherDuckAuthSettings,
    PySparkAuthSettings,
    TrinoAuthSettings,
    PyIcebergRestAuthSettings,
)
