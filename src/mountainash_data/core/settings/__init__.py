"""Backend settings — declarative descriptor + registry.

The *AuthSettings classes below are stable import anchors; internally each
class body is a two-line shell (``__descriptor__`` + ``__adapter__``).
"""

from __future__ import annotations

# Core primitives
from .descriptor import MISSING, BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    get_descriptor,
    get_settings_class,
    register,
)

# Auth union members
from mountainash_settings.auth import (
    AuthSpec,
    AzureADAuth,
    CertificateAuth,
    IAMAuth,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    ServiceAccountAuth,
    TokenAuth,
    WindowsAuth,
)

# Per-backend settings classes (these import-register themselves).
from .sqlite import SQLiteAuthSettings
from .duckdb import DuckDBAuthSettings
from .motherduck import MotherDuckAuthSettings
from .postgresql import PostgreSQLAuthSettings
from .clickhouse import ClickHouseAuthSettings
from .mysql import MySQLAuthSettings
from .mssql import MSSQLAuthSettings
from .snowflake import SnowflakeAuthSettings
from .bigquery import BigQueryAuthSettings
from .redshift import RedshiftAuthSettings
from .pyspark import PySparkAuthSettings
from .trino import TrinoAuthSettings
from .pyiceberg_rest import PyIcebergRestAuthSettings

__all__ = [
    # primitives
    "MISSING", "BackendDescriptor", "ParameterSpec", "ConnectionProfile",
    "DATABASES_REGISTRY", "REGISTRY",
    "get_descriptor", "get_settings_class", "register",
    # auth
    "AuthSpec", "NoAuth", "PasswordAuth", "TokenAuth", "JWTAuth",
    "OAuth2Auth", "ServiceAccountAuth", "IAMAuth", "WindowsAuth",
    "AzureADAuth", "KerberosAuth", "CertificateAuth",
    # backends
    "SQLiteAuthSettings", "DuckDBAuthSettings", "MotherDuckAuthSettings",
    "PostgreSQLAuthSettings", "ClickHouseAuthSettings",
    "MySQLAuthSettings", "MSSQLAuthSettings",
    "SnowflakeAuthSettings", "BigQueryAuthSettings", "RedshiftAuthSettings",
    "PySparkAuthSettings", "TrinoAuthSettings", "PyIcebergRestAuthSettings",
]
