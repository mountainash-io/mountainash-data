"""Backend settings — declarative spec + registry.

The *AuthSettings classes below are stable import anchors; internally each
class body is a two-line shell (``__spec__`` + ``__adapter__``).
"""

from __future__ import annotations

# Core primitives
from .descriptor import MISSING, Missing, BackendSpec, ParameterSpec
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
from .databricks import DatabricksAuthSettings
from .mysql import MySQLAuthSettings
from .singlestoredb import SingleStoreDBAuthSettings
from .mssql import MSSQLAuthSettings
from .snowflake import SnowflakeAuthSettings
from .bigquery import BigQueryAuthSettings
from .redshift import RedshiftAuthSettings
from .pyspark import PySparkAuthSettings
from .trino import TrinoAuthSettings
from .exasol import ExasolAuthSettings
from .impala import ImpalaAuthSettings
from .materialize import MaterializeAuthSettings
from .risingwave import RisingWaveAuthSettings
from .druid import DruidAuthSettings
from .pyiceberg_rest import PyIcebergRestAuthSettings

import warnings as _warnings


_DEPRECATED_PKG = {
    "BackendDescriptor": ("BackendSpec", BackendSpec),
    "_Missing":          ("Missing", Missing),
}


def __getattr__(name: str):
    if name in _DEPRECATED_PKG:
        new_name, obj = _DEPRECATED_PKG[name]
        _warnings.warn(
            f"{name!r} is renamed to {new_name!r} in mountainash-data. "
            f"Update imports to use the new name.",
            DeprecationWarning, stacklevel=2,
        )
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # primitives
    "MISSING", "Missing", "BackendSpec", "ParameterSpec", "ConnectionProfile",
    "DATABASES_REGISTRY", "REGISTRY",
    "get_descriptor", "get_settings_class", "register",
    # auth
    "AuthSpec", "NoAuth", "PasswordAuth", "TokenAuth", "JWTAuth",
    "OAuth2Auth", "ServiceAccountAuth", "IAMAuth", "WindowsAuth",
    "AzureADAuth", "KerberosAuth", "CertificateAuth",
    # backends
    "SQLiteAuthSettings", "DuckDBAuthSettings", "MotherDuckAuthSettings",
    "PostgreSQLAuthSettings", "ClickHouseAuthSettings",
    "DatabricksAuthSettings", "MySQLAuthSettings", "SingleStoreDBAuthSettings",
    "MSSQLAuthSettings",
    "SnowflakeAuthSettings", "BigQueryAuthSettings", "RedshiftAuthSettings",
    "PySparkAuthSettings", "TrinoAuthSettings",
    "ExasolAuthSettings", "ImpalaAuthSettings", "MaterializeAuthSettings",
    "RisingWaveAuthSettings", "DruidAuthSettings",
    "PyIcebergRestAuthSettings",
]
