"""Backend settings — declarative spec + registry."""

from __future__ import annotations

# Core primitives
from .descriptor import MISSING, Missing, BackendSpec, ParameterSpec
from .profile import BackendProfile, UrlParts
from .registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    get_descriptor,
    get_settings_class,
    register,
)

# Auth union members
from mountainash_auth_client import (
    AuthProfile, NoAuthProfile, PasswordAuthProfile, TokenAuthProfile,
    JWTAuthProfile, OAuth2AuthProfile, IAMAuthProfile, WindowsAuthProfile,
    AzureADAuthProfile, KerberosAuthProfile, CertificateAuthProfile,
    ServiceAccountAuthProfile,
)

# Per-backend profile classes (these import-register themselves).
from .sqlite import SQLiteBackendProfile
from .duckdb import DuckDBBackendProfile
from .motherduck import MotherDuckBackendProfile
from .postgresql import PostgreSQLBackendProfile
from .clickhouse import ClickHouseBackendProfile
from .databricks import DatabricksBackendProfile
from .mysql import MySQLBackendProfile
from .singlestoredb import SingleStoreDBBackendProfile
from .oracle import OracleBackendProfile
from .mssql import MSSQLBackendProfile
from .snowflake import SnowflakeBackendProfile
from .bigquery import BigQueryBackendProfile
from .redshift import RedshiftBackendProfile
from .pyspark import PySparkBackendProfile
from .trino import TrinoBackendProfile
from .exasol import ExasolBackendProfile
from .impala import ImpalaBackendProfile
from .materialize import MaterializeBackendProfile
from .risingwave import RisingWaveBackendProfile
from .druid import DruidBackendProfile

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
    "MISSING", "Missing", "BackendSpec", "ParameterSpec", "BackendProfile", "UrlParts",
    "DATABASES_REGISTRY", "REGISTRY",
    "get_descriptor", "get_settings_class", "register",
    # auth
    "AuthProfile", "NoAuthProfile", "PasswordAuthProfile", "TokenAuthProfile",
    "JWTAuthProfile", "OAuth2AuthProfile", "IAMAuthProfile", "WindowsAuthProfile",
    "AzureADAuthProfile", "KerberosAuthProfile", "CertificateAuthProfile",
    "ServiceAccountAuthProfile",
    # backends
    "SQLiteBackendProfile", "DuckDBBackendProfile", "MotherDuckBackendProfile",
    "PostgreSQLBackendProfile", "ClickHouseBackendProfile",
    "DatabricksBackendProfile", "MySQLBackendProfile", "SingleStoreDBBackendProfile",
    "MSSQLBackendProfile", "OracleBackendProfile",
    "SnowflakeBackendProfile", "BigQueryBackendProfile", "RedshiftBackendProfile",
    "PySparkBackendProfile", "TrinoBackendProfile",
    "ExasolBackendProfile", "ImpalaBackendProfile", "MaterializeBackendProfile",
    "RisingWaveBackendProfile", "DruidBackendProfile",
]
