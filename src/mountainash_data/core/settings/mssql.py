"""MSSQL backend settings.

Spec: ``mountainash-central/04.planning/mountainash-data/superpowers/specs/2026-04-15-settings-audit/mssql.md``.
Driver: PyODBC connect + ODBC Driver 17/18 for SQL Server.
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import AzureADAuthProfile, PasswordAuthProfile, WindowsAuthProfile
from .adapters import mssql as _mssql
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register


class MSSQLDriver(StrEnum):
    ODBC_18 = "ODBC Driver 18 for SQL Server"
    ODBC_17 = "ODBC Driver 17 for SQL Server"
    LEGACY = "SQL Server"


class MSSQLEncryption(StrEnum):
    DISABLED = "no"
    MANDATORY = "yes"
    STRICT = "strict"


MSSQL_SPEC = BackendSpec(
    name="mssql",
    provider_type=CONST_DB_PROVIDER_TYPE.MSSQL,
    default_port=1433,
    connection_string_scheme="mssql://",
    ibis_dialect="mssql",
    supported_auth=(PasswordAuthProfile, WindowsAuthProfile, AzureADAuthProfile),
    parameters=[
        ParameterSpec(
            name="HOST",
            type=str,
            tier="core",
            driver_key="host",
        ),
        ParameterSpec(
            name="PORT",
            type=int,
            tier="core",
            default=1433,
            driver_key="port",
        ),
        ParameterSpec(
            name="DATABASE",
            type=t.Optional[str],
            tier="core",
            default=None,
            driver_key="database",
        ),
        ParameterSpec(
            name="DRIVER",
            type=MSSQLDriver,
            tier="core",
            default=MSSQLDriver.ODBC_18,
            driver_key="driver",
        ),
        ParameterSpec(
            name="ENCRYPTION",
            type=t.Optional[MSSQLEncryption],
            tier="core",
            default=MSSQLEncryption.MANDATORY,
        ),
        ParameterSpec(
            name="TRUST_SERVER_CERTIFICATE",
            type=bool,
            tier="core",
            default=False,
        ),
        ParameterSpec(
            name="INSTANCE_NAME",
            type=t.Optional[str],
            tier="advanced",
            default=None,
        ),
        ParameterSpec(
            name="APP_NAME",
            type=str,
            tier="advanced",
            default="MountainAsh",
            driver_key="application_name",
        ),
        ParameterSpec(
            name="MARS_ENABLED",
            type=bool,
            tier="advanced",
            default=False,
        ),
        ParameterSpec(
            name="LOGIN_TIMEOUT",
            type=t.Optional[int],
            tier="advanced",
            default=None,
            driver_key="login_timeout",
        ),
    ],
)


@register
class MSSQLBackendProfile(BackendProfile):
    __spec__ = MSSQL_SPEC
    __adapters__ = {CONST_DB_PROVIDER_TYPE.MSSQL: _mssql.host_fold}
