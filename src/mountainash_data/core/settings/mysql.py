"""MySQL backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/mysql.md``.
Driver: https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes
Ibis: ``ibis.backends.mysql.do_connect(host='localhost', user=None, password=None,
       port=3306, autocommit=True, **kwargs)``
"""

from __future__ import annotations

import typing as t
from enum import StrEnum
from pathlib import Path

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import mysql as _adapter
from mountainash_settings.auth import PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class MySQLSSLMode(StrEnum):
    DISABLED = "DISABLED"
    PREFERRED = "PREFERRED"
    REQUIRED = "REQUIRED"
    VERIFY_CA = "VERIFY_CA"
    VERIFY_IDENTITY = "VERIFY_IDENTITY"


MYSQL_DESCRIPTOR = BackendDescriptor(
    name="mysql",
    provider_type=CONST_DB_PROVIDER_TYPE.MYSQL,
    default_port=3306,
    connection_string_scheme="mysql://",
    ibis_dialect="mysql",
    auth_modes=[PasswordAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=3306,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="CHARSET", type=str, tier="advanced",
                      default="utf8mb4", driver_key="charset"),
        ParameterSpec(name="COLLATION", type=str, tier="advanced",
                      default="utf8mb4_unicode_ci", driver_key="collation"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core",
                      default=True, driver_key="autocommit"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="connect_timeout"),
        ParameterSpec(name="READ_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None, driver_key="read_timeout"),
        ParameterSpec(name="WRITE_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="write_timeout"),
        ParameterSpec(name="UNIX_SOCKET", type=t.Optional[Path],
                      tier="advanced", default=None, driver_key="unix_socket",
                      transform=lambda p: str(p)),
        ParameterSpec(name="LOCAL_INFILE", type=t.Optional[bool],
                      tier="advanced", default=None, driver_key="local_infile"),
        ParameterSpec(name="INIT_COMMAND", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="init_command"),
        # SSL parameters — adapter handles assembly into ssl={} dict
        ParameterSpec(name="SSL_MODE", type=t.Optional[MySQLSSLMode],
                      tier="core", default=None),
        ParameterSpec(name="SSL_KEY", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CERT", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CA", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CAPATH", type=t.Optional[Path], tier="advanced",
                      default=None),
        ParameterSpec(name="SSL_CIPHER", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="CONV", type=t.Optional[dict[int, t.Any]],
                      tier="advanced", default=None),
    ],
)


@register(MYSQL_DESCRIPTOR)
class MySQLAuthSettings(ConnectionProfile):
    __descriptor__ = MYSQL_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
