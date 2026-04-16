"""PostgreSQL backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/postgresql.md``.
Driver: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
Ibis: ``ibis.backends.postgres.do_connect(host, user, password, port=5432,
       database, schema, autocommit=True, **kwargs)`` (psycopg).
"""

from __future__ import annotations

import typing as t
from enum import StrEnum
from pathlib import Path

from pydantic import SecretStr

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class PostgresSSLMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


class PostgresTargetSessionAttrs(StrEnum):
    ANY = "any"
    READ_WRITE = "read-write"
    READ_ONLY = "read-only"
    PRIMARY = "primary"
    STANDBY = "standby"
    PREFER_STANDBY = "prefer-standby"


class PostgresRequireAuthMethods(StrEnum):
    PASSWORD = "password"
    MD5 = "md5"
    GSS = "gss"
    SSPI = "sspi"
    SCRAM_SHA_256 = "scram-sha-256"
    NONE = "none"


class PostgresSSLNegotiation(StrEnum):
    POSTGRES = "postgres"
    DIRECT = "direct"


class PostgresSSLCertMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    REQUIRE = "require"


def _join_require_auth(v: list[PostgresRequireAuthMethods]) -> str:
    return ",".join(m.value for m in v)


POSTGRESQL_DESCRIPTOR = BackendDescriptor(
    name="postgresql",
    provider_type=CONST_DB_PROVIDER_TYPE.POSTGRESQL,
    default_port=5432,
    connection_string_scheme="postgresql://",
    ibis_dialect="postgres",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="HOSTADDR", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="hostaddr"),
        ParameterSpec(name="PORT", type=int, tier="core", default=5432,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core", default=True,
                      driver_key="autocommit"),
        ParameterSpec(name="CONNECT_TIMEOUT", type=t.Optional[int],
                      tier="core", default=None, driver_key="connect_timeout"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application_name"),
        ParameterSpec(name="PASSFILE", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="passfile",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SERVICE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="service"),
        ParameterSpec(name="OPTIONS", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="options"),
        ParameterSpec(name="CHANNEL_BINDING", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="channel_binding"),
        ParameterSpec(name="REQUIRE_AUTH",
                      type=t.Optional[list[PostgresRequireAuthMethods]],
                      tier="core", default=None, driver_key="require_auth",
                      transform=_join_require_auth),
        # Keepalives
        ParameterSpec(name="KEEPALIVES", type=bool, tier="advanced", default=True,
                      driver_key="keepalives",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="KEEPALIVES_IDLE", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_idle"),
        ParameterSpec(name="KEEPALIVES_INTERVAL", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_interval"),
        ParameterSpec(name="KEEPALIVES_COUNT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="keepalives_count"),
        ParameterSpec(name="TCP_USER_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="tcp_user_timeout"),
        # SSL / TLS
        ParameterSpec(name="SSL_MODE", type=PostgresSSLMode, tier="core",
                      default=PostgresSSLMode.PREFER, driver_key="sslmode"),
        ParameterSpec(name="SSL_NEGOTIATION", type=t.Optional[PostgresSSLNegotiation],
                      tier="advanced", default=None, driver_key="sslnegotiation"),
        ParameterSpec(name="SSL_COMPRESSION", type=t.Optional[bool],
                      tier="advanced", default=None, driver_key="sslcompression",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="SSL_CERT", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcert",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_KEY", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslkey",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_PASSWORD", type=t.Optional[SecretStr],
                      tier="advanced", default=None, driver_key="sslpassword",
                      secret=True),
        ParameterSpec(name="SSL_CERTMODE", type=t.Optional[PostgresSSLCertMode],
                      tier="advanced", default=None, driver_key="sslcertmode"),
        ParameterSpec(name="SSL_ROOTCERT", type=t.Optional[Path],
                      tier="advanced", default=None, driver_key="sslrootcert",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_CRL", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcrl",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_CRLDIR", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="sslcrldir",
                      transform=lambda p: str(p)),
        ParameterSpec(name="SSL_SNI", type=t.Optional[bool], tier="advanced",
                      default=None, driver_key="sslsni",
                      transform=lambda v: 1 if v else 0),
        ParameterSpec(name="TARGET_SESSION_ATTRS",
                      type=t.Optional[PostgresTargetSessionAttrs],
                      tier="advanced", default=None,
                      driver_key="target_session_attrs"),
    ],
)


@register(POSTGRESQL_DESCRIPTOR)
class PostgreSQLAuthSettings(ConnectionProfile):
    __descriptor__ = POSTGRESQL_DESCRIPTOR
