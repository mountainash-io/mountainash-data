"""Snowflake backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/snowflake.md``.
Driver: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import snowflake as _adapter
from mountainash_settings.auth import CertificateAuth, OAuth2Auth, PasswordAuth, TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class SnowflakeAuthenticator(StrEnum):
    SNOWFLAKE = "snowflake"
    OAUTH = "oauth"
    OKTA = "okta"
    EXTERNAL_BROWSER = "externalbrowser"
    PASSWORD_MFA = "username_password_mfa"


SNOWFLAKE_DESCRIPTOR = BackendDescriptor(
    name="snowflake",
    provider_type=CONST_DB_PROVIDER_TYPE.SNOWFLAKE,
    connection_string_scheme="snowflake://",
    ibis_dialect="snowflake",
    auth_modes=[PasswordAuth, OAuth2Auth, CertificateAuth, TokenAuth],
    parameters=[
        ParameterSpec(name="ACCOUNT", type=str, tier="core",
                      driver_key="account"),
        ParameterSpec(name="WAREHOUSE", type=t.Optional[str], tier="core",
                      default=None, driver_key="warehouse"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="ROLE", type=t.Optional[str], tier="core",
                      default=None, driver_key="role"),
        ParameterSpec(name="AUTHENTICATOR",
                      type=t.Optional[SnowflakeAuthenticator], tier="core",
                      default=None),
        ParameterSpec(name="CONNECTION_NAME", type=t.Optional[str],
                      tier="core", default=None, driver_key="connection_name"),
        ParameterSpec(name="TIMEZONE", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="QUERY_TAG", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="APPLICATION", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application"),
        ParameterSpec(name="LOGIN_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="login_timeout"),
        ParameterSpec(name="NETWORK_TIMEOUT", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="network_timeout"),
        ParameterSpec(name="OKTA_ACCOUNT_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="okta_account_name"),
    ],
)


@register(SNOWFLAKE_DESCRIPTOR)
class SnowflakeAuthSettings(ConnectionProfile):
    __descriptor__ = SNOWFLAKE_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
