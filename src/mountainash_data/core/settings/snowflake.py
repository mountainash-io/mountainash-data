"""Snowflake backend settings.

Spec: ``mountainash-central/04.planning/mountainash-data/superpowers/specs/2026-04-15-settings-audit/snowflake.md``.
Driver: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import CertificateAuthProfile, OAuth2AuthProfile, PasswordAuthProfile, TokenAuthProfile
from .adapters import snowflake as _snow
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register


class SnowflakeAuthenticator(StrEnum):
    SNOWFLAKE = "snowflake"
    OAUTH = "oauth"
    OKTA = "okta"
    EXTERNAL_BROWSER = "externalbrowser"
    PASSWORD_MFA = "username_password_mfa"


SNOWFLAKE_SPEC = BackendSpec(
    name="snowflake",
    provider_type=CONST_DB_PROVIDER_TYPE.SNOWFLAKE,
    connection_string_scheme="snowflake://",
    ibis_dialect="snowflake",
    supported_auth=(PasswordAuthProfile, OAuth2AuthProfile, CertificateAuthProfile, TokenAuthProfile),
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
                      default=None, driver_key="authenticator",
                      transform=lambda p: str(p)),
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


@register
class SnowflakeBackendProfile(BackendProfile):
    __spec__ = SNOWFLAKE_SPEC
    __adapters__ = {CONST_DB_PROVIDER_TYPE.SNOWFLAKE: _snow.session_params}
