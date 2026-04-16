"""Trino backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/trino.md``.
Driver: https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py
Ibis: ``ibis.backends.trino.do_connect``
"""

from __future__ import annotations

import typing as t
from pathlib import Path

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import trino as _adapter
from .auth import JWTAuth, KerberosAuth, NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


TRINO_DESCRIPTOR = BackendDescriptor(
    name="trino",
    provider_type=CONST_DB_PROVIDER_TYPE.TRINO,
    default_port=8080,
    connection_string_scheme="trino://",
    ibis_dialect="trino",
    auth_modes=[PasswordAuth, JWTAuth, KerberosAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=8080,
                      driver_key="port"),
        ParameterSpec(name="CATALOG", type=str, tier="core", driver_key="catalog"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="HTTP_SCHEME", type=str, tier="core",
                      default="https", driver_key="http_scheme"),
        ParameterSpec(name="VERIFY", type=t.Optional[t.Union[bool, Path]],
                      tier="core", default=True, driver_key="verify",
                      transform=lambda v: str(v) if isinstance(v, Path) else v),
        ParameterSpec(name="SOURCE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="source"),
        ParameterSpec(name="TIMEZONE", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="timezone"),
        ParameterSpec(name="MAX_ATTEMPTS", type=t.Optional[int],
                      tier="advanced", default=None,
                      driver_key="max_attempts"),
        ParameterSpec(name="REQUEST_TIMEOUT", type=t.Optional[float],
                      tier="advanced", default=None,
                      driver_key="request_timeout"),
        ParameterSpec(name="SESSION_PROPERTIES",
                      type=t.Optional[dict[str, str]], tier="advanced",
                      default=None, driver_key="session_properties"),
        ParameterSpec(name="HTTP_HEADERS", type=t.Optional[dict[str, str]],
                      tier="advanced", default=None,
                      driver_key="http_headers"),
        ParameterSpec(name="EXTRA_CREDENTIAL",
                      type=t.Optional[list[tuple[str, str]]], tier="advanced",
                      default=None, driver_key="extra_credential"),
        ParameterSpec(name="CLIENT_TAGS", type=t.Optional[list[str]],
                      tier="advanced", default=None,
                      driver_key="client_tags"),
        ParameterSpec(name="ROLES",
                      type=t.Optional[t.Union[dict[str, str], str]],
                      tier="advanced", default=None, driver_key="roles"),
        ParameterSpec(name="LEGACY_PRIMITIVE_TYPES", type=t.Optional[bool],
                      tier="advanced", default=None,
                      driver_key="legacy_primitive_types"),
        ParameterSpec(name="LEGACY_PREPARED_STATEMENTS",
                      type=t.Optional[bool], tier="advanced",
                      default=None,
                      driver_key="legacy_prepared_statements"),
        ParameterSpec(name="ENCODING", type=t.Optional[t.Union[str, list[str]]],
                      tier="advanced", default=None, driver_key="encoding"),
    ],
)


@register(TRINO_DESCRIPTOR)
class TrinoAuthSettings(ConnectionProfile):
    __descriptor__ = TRINO_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
