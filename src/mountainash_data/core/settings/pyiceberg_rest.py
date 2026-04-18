"""PyIceberg REST catalog backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/pyiceberg_rest.md``.
Driver: https://py.iceberg.apache.org/configuration/
"""

from __future__ import annotations

import typing as t

from pydantic import SecretStr

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import pyiceberg_rest as _adapter
from mountainash_settings.auth import OAuth2Auth, TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


PYICEBERG_REST_DESCRIPTOR = BackendDescriptor(
    name="pyiceberg_rest",
    provider_type=CONST_DB_PROVIDER_TYPE.PYICEBERG_REST,
    connection_string_scheme=None,  # uri= kwarg, not URL form
    auth_modes=[TokenAuth, OAuth2Auth],
    parameters=[
        ParameterSpec(name="CATALOG_NAME", type=str, tier="core",
                      driver_key="name"),
        ParameterSpec(name="CATALOG_URI", type=str, tier="core",
                      driver_key="uri"),
        ParameterSpec(name="WAREHOUSE", type=t.Optional[str], tier="core",
                      default=None, driver_key="warehouse"),
        ParameterSpec(name="VERIFY_SSL", type=bool, tier="advanced",
                      default=True, driver_key="verify-ssl"),
        # S3 family (adapter emits dotted keys)
        ParameterSpec(name="S3_REGION", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="S3_ENDPOINT", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="S3_ACCESS_KEY_ID", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="S3_SECRET_ACCESS_KEY",
                      type=t.Optional[SecretStr], tier="advanced",
                      default=None),
        ParameterSpec(name="S3_SESSION_TOKEN", type=t.Optional[SecretStr],
                      tier="advanced", default=None),
        # SigV4
        ParameterSpec(name="REST_SIGV4_ENABLED", type=t.Optional[bool],
                      tier="advanced", default=None),
        ParameterSpec(name="REST_SIGNING_REGION", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="REST_SIGNING_NAME", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="HEADERS", type=t.Optional[dict[str, str]],
                      tier="advanced", default=None),
    ],
)


@register(PYICEBERG_REST_DESCRIPTOR)
class PyIcebergRestAuthSettings(ConnectionProfile):
    __descriptor__ = PYICEBERG_REST_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
