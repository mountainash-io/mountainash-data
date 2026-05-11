"""Druid backend settings.

Driver: https://github.com/druid-io/pydruid
Ibis: ``ibis.druid.connect(**kwargs)``

Druid's ibis backend accepts fully dynamic kwargs passed through to pydruid.
Core parameters are host, port, and path for the Druid broker endpoint.
"""

from __future__ import annotations

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


DRUID_DESCRIPTOR = BackendDescriptor(
    name="druid",
    provider_type=CONST_DB_PROVIDER_TYPE.DRUID,
    default_port=8082,
    connection_string_scheme="druid://",
    ibis_dialect="druid",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=8082,
                      driver_key="port"),
        ParameterSpec(name="ENDPOINT_PATH", type=str, tier="core",
                      default="/druid/v2/sql", driver_key="path"),
        ParameterSpec(name="SCHEME", type=str, tier="core",
                      default="http", driver_key="scheme"),
    ],
)


@register(DRUID_DESCRIPTOR)
class DruidAuthSettings(ConnectionProfile):
    __descriptor__ = DRUID_DESCRIPTOR
