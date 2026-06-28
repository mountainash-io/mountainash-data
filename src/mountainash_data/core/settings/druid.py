"""Druid backend settings.

Driver: https://github.com/druid-io/pydruid
Ibis: ``ibis.druid.connect(**kwargs)``

Druid's ibis backend accepts fully dynamic kwargs passed through to pydruid.
Core parameters are host, port, and path for the Druid broker endpoint.
"""

from __future__ import annotations

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register


DRUID_SPEC = BackendSpec(
    name="druid",
    provider_type=CONST_DB_PROVIDER_TYPE.DRUID,
    default_port=8082,
    connection_string_scheme="druid://",
    ibis_dialect="druid",
    supported_auth=(PasswordAuthProfile, NoAuthProfile),
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


@register
class DruidBackendProfile(BackendProfile):
    __spec__ = DRUID_SPEC
