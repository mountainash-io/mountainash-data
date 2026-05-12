"""RisingWave backend settings.

Driver: https://docs.risingwave.com/docs/current/install-psycopg2/
Ibis: ``ibis.risingwave.connect(host, user, password, port, database,
       schema)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


RISINGWAVE_DESCRIPTOR = BackendDescriptor(
    name="risingwave",
    provider_type=CONST_DB_PROVIDER_TYPE.RISINGWAVE,
    default_port=5432,
    connection_string_scheme="risingwave://",
    ibis_dialect="risingwave",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=5432,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
    ],
)


@register(RISINGWAVE_DESCRIPTOR)
class RisingWaveAuthSettings(ConnectionProfile):
    __descriptor__ = RISINGWAVE_DESCRIPTOR
