"""Exasol backend settings.

Driver: https://github.com/exasol/pyexasol
Ibis: ``ibis.exasol.connect(user, password, host, port, timezone,
       websocket_sslopt, **kwargs)``
"""

from __future__ import annotations

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


EXASOL_DESCRIPTOR = BackendDescriptor(
    name="exasol",
    provider_type=CONST_DB_PROVIDER_TYPE.EXASOL,
    default_port=8563,
    connection_string_scheme="exasol://",
    ibis_dialect="exasol",
    auth_modes=[PasswordAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=8563,
                      driver_key="port"),
        ParameterSpec(name="TIMEZONE", type=str, tier="core",
                      default="UTC", driver_key="timezone"),
    ],
)


@register(EXASOL_DESCRIPTOR)
class ExasolAuthSettings(ConnectionProfile):
    __descriptor__ = EXASOL_DESCRIPTOR
