"""Exasol backend settings.

Driver: https://github.com/exasol/pyexasol
Ibis: ``ibis.exasol.connect(user, password, host, port, timezone,
       websocket_sslopt, **kwargs)``
"""

from __future__ import annotations

from dataclasses import replace
from urllib.parse import quote

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import PasswordAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile, UrlParts
from .registry import register


EXASOL_SPEC = BackendSpec(
    name="exasol",
    provider_type=CONST_DB_PROVIDER_TYPE.EXASOL,
    default_port=8563,
    connection_string_scheme="exasol://",
    ibis_dialect="exasol",
    supported_auth=(PasswordAuthProfile,),
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=8563,
                      driver_key="port"),
        ParameterSpec(name="SCHEMA", type=str | None, tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="TIMEZONE", type=str, tier="core",
                      default="UTC", driver_key="timezone"),
    ],
)


@register
class ExasolBackendProfile(BackendProfile):
    __spec__ = EXASOL_SPEC

    def to_url_parts(self) -> UrlParts:
        # Ibis exasol _from_url: exasol://u:p@host:port/<schema>
        base = super().to_url_parts()
        schema = getattr(self, "SCHEMA", None)
        return replace(base, database=None, path="/" + quote(str(schema), safe="") if schema else None)
