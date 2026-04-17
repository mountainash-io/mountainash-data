"""MotherDuck backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/motherduck.md``.
Driver auth docs:
  https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/
Ibis: routes via the duckdb backend (``rides_on="duckdb"``).
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import TokenAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["MotherDuckAuthSettings", "MOTHERDUCK_DESCRIPTOR"]


MOTHERDUCK_DESCRIPTOR = BackendDescriptor(
    name="motherduck",
    provider_type=CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
    connection_string_scheme="duckdb://md:",  # md:<db>?motherduck_token=...
    ibis_dialect="duckdb",
    rides_on="duckdb",
    auth_modes=[TokenAuth],
    parameters=[
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None),
        ParameterSpec(name="READ_ONLY", type=bool, tier="core", default=False,
                      driver_key="read_only"),
    ],
)


@register(MOTHERDUCK_DESCRIPTOR)
class MotherDuckAuthSettings(ConnectionProfile):
    __descriptor__ = MOTHERDUCK_DESCRIPTOR
