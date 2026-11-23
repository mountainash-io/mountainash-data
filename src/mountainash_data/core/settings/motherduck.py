"""MotherDuck backend settings.

Spec: audit report ``mountainash-central/04.planning/mountainash-data/superpowers/specs/2026-04-15-settings-audit/motherduck.md``.
Driver auth docs:
  https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/
Ibis: routes via the duckdb backend (``rides_on="duckdb"``).
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import TokenAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile, UrlParts
from .registry import register

__all__ = ["MotherDuckBackendProfile", "MOTHERDUCK_SPEC"]


MOTHERDUCK_SPEC = BackendSpec(
    name="motherduck",
    provider_type=CONST_DB_PROVIDER_TYPE.MOTHERDUCK,
    connection_string_scheme="duckdb://md:",  # duckdb://md:<db>?motherduck_token=...
    ibis_dialect="motherduck",
    rides_on="duckdb",
    supported_auth=(TokenAuthProfile,),
    parameters=[
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="READ_ONLY", type=bool, tier="core", default=False,
                      driver_key="read_only"),
    ],
)


@register
class MotherDuckBackendProfile(BackendProfile):
    __spec__ = MOTHERDUCK_SPEC

    def to_url_parts(self) -> UrlParts:
        # Ibis accepts only duckdb://md:<db>; bare "md:<db>" is rejected.
        # Authority-less form: "duckdb:" + "//md:<db>".
        return UrlParts(
            scheme="duckdb",
            host=None,
            database=f"//md:{getattr(self, 'DATABASE', None) or ''}",
        )
