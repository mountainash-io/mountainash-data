"""Database-flavored ProfileSpec with typed metadata fields.

Retained in mountainash-data (rather than lifted to mountainash-settings)
because these fields are domain-specific: ``connection_string_scheme`` and
``ibis_dialect`` are meaningful only for SQL-like databases.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import StrEnum

from mountainash_settings.profiles import (
    MISSING,
    Missing,
    ParameterSpec,
    ProfileSpec,
)

__all__ = [
    "MISSING",
    "Missing",
    "ParameterSpec",
    "BackendSpec",
    "DatabaseResourceReadDisposition",
    "LocatorProjectionPhase",
]


class DatabaseResourceReadDisposition(StrEnum):
    """Credential-safe connection mode for a database resource read."""

    SETTINGS_URL = "settings_url"
    CONNECTED_IDENTITY = "connected_identity"
    OVERRIDE_ONLY = "override_only"


class LocatorProjectionPhase(StrEnum):
    """When a resource locator can be compared with a connected identity."""

    BEFORE_CONNECT = "before_connect"
    AFTER_CONNECT = "after_connect"


@dataclass(frozen=True, kw_only=True)
class BackendSpec(ProfileSpec):
    """ProfileSpec with database-specific typed metadata.

    Extra fields:
        default_port: Default TCP port if the backend listens on one.
        connection_string_scheme: URL scheme prefix (``"postgresql://"``) or
            ``None`` if the backend has no URL form.
        ibis_dialect: Name of the Ibis backend if Ibis handles this backend.
        rides_on: Name of another backend whose Ibis path this one routes
            through (e.g. ``motherduck`` -> ``duckdb``). Metadata only.
        supported_auth: Tuple of AuthProfile types this backend accepts.
    """

    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None
    supported_auth: tuple[type, ...] = ()
    resource_read_disposition: DatabaseResourceReadDisposition | None = None
    resource_read_locator_prefixes: tuple[str, ...] = ()
    resource_read_projection_phase: LocatorProjectionPhase | None = None
    resource_read_override_reason: str | None = None
    resource_read_override_date: str | None = None

    def __post_init__(self) -> None:
        if not self.supported_auth:
            raise ValueError(f"{self.name}: supported_auth must be non-empty")
        if self.resource_read_disposition is None:
            if self.name == "pyspark":
                disposition = DatabaseResourceReadDisposition.CONNECTED_IDENTITY
                prefixes = ("pyspark:///",)
                phase = LocatorProjectionPhase.AFTER_CONNECT
                reason = None
                date = None
            elif self.name == "databricks":
                disposition = DatabaseResourceReadDisposition.OVERRIDE_ONLY
                prefixes = ()
                phase = None
                reason = "no approved credential-free locator identity"
                date = "2026-08-29"
            elif self.connection_string_scheme is not None:
                disposition = DatabaseResourceReadDisposition.SETTINGS_URL
                prefixes = (self.connection_string_scheme,)
                phase = LocatorProjectionPhase.BEFORE_CONNECT
                reason = None
                date = None
            else:
                disposition = DatabaseResourceReadDisposition.OVERRIDE_ONLY
                prefixes = ()
                phase = None
                reason = "backend has no approved resource locator"
                date = "2026-08-29"
            object.__setattr__(self, "resource_read_disposition", disposition)
            object.__setattr__(self, "resource_read_locator_prefixes", prefixes)
            object.__setattr__(self, "resource_read_projection_phase", phase)
            object.__setattr__(self, "resource_read_override_reason", reason)
            object.__setattr__(self, "resource_read_override_date", date)


_DEPRECATED = {
    "BackendDescriptor": ("BackendSpec", BackendSpec),
    "_Missing":          ("Missing", Missing),
}


def __getattr__(name: str):
    if name in _DEPRECATED:
        new_name, obj = _DEPRECATED[name]
        warnings.warn(
            f"{name!r} is renamed to {new_name!r} in mountainash-data. "
            f"Update imports to use the new name.",
            DeprecationWarning, stacklevel=2,
        )
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
