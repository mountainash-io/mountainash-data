"""Database-flavored ProfileDescriptor with typed metadata fields.

Retained in mountainash-data (rather than lifted to mountainash-settings)
because these fields are domain-specific: ``connection_string_scheme`` and
``ibis_dialect`` are meaningful only for SQL-like databases.
"""

from __future__ import annotations

from dataclasses import dataclass

from mountainash_settings.profiles import (
    MISSING,
    ParameterSpec,
    ProfileDescriptor,
)
from mountainash_settings.profiles.descriptor import _Missing

__all__ = ["MISSING", "_Missing", "BackendDescriptor", "ParameterSpec"]


@dataclass(frozen=True, kw_only=True)
class BackendDescriptor(ProfileDescriptor):
    """ProfileDescriptor with database-specific typed metadata.

    Extra fields:
        default_port: Default TCP port if the backend listens on one.
        connection_string_scheme: URL scheme prefix (``"postgresql://"``) or
            ``None`` if the backend has no URL form.
        ibis_dialect: Name of the Ibis backend if Ibis handles this backend.
        rides_on: Name of another backend whose Ibis path this one routes
            through (e.g. ``motherduck`` -> ``duckdb``). Metadata only.
    """

    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None
