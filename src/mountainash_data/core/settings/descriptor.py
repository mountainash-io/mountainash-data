"""Database-flavored ProfileSpec with typed metadata fields.

Retained in mountainash-data (rather than lifted to mountainash-settings)
because these fields are domain-specific: ``connection_string_scheme`` and
``ibis_dialect`` are meaningful only for SQL-like databases.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

from mountainash_settings.profiles import (
    MISSING,
    Missing,
    ParameterSpec,
    ProfileSpec,
)

__all__ = ["MISSING", "Missing", "ParameterSpec", "BackendSpec"]


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
    """

    default_port: int | None = None
    connection_string_scheme: str | None = None
    ibis_dialect: str | None = None
    rides_on: str | None = None


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
