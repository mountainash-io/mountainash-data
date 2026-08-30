"""Immutable database resource-provider boundary values."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from mountainash_resource_provider import RedactedValue


class DatabaseConnectionMode(StrEnum):
    SETTINGS = "settings"
    RESOURCE_URL = "resource_url"


@dataclass(frozen=True)
class SensitiveDatabaseUrl(RedactedValue):
    """A direct database URL that never exposes its credentials in diagnostics."""


@dataclass(frozen=True)
class DatabaseConnectionParameters:
    backend: str
    mode: DatabaseConnectionMode
    resource_url: SensitiveDatabaseUrl | None = None
