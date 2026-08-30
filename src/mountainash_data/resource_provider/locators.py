"""Credential-free database locator normalization."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class UrlDatabaseLocatorIdentity:
    backend: str
    host: str | None
    port: int | None
    database: str | None


_SCHEME_ALIASES = {"postgres": "postgresql"}
_DEFAULT_PORTS = {"postgresql": 5432, "mysql": 3306, "mssql": 1433}


def normalize_database_url(raw: str) -> UrlDatabaseLocatorIdentity:
    parsed = urlsplit(raw)
    backend = _SCHEME_ALIASES.get(parsed.scheme.casefold(), parsed.scheme.casefold())
    host = parsed.hostname.casefold() if parsed.hostname else None
    port = parsed.port
    if port == _DEFAULT_PORTS.get(backend):
        port = None
    database = parsed.path.lstrip("/") or None
    return UrlDatabaseLocatorIdentity(backend, host, port, database)
