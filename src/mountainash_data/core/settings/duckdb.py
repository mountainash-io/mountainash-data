"""DuckDB backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/duckdb.md``.
Driver: https://duckdb.org/docs/current/configuration/overview.html
Ibis: ``ibis.backends.duckdb.do_connect(database=':memory:', read_only=False,
       extensions=None, **config)``
"""

from __future__ import annotations

import re
import typing as t

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["DuckDBAuthSettings", "DUCKDB_DESCRIPTOR"]

_MEMORY_LIMIT_RE = re.compile(r"^(?:\d+(?:\.\d+)?\s*[KMG]i?B|\d+%)$", re.IGNORECASE)


def _validate_memory_limit(value: t.Any) -> t.Any:
    if value is None:
        return value
    if not _MEMORY_LIMIT_RE.match(str(value)):
        raise ValueError(
            "MEMORY_LIMIT must match e.g. '500MB', '1.5GB', '1024KiB', or '80%'"
        )
    return value


DUCKDB_DESCRIPTOR = BackendDescriptor(
    name="duckdb",
    provider_type=CONST_DB_PROVIDER_TYPE.DUCKDB,
    connection_string_scheme="duckdb://",
    ibis_dialect="duckdb",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(
            name="DATABASE",
            type=t.Optional[str],
            tier="core",
            default=None,
            driver_key="database",
            description="Path to the DuckDB file, or ':memory:' for in-memory.",
        ),
        ParameterSpec(
            name="READ_ONLY",
            type=bool,
            tier="core",
            default=False,
            driver_key="read_only",
            description="Open database in read-only mode.",
        ),
        ParameterSpec(
            name="EXTENSIONS",
            type=list[str],
            tier="core",
            default=[],
            driver_key="extensions",
            description="List of extensions to load (e.g., ['httpfs', 'json']).",
        ),
        ParameterSpec(
            name="THREADS",
            type=t.Optional[int],
            tier="advanced",
            default=None,
            description="Number of threads to use. Passed to config dict (Phase 4).",
        ),
        ParameterSpec(
            name="MEMORY_LIMIT",
            type=t.Optional[str],
            tier="advanced",
            default=None,
            validator=_validate_memory_limit,
            description="Memory limit as string: '500MB', '1.5GB', '1024KiB', or '80%'.",
        ),
    ],
)


@register(DUCKDB_DESCRIPTOR)
class DuckDBAuthSettings(ConnectionProfile):
    __descriptor__ = DUCKDB_DESCRIPTOR

    @field_validator("MEMORY_LIMIT", check_fields=False)
    @classmethod
    def _mem_limit(cls, v: t.Any) -> t.Any:
        return _validate_memory_limit(v)
