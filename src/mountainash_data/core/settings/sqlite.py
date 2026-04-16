"""SQLite backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/sqlite.md``.
Driver: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
Ibis: ``ibis.backends.sqlite.do_connect(database, type_map=None)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from .auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["SQLiteAuthSettings", "SQLITE_DESCRIPTOR"]


SQLITE_DESCRIPTOR = BackendDescriptor(
    name="sqlite",
    provider_type=CONST_DB_PROVIDER_TYPE.SQLITE,
    connection_string_scheme="sqlite://",
    ibis_dialect="sqlite",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(
            name="DATABASE",
            type=t.Optional[str],
            tier="core",
            default=None,
            driver_key="database",
            description="Path to the SQLite file, or ':memory:' for in-memory.",
        ),
        ParameterSpec(
            name="TYPE_MAP",
            type=t.Optional[dict[str, t.Any]],
            tier="advanced",
            default=None,
            driver_key="type_map",
            description="Optional SQLite column-type → Ibis dtype overrides.",
        ),
    ],
)


@register(SQLITE_DESCRIPTOR)
class SQLiteAuthSettings(ConnectionProfile):
    __descriptor__ = SQLITE_DESCRIPTOR
