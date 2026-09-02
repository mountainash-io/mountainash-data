"""Compatibility shims for Ibis's Exasol backend.

Ibis 12 exposes only the private ``_safe_raw_sql`` context manager on its
Exasol backend. Mountainash's generic rename, schema-evolution, and MERGE
operations use the public ``raw_sql`` boundary available on the other SQL
backends. Adapt the existing committed execution path instead of duplicating
PyExasol transaction handling.
"""
from __future__ import annotations

import typing as t


def patch_exasol_connection(con: t.Any) -> t.Any:
    """Expose Ibis Exasol's committed raw-SQL path through ``raw_sql``."""
    if getattr(con, "_mountainash_exasol_patched", False):
        return con

    def raw_sql(query: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
        with con._safe_raw_sql(query, *args, **kwargs) as result:
            return result

    con.raw_sql = raw_sql
    con._mountainash_exasol_patched = True
    return con
