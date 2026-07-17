"""Shared native-handle statement transport (Gap 3, fable finding 3).

The single seam for "run one statement on the raw driver handle", shared by
_transaction (BEGIN/COMMIT/ROLLBACK) and, later, _adoption (session
snapshot/restore). ``.execute()`` is NOT uniform across DBAPI drivers, so this
falls back to ``.cursor().execute()``. A per-dialect
``DialectSpec.raw_execute_hook`` overrides the write transport entirely.
"""
from __future__ import annotations

import typing as t


def raw_execute(
    handle: t.Any,
    sql: str,
    *,
    hook: t.Optional[t.Callable[[t.Any, str], None]] = None,
) -> None:
    """Execute ``sql`` on the native handle (no result).

    hook, if given, is the whole transport. Else use ``handle.execute`` when
    present (duckdb / sqlite / psycopg3 / pyodbc), otherwise a cursor
    (mysqlclient / oracledb / trino), closing the cursor afterward.
    """
    if hook is not None:
        hook(handle, sql)
        return
    execute = getattr(handle, "execute", None)
    if callable(execute):
        execute(sql)
        return
    cur = handle.cursor()
    try:
        cur.execute(sql)
    finally:
        close = getattr(cur, "close", None)
        if callable(close):
            close()


def raw_fetch_scalar(
    handle: t.Any,
    sql: str,
    *,
    hook: t.Optional[t.Callable[[t.Any, str], None]] = None,
) -> t.Any:
    """Run ``sql`` and return the first column of the first row, or ``None``.

    Same execute-or-cursor transport as :func:`raw_execute`. A void ``hook``
    cannot return rows, so reads always go through the direct execute/cursor
    path; ``hook`` is accepted for signature symmetry and ignored for the
    fetch (no dialect sets ``raw_execute_hook`` today).
    """
    execute = getattr(handle, "execute", None)
    if callable(execute):
        result = execute(sql)
        fetchone = getattr(result, "fetchone", None)
        if callable(fetchone):
            row = fetchone()
            return row[0] if row else None
        # A handle with a callable .execute has already run the SQL once;
        # falling through to the cursor path would re-execute it. If the
        # result has no fetchone, there is nothing more to try.
        return None
    cur = handle.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        close = getattr(cur, "close", None)
        if callable(close):
            close()
