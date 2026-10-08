"""Shared real-database helpers and cases for explicit-transaction tests.

Every case takes a connected IbisBackend (SQLite, DuckDB or PostgreSQL) and
works through the public API plus the native driver for caller-side setup.
Table names are generated so cases can share a live server database.
"""

from __future__ import annotations

import uuid

import pytest


def table_name(prefix: str = "tx") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def execute(backend, sql: str) -> None:
    """Run one statement on the native handle as the caller would."""
    raw = backend.raw_driver_connection()
    if backend.dialect == "duckdb":
        raw.execute(sql)  # returns the connection itself; never close it
        return
    cur = raw.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


def rows(backend, table: str) -> list[tuple]:
    """Read (id, v) rows without leaving a transaction the read itself opened.

    PostgreSQL with autocommit off begins implicitly on any statement; if the
    connection was idle before the read, end that implicit transaction so the
    next transaction() sees an idle connection. A caller's open transaction is
    left untouched.
    """
    raw = backend.raw_driver_connection()
    sql = f"SELECT id, v FROM {table} ORDER BY id"
    if backend.dialect == "duckdb":
        return [tuple(r) for r in raw.execute(sql).fetchall()]
    implicit = backend.dialect == "postgres" and not raw.autocommit and backend.native_transaction_open() is False
    cur = raw.cursor()
    try:
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        cur.close()
        if implicit:
            raw.rollback()


def seed(backend, table: str, data: list[tuple[int, int]]) -> None:
    """Create and populate (id, v) outside any transaction under test."""
    execute(backend, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, v INTEGER)")
    for i, v in data:
        execute(backend, f"INSERT INTO {table} VALUES ({i}, {v})")
    raw = backend.raw_driver_connection()
    if backend.dialect == "sqlite" or (backend.dialect == "postgres" and not raw.autocommit):
        raw.commit()


def drop(backend, table: str) -> None:
    execute(backend, f"DROP TABLE IF EXISTS {table}")
    raw = backend.raw_driver_connection()
    if backend.dialect == "sqlite" or (backend.dialect == "postgres" and not raw.autocommit):
        raw.commit()


# --- Cases ------------------------------------------------------------------


def case_predicate_tracks_caller(backend) -> None:
    assert backend.native_transaction_open() is False
    execute(backend, "BEGIN")
    assert backend.native_transaction_open() is True
    execute(backend, "COMMIT")
    assert backend.native_transaction_open() is False
    execute(backend, "BEGIN")
    assert backend.native_transaction_open() is True
    execute(backend, "ROLLBACK")
    assert backend.native_transaction_open() is False


def case_owned_commit_and_rollback(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        with backend.transaction():
            execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
        assert rows(backend, t) == [(1, 20)]

        abort = ValueError("abort owned work")
        with pytest.raises(ValueError) as caught:
            with backend.transaction():
                execute(backend, f"UPDATE {t} SET v = 30 WHERE id = 1")
                raise abort
        assert caught.value is abort
        assert rows(backend, t) == [(1, 20)]
        assert backend.native_transaction_open() is False
    finally:
        drop(backend, t)


def case_joined_scope_leaves_completion_to_caller(backend, completion: str) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        execute(backend, "BEGIN")
        execute(backend, f"UPDATE {t} SET v = 11 WHERE id = 1")
        assert backend.in_transaction() is False
        with backend.transaction():
            assert backend.in_transaction() is True
            execute(backend, f"UPDATE {t} SET v = 12 WHERE id = 1")
        assert backend.in_transaction() is False
        assert backend.native_transaction_open() is True
        execute(backend, completion)
        assert rows(backend, t) == [(1, 12 if completion == "COMMIT" else 10)]
    finally:
        drop(backend, t)


def case_two_wrappers_share_one_scope(backend, other) -> None:
    """`other` wraps the same native handle as `backend`."""
    with backend.transaction():
        assert other.in_transaction() is True
        with other.transaction():  # joins; no second BEGIN
            pass
        assert backend.native_transaction_open() is True
    assert backend.native_transaction_open() is False


def case_close_inside_scope_refused(backend) -> None:
    with backend.transaction():
        with pytest.raises(RuntimeError):
            backend.close()
    assert backend.native_transaction_open() is False
