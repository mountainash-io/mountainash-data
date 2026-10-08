"""Shared real-database helpers and cases for explicit-transaction tests.

Every case takes a connected IbisBackend (SQLite, DuckDB or PostgreSQL) and
works through the public API plus the native driver for caller-side setup.
Table names are generated so cases can share a live server database.
"""

from __future__ import annotations

import uuid

import polars as pl
import pytest

from mountainash_data.core.errors import TransactionPoisonedError


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


# --- Protected package calls --------------------------------------------------


def _expr_rows(backend, t: str) -> list[tuple]:
    out = backend.run_expr(backend.table(t).order_by("id"))
    return [tuple(int(x) for x in r) for r in out.itertuples(index=False, name=None)]


def case_owned_upsert_read_rolls_back(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        abort = ValueError("abort owned work")
        with pytest.raises(ValueError) as caught:
            with backend.transaction():
                backend.upsert(t, pl.DataFrame({"id": [1, 2], "v": [20, 30]}), conflict_columns=["id"])
                assert _expr_rows(backend, t) == [(1, 20), (2, 30)]
                raise abort
        assert caught.value is abort
        assert rows(backend, t) == [(1, 10)]

        with backend.transaction():
            backend.upsert(t, pl.DataFrame({"id": [1], "v": [40]}), conflict_columns=["id"])
        assert rows(backend, t) == [(1, 40)]
    finally:
        drop(backend, t)


def case_joined_upsert_leaves_completion_to_caller(backend, completion: str) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        execute(backend, "BEGIN")
        with backend.transaction():
            backend.upsert(t, pl.DataFrame({"id": [1, 2], "v": [20, 30]}), conflict_columns=["id"])
            assert _expr_rows(backend, t) == [(1, 20), (2, 30)]
        assert backend.native_transaction_open() is True
        execute(backend, completion)
        expected = [(1, 20), (2, 30)] if completion == "COMMIT" else [(1, 10)]
        assert rows(backend, t) == expected
    finally:
        drop(backend, t)


def case_schema_inference_keeps_pending_work(backend) -> None:
    """run_sql infers a schema by querying; that must not commit pending work."""
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        abort = ValueError("abort")
        with pytest.raises(ValueError) as caught:
            with backend.transaction():
                execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
                expr = backend.run_sql(f"SELECT id, v FROM {t}")
                assert list(expr.columns) == ["id", "v"]
                raise abort
        assert caught.value is abort
        assert rows(backend, t) == [(1, 10)]
    finally:
        drop(backend, t)


def case_caught_operation_failure_poisons_scope(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        with pytest.raises(TransactionPoisonedError):
            with backend.transaction():
                execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
                try:
                    backend.upsert(t, pl.DataFrame({"id": [1], "v": [1]}), conflict_columns=["no_such_column"])
                except TransactionPoisonedError:
                    raise
                except Exception:
                    pass  # caller swallows the failure; the scope must not commit
                with pytest.raises(TransactionPoisonedError):
                    backend.run_sql(f"SELECT id, v FROM {t}")  # rejected before any query
        assert rows(backend, t) == [(1, 10)]
    finally:
        drop(backend, t)


def case_other_wrapper_operation_joins_scope(backend, other) -> None:
    """`other` wraps the same native handle; its operations join backend's unit."""
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        abort = ValueError("abort")
        with pytest.raises(ValueError) as caught:
            with backend.transaction():
                other.upsert(t, pl.DataFrame({"id": [1], "v": [20]}), conflict_columns=["id"])
                assert _expr_rows(other, t) == [(1, 20)]
                raise abort
        assert caught.value is abort
        assert rows(backend, t) == [(1, 10)]
    finally:
        drop(backend, t)
