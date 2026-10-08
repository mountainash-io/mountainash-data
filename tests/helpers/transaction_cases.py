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
        with pytest.raises(RuntimeError):
            backend.get_connection().close()
    assert backend.native_transaction_open() is False
    assert backend.get_connection()._closed is False


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


# --- Remaining operation families: one owned rollback each ---------------------


def _abort_owned(backend, work) -> None:
    """Run `work()` in an owned scope, then abort; assert the abort surfaced."""
    abort = ValueError("abort owned work")
    with pytest.raises(ValueError) as caught:
        with backend.transaction():
            work()
            raise abort
    assert caught.value is abort


def _drop_any(backend, *names: str, views: tuple[str, ...] = ()) -> None:
    for name in views:
        execute(backend, f"DROP VIEW IF EXISTS {name}")
    for name in names:
        execute(backend, f"DROP TABLE IF EXISTS {name}")
    raw = backend.raw_driver_connection()
    if backend.dialect == "sqlite" or (backend.dialect == "postgres" and not raw.autocommit):
        raw.commit()


def case_table_ddl_rolls_back(backend) -> None:
    t, new = table_name(), table_name("copy")
    seed(backend, t, [(1, 10)])
    try:
        def work():
            backend.create_table(new, pl.DataFrame({"id": [3], "flag": [True]}))
            backend.create_table(new, pl.DataFrame({"id": [4], "flag": [False]}), overwrite=True)
            out = backend.run_expr(backend.table(new))
            assert [(int(i), bool(f)) for i, f in out.itertuples(index=False, name=None)] == [(4, False)]
            backend.drop_table(t)
            assert t not in backend.list_tables()

        _abort_owned(backend, work)
        assert new not in backend.list_tables()
        assert rows(backend, t) == [(1, 10)]
    finally:
        _drop_any(backend, t, new)


def case_view_ddl_rolls_back(backend) -> None:
    t, kept, created = table_name(), table_name("kept"), table_name("view")
    seed(backend, t, [(1, 10), (2, 20)])
    backend.create_view(kept, backend.table(t))
    if backend.dialect == "postgres" and not backend.raw_driver_connection().autocommit:
        backend.raw_driver_connection().commit()
    try:
        def work():
            backend.create_view(created, backend.table(t).filter(lambda r: r.v > 10))
            assert created in backend.list_tables()
            backend.drop_view(kept)
            assert kept not in backend.list_tables()

        _abort_owned(backend, work)
        tables = backend.list_tables()
        assert created not in tables and kept in tables
    finally:
        _drop_any(backend, t, views=(created, kept))


def case_insert_rolls_back(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        def work():
            backend.insert(t, pl.DataFrame({"id": [2], "v": [20]}))
            assert _expr_rows(backend, t) == [(1, 10), (2, 20)]
            backend.insert(t, pl.DataFrame({"id": [5], "v": [50]}), overwrite=True)
            assert _expr_rows(backend, t) == [(5, 50)]

        _abort_owned(backend, work)
        assert rows(backend, t) == [(1, 10)]
    finally:
        drop(backend, t)


def case_truncate_rolls_back(backend) -> None:
    if backend.dialect == "sqlite":
        pytest.skip("truncate() fails on SQLite even outside a transaction (Ibis emits "
                    "TRUNCATE); tracked separately in sqlite-truncate-unsupported")
    t = table_name()
    seed(backend, t, [(1, 10), (2, 20)])
    try:
        def work():
            backend.truncate(t)
            assert _expr_rows(backend, t) == []

        _abort_owned(backend, work)
        assert rows(backend, t) == [(1, 10), (2, 20)]
    finally:
        drop(backend, t)


def case_rename_rolls_back(backend) -> None:
    t, new = table_name(), table_name("renamed")
    seed(backend, t, [(1, 10)])
    try:
        def work():
            backend.rename_table(t, new)
            tables = backend.list_tables()
            assert new in tables and t not in tables

        _abort_owned(backend, work)
        tables = backend.list_tables()
        assert t in tables and new not in tables
    finally:
        _drop_any(backend, t, new)


def case_add_columns_rolls_back(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        def work():
            backend.add_columns(t, {"a": "int64", "b": "string"})  # two ALTER statements
            assert list(backend.table(t).schema().names) == ["id", "v", "a", "b"]

        _abort_owned(backend, work)
        assert list(backend.table(t).schema().names) == ["id", "v"]
    finally:
        drop(backend, t)


def case_index_ops_roll_back(backend, *, partial: bool) -> None:
    t, ix, ux = table_name(), table_name("ix"), table_name("ux")
    seed(backend, t, [(1, 10)])
    backend.create_index(t, ["v"], index_name=ix)
    if backend.dialect == "postgres" and not backend.raw_driver_connection().autocommit:
        backend.raw_driver_connection().commit()
    try:
        def work():
            where = (lambda r: r.v > 0) if partial else None
            backend.create_unique_index(t, ["id", "v"], index_name=ux, where=where)
            assert backend.index_exists(ux, table_name=t)
            assert ux in [i.name for i in backend.list_indexes(t)]
            backend.drop_index(ix, table_name=t)
            assert not backend.index_exists(ix, table_name=t)

        _abort_owned(backend, work)
        assert not backend.index_exists(ux, table_name=t)
        assert backend.index_exists(ix, table_name=t)
    finally:
        drop(backend, t)


def case_inspection_keeps_pending_work(backend) -> None:
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        def work():
            execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
            catalog = backend.inspect_catalog()
            assert any(t in ns.tables for ns in catalog.namespaces)
            assert [c.name for c in backend.inspect_table(t).columns] == ["id", "v"]
            assert backend.table_exists(t)

        _abort_owned(backend, work)
        assert rows(backend, t) == [(1, 10)]
    finally:
        drop(backend, t)


def case_local_rejection_does_not_poison(backend) -> None:
    """An argument rejected before any database work leaves the unit committable."""
    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        with backend.transaction():
            execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
            with pytest.raises(ValueError):
                backend.create_index(t, ["v"], index_name="bad name")
            with pytest.raises(ValueError):
                backend.rename_table(t, "bad.name")
            with pytest.raises(Exception):
                backend.add_columns(t, {"a": "not_a_real_dtype"})  # rejected before any query
        assert rows(backend, t) == [(1, 20)]
    finally:
        drop(backend, t)


def case_raw_commit_detected_at_exit(backend) -> None:
    """A raw COMMIT just before scope exit is reported; its data stays committed."""
    from mountainash_data.core.errors import TransactionIntegrityError

    t = table_name()
    seed(backend, t, [(1, 10)])
    try:
        with pytest.raises(TransactionIntegrityError):
            with backend.transaction():
                execute(backend, f"UPDATE {t} SET v = 20 WHERE id = 1")
                execute(backend, "COMMIT")
        assert rows(backend, t) == [(1, 20)]
    finally:
        drop(backend, t)
