"""Explicit transactions on real SQLite and DuckDB through the public API."""

from __future__ import annotations

import polars as pl
import pytest

from mountainash_data import IbisBackend
from tests.helpers import transaction_cases as cases

pytestmark = pytest.mark.unit


@pytest.fixture(params=["sqlite", "duckdb"])
def backend(request):
    b = IbisBackend(dialect=request.param, database=":memory:").connect()
    try:
        yield b
    finally:
        if b.in_transaction():
            pytest.fail("test left a registered transaction scope open")
        b.close()


def test_predicate_tracks_caller(backend):
    cases.case_predicate_tracks_caller(backend)


def test_predicate_false_when_unconnected_or_closed():
    b = IbisBackend(dialect="sqlite", database=":memory:")
    assert b.native_transaction_open() is False
    b.connect().close()
    assert b.native_transaction_open() is False


def test_owned_commit_and_rollback(backend):
    cases.case_owned_commit_and_rollback(backend)


@pytest.mark.parametrize("completion", ["COMMIT", "ROLLBACK"])
def test_joined_scope_leaves_completion_to_caller(backend, completion):
    cases.case_joined_scope_leaves_completion_to_caller(backend, completion)


def test_two_wrappers_share_one_scope(backend):
    other = IbisBackend.from_ibis_connection(backend.ibis_connection(), dialect=backend.dialect)
    cases.case_two_wrappers_share_one_scope(backend, other)


def test_close_inside_scope_refused(backend):
    cases.case_close_inside_scope_refused(backend)


# --- Protected package calls --------------------------------------------------


def test_owned_upsert_read_rolls_back(backend):
    cases.case_owned_upsert_read_rolls_back(backend)


@pytest.mark.parametrize("completion", ["COMMIT", "ROLLBACK"])
def test_joined_upsert_leaves_completion_to_caller(backend, completion):
    cases.case_joined_upsert_leaves_completion_to_caller(backend, completion)


def test_schema_inference_keeps_pending_work(backend):
    cases.case_schema_inference_keeps_pending_work(backend)


def test_caught_operation_failure_poisons_scope(backend):
    cases.case_caught_operation_failure_poisons_scope(backend)


def test_other_wrapper_operation_joins_scope(backend):
    other = IbisBackend.from_ibis_connection(backend.ibis_connection(), dialect=backend.dialect)
    cases.case_other_wrapper_operation_joins_scope(backend, other)


def test_metadata_failure_propagates_inside_scope_only():
    """A denied metadata query is an error inside a scope, not an empty result."""
    import sqlite3

    from mountainash_data.core.errors import TransactionPoisonedError

    b = IbisBackend(dialect="sqlite", database=":memory:").connect()
    raw = b.raw_driver_connection()
    t = cases.table_name()
    cases.seed(b, t, [(1, 10)])

    def deny_master(action, arg1, *_):
        # Ibis lists SQLite tables through the table_list pragma.
        if action == sqlite3.SQLITE_PRAGMA and arg1 == "table_list":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    try:
        raw.set_authorizer(deny_master)
        assert b.list_tables() == []  # standalone: existing fallback unchanged
        raw.set_authorizer(None)

        with pytest.raises(TransactionPoisonedError):
            with b.transaction():
                cases.execute(b, f"UPDATE {t} SET v = 20 WHERE id = 1")
                raw.set_authorizer(deny_master)
                try:
                    with pytest.raises(sqlite3.DatabaseError):
                        b.list_tables()
                finally:
                    raw.set_authorizer(None)
        assert cases.rows(b, t) == [(1, 10)]
    finally:
        raw.set_authorizer(None)
        b.close()


# --- Remaining operation families ---------------------------------------------


@pytest.mark.parametrize("case", [
    cases.case_table_ddl_rolls_back,
    cases.case_view_ddl_rolls_back,
    cases.case_insert_rolls_back,
    cases.case_truncate_rolls_back,
    cases.case_rename_rolls_back,
    cases.case_add_columns_rolls_back,
    cases.case_inspection_keeps_pending_work,
    cases.case_local_rejection_does_not_poison,
    cases.case_raw_commit_detected_at_exit,
], ids=lambda f: f.__name__.removeprefix("case_"))
def test_operation_family(backend, case):
    case(backend)


def test_index_ops_roll_back(backend):
    cases.case_index_ops_roll_back(backend, partial=backend.dialect == "sqlite")


def test_sqlite_conflict_rollback_then_drop_is_refused():
    """ON CONFLICT ROLLBACK ends SQLite's transaction by itself. Without the
    poisoned-scope check, a later DROP would run outside any transaction and
    persist."""
    from mountainash_data.core.errors import TransactionPoisonedError

    b = IbisBackend(dialect="sqlite", database=":memory:").connect()
    t, keep = cases.table_name(), cases.table_name("keep")
    cases.execute(b, f"CREATE TABLE {t} (id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK, v INTEGER)")
    cases.execute(b, f"INSERT INTO {t} VALUES (1, 10)")
    cases.seed(b, keep, [(1, 1)])
    try:
        with pytest.raises(TransactionPoisonedError):
            with b.transaction():
                try:
                    b.insert(t, pl.DataFrame({"id": [1], "v": [99]}))
                except TransactionPoisonedError:
                    raise
                except Exception:
                    pass  # constraint error; SQLite has already rolled back
                with pytest.raises(TransactionPoisonedError):
                    b.drop_table(keep)
        assert keep in b.list_tables()
    finally:
        b.close()
