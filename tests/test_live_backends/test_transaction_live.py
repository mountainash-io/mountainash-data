"""Explicit transactions on a live PostgreSQL target.

Every test is named test_postgres_*: the live-db harness selects the
PostgreSQL suite with ``-k postgres``. Shared cases live in
tests/helpers/transaction_cases.py and also run on SQLite and DuckDB.
"""

import pytest

from mountainash_data.core.errors import TransactionIntegrityError
from tests.helpers import transaction_cases as cases

pytestmark = pytest.mark.integration


@pytest.fixture(params=[True, False], ids=["autocommit", "no_autocommit"])
def pg(request, postgres_backend):
    """postgres_backend with the driver's autocommit set for the test.

    Asserts that no transaction() scope changed the setting, then restores it.
    """
    raw = postgres_backend.raw_driver_connection()
    original = raw.autocommit
    raw.autocommit = request.param
    try:
        yield postgres_backend
        assert raw.autocommit is request.param, "transaction() changed autocommit"
    finally:
        if raw.info.transaction_status != 0:
            raw.rollback()
        raw.autocommit = original


def test_postgres_transaction_rollback(postgres_backend):
    """A transaction() that raises rolls the whole unit back."""
    be = postgres_backend
    raw = be.raw_driver_connection()
    cur = raw.cursor()
    cur.execute("CREATE TEMP TABLE t_tx (x INT)")
    with pytest.raises(ValueError):
        with be.transaction():
            cur.execute("INSERT INTO t_tx VALUES (1)")
            raise ValueError("boom")
    cur.execute("SELECT count(*) FROM t_tx")
    assert cur.fetchone()[0] == 0


def test_postgres_predicate_tracks_caller(pg):
    cases.case_predicate_tracks_caller(pg)


def test_postgres_owned_commit_and_rollback(pg):
    cases.case_owned_commit_and_rollback(pg)


@pytest.mark.parametrize("completion", ["COMMIT", "ROLLBACK"])
def test_postgres_joined_scope_leaves_completion_to_caller(pg, completion):
    cases.case_joined_scope_leaves_completion_to_caller(pg, completion)


def test_postgres_close_inside_scope_refused(pg):
    cases.case_close_inside_scope_refused(pg)


def test_postgres_owned_upsert_read_rolls_back(pg):
    cases.case_owned_upsert_read_rolls_back(pg)


@pytest.mark.parametrize("completion", ["COMMIT", "ROLLBACK"])
def test_postgres_joined_upsert_leaves_completion_to_caller(pg, completion):
    cases.case_joined_upsert_leaves_completion_to_caller(pg, completion)


def test_postgres_schema_inference_keeps_pending_work(pg):
    cases.case_schema_inference_keeps_pending_work(pg)


def test_postgres_caught_operation_failure_poisons_scope(pg):
    cases.case_caught_operation_failure_poisons_scope(pg)


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
def test_postgres_operation_family(pg, case):
    case(pg)


def test_postgres_index_ops_roll_back(pg):
    cases.case_index_ops_roll_back(pg, partial=True)


def test_postgres_joined_inerror_is_reported_not_rolled_back(pg):
    """A caller transaction already aborted (INERROR) is joined but never
    repaired or rolled back; clean exit reports it as not committable."""
    raw = pg.raw_driver_connection()
    cases.execute(pg, "BEGIN")
    with pytest.raises(Exception):
        cases.execute(pg, "SELECT 1/0")
    assert pg.native_transaction_open() is True  # INERROR still counts as open
    with pytest.raises(TransactionIntegrityError):
        with pg.transaction():
            pass
    assert raw.info.transaction_status.name == "INERROR"  # caller still owns it
    raw.rollback()


def test_postgres_owned_inerror_is_rolled_back_at_exit(pg):
    """A raw statement error caught inside an owned scope leaves PostgreSQL
    aborted without poisoning the scope; exit must roll back, not commit."""
    t = cases.table_name()
    cases.seed(pg, t, [(1, 10)])
    try:
        with pytest.raises(TransactionIntegrityError):
            with pg.transaction():
                cases.execute(pg, f"UPDATE {t} SET v = 20 WHERE id = 1")
                try:
                    cases.execute(pg, "SELECT 1/0")
                except Exception:
                    pass
        assert pg.native_transaction_open() is False
        assert cases.rows(pg, t) == [(1, 10)]
    finally:
        cases.drop(pg, t)
