import pytest

pytestmark = pytest.mark.integration


def test_postgres_transaction_rollback(postgres_backend):
    """A transaction() that raises must roll the whole unit of work back on a
    real postgres connection. Uses the shared postgres_backend fixture, which
    reads the explicit MOUNTAINASH_LIVE_DB_TARGET/_BACKEND/_CONFIG identity the
    `python -m scripts.live_db` runner injects (see tests/fixtures/live_db_fixtures.py)
    and honours MOUNTAINASH_REQUIRE_LIVE_DB=1 by fail-closing only when the
    service is genuinely unreachable — the same convention as every other live
    test."""
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
