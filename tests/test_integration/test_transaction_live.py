import os
import pytest
from mountainash_data import IbisBackend

pytestmark = pytest.mark.integration

REQUIRE = os.environ.get("MOUNTAINASH_REQUIRE_LIVE_DB") == "1"
PG_URL = os.environ.get("MOUNTAINASH_TEST_POSTGRES_URL")


def _skip_or_fail(reason):
    if REQUIRE:
        pytest.fail(reason)
    pytest.skip(reason)


def test_postgres_transaction_rollback():
    if not PG_URL:
        _skip_or_fail("MOUNTAINASH_TEST_POSTGRES_URL not set")
    with IbisBackend(PG_URL) as be:
        raw = be.raw_driver_connection()
        cur = raw.cursor()
        cur.execute("CREATE TEMP TABLE t_tx (x INT)")
        with pytest.raises(ValueError):
            with be.transaction():
                cur.execute("INSERT INTO t_tx VALUES (1)")
                raise ValueError("boom")
        cur.execute("SELECT count(*) FROM t_tx")
        assert cur.fetchone()[0] == 0
