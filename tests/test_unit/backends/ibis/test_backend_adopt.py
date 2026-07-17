"""IbisBackend.from_ibis_connection — adopt an existing live ibis connection."""

from __future__ import annotations

import duckdb
import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis.backend import IbisBackend


@pytest.fixture
def raw_db():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    yield conn
    conn.close()


def _adopt(raw_db) -> IbisBackend:
    return IbisBackend.from_ibis_connection(
        ibis.duckdb.from_connection(raw_db), dialect="duckdb"
    )


def test_adopted_backend_writes_through_same_connection(raw_db):
    backend = _adopt(raw_db)
    backend.insert("t", pl.DataFrame({"id": [1], "name": ["a"]}))
    # visible on the RAW connection — proof it is the same session
    assert raw_db.execute("SELECT count(*) FROM t").fetchone()[0] == 1


def test_adopted_backend_shares_transaction_state(raw_db):
    backend = _adopt(raw_db)
    raw_db.execute("BEGIN")
    backend.insert("t", pl.DataFrame({"id": [1], "name": ["a"]}))
    raw_db.execute("ROLLBACK")
    assert raw_db.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_upsert_through_adopted_connection(raw_db):
    backend = _adopt(raw_db)
    backend.insert("t", pl.DataFrame({"id": [1], "name": ["a"]}))
    backend.upsert(
        "t", pl.DataFrame({"id": [1], "name": ["b"]}),
        conflict_columns=["id"], update_columns=["name"],
    )
    assert raw_db.execute("SELECT name FROM t WHERE id = 1").fetchone()[0] == "b"


def test_upsert_stages_full_rows_against_not_null_columns(raw_db):
    """DuckDB validates NOT NULL on the INSERT path even when ON CONFLICT
    resolves to UPDATE — consumers upserting a narrow update set must still
    stage FULL rows. This pins both sides of that contract."""
    raw_db.execute(
        "CREATE TABLE nn (k INTEGER PRIMARY KEY, flag BOOLEAN, req TEXT NOT NULL)"
    )
    backend = _adopt(raw_db)
    backend.insert("nn", pl.DataFrame({"k": [1], "flag": [True], "req": ["x"]}))

    # full-row frame + narrow update_columns: works, updates only flag
    backend.upsert(
        "nn", pl.DataFrame({"k": [1], "flag": [False], "req": ["IGNORED"]}),
        conflict_columns=["k"], update_columns=["flag"],
    )
    assert raw_db.execute("SELECT flag, req FROM nn").fetchone() == (False, "x")

    # partial-column frame: DuckDB rejects it before conflict resolution
    with pytest.raises(Exception, match="NOT NULL"):
        backend.upsert(
            "nn", pl.DataFrame({"k": [1], "flag": [True]}),
            conflict_columns=["k"], update_columns=["flag"],
        )


def test_close_leaves_unowned_connection_open(raw_db):
    backend = _adopt(raw_db)  # owns_connection defaults to False
    backend.close()
    # the caller's connection must survive
    assert raw_db.execute("SELECT 1").fetchone()[0] == 1


class _StubIbisConn:
    """Counted disconnect — proves close() actually calls it (or doesn't)."""

    def __init__(self):
        self.disconnects = 0

    def disconnect(self):
        self.disconnects += 1


def test_close_disconnects_owned_connection():
    stub = _StubIbisConn()
    backend = IbisBackend.from_ibis_connection(
        stub, dialect="duckdb", owns_connection=True,
    )
    backend.close()
    assert stub.disconnects == 1


def test_close_never_disconnects_unowned_connection():
    stub = _StubIbisConn()
    backend = IbisBackend.from_ibis_connection(stub, dialect="duckdb")
    backend.close()
    assert stub.disconnects == 0


def test_unknown_dialect_raises():
    raw = duckdb.connect(":memory:")
    with pytest.raises(KeyError):
        IbisBackend.from_ibis_connection(
            ibis.duckdb.from_connection(raw), dialect="not-a-dialect"
        )


def test_from_raw_connection_preserves_python_enable_replacements():
    raw = duckdb.connect()
    # caller's session default is True
    before = raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0]
    assert before is True
    be = IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=True)
    after = raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0]
    assert after is True  # restored despite ibis stomping it to False during adoption
    be.close()


def test_from_raw_connection_without_preserve_leaves_ibis_default():
    raw = duckdb.connect()
    be = IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=False)
    after = raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0]
    assert after is False  # ibis stomped it; we did not restore
    be.close()


def test_from_raw_connection_returns_working_backend():
    raw = duckdb.connect()
    be = IbisBackend.from_raw_connection(raw, dialect="duckdb")
    assert be.raw_driver_connection() is raw
    be.close()


def test_from_raw_connection_gated_on_unverified_dialect():
    # postgres has raw_adoption_verified=False -> clear error, not a cryptic ibis failure
    with pytest.raises(NotImplementedError, match="raw adoption not yet verified"):
        IbisBackend.from_raw_connection(object(), dialect="postgres")


def test_apply_session_options_reenables_replacements():
    raw = duckdb.connect()
    adopted = ibis.duckdb.from_connection(raw)  # ibis stomps replacements to False
    assert raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0] is False
    be = IbisBackend.from_ibis_connection(
        adopted, dialect="duckdb",
        apply_session_options={"python_enable_replacements": True},
    )
    assert raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0] is True
    be.close()


def test_from_ibis_connection_default_unchanged():
    raw = duckdb.connect()
    adopted = ibis.duckdb.from_connection(raw)
    be = IbisBackend.from_ibis_connection(adopted, dialect="duckdb")
    # no apply -> ibis default stands
    assert raw.execute("SELECT current_setting('python_enable_replacements')").fetchone()[0] is False
    be.close()


def test_in_transaction_visible_across_wrappers_from_ibis_connection(raw_db):
    # Two IbisBackends adopting ONE raw connection must agree on tx state —
    # the pointbreak F-09 scenario (guard runs on a different wrapper).
    ibis_conn = ibis.duckdb.from_connection(raw_db)
    a = IbisBackend.from_ibis_connection(ibis_conn, dialect="duckdb")
    b = IbisBackend.from_ibis_connection(ibis_conn, dialect="duckdb")
    assert a.in_transaction() is False
    assert b.in_transaction() is False
    with a.transaction():
        assert b.in_transaction() is True  # B sees A's open unit of work
    assert a.in_transaction() is False
    assert b.in_transaction() is False


def test_in_transaction_visible_across_wrappers_from_raw_connection(raw_db):
    # from_raw_connection resolves the handle differently; must still agree.
    # (duckdb has raw_adoption_verified=True.) ibis stores the passed raw conn
    # directly as .con, so both wrappers key on id(raw_db).
    a = IbisBackend.from_raw_connection(raw_db, dialect="duckdb")
    b = IbisBackend.from_raw_connection(raw_db, dialect="duckdb")
    with a.transaction():
        assert a.in_transaction() is True
        assert b.in_transaction() is True
    assert b.in_transaction() is False
