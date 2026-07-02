"""Generic index dispatchers, exercised on in-memory sqlite/duckdb."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
from mountainash_data.backends.ibis.dialects._registry import DIALECTS

_SQLITE = DIALECTS["sqlite"].index_caps
_SQLITE_FN = DIALECTS["sqlite"].get_index_exists_sql
_DUCKDB = DIALECTS["duckdb"].index_caps
_DUCKDB_FN = DIALECTS["duckdb"].get_index_exists_sql


def _seed_sqlite():
    con = ibis.sqlite.connect()
    con.create_table("t", pl.DataFrame({"id": [1, 2], "active": [True, False]}))
    return con


class TestCreateDropExistsRoundtrip:
    def test_create_then_exists_then_drop(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_t_id", caps=_SQLITE,
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True
        _generic_drop_index(con, "idx_t_id", table_name="t", caps=_SQLITE,
                            exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is False

    def test_create_if_not_exists_is_idempotent_native(self):
        con = _seed_sqlite()
        for _ in range(2):
            _generic_create_index(
                con, "t", ["id"], index_name="idx_t_id", if_not_exists=True,
                caps=_SQLITE, exists_sql_fn=_SQLITE_FN,
            )  # second call must not raise (native IF NOT EXISTS)

    def test_default_index_name_generated(self):
        con = _seed_sqlite()
        _generic_create_index(con, "t", ["id"], caps=_SQLITE, exists_sql_fn=_SQLITE_FN)
        assert _generic_index_exists(con, "idx_t_id", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True


class TestPartialIndex:
    def test_partial_where_on_sqlite(self):
        con = _seed_sqlite()
        _generic_create_index(
            con, "t", ["id"], index_name="idx_active",
            where=lambda r: r.active == True, caps=_SQLITE,  # noqa: E712
            exists_sql_fn=_SQLITE_FN,
        )
        assert _generic_index_exists(con, "idx_active", table_name="t",
                                     exists_sql_fn=_SQLITE_FN) is True

    def test_where_on_non_partial_dialect_raises(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "active": [True]}))
        with pytest.raises(ValueError, match="partial"):
            _generic_create_index(
                con, "t", ["id"], where=lambda r: r.active, caps=_DUCKDB,
                exists_sql_fn=_DUCKDB_FN,
            )


class TestValidationErrors:
    def test_unsupported_index_type_raises(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="index_type"):
            _generic_create_index(
                con, "t", ["id"], index_type="hash", caps=_SQLITE,
                exists_sql_fn=_SQLITE_FN,
            )

    def test_table_scoped_drop_requires_table_name(self):
        con = _seed_sqlite()
        mysql_caps = DIALECTS["mysql"].index_caps
        with pytest.raises(ValueError, match="table_name"):
            _generic_drop_index(con, "idx", table_name=None, caps=mysql_caps,
                                exists_sql_fn=DIALECTS["mysql"].get_index_exists_sql)

    def test_bad_identifier_rejected(self):
        con = _seed_sqlite()
        with pytest.raises(ValueError, match="simple identifier"):
            _generic_create_index(con, "t", ["id"], index_name="x; DROP",
                                  caps=_SQLITE, exists_sql_fn=_SQLITE_FN)

    def test_drop_if_exists_absent_is_noop_native(self):
        con = _seed_sqlite()
        _generic_drop_index(con, "nope", table_name="t", if_exists=True,
                            caps=_SQLITE, exists_sql_fn=_SQLITE_FN)  # no raise
