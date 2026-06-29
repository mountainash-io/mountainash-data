"""Generic upsert — ON CONFLICT family (sqlite/duckdb)."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
from mountainash_data.backends.ibis.operations import _generic_upsert


def _seed(con):
    con.create_table("t", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))
    con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")


class TestOnConflictUpsert:
    def test_insert_and_update_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        rows = dict(con.table("t").order_by("id").execute()[["id", "v"]].itertuples(index=False))
        assert rows == {1: "a", 2: "B", 3: "c"}

    def test_do_nothing_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2], "v": ["X"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns="id",
            update_columns=None, conflict_action="NOTHING",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").filter(ibis._.id == 2).execute()["v"].iloc[0] == "b"

    def test_composite_key_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("t", pl.DataFrame({"a": [1], "b": [1], "v": ["x"]}))
        # needs a composite unique index for ON CONFLICT to detect
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (a, b)")
        _generic_upsert(
            con, "t", pl.DataFrame({"a": [1], "b": [1], "v": ["y"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["a", "b"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").execute()["v"].iloc[0] == "y"

    def test_conditional_update_only_when_newer_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "ver": [5], "v": ["old"]}))
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (id)")
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [1], "ver": [3], "v": ["stale"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=lambda inc, exi: inc.ver > exi.ver,
            database=None, schema=None,
        )
        # incoming ver(3) is NOT newer than existing(5) -> unchanged
        assert con.table("t").execute()["v"].iloc[0] == "old"

    def test_unknown_style_raises_notimplemented(self):
        con = ibis.duckdb.connect()
        _seed(con)
        with pytest.raises(NotImplementedError):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [9], "v": ["z"]}),
                style=None, conflict_columns=["id"], update_columns=None,
                conflict_action="UPDATE", update_condition=None,
                database=None, schema=None,
            )
