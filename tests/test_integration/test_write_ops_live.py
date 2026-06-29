"""Live round-trip tests for generic write ops (postgres + mysql)."""

import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
from mountainash_data.backends.ibis.operations import _generic_upsert


@pytest.mark.integration
def test_rename_table_live_postgres(postgres_backend):
    be = postgres_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)


@pytest.mark.integration
def test_rename_table_live_mysql(mysql_backend):
    be = mysql_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)


@pytest.mark.integration
def test_merge_insert_and_update_postgres(postgres_backend):
    """MERGE UPDATE: existing row updated, new row inserted."""
    be = postgres_backend
    con = be._require_connected()._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS mrg")
    con.create_table("mrg", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))
    _generic_upsert(
        con, "mrg", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
        style=UpsertStyle.MERGE, conflict_columns=["id"], update_columns=None,
        conflict_action="UPDATE", update_condition=None, database=None, schema=None,
    )
    rows = dict(
        con.table("mrg").order_by("id").execute()[["id", "v"]].itertuples(index=False)
    )
    assert rows == {1: "a", 2: "B", 3: "c"}
    con.raw_sql("DROP TABLE mrg")


@pytest.mark.integration
def test_merge_nothing_postgres(postgres_backend):
    """MERGE NOTHING: existing row NOT updated, new row inserted."""
    be = postgres_backend
    con = be._require_connected()._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS mrg_nothing")
    con.create_table("mrg_nothing", pl.DataFrame({"id": [1], "v": ["a"]}))
    _generic_upsert(
        con, "mrg_nothing", pl.DataFrame({"id": [1, 2], "v": ["X", "b"]}),
        style=UpsertStyle.MERGE, conflict_columns=["id"], update_columns=None,
        conflict_action="NOTHING", update_condition=None, database=None, schema=None,
    )
    rows = dict(
        con.table("mrg_nothing").order_by("id").execute()[["id", "v"]].itertuples(index=False)
    )
    assert rows == {1: "a", 2: "b"}, f"Expected {{1:'a', 2:'b'}}, got {rows}"
    con.raw_sql("DROP TABLE mrg_nothing")


@pytest.mark.integration
def test_upsert_via_dispatch_postgres(postgres_backend):
    """be.upsert() public dispatch — ON_CONFLICT via generic path (postgres)."""
    be = postgres_backend
    be.create_table("up_pg", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), overwrite=True)
    be._require_connected()._ibis_conn.raw_sql("ALTER TABLE up_pg ADD PRIMARY KEY (id)")
    be.upsert("up_pg", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}), conflict_columns=["id"])
    rows = dict(be.table("up_pg").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "a", 2: "B", 3: "c"}
    be.drop_table("up_pg", force=True)


@pytest.mark.integration
def test_upsert_via_dispatch_mysql(mysql_backend):
    """be.upsert() public dispatch — ON_DUPLICATE_KEY via generic path (mysql/mariadb)."""
    be = mysql_backend
    con = be._require_connected()._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS up_my")
    con.raw_sql("CREATE TABLE up_my (id INT PRIMARY KEY, v VARCHAR(16) NOT NULL)")
    con.raw_sql("INSERT INTO up_my VALUES (1, 'a')")
    be.upsert("up_my", pl.DataFrame({"id": [1, 2], "v": ["A", "b"]}), conflict_columns=["id"])
    rows = dict(con.table("up_my").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "A", 2: "b"}
    con.raw_sql("DROP TABLE up_my")
