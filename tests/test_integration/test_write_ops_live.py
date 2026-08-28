"""Live round-trip tests for generic write ops (postgres + mysql)."""

import pandas as pd
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
        conflict_action="UPDATE", update_condition=None, namespace=None, schema=None,
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
        conflict_action="NOTHING", update_condition=None, namespace=None, schema=None,
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


@pytest.mark.integration
def test_upsert_key_only_source_preserves_value_postgres(postgres_backend):
    """DEBT-12 defect 2, live: a key-only source frame must not null an
    existing row's non-key column via the ON_CONFLICT family."""
    be = postgres_backend
    be.create_table(
        "up_pg_keyonly", pl.DataFrame({"id": [1, 2], "value": ["keep-me", "also-keep"]}),
        overwrite=True,
    )
    be._require_connected()._ibis_conn.raw_sql("ALTER TABLE up_pg_keyonly ADD PRIMARY KEY (id)")
    with pytest.warns(UserWarning, match="no columns to update"):
        be.upsert("up_pg_keyonly", pl.DataFrame({"id": [1, 3]}), conflict_columns=["id"])
    rows = dict(
        be.table("up_pg_keyonly").order_by("id").execute()[["id", "value"]]
        .itertuples(index=False)
    )
    assert rows[1] == "keep-me" and rows[2] == "also-keep"
    assert pd.isna(rows[3])  # postgres NULL surfaces as pandas NaN via itertuples (verified)
    be.drop_table("up_pg_keyonly", force=True)


@pytest.mark.integration
def test_upsert_key_only_source_preserves_value_mysql(mysql_backend):
    """DEBT-12 defect 2, live: same regression via ON_DUPLICATE_KEY —
    proves effective_conflict_action reaches _render_on_duplicate_key
    (the no-live unit tests cannot exercise this; see plan Task 2 / spec §4)."""
    be = mysql_backend
    con = be._require_connected()._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS up_my_keyonly")
    con.raw_sql(
        "CREATE TABLE up_my_keyonly (id INT PRIMARY KEY, value VARCHAR(32))"
    )
    con.raw_sql("INSERT INTO up_my_keyonly VALUES (1, 'keep-me'), (2, 'also-keep')")
    with pytest.warns(UserWarning, match="no columns to update"):
        be.upsert("up_my_keyonly", pl.DataFrame({"id": [1, 3]}), conflict_columns=["id"])
    rows = dict(
        con.table("up_my_keyonly").order_by("id").execute()[["id", "value"]]
        .itertuples(index=False)
    )
    assert rows[1] == "keep-me" and rows[2] == "also-keep"
    assert pd.isna(rows[3])
    con.raw_sql("DROP TABLE up_my_keyonly")
