"""MySQL ON DUPLICATE KEY preflight: prove-safe-or-raise (spec §6.2).

Calls `_generic_upsert(...)` DIRECTLY against the raw mariadb connection — the
`be.upsert()` dispatch is not flipped until Task 9, so testing the generic
function directly is what keeps this task self-contained (Codex finding).
"""

import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
from mountainash_data.backends.ibis.operations import _generic_upsert


def _raw(be):
    # The fixture yields a connected IbisBackend; reach its raw ibis conn.
    return be._require_connected()._ibis_conn


def _odk(con, name, df, conflict):
    _generic_upsert(
        con, name, df, style=UpsertStyle.ON_DUPLICATE_KEY,
        conflict_columns=conflict, update_columns=None, conflict_action="UPDATE",
        update_condition=None, namespace=None, schema=None,
    )


@pytest.mark.integration
def test_single_pk_proceeds(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_ok")
    con.raw_sql("CREATE TABLE odk_ok (id INT PRIMARY KEY, v VARCHAR(16) NOT NULL)")
    con.raw_sql("INSERT INTO odk_ok VALUES (1, 'a')")
    _odk(con, "odk_ok", pl.DataFrame({"id": [1, 2], "v": ["A", "b"]}), ["id"])
    rows = dict(con.table("odk_ok").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "A", 2: "b"}
    con.raw_sql("DROP TABLE odk_ok")


@pytest.mark.integration
def test_multiple_unique_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_multi")
    con.raw_sql(
        "CREATE TABLE odk_multi "
        "(id INT PRIMARY KEY, email VARCHAR(64) NOT NULL UNIQUE, v VARCHAR(16) NOT NULL)"
    )
    with pytest.raises(ValueError, match="unique"):
        _odk(con, "odk_multi", pl.DataFrame({"id": [1], "email": ["x"], "v": ["a"]}), ["id"])
    con.raw_sql("DROP TABLE odk_multi")


@pytest.mark.integration
def test_prefix_index_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_prefix")
    con.raw_sql("CREATE TABLE odk_prefix (email VARCHAR(64) NOT NULL, v VARCHAR(16) NOT NULL, UNIQUE (email(10)))")
    with pytest.raises(ValueError, match="prefix|SUB_PART"):
        _odk(con, "odk_prefix", pl.DataFrame({"email": ["x"], "v": ["a"]}), ["email"])
    con.raw_sql("DROP TABLE odk_prefix")


@pytest.mark.integration
def test_nullable_conflict_column_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_null")
    con.raw_sql("CREATE TABLE odk_null (k INT NULL UNIQUE, v VARCHAR(16) NOT NULL)")
    with pytest.raises(ValueError, match="nullable|NOT NULL"):
        _odk(con, "odk_null", pl.DataFrame({"k": [1], "v": ["a"]}), ["k"])
    con.raw_sql("DROP TABLE odk_null")
