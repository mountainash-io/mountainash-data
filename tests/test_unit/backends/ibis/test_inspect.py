"""Tests for ibis -> core.inspection conversion."""

import ibis

from mountainash_data.backends.ibis.inspect import table_to_info
from mountainash_data.core.inspection import TableInfo
from mountainash_data.core.namespace import Namespace


def test_table_to_info_from_ibis_table():
    # Build an in-memory sqlite ibis backend with one table
    conn = ibis.sqlite.connect()
    conn.create_table(
        "users",
        schema=ibis.schema({"id": "int64", "name": "string"}),
    )
    table = conn.table("users")
    info = table_to_info(table, name="users", location=Namespace(path=("main",)))
    assert isinstance(info, TableInfo)
    assert info.name == "users"
    assert info.location == Namespace(path=("main",))
    assert info.column_names == ["id", "name"]
    assert info.columns[0].type_name == "int64"


def test_table_to_info_builds_location():
    con = ibis.duckdb.connect()
    con.create_table("t", schema=ibis.schema({"id": "int64"}))
    info = table_to_info(con.table("t"), name="t", location=Namespace(path=("main",)))
    assert info.location == Namespace(path=("main",))
    assert info.column_names == ["id"]
