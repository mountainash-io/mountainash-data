"""Tests for ibis -> core.inspection conversion."""

import ibis

from mountainash_data.backends.ibis.inspect import table_to_info
from mountainash_data.core.inspection import TableInfo


def test_table_to_info_from_ibis_table():
    # Build an in-memory sqlite ibis backend with one table
    conn = ibis.sqlite.connect()
    conn.create_table(
        "users",
        schema=ibis.schema({"id": "int64", "name": "string"}),
    )
    table = conn.table("users")
    info = table_to_info(table, name="users", namespace="main")
    assert isinstance(info, TableInfo)
    assert info.name == "users"
    assert info.namespace == "main"
    assert info.column_names == ["id", "name"]
    assert info.columns[0].type_name == "int64"
