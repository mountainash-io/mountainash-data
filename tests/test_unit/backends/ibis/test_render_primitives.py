"""Unit tests for the shared sqlglot rendering primitives."""

import ibis

from mountainash_data.backends.ibis._render import (
    dialect_of,
    qualified_name,
    quote_identifier,
    render_type,
)


class TestRenderPrimitives:
    def test_quote_identifier_duckdb(self):
        d = dialect_of(ibis.duckdb.connect())
        assert quote_identifier("new col", d) == '"new col"'

    def test_quote_identifier_mysql_backticks(self):
        # mysql connect needs a server; render via a sqlglot dialect string instead
        assert quote_identifier("c", "mysql") == "`c`"

    def test_qualified_name_two_parts(self):
        assert qualified_name(["db", "t"], "duckdb") == '"db"."t"'

    def test_render_type_matches_create_table_mapper(self):
        con = ibis.duckdb.connect()
        tm = con.compiler.type_mapper
        assert render_type(tm, ibis.dtype("int64")) == tm.to_string(ibis.dtype("int64"))
