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


import polars as pl
import pytest

from mountainash_data.backends.ibis._render import (
    compile_projected_source,
    resolve_source,
)


class TestResolveSource:
    def test_intersects_target_and_orders_by_target_schema(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "value": [2], "extra": [3]}))
        target_schema = con.table("t").schema()
        # source provides "extra" and "id" (out of target order), omits "value"
        src, cols = resolve_source(
            pl.DataFrame({"extra": [9], "id": [1]}), target_schema
        )
        assert cols == ["id", "extra"]  # target-schema order, "value" excluded

    def test_raises_on_source_column_absent_from_target(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        target_schema = con.table("t").schema()
        with pytest.raises(ValueError, match="source columns absent from target"):
            resolve_source(pl.DataFrame({"id": [1], "bogus": [2]}), target_schema)


class TestCompileProjectedSource:
    def test_casts_and_projects_in_target_order(self):
        con = ibis.duckdb.connect()
        # target is int64; source is deliberately int32 so a real CAST is
        # forced — ibis's cast() is a no-op (no CAST emitted) when the
        # source and requested types already match (verified against
        # ibis 12.0.0: Value.cast returns the original expression unchanged).
        con.create_table(
            "t", pl.DataFrame({"id": [1], "value": [2]}, schema={"id": pl.Int64, "value": pl.Int64})
        )
        target_schema = con.table("t").schema()
        src, cols = resolve_source(
            pl.DataFrame({"id": [1], "value": [2]}, schema={"id": pl.Int32, "value": pl.Int32}),
            target_schema,
        )
        sql = compile_projected_source(con, src, cols, target_schema)
        assert cols == ["id", "value"]
        assert "SELECT" in sql.upper()
        assert "CAST" in sql.upper()  # int32 source cast to the int64 target type
