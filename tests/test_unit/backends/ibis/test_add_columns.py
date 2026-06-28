"""Tests for dialect-agnostic add_columns (schema evolution)."""

import ibis
import polars as pl
import pytest

from mountainash.core.dtypes.canonical import MountainashDtype
from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.backends.ibis.operations import (
    _coerce_dtype,
    _generic_add_columns,
    _normalize_to_schema,
)


class TestDialectSpecField:
    def test_add_columns_hook_defaults_none(self):
        spec = DialectSpec(
            ibis_backend_name="duckdb",
            connection_mode="connection_string",
            connection_string_scheme="duckdb://",
        )
        assert spec.add_columns_hook is None

    def test_registered_dialects_have_no_hook_initially(self):
        # The generic path covers every dialect; none registers an override.
        assert DIALECTS["duckdb"].add_columns_hook is None
        assert DIALECTS["sqlite"].add_columns_hook is None
        assert DIALECTS["postgres"].add_columns_hook is None


class TestCoerceDtype:
    def test_passes_through_ibis_datatype(self):
        dt = ibis.dtype("float64")
        assert _coerce_dtype(dt) is dt

    def test_from_type_string(self):
        assert _coerce_dtype("float64") == ibis.dtype("float64")

    def test_from_mountainash_scalar_dtype(self):
        assert _coerce_dtype(MountainashDtype.FP64) == ibis.dtype("float64")
        assert _coerce_dtype(MountainashDtype.U8) == ibis.dtype("uint8")

    def test_parametric_mountainash_dtype_raises_valueerror(self):
        with pytest.raises(ValueError, match="parametric"):
            _coerce_dtype(MountainashDtype.LIST)


class TestNormalizeToSchema:
    def test_mapping_of_mixed_dtype_specs(self):
        sch = _normalize_to_schema({"a": "float64", "b": ibis.dtype("int64")})
        assert dict(sch.items()) == dict(
            ibis.schema({"a": "float64", "b": "int64"}).items()
        )

    def test_frame_inference(self):
        sch = _normalize_to_schema(pl.DataFrame({"a": [1], "b": ["x"]}))
        assert set(sch.names) == {"a", "b"}


class TestGenericAddColumns:
    def test_adds_missing_column_from_frame_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "name": ["a"]}))
        _generic_add_columns(
            con, "t", pl.DataFrame({"id": [1], "name": ["a"], "score": [1.5]})
        )
        assert "score" in con.table("t").schema().names

    def test_idempotent_second_call_is_noop(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"x": "float64"})
        _generic_add_columns(con, "t", {"x": "float64"})
        assert list(con.table("t").schema().names).count("x") == 1

    def test_null_typed_column_becomes_string(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(
            con, "t",
            pl.DataFrame({"id": [1], "note": pl.Series([None], dtype=pl.Null)}),
        )
        assert str(con.table("t").schema()["note"]) == "string"

    def test_quotes_identifiers_needing_quoting(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"new col": "float64"})
        assert "new col" in con.table("t").schema().names

    def test_works_on_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        _generic_add_columns(con, "t", {"score": "float64"})
        assert "score" in con.table("t").schema().names

    def test_rejects_dotted_table_name(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_add_columns(con, "schema.t", {"x": "float64"})

    def test_rejects_dotted_database(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_add_columns(con, "t", {"x": "float64"}, database="a.b")
