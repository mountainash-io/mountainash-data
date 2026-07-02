"""Tests for dialect-agnostic add_columns (schema evolution)."""

import dataclasses
import ibis
import polars as pl
import pytest

from mountainash.core.dtypes.canonical import MountainashDtype
from mountainash_data import IbisBackend
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
        assert sch["a"] == ibis.dtype("int64")
        assert sch["b"] == ibis.dtype("string")


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

    def test_rejects_dotted_namespace(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_add_columns(con, "t", {"x": "float64"}, namespace="a.b")

    def test_namespace_qualified_add_on_duckdb(self):
        """Happy-path: two-part qualified quoting (namespace.table) via ATTACH.

        ibis 10.4.0's duckdb backend does not support create_table(database=...)
        for attached databases — ``database=`` resolves to the DuckDB *schema*
        slot, not the catalog. The table is therefore created via raw_sql, which
        is the realistic path for an attached second database. The
        _generic_add_columns path itself (table look-up + ALTER) is what we
        exercise here.
        """
        con = ibis.duckdb.connect()
        con.raw_sql("ATTACH ':memory:' AS mem2")
        con.raw_sql("CREATE TABLE mem2.t (id INTEGER)")
        con.raw_sql("INSERT INTO mem2.t VALUES (1)")
        _generic_add_columns(con, "t", {"score": "float64"}, namespace="mem2")
        assert "score" in con.table("t", database="mem2").schema().names


class TestIbisBackendAddColumns:
    def test_frame_form_returns_self_and_adds_column(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", pl.DataFrame({"id": [1], "name": ["a"]}))
            ret = be.add_columns(
                "t", pl.DataFrame({"id": [1], "name": ["a"], "score": [1.5]})
            )
            assert ret is be
            cols = {c.name for c in be.inspect_table("t").columns}
            assert "score" in cols

    def test_explicit_mountainash_dtype(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", {"id": [1]})
            be.add_columns("t", {"hrv": MountainashDtype.FP64})
            cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
            assert "hrv" in cols
            assert cols["hrv"] == "float64"

    def test_create_evolve_type_parity_sqlite(self):
        """The core invariant: an evolved column types like a created one."""
        with IbisBackend(dialect="sqlite", database=":memory:") as be:
            be.create_table(
                "fresh", pl.DataFrame({"cnt": pl.Series([3], dtype=pl.UInt8)})
            )
            be.create_table("evo", pl.DataFrame({"id": [1]}))
            be.add_columns(
                "evo",
                pl.DataFrame({"id": [1], "cnt": pl.Series([3], dtype=pl.UInt8)}),
            )
            fresh = {c.name: c.type_name for c in be.inspect_table("fresh").columns}
            evolved = {c.name: c.type_name for c in be.inspect_table("evo").columns}
            assert evolved["cnt"] == fresh["cnt"]

    def test_hook_override_wins_over_generic(self):
        calls = []

        def fake_hook(ibis_conn, name, source, *, namespace=None):
            calls.append((name, source))

        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", {"id": [1]})
            be._spec = dataclasses.replace(be._spec, add_columns_hook=fake_hook)
            be.add_columns("t", {"x": "float64"})
            assert calls == [("t", {"x": "float64"})]
            # generic path did NOT run -> column absent
            cols = {c.name for c in be.inspect_table("t").columns}
            assert "x" not in cols


def test_add_columns_accepts_namespace_kwarg():
    from mountainash_data import IbisBackend

    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.ibis_connection().raw_sql("CREATE SCHEMA tn")
        backend.create_table("evolving", {"id": [1]}, namespace="tn")
        backend.add_columns("evolving", {"id": "int64", "extra": "string"}, namespace="tn")
        info = backend.inspect_table("evolving", namespace="tn")
        assert "extra" in info.column_names


def test_add_columns_rejects_database_kwarg():
    import pytest
    from mountainash_data import IbisBackend

    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        with pytest.raises(TypeError):
            backend.add_columns("t", {"id": "int64"}, database="x")
