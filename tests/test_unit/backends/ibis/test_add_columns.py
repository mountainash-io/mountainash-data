"""Tests for dialect-agnostic add_columns (schema evolution)."""

import ibis
import polars as pl
import pytest

from mountainash.core.dtypes.canonical import MountainashDtype
from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.backends.ibis.operations import (
    _coerce_dtype,
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
