"""Real dtype bridge tests requiring the optional mountainash package."""

import ibis
import pytest

from mountainash.core.dtypes.canonical import MountainashDtype
from mountainash_data import IbisBackend
from mountainash_data.backends.ibis.operations import _coerce_dtype


class TestCoerceDtype:
    def test_from_mountainash_scalar_dtype(self):
        assert _coerce_dtype(MountainashDtype.FP64) == ibis.dtype("float64")
        assert _coerce_dtype(MountainashDtype.U8) == ibis.dtype("uint8")

    def test_parametric_mountainash_dtype_raises_valueerror(self):
        with pytest.raises(ValueError, match="parametric"):
            _coerce_dtype(MountainashDtype.LIST)


class TestIbisBackendAddColumns:
    def test_explicit_mountainash_dtype(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("t", {"id": [1]})
            be.add_columns("t", {"hrv": MountainashDtype.FP64})
            cols = {c.name: c.type_name for c in be.inspect_table("t").columns}
            assert "hrv" in cols
            assert cols["hrv"] == "float64"
