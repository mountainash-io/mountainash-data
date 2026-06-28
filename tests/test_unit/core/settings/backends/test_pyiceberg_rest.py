# tests/test_unit/core/settings/backends/test_pyiceberg_rest.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.pyiceberg_rest import PyIcebergRestBackendProfile


@pytest.mark.unit
class TestPyIcebergRestBackendProfile:
    def _min(self, **extra):
        return PyIcebergRestBackendProfile(
            CATALOG_NAME="cat",
            CATALOG_URI="https://catalog.example/v1",
            **extra,
        )

    def test_warehouse_optional(self):
        """Audit regression: WAREHOUSE was over-required."""
        s = self._min()
        assert s.WAREHOUSE is None

    def test_emit_plumbs_uri(self):
        s = self._min()
        kwargs = s.emit(CONST_DB_PROVIDER_TYPE.PYICEBERG_REST)
        assert kwargs["uri"] == "https://catalog.example/v1"
        assert kwargs["name"] == "cat"

    def test_s3_params_stored(self):
        """Audit regression: s3.* family was absent from the spec."""
        s = self._min(
            S3_ENDPOINT="https://r2.example.com",
            S3_REGION="auto",
        )
        assert s.S3_ENDPOINT == "https://r2.example.com"
        assert s.S3_REGION == "auto"
