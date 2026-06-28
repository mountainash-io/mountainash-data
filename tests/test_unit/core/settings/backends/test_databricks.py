# tests/test_unit/core/settings/backends/test_databricks.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.databricks import DatabricksBackendProfile


@pytest.mark.unit
class TestDatabricksBackendProfile:
    def _minimal(self, **extra):
        return DatabricksBackendProfile(
            SERVER_HOSTNAME="adb-123.12.azuredatabricks.net",
            HTTP_PATH="/sql/1.0/warehouses/abc123",
            **extra,
        )

    def test_provider_type_is_databricks(self):
        s = self._minimal()
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.DATABRICKS

    def test_schema_default(self):
        s = self._minimal()
        assert s.SCHEMA == "default"

    def test_use_cloud_fetch_default_false(self):
        s = self._minimal()
        assert s.USE_CLOUD_FETCH is False

    def test_emit_core_fields(self):
        s = self._minimal(CATALOG="analytics", SCHEMA="gold")
        kwargs = s.emit()
        assert kwargs["server_hostname"] == "adb-123.12.azuredatabricks.net"
        assert kwargs["http_path"] == "/sql/1.0/warehouses/abc123"
        assert kwargs["catalog"] == "analytics"
        assert kwargs["schema"] == "gold"

    def test_ibis_dialect(self):
        s = self._minimal()
        assert s.backend == "databricks"

    def test_use_cloud_fetch_plumbed(self):
        s = self._minimal(USE_CLOUD_FETCH=True)
        kwargs = s.emit()
        assert kwargs["use_cloud_fetch"] is True
