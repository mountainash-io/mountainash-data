# tests/test_unit/core/settings/backends/test_bigquery.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.bigquery import BigQueryBackendProfile


@pytest.mark.unit
class TestBigQueryBackendProfile:
    def test_partition_column_default(self):
        """Audit regression: default was None, should be 'PARTITIONTIME'."""
        s = BigQueryBackendProfile(PROJECT_ID="myproj12")
        assert s.PARTITION_COLUMN == "PARTITIONTIME"

    def test_emit_project_id(self):
        s = BigQueryBackendProfile(PROJECT_ID="myproj12")
        kwargs = s.emit()
        assert kwargs["project_id"] == "myproj12"

    def test_auth_local_webserver_plumbed(self):
        """Audit regression: field didn't exist."""
        s = BigQueryBackendProfile(PROJECT_ID="myproj12", AUTH_LOCAL_WEBSERVER=False)
        assert s.emit()["auth_local_webserver"] is False
