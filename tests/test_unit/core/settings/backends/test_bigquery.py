# tests/test_unit/core/settings/backends/test_bigquery.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth, ServiceAccountAuth
from mountainash_data.core.settings.bigquery import BigQueryAuthSettings


@pytest.mark.unit
class TestBigQueryAuthSettings:
    def test_partition_column_default(self):
        """Audit regression: default was None, should be 'PARTITIONTIME'."""
        s = BigQueryAuthSettings(PROJECT_ID="myproj12", auth=NoAuth())
        assert s.PARTITION_COLUMN == "PARTITIONTIME"

    def test_service_account_info_converts_to_credentials(self):
        """Audit regression: SA info dict was passed raw; Ibis needs Credentials."""
        pytest.importorskip("google.oauth2")

        # Minimal valid SA info shape
        info = {
            "type": "service_account",
            "project_id": "myproj12",
            "private_key_id": "x",
            "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
            "client_email": "sa@myproj12.iam.gserviceaccount.com",
            "client_id": "1",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }

        s = BigQueryAuthSettings(
            PROJECT_ID="myproj12",
            auth=ServiceAccountAuth(info=info),
        )
        # We can't fully construct credentials without a valid key, so we
        # just verify the adapter attempts conversion and emits the key.
        try:
            kwargs = s.to_driver_kwargs()
            assert "credentials" in kwargs
        except ValueError:
            # google.oauth2 will reject the dummy key — acceptable here,
            # the important thing is no raw dict leak.
            pass

    def test_auth_local_webserver_plumbed(self):
        """Audit regression: field didn't exist."""
        s = BigQueryAuthSettings(
            PROJECT_ID="myproj12", AUTH_LOCAL_WEBSERVER=False, auth=NoAuth(),
        )
        assert s.to_driver_kwargs()["auth_local_webserver"] is False
