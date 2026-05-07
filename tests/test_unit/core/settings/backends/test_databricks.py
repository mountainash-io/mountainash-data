# tests/test_unit/core/settings/backends/test_databricks.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth, TokenAuth
from mountainash_data.core.settings.databricks import DatabricksAuthSettings


@pytest.mark.unit
class TestDatabricksAuthSettings:
    def _token(self, **extra):
        return DatabricksAuthSettings(
            SERVER_HOSTNAME="adb-123.12.azuredatabricks.net",
            HTTP_PATH="/sql/1.0/warehouses/abc123",
            auth=TokenAuth(token=SecretStr("dapi-xyz")),
            **extra,
        )

    def test_provider_type_is_databricks(self):
        s = self._token()
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.DATABRICKS

    def test_schema_default(self):
        s = self._token()
        assert s.SCHEMA == "default"

    def test_use_cloud_fetch_default_false(self):
        s = self._token()
        assert s.USE_CLOUD_FETCH is False

    def test_token_auth_kwargs(self):
        s = self._token(CATALOG="analytics", SCHEMA="gold")
        kwargs = s.to_driver_kwargs()
        assert kwargs["server_hostname"] == "adb-123.12.azuredatabricks.net"
        assert kwargs["http_path"] == "/sql/1.0/warehouses/abc123"
        assert kwargs["access_token"] == "dapi-xyz"
        assert kwargs["catalog"] == "analytics"
        assert kwargs["schema"] == "gold"
        assert "username" not in kwargs
        assert "password" not in kwargs

    def test_password_auth_kwargs(self):
        s = DatabricksAuthSettings(
            SERVER_HOSTNAME="adb-123.12.azuredatabricks.net",
            HTTP_PATH="/sql/1.0/warehouses/abc123",
            auth=PasswordAuth(username="user", password=SecretStr("pass")),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["username"] == "user"
        assert kwargs["password"] == "pass"
        assert "access_token" not in kwargs

    def test_no_auth(self):
        s = DatabricksAuthSettings(
            SERVER_HOSTNAME="adb-123.12.azuredatabricks.net",
            HTTP_PATH="/sql/1.0/warehouses/abc123",
            auth=NoAuth(),
        )
        kwargs = s.to_driver_kwargs()
        assert "access_token" not in kwargs
        assert "username" not in kwargs

    def test_ibis_dialect(self):
        s = self._token()
        assert s.backend == "databricks"

    def test_use_cloud_fetch_plumbed(self):
        s = self._token(USE_CLOUD_FETCH=True)
        kwargs = s.to_driver_kwargs()
        assert kwargs["use_cloud_fetch"] is True
