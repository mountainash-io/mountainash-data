# tests/test_unit/core/settings/backends/test_pyiceberg_rest.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import OAuth2Auth, TokenAuth
from mountainash_data.core.settings.pyiceberg_rest import PyIcebergRestAuthSettings


@pytest.mark.unit
class TestPyIcebergRestAuthSettings:
    def _min(self, auth, **extra):
        return PyIcebergRestAuthSettings(
            CATALOG_NAME="cat",
            CATALOG_URI="https://catalog.example/v1",
            auth=auth, **extra,
        )

    def test_warehouse_optional(self):
        """Audit regression: WAREHOUSE was over-required."""
        s = self._min(auth=TokenAuth(token=SecretStr("t")))
        assert s.WAREHOUSE is None

    def test_token_auth(self):
        s = self._min(auth=TokenAuth(token=SecretStr("tok")))
        kwargs = s.to_driver_kwargs()
        assert kwargs["token"] == "tok"
        assert kwargs["uri"] == "https://catalog.example/v1"

    def test_oauth2_credential_form(self):
        s = self._min(
            auth=OAuth2Auth(client_id="cid", client_secret=SecretStr("sec")),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["credential"] == "cid:sec"

    def test_s3_params_prefixed(self):
        """Audit regression: s3.* family was absent."""
        s = self._min(
            auth=TokenAuth(token=SecretStr("t")),
            S3_ENDPOINT="https://r2.example.com",
            S3_REGION="auto",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["s3.endpoint"] == "https://r2.example.com"
        assert kwargs["s3.region"] == "auto"
