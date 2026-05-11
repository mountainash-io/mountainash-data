# tests/test_unit/core/settings/backends/test_druid.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.druid import DruidAuthSettings


@pytest.mark.unit
class TestDruidAuthSettings:
    def _minimal(self, **extra):
        return DruidAuthSettings(
            HOST="druid.example.com",
            auth=NoAuth(),
            **extra,
        )

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.DRUID

    def test_default_port(self):
        assert self._minimal().PORT == 8082

    def test_default_path(self):
        assert self._minimal().ENDPOINT_PATH == "/druid/v2/sql"

    def test_default_scheme(self):
        assert self._minimal().SCHEME == "http"

    def test_to_driver_kwargs(self):
        kwargs = self._minimal(PORT=8888, SCHEME="https").to_driver_kwargs()
        assert kwargs["host"] == "druid.example.com"
        assert kwargs["port"] == 8888
        assert kwargs["path"] == "/druid/v2/sql"
        assert kwargs["scheme"] == "https"

    def test_with_password_auth(self):
        s = DruidAuthSettings(
            HOST="druid.example.com",
            auth=PasswordAuth(username="admin", password=SecretStr("pass")),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["user"] == "admin"
        assert kwargs["password"] == "pass"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "druid"
