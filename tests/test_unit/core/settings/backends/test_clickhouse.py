# tests/test_unit/core/settings/backends/test_clickhouse.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.clickhouse import ClickHouseAuthSettings


@pytest.mark.unit
class TestClickHouseAuthSettings:
    def _minimal(self, **extra):
        return ClickHouseAuthSettings(
            HOST="ch.example.com",
            auth=PasswordAuth(username="demo", password=SecretStr("s3cret")),
            **extra,
        )

    def test_provider_type_is_clickhouse(self):
        s = self._minimal()
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.CLICKHOUSE

    def test_default_port(self):
        s = self._minimal()
        assert s.PORT == 9000

    def test_custom_port(self):
        s = self._minimal(PORT=443)
        assert s.PORT == 443

    def test_secure_default_false(self):
        s = self._minimal()
        assert s.SECURE is False

    def test_secure_true(self):
        s = self._minimal(SECURE=True)
        assert s.SECURE is True

    def test_no_auth(self):
        s = ClickHouseAuthSettings(HOST="ch.example.com", auth=NoAuth())
        assert s.HOST == "ch.example.com"

    def test_to_driver_kwargs_plumbs_core_fields(self):
        s = self._minimal(PORT=443, DATABASE="pypi", SECURE=True)
        kwargs = s.to_driver_kwargs()
        assert kwargs["host"] == "ch.example.com"
        assert kwargs["port"] == 443
        assert kwargs["database"] == "pypi"
        assert kwargs["secure"] is True
        assert kwargs["user"] == "demo"
        assert kwargs["password"] == "s3cret"

    def test_ibis_dialect(self):
        s = self._minimal()
        assert s.backend == "clickhouse"
