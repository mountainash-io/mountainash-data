# tests/test_unit/core/settings/backends/test_risingwave.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.risingwave import RisingWaveAuthSettings


@pytest.mark.unit
class TestRisingWaveAuthSettings:
    def _minimal(self, **extra):
        return RisingWaveAuthSettings(
            HOST="rw.example.com",
            auth=PasswordAuth(username="root", password=SecretStr("s3cret")),
            **extra,
        )

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.RISINGWAVE

    def test_default_port(self):
        assert self._minimal().PORT == 5432

    def test_to_driver_kwargs(self):
        kwargs = self._minimal(DATABASE="dev", SCHEMA="public").to_driver_kwargs()
        assert kwargs["host"] == "rw.example.com"
        assert kwargs["port"] == 5432
        assert kwargs["database"] == "dev"
        assert kwargs["schema"] == "public"
        assert kwargs["user"] == "root"
        assert kwargs["password"] == "s3cret"

    def test_no_auth(self):
        s = RisingWaveAuthSettings(HOST="rw.local", auth=NoAuth())
        assert s.HOST == "rw.local"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "risingwave"
