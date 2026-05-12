# tests/test_unit/core/settings/backends/test_exasol.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import PasswordAuth
from mountainash_data.core.settings.exasol import ExasolAuthSettings


@pytest.mark.unit
class TestExasolAuthSettings:
    def _minimal(self, **extra):
        return ExasolAuthSettings(
            HOST="exasol.example.com",
            auth=PasswordAuth(username="sys", password=SecretStr("s3cret")),
            **extra,
        )

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.EXASOL

    def test_default_port(self):
        assert self._minimal().PORT == 8563

    def test_default_timezone(self):
        assert self._minimal().TIMEZONE == "UTC"

    def test_to_driver_kwargs(self):
        kwargs = self._minimal().to_driver_kwargs()
        assert kwargs["host"] == "exasol.example.com"
        assert kwargs["port"] == 8563
        assert kwargs["timezone"] == "UTC"
        assert kwargs["user"] == "sys"
        assert kwargs["password"] == "s3cret"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "exasol"
