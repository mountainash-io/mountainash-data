# tests/test_unit/core/settings/backends/test_druid.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.druid import DruidBackendProfile


@pytest.mark.unit
class TestDruidBackendProfile:
    def _minimal(self, **extra):
        return DruidBackendProfile(HOST="druid.example.com", **extra)

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.DRUID

    def test_default_port(self):
        assert self._minimal().PORT == 8082

    def test_default_path(self):
        assert self._minimal().ENDPOINT_PATH == "/druid/v2/sql"

    def test_default_scheme(self):
        assert self._minimal().SCHEME == "http"

    def test_emit(self):
        kwargs = self._minimal(PORT=8888, SCHEME="https").emit()
        assert kwargs["host"] == "druid.example.com"
        assert kwargs["port"] == 8888
        assert kwargs["path"] == "/druid/v2/sql"
        assert kwargs["scheme"] == "https"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "druid"
