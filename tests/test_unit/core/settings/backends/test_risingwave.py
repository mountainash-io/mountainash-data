# tests/test_unit/core/settings/backends/test_risingwave.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.risingwave import RisingWaveBackendProfile


@pytest.mark.unit
class TestRisingWaveBackendProfile:
    def _minimal(self, **extra):
        return RisingWaveBackendProfile(HOST="rw.example.com", **extra)

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.RISINGWAVE

    def test_default_port(self):
        assert self._minimal().PORT == 5432

    def test_emit(self):
        kwargs = self._minimal(DATABASE="dev", SCHEMA="public").emit()
        assert kwargs["host"] == "rw.example.com"
        assert kwargs["port"] == 5432
        assert kwargs["database"] == "dev"
        assert kwargs["schema"] == "public"

    def test_minimal_construction(self):
        s = RisingWaveBackendProfile(HOST="rw.local")
        assert s.HOST == "rw.local"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "risingwave"
