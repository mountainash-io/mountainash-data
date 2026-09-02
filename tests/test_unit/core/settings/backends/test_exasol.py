# tests/test_unit/core/settings/backends/test_exasol.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.exasol import ExasolBackendProfile


@pytest.mark.unit
class TestExasolBackendProfile:
    def _minimal(self, **extra):
        return ExasolBackendProfile(HOST="exasol.example.com", **extra)

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.EXASOL

    def test_default_port(self):
        assert self._minimal().PORT == 8563

    def test_default_timezone(self):
        assert self._minimal().TIMEZONE == "UTC"

    def test_emit(self):
        kwargs = self._minimal().emit()
        assert kwargs["host"] == "exasol.example.com"
        assert kwargs["port"] == 8563
        assert kwargs["timezone"] == "UTC"

    def test_schema_is_emitted_for_initial_session(self):
        kwargs = self._minimal(SCHEMA="APP").emit()
        assert kwargs["schema"] == "APP"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "exasol"
