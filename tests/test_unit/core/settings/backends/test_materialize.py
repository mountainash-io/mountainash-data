# tests/test_unit/core/settings/backends/test_materialize.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.materialize import MaterializeBackendProfile


@pytest.mark.unit
class TestMaterializeBackendProfile:
    def _minimal(self, **extra):
        return MaterializeBackendProfile(HOST="materialize.example.com", **extra)

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.MATERIALIZE

    def test_default_port(self):
        assert self._minimal().PORT == 6875

    def test_default_autocommit(self):
        assert self._minimal().AUTOCOMMIT is True

    def test_cluster_param(self):
        s = self._minimal(CLUSTER="quickstart")
        kwargs = s.emit()
        assert kwargs["cluster"] == "quickstart"

    def test_emit(self):
        kwargs = self._minimal(DATABASE="mydb", SCHEMA="public").emit()
        assert kwargs["host"] == "materialize.example.com"
        assert kwargs["port"] == 6875
        assert kwargs["database"] == "mydb"
        assert kwargs["schema"] == "public"

    def test_minimal_construction(self):
        s = MaterializeBackendProfile(HOST="mz.local")
        assert s.HOST == "mz.local"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "materialize"
