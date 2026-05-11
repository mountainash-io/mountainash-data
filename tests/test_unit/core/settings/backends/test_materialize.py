# tests/test_unit/core/settings/backends/test_materialize.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.materialize import MaterializeAuthSettings


@pytest.mark.unit
class TestMaterializeAuthSettings:
    def _minimal(self, **extra):
        return MaterializeAuthSettings(
            HOST="materialize.example.com",
            auth=PasswordAuth(username="mz", password=SecretStr("s3cret")),
            **extra,
        )

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.MATERIALIZE

    def test_default_port(self):
        assert self._minimal().PORT == 6875

    def test_default_autocommit(self):
        assert self._minimal().AUTOCOMMIT is True

    def test_cluster_param(self):
        s = self._minimal(CLUSTER="quickstart")
        kwargs = s.to_driver_kwargs()
        assert kwargs["cluster"] == "quickstart"

    def test_to_driver_kwargs(self):
        kwargs = self._minimal(DATABASE="mydb", SCHEMA="public").to_driver_kwargs()
        assert kwargs["host"] == "materialize.example.com"
        assert kwargs["port"] == 6875
        assert kwargs["database"] == "mydb"
        assert kwargs["schema"] == "public"
        assert kwargs["user"] == "mz"
        assert kwargs["password"] == "s3cret"

    def test_no_auth(self):
        s = MaterializeAuthSettings(HOST="mz.local", auth=NoAuth())
        assert s.HOST == "mz.local"

    def test_ibis_dialect(self):
        assert self._minimal().backend == "materialize"
