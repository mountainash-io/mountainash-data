# tests/test_unit/core/settings/backends/test_singlestoredb.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.singlestoredb import (
    SingleStoreDBAuthSettings,
    SingleStoreDriver,
)


@pytest.mark.unit
class TestSingleStoreDBAuthSettings:
    def _minimal(self, **extra):
        return SingleStoreDBAuthSettings(
            HOST="svc-123.svc.singlestore.com",
            auth=PasswordAuth(username="admin", password=SecretStr("s3cret")),
            **extra,
        )

    def test_provider_type_is_singlestoredb(self):
        s = self._minimal()
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.SINGLESTOREDB

    def test_default_port(self):
        s = self._minimal()
        assert s.PORT == 3306

    def test_custom_port(self):
        s = self._minimal(PORT=3307)
        assert s.PORT == 3307

    def test_driver_enum_enforced(self):
        with pytest.raises(ValidationError):
            self._minimal(DRIVER="nonsense")

    def test_driver_mysql(self):
        s = self._minimal(DRIVER=SingleStoreDriver.MYSQL)
        assert s.DRIVER == SingleStoreDriver.MYSQL

    def test_driver_https(self):
        s = self._minimal(DRIVER=SingleStoreDriver.HTTPS)
        kwargs = s.to_driver_kwargs()
        assert kwargs["driver"] == "https"

    def test_autocommit_default_true(self):
        s = self._minimal()
        assert s.AUTOCOMMIT is True

    def test_local_infile_default_true(self):
        s = self._minimal()
        assert s.LOCAL_INFILE is True

    def test_no_auth(self):
        s = SingleStoreDBAuthSettings(
            HOST="svc-123.svc.singlestore.com", auth=NoAuth(),
        )
        assert s.HOST == "svc-123.svc.singlestore.com"

    def test_to_driver_kwargs_plumbs_core_fields(self):
        s = self._minimal(PORT=3307, DATABASE="mydb")
        kwargs = s.to_driver_kwargs()
        assert kwargs["host"] == "svc-123.svc.singlestore.com"
        assert kwargs["port"] == 3307
        assert kwargs["database"] == "mydb"
        assert kwargs["user"] == "admin"
        assert kwargs["password"] == "s3cret"

    def test_ibis_dialect(self):
        s = self._minimal()
        assert s.backend == "singlestoredb"
