# tests/test_unit/core/settings/backends/test_mssql.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.mssql import (
    MSSQLBackendProfile,
    MSSQLEncryption,
)


@pytest.mark.unit
class TestMSSQLBackendProfile:
    def _minimal(self, **extra):
        return MSSQLBackendProfile(HOST="h", DATABASE="d", **extra)

    def test_emit_plumbs_host_and_database(self):
        s = self._minimal()
        kwargs = s.emit(CONST_DB_PROVIDER_TYPE.MSSQL)
        assert kwargs["host"] == "h"
        assert kwargs["database"] == "d"

    def test_encryption_default(self):
        """Audit regression: ODBC Driver 18 default Encrypt=Yes requires explicit setting."""
        s = self._minimal()
        assert s.ENCRYPTION is MSSQLEncryption.MANDATORY

    def test_default_port(self):
        s = self._minimal()
        assert s.PORT == 1433

    def test_instance_name_stored(self):
        """Audit regression: code referenced args['server'] (KeyError)."""
        s = self._minimal(INSTANCE_NAME="SQLEXPRESS")
        assert s.INSTANCE_NAME == "SQLEXPRESS"
