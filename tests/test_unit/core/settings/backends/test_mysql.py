# tests/test_unit/core/settings/backends/test_mysql.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.mysql import MySQLBackendProfile, MySQLSSLMode


@pytest.mark.unit
class TestMySQLBackendProfile:
    def _minimal(self, **extra):
        return MySQLBackendProfile(HOST="h", DATABASE="d", **extra)

    def test_provider_type_is_mysql(self):
        """Audit regression: previously returned BIGQUERY."""
        from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.MYSQL

    def test_ssl_capath_stored(self):
        """Audit regression: SSL_CAPATH was gated on SSL_CA."""
        from pathlib import Path
        s = self._minimal(SSL_CAPATH="/etc/ssl/ca-dir")
        assert s.SSL_CAPATH == Path("/etc/ssl/ca-dir")

    def test_ssl_mode_none_default(self):
        """Audit regression: SSL branch fired when SSL_MODE was None."""
        s = self._minimal()
        assert s.SSL_MODE is None

    def test_ssl_mode_preferred_stored(self):
        s = self._minimal(SSL_MODE=MySQLSSLMode.PREFERRED)
        assert s.SSL_MODE == MySQLSSLMode.PREFERRED

    def test_autocommit_false_honored(self):
        """Audit regression: `if self.AUTOCOMMIT:` dropped explicit False."""
        s = self._minimal(AUTOCOMMIT=False)
        assert s.emit()["autocommit"] is False
