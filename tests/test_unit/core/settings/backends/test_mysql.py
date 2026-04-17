# tests/test_unit/core/settings/backends/test_mysql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import PasswordAuth
from mountainash_data.core.settings.mysql import MySQLAuthSettings, MySQLSSLMode


@pytest.mark.unit
class TestMySQLAuthSettings:
    def _minimal(self, **extra):
        return MySQLAuthSettings(
            HOST="h", DATABASE="d",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_provider_type_is_mysql(self):
        """Audit regression: previously returned BIGQUERY."""
        from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.MYSQL

    def test_ssl_dict_assembled_when_capath_only_no_ca(self):
        """Audit regression: SSL_CAPATH was gated on SSL_CA."""
        s = self._minimal(SSL_CAPATH="/etc/ssl/ca-dir")
        kwargs = s.to_driver_kwargs()
        assert kwargs["ssl"] == {"ssl-capath": "/etc/ssl/ca-dir"}

    def test_ssl_not_emitted_when_ssl_mode_none(self):
        """Audit regression: SSL branch fired when SSL_MODE was None."""
        s = self._minimal()
        kwargs = s.to_driver_kwargs()
        assert "ssl_mode" not in kwargs
        assert "ssl" not in kwargs

    def test_ssl_mode_preferred(self):
        s = self._minimal(SSL_MODE=MySQLSSLMode.PREFERRED)
        kwargs = s.to_driver_kwargs()
        assert kwargs["ssl_mode"] == "PREFERRED"

    def test_autocommit_false_honored(self):
        """Audit regression: `if self.AUTOCOMMIT:` dropped explicit False."""
        s = self._minimal(AUTOCOMMIT=False)
        assert s.to_driver_kwargs()["autocommit"] is False
