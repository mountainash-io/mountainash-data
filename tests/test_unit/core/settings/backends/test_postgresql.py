# tests/test_unit/core/settings/backends/test_postgresql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import PasswordAuth
from mountainash_data.core.settings.postgresql import (
    PostgresRequireAuthMethods,
    PostgresSSLMode,
    PostgreSQLAuthSettings,
)


@pytest.mark.unit
class TestPostgreSQLAuthSettings:
    def _minimal(self, **extra):
        return PostgreSQLAuthSettings(
            HOST="h", DATABASE="d",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_provider_type_is_postgresql(self):
        """Audit regression: previously returned BIGQUERY."""
        s = self._minimal()
        from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.POSTGRESQL

    def test_ssl_mode_enum_enforced(self):
        """Audit regression: SSL_MODE was bare str."""
        with pytest.raises(ValidationError):
            self._minimal(SSL_MODE="nonsense")

    def test_ssl_cert_is_path(self):
        """Audit regression: SSL_CERT was bool."""
        from pathlib import Path
        s = self._minimal(SSL_CERT=Path("/etc/ssl/client.crt"))
        assert s.SSL_CERT == Path("/etc/ssl/client.crt")

    def test_require_auth_is_list_of_enum(self):
        """Audit regression: REQUIRE_AUTH was bool."""
        s = self._minimal(
            REQUIRE_AUTH=[PostgresRequireAuthMethods.SCRAM_SHA_256,
                          PostgresRequireAuthMethods.MD5]
        )
        assert len(s.REQUIRE_AUTH) == 2

    def test_to_driver_kwargs_plumbs_ssl_and_keepalives(self):
        """Audit regression: only SCHEMA was being plumbed."""
        s = self._minimal(SSL_MODE=PostgresSSLMode.REQUIRE, KEEPALIVES_IDLE=30)
        kwargs = s.to_driver_kwargs()
        assert kwargs["sslmode"] == "require"
        assert kwargs["keepalives_idle"] == 30
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"  # SecretStr unwrapped
