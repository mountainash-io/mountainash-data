"""Snowflake backend settings tests."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    CertificateAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)
from mountainash_data.core.settings.snowflake import (
    SnowflakeAuthenticator,
    SnowflakeAuthSettings,
)


@pytest.mark.unit
class TestSnowflakeAuthSettings:
    def _minimal(self, auth, **extra):
        return SnowflakeAuthSettings(
            ACCOUNT="acc", WAREHOUSE="wh", auth=auth, **extra,
        )

    def test_authenticator_enum_has_no_whitespace(self):
        """Audit regression: enum values had trailing spaces."""
        assert SnowflakeAuthenticator.SNOWFLAKE.value == "snowflake"
        assert SnowflakeAuthenticator.PASSWORD_MFA.value == "username_password_mfa"

    def test_password_auth(self):
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["account"] == "acc"
        assert kwargs["warehouse"] == "wh"
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_role_is_plumbed(self):
        """Audit regression: ROLE was declared but never emitted."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            ROLE="analyst",
        )
        assert s.to_driver_kwargs()["role"] == "analyst"

    def test_timezone_goes_to_session_parameters(self):
        """Audit regression: TIMEZONE was top-level, should be in session_parameters."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            TIMEZONE="UTC",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["session_parameters"] == {"TIMEZONE": "UTC"}

    def test_certificate_auth(self):
        s = self._minimal(
            auth=CertificateAuth(private_key=SecretStr("KEYCONTENT"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["private_key"] == "KEYCONTENT"
