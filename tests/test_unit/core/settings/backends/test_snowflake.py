"""Snowflake backend settings tests."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.snowflake import (
    SnowflakeAuthenticator,
    SnowflakeBackendProfile,
)


@pytest.mark.unit
class TestSnowflakeBackendProfile:
    def _minimal(self, **extra):
        return SnowflakeBackendProfile(ACCOUNT="acc", WAREHOUSE="wh", **extra)

    def test_authenticator_enum_has_no_whitespace(self):
        """Audit regression: enum values had trailing spaces."""
        assert SnowflakeAuthenticator.SNOWFLAKE.value == "snowflake"
        assert SnowflakeAuthenticator.PASSWORD_MFA.value == "username_password_mfa"

    def test_emit_plumbs_account_and_warehouse(self):
        s = self._minimal()
        kwargs = s.emit()
        assert kwargs["account"] == "acc"
        assert kwargs["warehouse"] == "wh"

    def test_role_is_plumbed(self):
        """Audit regression: ROLE was declared but never emitted."""
        s = self._minimal(ROLE="analyst")
        assert s.emit()["role"] == "analyst"

    def test_timezone_stored(self):
        """Audit regression: TIMEZONE was top-level."""
        s = self._minimal(TIMEZONE="UTC")
        assert s.TIMEZONE == "UTC"
