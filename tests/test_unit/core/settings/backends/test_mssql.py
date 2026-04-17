# tests/test_unit/core/settings/backends/test_mssql.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    AzureADAuth,
    PasswordAuth,
    WindowsAuth,
)
from mountainash_data.core.settings.mssql import (
    MSSQLAuthSettings,
    MSSQLEncryption,
)


@pytest.mark.unit
class TestMSSQLAuthSettings:
    def _minimal(self, auth, **extra):
        return MSSQLAuthSettings(HOST="h", DATABASE="d", auth=auth, **extra)

    def test_password_auth(self):
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"
        assert kwargs["host"] == "h"

    def test_windows_auth_sets_trusted_connection(self):
        s = self._minimal(auth=WindowsAuth(username="u", domain="CORP"))
        kwargs = s.to_driver_kwargs()
        assert kwargs["trusted_connection"] == "yes"
        assert kwargs["user"] == r"CORP\u"

    def test_azure_ad_managed_identity(self):
        """Audit regression: AZURE_MANAGED_IDENTITY/MSI_ENDPOINT were
        referenced but not declared — now live on AzureADAuth."""
        s = self._minimal(
            auth=AzureADAuth(
                managed_identity=True,
                msi_endpoint="http://169.254.169.254/",
            )
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["authentication"] == "ActiveDirectoryMsi"
        assert kwargs["msi_endpoint"] == "http://169.254.169.254/"

    def test_instance_name_appended_to_host(self):
        """Audit regression: code referenced args['server'] (KeyError)."""
        s = self._minimal(
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            INSTANCE_NAME="SQLEXPRESS",
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["host"] == r"h\SQLEXPRESS"

    def test_encryption_default(self):
        """Audit regression: ODBC Driver 18 default Encrypt=Yes requires explicit setting."""
        s = self._minimal(auth=PasswordAuth(username="u", password=SecretStr("p")))
        assert s.ENCRYPTION is MSSQLEncryption.MANDATORY
