# tests/test_unit/core/settings/backends/test_redshift.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import IAMAuth, PasswordAuth
from mountainash_data.core.settings.redshift import (
    RedshiftAuthSettings,
    RedshiftSSLMode,
)


@pytest.mark.unit
class TestRedshiftAuthSettings:
    def _password(self, **extra):
        return RedshiftAuthSettings(
            HOST="cluster.abc.us-east-1.redshift.amazonaws.com",
            DATABASE="dev",
            REGION="us-east-1",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
            **extra,
        )

    def test_port_default_5439(self):
        s = self._password()
        assert s.PORT == 5439

    def test_region_govcloud_accepted(self):
        """Audit regression: region regex rejected GovCloud."""
        s = RedshiftAuthSettings(
            HOST="h", DATABASE="d", REGION="us-gov-west-1",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        assert s.REGION == "us-gov-west-1"

    def test_role_arn_govcloud_accepted(self):
        """Audit regression: role-ARN regex rejected non-commercial partitions."""
        s = self._password(IAM_ROLE_ARN="arn:aws-us-gov:iam::123456789012:role/x")
        assert s.IAM_ROLE_ARN.startswith("arn:aws-us-gov:")

    def test_iam_auth(self):
        s = RedshiftAuthSettings(
            HOST="h", DATABASE="d", REGION="us-east-1",
            auth=IAMAuth(
                access_key_id="AKIA", secret_access_key=SecretStr("sk"),
            ),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["aws_access_key_id"] == "AKIA"
        assert kwargs["aws_secret_access_key"] == "sk"

    def test_ssl_mode_enum(self):
        """Audit regression: SSL was bool, hardcoded verify-full."""
        s = self._password(SSL_MODE=RedshiftSSLMode.REQUIRE)
        assert s.to_driver_kwargs()["sslmode"] == "require"
