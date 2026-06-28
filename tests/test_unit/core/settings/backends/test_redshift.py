# tests/test_unit/core/settings/backends/test_redshift.py
from __future__ import annotations

import pytest
from pydantic import ValidationError

from mountainash_data.core.settings.redshift import (
    RedshiftBackendProfile,
    RedshiftSSLMode,
)


@pytest.mark.unit
class TestRedshiftBackendProfile:
    def _minimal(self, **extra):
        return RedshiftBackendProfile(
            HOST="cluster.abc.us-east-1.redshift.amazonaws.com",
            DATABASE="dev",
            REGION="us-east-1",
            **extra,
        )

    def test_port_default_5439(self):
        s = self._minimal()
        assert s.PORT == 5439

    def test_region_govcloud_accepted(self):
        """Audit regression: region regex rejected GovCloud."""
        s = RedshiftBackendProfile(
            HOST="h", DATABASE="d", REGION="us-gov-west-1",
        )
        assert s.REGION == "us-gov-west-1"

    def test_role_arn_govcloud_accepted(self):
        """Audit regression: role-ARN regex rejected non-commercial partitions."""
        s = self._minimal(IAM_ROLE_ARN="arn:aws-us-gov:iam::123456789012:role/x")
        assert s.IAM_ROLE_ARN.startswith("arn:aws-us-gov:")

    def test_emit_plumbs_host_and_port(self):
        s = self._minimal()
        kwargs = s.emit()
        assert kwargs["host"] == "cluster.abc.us-east-1.redshift.amazonaws.com"
        assert kwargs["port"] == 5439

    def test_ssl_mode_enum(self):
        """Audit regression: SSL was bool, hardcoded verify-full."""
        s = self._minimal(SSL_MODE=RedshiftSSLMode.REQUIRE)
        assert s.emit()["sslmode"] == "require"
