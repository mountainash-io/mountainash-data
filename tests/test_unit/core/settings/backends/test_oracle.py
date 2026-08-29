# tests/test_unit/core/settings/backends/test_oracle.py
from __future__ import annotations

import pytest

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.oracle import OracleBackendProfile


@pytest.mark.unit
class TestOracleBackendProfile:
    def _minimal(self, **extra):
        return OracleBackendProfile(HOST="h", DATABASE="d", **extra)

    def test_provider_type_is_oracle(self):
        s = self._minimal()
        assert s.provider_type == CONST_DB_PROVIDER_TYPE.ORACLE

    def test_default_port(self):
        s = self._minimal()
        assert s.PORT == 1521

    def test_emit_plumbs_host_and_database(self):
        """DATABASE maps to ibis's ``database`` kwarg, which the Oracle ibis
        backend treats as the connection's service_name."""
        s = self._minimal()
        kwargs = s.emit(CONST_DB_PROVIDER_TYPE.ORACLE)
        assert kwargs["host"] == "h"
        assert kwargs["database"] == "d"
        assert kwargs["port"] == 1521

    def test_sid_optional_and_unset_by_default(self):
        s = self._minimal()
        assert s.SID is None

    def test_sid_stored_and_plumbed(self):
        s = self._minimal(SID="ORCL")
        kwargs = s.emit(CONST_DB_PROVIDER_TYPE.ORACLE)
        assert kwargs["sid"] == "ORCL"

    def test_ibis_dialect_is_oracle(self):
        assert OracleBackendProfile.__spec__.ibis_dialect == "oracle"

    def test_connection_string_scheme(self):
        assert OracleBackendProfile.__spec__.connection_string_scheme == "oracle://"

    def test_only_password_auth_supported(self):
        from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile

        assert OracleBackendProfile.__spec__.supported_auth == (PasswordAuthProfile,)
        assert NoAuthProfile not in OracleBackendProfile.__spec__.supported_auth
