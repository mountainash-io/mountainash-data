# tests/test_unit/core/settings/backends/test_impala.py
from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.impala import (
    ImpalaAuthSettings,
    ImpalaAuthMechanism,
)


@pytest.mark.unit
class TestImpalaAuthSettings:
    def _minimal(self, **extra):
        return ImpalaAuthSettings(
            HOST="impala.example.com",
            auth=NoAuth(),
            **extra,
        )

    def test_provider_type(self):
        assert self._minimal().provider_type == CONST_DB_PROVIDER_TYPE.IMPALA

    def test_default_port(self):
        assert self._minimal().PORT == 21050

    def test_default_database(self):
        assert self._minimal().DATABASE == "default"

    def test_default_auth_mechanism(self):
        assert self._minimal().AUTH_MECHANISM == ImpalaAuthMechanism.NOSASL

    def test_auth_mechanism_enum_enforced(self):
        with pytest.raises(ValidationError):
            self._minimal(AUTH_MECHANISM="nonsense")

    def test_gssapi_mechanism(self):
        s = self._minimal(AUTH_MECHANISM=ImpalaAuthMechanism.GSSAPI)
        kwargs = s.to_driver_kwargs()
        assert kwargs["auth_mechanism"] == "GSSAPI"

    def test_ldap_with_password(self):
        s = ImpalaAuthSettings(
            HOST="impala.example.com",
            auth=PasswordAuth(username="user", password=SecretStr("pass")),
            AUTH_MECHANISM=ImpalaAuthMechanism.LDAP,
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["auth_mechanism"] == "LDAP"
        assert kwargs["user"] == "user"
        assert kwargs["password"] == "pass"

    def test_to_driver_kwargs_plumbs_ssl(self):
        kwargs = self._minimal(USE_SSL=True).to_driver_kwargs()
        assert kwargs["use_ssl"] is True

    def test_ibis_dialect(self):
        assert self._minimal().backend == "impala"
