"""Trino backend settings tests.

Tests migration with auth-wrapper adapter.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    JWTAuth,
    KerberosAuth,
    NoAuth,
    PasswordAuth,
)
from mountainash_data.core.settings.trino import TrinoAuthSettings


@pytest.mark.unit
class TestTrinoAuthSettings:
    def _minimal(self, auth, **extra):
        return TrinoAuthSettings(HOST="h", CATALOG="c", auth=auth, **extra)

    def test_port_default_8080(self):
        s = self._minimal(auth=NoAuth())
        assert s.PORT == 8080

    def test_password_wraps_basic_auth(self):
        """Audit regression: previously emitted bare `password=` kwarg.

        The driver has NO `password` kwarg — it must be wrapped.
        """
        pytest.importorskip("trino")
        from trino.auth import BasicAuthentication

        s = self._minimal(
            auth=PasswordAuth(username="alice", password=SecretStr("pw"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["user"] == "alice"
        assert isinstance(kwargs["auth"], BasicAuthentication)
        assert "password" not in kwargs  # must NOT be bare

    def test_jwt_auth_wraps(self):
        pytest.importorskip("trino")
        from trino.auth import JWTAuthentication

        s = self._minimal(auth=JWTAuth(token=SecretStr("tok")))
        kwargs = s.to_driver_kwargs()
        assert isinstance(kwargs["auth"], JWTAuthentication)

    def test_noauth_no_auth_key(self):
        s = self._minimal(auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert "auth" not in kwargs
