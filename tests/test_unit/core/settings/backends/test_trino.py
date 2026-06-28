"""Trino backend settings tests."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.trino import TrinoBackendProfile


@pytest.mark.unit
class TestTrinoBackendProfile:
    def _minimal(self, **extra):
        return TrinoBackendProfile(HOST="h", CATALOG="c", **extra)

    def test_port_default_8080(self):
        s = self._minimal()
        assert s.PORT == 8080

    def test_emit_core_fields(self):
        s = self._minimal()
        kwargs = s.emit()
        assert kwargs["host"] == "h"
        assert kwargs["catalog"] == "c"
        assert kwargs["port"] == 8080

    def test_http_scheme_default_https(self):
        s = self._minimal()
        assert s.HTTP_SCHEME == "https"

    def test_noauth_emits_no_auth_key(self):
        """Profile emit() must not include an 'auth' key — auth is orthogonal."""
        s = self._minimal()
        kwargs = s.emit()
        assert "auth" not in kwargs
        assert "password" not in kwargs
