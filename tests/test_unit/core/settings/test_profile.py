"""Tests for BackendProfile — database-flavored Profile.

Profile mechanism tests live in mountainash-settings. Here we only
exercise the database-specific methods: emit() and to_url_parts().
"""

from __future__ import annotations

import pytest

from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from mountainash_data.core.settings.descriptor import (
    BackendSpec,
    ParameterSpec,
)
from mountainash_data.core.settings.profile import BackendProfile


DUMMY_SPEC = BackendSpec(
    name="dummy",
    provider_type="dummy",
    default_port=9999,
    connection_string_scheme="dummy://",
    supported_auth=(NoAuthProfile, PasswordAuthProfile),
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=9999, driver_key="port"),
        ParameterSpec(name="DATABASE", type=str, tier="core", default=None,
                      driver_key="database"),
    ],
)


class DummyProfile(BackendProfile):
    __spec__ = DUMMY_SPEC


@pytest.mark.unit
class TestBackendProfile:
    def test_emit_default(self):
        p = DummyProfile(HOST="h", PORT=1234, DATABASE="db")
        kwargs = p.emit()
        assert kwargs["host"] == "h"
        assert kwargs["port"] == 1234
        assert kwargs["database"] == "db"

    def test_emit_adapter_owns_pipeline(self):
        def _adapter(profile):
            return {"only": "thing"}

        class Adapted(BackendProfile):
            __spec__ = DUMMY_SPEC
            __adapter__ = staticmethod(_adapter)

        p = Adapted(HOST="h")
        assert p.emit() == {"only": "thing"}

    def test_to_url_parts_returns_skeleton(self):
        p = DummyProfile(HOST="h", PORT=9999, DATABASE="db")
        parts = p.to_url_parts()
        assert parts.scheme == "dummy"
        assert parts.host == "h"
        assert parts.port == 9999
        assert parts.database == "db"

    def test_to_url_parts_no_scheme_raises(self):
        spec = BackendSpec(
            name="x", provider_type="x",
            supported_auth=(NoAuthProfile,),
            parameters=[],
            connection_string_scheme=None,
        )

        class P(BackendProfile):
            __spec__ = spec

        p = P()
        with pytest.raises(NotImplementedError):
            p.to_url_parts()
