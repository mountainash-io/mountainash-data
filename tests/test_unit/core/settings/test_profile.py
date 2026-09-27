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

    def test_emit_target_adapter_composes_mapped_kwargs_over_base(self):
        def _adapter(profile, kwargs):
            return {
                **kwargs,
                "endpoint": f"{kwargs['host']}:{kwargs['port']}",
                "profile": profile.profile_name,
            }

        class Adapted(BackendProfile):
            __spec__ = DUMMY_SPEC
            __adapters__ = {"dummy": _adapter}

        p = Adapted(HOST="h")
        base = {"timeout": 30, "port": 1234}
        assert p.emit("dummy", base=base) == {
            "timeout": 30, "host": "h", "port": 9999,
            "endpoint": "h:9999", "profile": "dummy",
        }
        assert base == {"timeout": 30, "port": 1234}

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
        with pytest.raises(NotImplementedError, match="Profile 'x' has no URL form"):
            p.to_url_parts()
