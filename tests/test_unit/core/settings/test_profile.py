"""Tests for ConnectionProfile — database-flavored DescriptorProfile.

DescriptorProfile mechanism tests live in mountainash-settings. Here we only
exercise the database-specific methods: to_driver_kwargs() and
to_connection_string().
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import NoAuth, PasswordAuth
from mountainash_data.core.settings.descriptor import (
    BackendDescriptor,
    ParameterSpec,
)
from mountainash_data.core.settings.profile import ConnectionProfile


DUMMY_DESCRIPTOR = BackendDescriptor(
    name="dummy",
    provider_type="dummy",
    default_port=9999,
    connection_string_scheme="dummy://",
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=9999, driver_key="port"),
        ParameterSpec(name="DATABASE", type=str, tier="core", default=None,
                      driver_key="database"),
    ],
    auth_modes=[NoAuth, PasswordAuth],
)


class DummyProfile(ConnectionProfile):
    __descriptor__ = DUMMY_DESCRIPTOR


@pytest.mark.unit
class TestConnectionProfile:
    def test_to_driver_kwargs_default(self):
        p = DummyProfile(HOST="h", PORT=1234, DATABASE="db", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert kwargs["host"] == "h"
        assert kwargs["port"] == 1234
        assert kwargs["database"] == "db"

    def test_to_driver_kwargs_password_unwrapped(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        kwargs = p.to_driver_kwargs()
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_to_driver_kwargs_adapter_owns_pipeline(self):
        def _adapter(profile):
            return {"only": "thing"}

        class Adapted(ConnectionProfile):
            __descriptor__ = DUMMY_DESCRIPTOR
            __adapter__ = staticmethod(_adapter)

        p = Adapted(HOST="h", auth=NoAuth())
        assert p.to_driver_kwargs() == {"only": "thing"}

    def test_to_connection_string_full(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        url = p.to_connection_string()
        assert url == "dummy://u:p@h:9999/db"

    def test_to_connection_string_url_encodes_secrets(self):
        p = DummyProfile(
            HOST="h", DATABASE="db",
            auth=PasswordAuth(username="user@corp", password=SecretStr("p@ss:w/ord")),
        )
        url = p.to_connection_string()
        assert "user%40corp" in url
        assert "p%40ss%3Aw%2Ford" in url

    def test_to_connection_string_no_scheme_raises(self):
        desc = BackendDescriptor(
            name="x", provider_type="x", parameters=[], auth_modes=[NoAuth],
            connection_string_scheme=None,
        )

        class P(ConnectionProfile):
            __descriptor__ = desc

        p = P(auth=NoAuth())
        with pytest.raises(NotImplementedError):
            p.to_connection_string()
