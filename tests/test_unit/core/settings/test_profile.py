"""Unit tests for the generic ConnectionProfile base."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

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
        ParameterSpec(name="PASSWORD", type=str, tier="core", secret=True,
                      driver_key="password", default=None),
    ],
    auth_modes=[NoAuth, PasswordAuth],
)


class DummyProfile(ConnectionProfile):
    __descriptor__ = DUMMY_DESCRIPTOR


@pytest.mark.unit
class TestConnectionProfile:
    def test_required_field_enforced(self):
        with pytest.raises(ValidationError):
            DummyProfile(auth=NoAuth())  # HOST missing

    def test_default_used_when_not_provided(self):
        p = DummyProfile(HOST="localhost", auth=NoAuth())
        assert p.PORT == 9999

    def test_to_driver_kwargs_noauth(self):
        p = DummyProfile(HOST="h", PORT=1234, auth=NoAuth())
        assert p.to_driver_kwargs() == {"host": "h", "port": 1234}

    def test_to_driver_kwargs_password_auth_unwraps_secret(self):
        p = DummyProfile(
            HOST="h",
            auth=PasswordAuth(username="u", password=SecretStr("p")),
        )
        kwargs = p.to_driver_kwargs()
        assert kwargs["host"] == "h"
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"

    def test_secret_field_unwrapped_in_driver_kwargs(self):
        p = DummyProfile(HOST="h", PASSWORD="literal-secret", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert kwargs["password"] == "literal-secret"

    def test_none_values_skipped_from_driver_kwargs(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert "password" not in kwargs  # PASSWORD default is None

    def test_provider_type_property(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        assert p.provider_type == "dummy"

    def test_backend_property(self):
        p = DummyProfile(HOST="h", auth=NoAuth())
        assert p.backend == "dummy"

    def test_to_connection_string_raises_when_scheme_none(self):
        desc = BackendDescriptor(
            name="x", provider_type="x", parameters=[], auth_modes=[NoAuth],
            connection_string_scheme=None,
        )

        class P(ConnectionProfile):
            __descriptor__ = desc

        p = P(auth=NoAuth())
        with pytest.raises(NotImplementedError):
            p.to_connection_string()
