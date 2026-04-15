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

    # --- Item 4: adapter replaces pipeline output --------------------------------

    def test_adapter_replaces_pipeline_output(self):
        """When __adapter__ is set, it owns the full kwargs pipeline."""
        def _adapter(profile: "ConnectionProfile") -> dict:
            # Adapter can still call the default helpers if it wants
            kwargs = profile._default_driver_kwargs()
            kwargs["adapter_added"] = True
            return kwargs

        class AdaptedProfile(ConnectionProfile):
            __descriptor__ = DUMMY_DESCRIPTOR
            __adapter__ = staticmethod(_adapter)

        p = AdaptedProfile(HOST="h", auth=NoAuth())
        kwargs = p.to_driver_kwargs()
        assert kwargs["host"] == "h"
        assert kwargs["adapter_added"] is True

    def test_adapter_can_return_fresh_dict(self):
        """Adapter return value is used verbatim; it need not extend defaults."""
        class FreshProfile(ConnectionProfile):
            __descriptor__ = DUMMY_DESCRIPTOR
            __adapter__ = staticmethod(lambda self: {"only_key": "only_val"})

        p = FreshProfile(HOST="h", auth=NoAuth())
        assert p.to_driver_kwargs() == {"only_key": "only_val"}

    # --- Item 5: ParameterSpec.transform is applied ------------------------------

    def test_parameter_spec_transform_is_applied(self):
        """transform= is applied at the kwargs boundary."""
        desc = BackendDescriptor(
            name="tf",
            provider_type="tf",
            auth_modes=[NoAuth],
            parameters=[
                ParameterSpec(
                    name="FLAG", type=bool, tier="core",
                    default=True, driver_key="flag",
                    transform=lambda v: 1 if v else 0,
                ),
            ],
        )

        class P(ConnectionProfile):
            __descriptor__ = desc

        p = P(auth=NoAuth())
        assert p.to_driver_kwargs() == {"flag": 1}

        p2 = P(FLAG=False, auth=NoAuth())
        assert p2.to_driver_kwargs() == {"flag": 0}

    # --- Item 6: URL-encoded password in to_connection_string --------------------

    def test_to_connection_string_url_encodes_password(self):
        """Password special chars must be URL-encoded, not passed raw."""
        p = DummyProfile(
            HOST="h",
            auth=PasswordAuth(username="user@corp", password=SecretStr("p@ss:w/ord")),
        )
        url = p.to_connection_string()
        # '@' in username → %40; ':', '@', '/' in password → %3A, %40, %2F
        assert "user%40corp" in url
        assert "p%40ss%3Aw%2Ford" in url
