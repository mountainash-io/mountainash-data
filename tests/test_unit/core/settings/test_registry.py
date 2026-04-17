"""Unit tests for the backend registry."""

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.descriptor import BackendDescriptor
from mountainash_data.core.settings.profile import ConnectionProfile
from mountainash_data.core.settings.registry import (
    REGISTRY,
    _reset_for_tests,
    _snapshot_for_tests,
    get_descriptor,
    get_settings_class,
    register,
)


@pytest.mark.unit
class TestRegistry:
    def setup_method(self):
        self._snapshot = _snapshot_for_tests()

    def teardown_method(self):
        _reset_for_tests(*self._snapshot)

    def test_register_inserts_into_registry(self):
        desc = BackendDescriptor(
            name="my_backend", provider_type="my_backend",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class MyProfile(ConnectionProfile):
            __descriptor__ = desc

        assert REGISTRY["my_backend"] is desc
        assert MyProfile.__descriptor__ is desc

    def test_get_descriptor_returns_registered(self):
        desc = BackendDescriptor(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class X(ConnectionProfile):
            __descriptor__ = desc

        assert get_descriptor("x") is desc

    def test_get_descriptor_unknown_raises(self):
        with pytest.raises(KeyError):
            get_descriptor("not_a_real_backend")

    def test_get_settings_class_returns_class(self):
        desc = BackendDescriptor(
            name="y", provider_type="y",
            parameters=[], auth_modes=[NoAuth],
        )

        @register(desc)
        class YProfile(ConnectionProfile):
            __descriptor__ = desc

        assert get_settings_class("y") is YProfile

    def test_register_rejects_duplicate_name(self):
        desc1 = BackendDescriptor(name="dup", provider_type="dup",
                                  parameters=[], auth_modes=[NoAuth])
        desc2 = BackendDescriptor(name="dup", provider_type="dup",
                                  parameters=[], auth_modes=[NoAuth])

        @register(desc1)
        class P1(ConnectionProfile):
            __descriptor__ = desc1

        with pytest.raises(ValueError, match="already registered"):
            @register(desc2)
            class P2(ConnectionProfile):
                __descriptor__ = desc2

    def test_get_settings_class_unknown_raises(self):
        with pytest.raises(KeyError):
            get_settings_class("not_a_real_backend")

    def test_register_duplicate_does_not_pollute_classes_dict(self):
        """REGISTRY and _CLASSES stay in sync after a rejected duplicate."""
        desc1 = BackendDescriptor(name="inv", provider_type="inv",
                                  parameters=[], auth_modes=[NoAuth])
        desc2 = BackendDescriptor(name="inv", provider_type="inv",
                                  parameters=[], auth_modes=[NoAuth])

        @register(desc1)
        class First(ConnectionProfile):
            __descriptor__ = desc1

        with pytest.raises(ValueError):
            @register(desc2)
            class Second(ConnectionProfile):
                __descriptor__ = desc2

        # Both dicts still map 'inv' to First — no leak of Second
        assert get_settings_class("inv") is First
        assert get_descriptor("inv") is desc1
