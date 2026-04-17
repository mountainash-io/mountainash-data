"""Unit tests for settings descriptor primitives."""

import pytest
from dataclasses import FrozenInstanceError

from mountainash_data.core.settings.descriptor import (
    BackendDescriptor,
    MISSING,
    ParameterSpec,
)


@pytest.mark.unit
class TestParameterSpec:
    def test_minimal_parameter_spec(self):
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        assert spec.name == "FOO"
        assert spec.type is str
        assert spec.tier == "core"
        assert spec.default is MISSING
        assert spec.driver_key is None
        assert spec.secret is False
        assert spec.transform is None
        assert spec.validator is None

    def test_parameter_spec_is_frozen(self):
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        with pytest.raises(FrozenInstanceError):
            spec.name = "BAR"  # type: ignore[misc]

    def test_parameter_spec_with_default(self):
        spec = ParameterSpec(name="PORT", type=int, tier="core", default=5432)
        assert spec.default == 5432

    def test_parameter_spec_secret_flag(self):
        spec = ParameterSpec(name="PASSWORD", type=str, tier="core", secret=True)
        assert spec.secret is True

    def test_parameter_spec_accepts_advanced_tier(self):
        spec = ParameterSpec(name="X", type=str, tier="advanced")
        assert spec.tier == "advanced"

    def test_missing_sentinel_is_falsy_and_singleton(self):
        from mountainash_data.core.settings.descriptor import _Missing
        assert bool(MISSING) is False
        assert repr(MISSING) == "MISSING"
        assert _Missing() is MISSING


@pytest.mark.unit
class TestBackendDescriptor:
    def test_minimal_descriptor(self):
        desc = BackendDescriptor(
            name="sqlite",
            provider_type="sqlite",
            parameters=[],
            auth_modes=[],
        )
        assert desc.name == "sqlite"
        assert desc.default_port is None
        assert desc.connection_string_scheme is None
        assert desc.ibis_dialect is None
        assert desc.rides_on is None

    def test_descriptor_is_frozen(self):
        desc = BackendDescriptor(
            name="sqlite", provider_type="sqlite", parameters=[], auth_modes=[]
        )
        with pytest.raises(FrozenInstanceError):
            desc.name = "mysql"  # type: ignore[misc]
