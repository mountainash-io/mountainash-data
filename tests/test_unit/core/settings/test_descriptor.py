"""Unit tests for settings descriptor primitives."""

import pytest
from typing import Literal, Optional

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
        with pytest.raises(Exception):
            spec.name = "BAR"  # type: ignore[misc]

    def test_parameter_spec_with_default(self):
        spec = ParameterSpec(name="PORT", type=int, tier="core", default=5432)
        assert spec.default == 5432

    def test_parameter_spec_secret_flag(self):
        spec = ParameterSpec(name="PASSWORD", type=str, tier="core", secret=True)
        assert spec.secret is True

    def test_parameter_spec_tier_must_be_valid(self):
        spec = ParameterSpec(name="FOO", type=str, tier="core")
        assert spec.tier in {"core", "advanced"}


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
        with pytest.raises(Exception):
            desc.name = "mysql"  # type: ignore[misc]
