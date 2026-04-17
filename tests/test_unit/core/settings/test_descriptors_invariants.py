# tests/test_unit/core/settings/test_descriptors_invariants.py
"""Parametric invariants every registered backend must satisfy.

Runs once per :data:`REGISTRY` entry. New backends get coverage for free.
"""

from __future__ import annotations

import pytest

# Ensure every backend module that calls @register is imported before we
# snapshot REGISTRY for the parametrize decorator. Today this is a no-op
# (no backends registered yet); Task 19 wires __init__.py re-exports that
# trigger @register at import time.
import mountainash_data.core.settings  # noqa: F401

from mountainash_data.core.settings.auth.base import AuthSpec
from mountainash_data.core.settings.registry import REGISTRY


@pytest.mark.unit
@pytest.mark.parametrize(
    "name,descriptor",
    list(REGISTRY.items()),
    ids=list(REGISTRY.keys()) or [""],
)
class TestDescriptorInvariants:
    def test_name_matches_registry_key(self, name, descriptor):
        assert descriptor.name == name

    def test_parameter_names_unique(self, name, descriptor):
        names = [p.name for p in descriptor.parameters]
        assert len(names) == len(set(names)), f"duplicate param in {name}"

    def test_driver_keys_unique(self, name, descriptor):
        keys = [p.driver_key for p in descriptor.parameters if p.driver_key]
        assert len(keys) == len(set(keys)), f"duplicate driver_key in {name}"

    def test_parameter_tiers_valid(self, name, descriptor):
        for p in descriptor.parameters:
            assert p.tier in {"core", "advanced"}, (
                f"{name}.{p.name} has invalid tier {p.tier!r}"
            )

    def test_auth_modes_are_authspec_subclasses(self, name, descriptor):
        for mode in descriptor.auth_modes:
            assert issubclass(mode, AuthSpec), (
                f"{name}.auth_modes contains non-AuthSpec: {mode}"
            )

    def test_provider_type_is_not_none(self, name, descriptor):
        assert descriptor.provider_type is not None, (
            f"{name} has no provider_type"
        )

    def test_name_is_lowercase_nonempty(self, name, descriptor):
        assert descriptor.name, f"{name}: BackendDescriptor.name is empty"
        assert descriptor.name == descriptor.name.lower(), (
            f"{name}: BackendDescriptor.name must be lowercase"
        )

    def test_auth_modes_nonempty(self, name, descriptor):
        assert descriptor.auth_modes, (
            f"{name}: auth_modes is empty — use [NoAuth] for no-auth backends"
        )

    def test_parameter_names_are_uppercase(self, name, descriptor):
        for p in descriptor.parameters:
            assert p.name == p.name.upper(), (
                f"{name}.{p.name}: ParameterSpec.name must be UPPERCASE"
            )
            assert p.name, f"{name}: ParameterSpec.name is empty"

    def test_default_port_in_valid_range(self, name, descriptor):
        if descriptor.default_port is None:
            return
        assert isinstance(descriptor.default_port, int), (
            f"{name}: default_port must be int, got {type(descriptor.default_port)}"
        )
        assert 1 <= descriptor.default_port <= 65535, (
            f"{name}: default_port {descriptor.default_port} out of TCP range"
        )
