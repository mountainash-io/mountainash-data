# tests/test_unit/core/settings/test_descriptors_invariants.py
"""Parametric invariants every registered backend must satisfy.

Runs once per :data:`REGISTRY` entry. New backends get coverage for free.
"""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth.base import AuthSpec
from mountainash_data.core.settings.registry import REGISTRY


def _ids(params):
    return [name for name, _ in params]


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
