"""Module-level registry of database backend descriptors.

Backed by :class:`mountainash_settings.profiles.Registry` — a per-domain
registry class. The old module-level ``REGISTRY`` dict is preserved as a
property-style alias for any downstream consumer that imports it directly.
"""

from __future__ import annotations

import typing as t
from collections.abc import Mapping

from mountainash_settings.profiles import Registry

if t.TYPE_CHECKING:
    from mountainash_settings.profiles import ProfileDescriptor
    from .profile import ConnectionProfile

__all__ = [
    "DATABASES_REGISTRY",
    "REGISTRY",
    "_reset_for_tests",
    "_snapshot_for_tests",
    "get_descriptor",
    "get_settings_class",
    "register",
]

DATABASES_REGISTRY = Registry("databases")

register = DATABASES_REGISTRY.decorator()


def get_descriptor(name: str) -> "ProfileDescriptor":
    return DATABASES_REGISTRY.get_descriptor(name)


def get_settings_class(name: str) -> type["ConnectionProfile"]:
    return DATABASES_REGISTRY.get_settings_class(name)  # type: ignore[return-value]


# Backwards-compatibility alias — preserves ``from ... import REGISTRY`` imports.
# Read-only from the outside; mutations should go through ``@register``.
class _RegistryDictView(Mapping):
    """Dict-like view that delegates to DATABASES_REGISTRY.descriptors."""

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in DATABASES_REGISTRY

    def __getitem__(self, name: str) -> "ProfileDescriptor":
        return DATABASES_REGISTRY.get_descriptor(name)

    def __iter__(self) -> t.Iterator[str]:
        return iter(DATABASES_REGISTRY.descriptors)

    def __len__(self) -> int:
        return len(DATABASES_REGISTRY)

    def items(self) -> t.ItemsView[str, "ProfileDescriptor"]:
        return DATABASES_REGISTRY.descriptors.items()

    def keys(self) -> t.KeysView[str]:
        return DATABASES_REGISTRY.descriptors.keys()

    def values(self) -> t.ValuesView["ProfileDescriptor"]:
        return DATABASES_REGISTRY.descriptors.values()


REGISTRY = _RegistryDictView()


# Test seams — thin wrappers around the Registry instance methods so test
# modules can import them as module-level names.

def _snapshot_for_tests() -> tuple[dict, dict]:
    """Return a snapshot of the registry state for test isolation."""
    return DATABASES_REGISTRY._snapshot_for_tests()


def _reset_for_tests(descriptors_snapshot: dict, classes_snapshot: dict) -> None:
    """Restore registry state from a prior snapshot (test-only)."""
    DATABASES_REGISTRY._reset_for_tests(descriptors_snapshot, classes_snapshot)
