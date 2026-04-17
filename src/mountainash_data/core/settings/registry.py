"""Module-level registry of backend descriptors and settings classes.

Registration happens at import time only; runtime re-registration is
unsupported. Mutating ``REGISTRY`` directly bypasses the duplicate-name
check — always use :func:`register`.
"""

from __future__ import annotations

import typing as t

from .descriptor import BackendDescriptor
from .profile import ConnectionProfile

__all__ = ["REGISTRY", "register", "get_descriptor", "get_settings_class"]

REGISTRY: dict[str, BackendDescriptor] = {}
_CLASSES: dict[str, type[ConnectionProfile]] = {}


T = t.TypeVar("T", bound=ConnectionProfile)


def register(
    descriptor: BackendDescriptor,
) -> t.Callable[[type[T]], type[T]]:
    """Class decorator that registers a :class:`ConnectionProfile` subclass.

    Raises:
        ValueError: if ``descriptor.name`` is already registered.

    Note:
        Subclasses must still declare ``__descriptor__ = desc`` in the class
        body for pydantic field materialization — ``ConnectionProfile``'s
        ``__pydantic_init_subclass__`` hook reads ``__descriptor__`` at class
        creation time, before this decorator runs. The decorator's
        ``cls.__descriptor__`` assignment is a post-hoc safety net only.
    """
    if descriptor.name in REGISTRY:
        existing = _CLASSES.get(descriptor.name)
        where = (
            f"{existing.__module__}.{existing.__qualname__}"
            if existing is not None
            else "<unknown class>"
        )
        raise ValueError(
            f"Backend {descriptor.name!r} is already registered by {where}"
        )

    def _wrap(cls: type[T]) -> type[T]:
        REGISTRY[descriptor.name] = descriptor
        _CLASSES[descriptor.name] = cls
        cls.__descriptor__ = descriptor  # optional: class body usually sets this; this line is a no-op safety net
        return cls

    return _wrap


def get_descriptor(name: str) -> BackendDescriptor:
    """Return the :class:`BackendDescriptor` for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    try:
        return REGISTRY[name]
    except KeyError:
        known = ", ".join(sorted(REGISTRY)) or "<none>"
        raise KeyError(
            f"No backend registered under {name!r}. Known: {known}"
        ) from None


def get_settings_class(name: str) -> type[ConnectionProfile]:
    """Return the registered settings class for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    try:
        return _CLASSES[name]
    except KeyError:
        known = ", ".join(sorted(_CLASSES)) or "<none>"
        raise KeyError(
            f"No settings class registered under {name!r}. Known: {known}"
        ) from None


def _reset_for_tests(
    registry_snapshot: dict[str, BackendDescriptor],
    classes_snapshot: dict[str, type[ConnectionProfile]],
) -> None:
    """Restore REGISTRY and _CLASSES to snapshots (test-only helper)."""
    REGISTRY.clear()
    REGISTRY.update(registry_snapshot)
    _CLASSES.clear()
    _CLASSES.update(classes_snapshot)


def _snapshot_for_tests() -> tuple[
    dict[str, BackendDescriptor],
    dict[str, type[ConnectionProfile]],
]:
    """Return a copy of REGISTRY and _CLASSES for later restore."""
    return REGISTRY.copy(), _CLASSES.copy()
