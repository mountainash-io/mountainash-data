"""Module-level registry of backend descriptors and settings classes."""

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
    """
    if descriptor.name in REGISTRY:
        raise ValueError(
            f"Backend {descriptor.name!r} is already registered"
        )

    def _wrap(cls: type[T]) -> type[T]:
        REGISTRY[descriptor.name] = descriptor
        _CLASSES[descriptor.name] = cls
        cls.__descriptor__ = descriptor  # belt-and-braces
        return cls

    return _wrap


def get_descriptor(name: str) -> BackendDescriptor:
    """Return the :class:`BackendDescriptor` for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    return REGISTRY[name]


def get_settings_class(name: str) -> type[ConnectionProfile]:
    """Return the registered settings class for ``name``.

    Raises:
        KeyError: if ``name`` is not registered.
    """
    return _CLASSES[name]
