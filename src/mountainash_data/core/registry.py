"""Backend registry — populated in Phase 4 once IbisBackend and
IcebergBackend exist. This module is intentionally a placeholder for
now so that imports from core.registry don't break across phases."""

from __future__ import annotations

import typing as t

from mountainash_data.core.protocol import Backend

_REGISTRY: dict[str, t.Callable[..., Backend]] = {}


def register(name: str, factory: t.Callable[..., Backend]) -> None:
    """Register a backend factory under a name."""
    _REGISTRY[name] = factory


def get(name: str, **config: t.Any) -> Backend:
    """Look up and instantiate a backend by name."""
    if name not in _REGISTRY:
        raise KeyError(
            f"No backend registered as {name!r}. "
            f"Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name](**config)


def names() -> list[str]:
    return sorted(_REGISTRY)
