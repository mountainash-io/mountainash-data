"""Backend-agnostic table location — the Namespace value object.

A location has two dimensions kept in NAMED fields so nothing is inferred
from tuple position: `path` (the schema/namespace levels between catalog and
table) and `catalog` (the top-level catalog, always explicit). Rendering to a
backend's native form lives with each backend, never here.
"""

from __future__ import annotations

from dataclasses import dataclass
import typing as t

NamespaceLike = t.Union["Namespace", str, tuple, None]


@dataclass(frozen=True)
class Namespace:
    """A backend-agnostic table location.

    `path` is the schema/namespace path (the levels between catalog and table).
    `catalog` is the top-level catalog, always explicit — never encoded by tuple
    position. `None`/empty mean "the connection's current/default".
    """

    path: tuple[str, ...] = ()
    catalog: t.Optional[str] = None

    def __post_init__(self) -> None:
        if any(not isinstance(p, str) or p == "" for p in self.path):
            raise ValueError(
                f"Namespace.path segments must be non-empty strings: {self.path!r}"
            )

    @property
    def is_default(self) -> bool:
        return not self.path and self.catalog is None

    @property
    def dotted(self) -> str:
        """Human-readable `catalog.level1.level2` (for messages/logging only)."""
        return ".".join(p for p in ((self.catalog,) + self.path) if p)

    @classmethod
    def coerce(cls, value: NamespaceLike) -> "Namespace":
        if value is None:
            return cls()
        if isinstance(value, Namespace):
            return value
        if isinstance(value, str):
            return cls(path=(value,))
        if isinstance(value, tuple):
            return cls(path=value)
        raise TypeError(f"Cannot coerce {value!r} to Namespace")
