"""BackendProfile — database-flavored subclass of Profile.

Pure L1 config emitter. Auth is orthogonal — applied by ConnectionFactory,
never here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mountainash_settings import lookup_class_var
from mountainash_settings.profiles import Profile

__all__ = ["BackendProfile", "UrlParts"]


@dataclass(frozen=True)
class UrlParts:
    """Credential-free URL skeleton (L1). Every authority component optional."""
    scheme: str
    database: str | None = None
    host: str | None = None
    port: int | None = None
    path: str | None = None
    query: dict[str, str] = field(default_factory=dict)


class BackendProfile(Profile):
    """Database backend CONFIG. Pure L1 emitter — no auth methods.

    Auth is orthogonal, applied by ConnectionFactory, never here.
    """

    def to_url_parts(self) -> UrlParts:
        desc = lookup_class_var(type(self), "__spec__")
        scheme = getattr(desc, "connection_string_scheme", None)
        if scheme is None:
            raise NotImplementedError(f"Profile {self.backend!r} has no URL form")
        scheme = scheme.removesuffix("://").removesuffix(":")
        return UrlParts(
            scheme=scheme,
            host=getattr(self, "HOST", None),
            port=getattr(self, "PORT", None),
            database=getattr(self, "DATABASE", None),
        )
