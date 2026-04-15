"""Base class for discriminated-union auth specifications."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

__all__ = ["AuthSpec"]


class AuthSpec(BaseModel):
    """Abstract tagged base for authentication specifications.

    Each concrete subclass sets a ``kind`` Literal used as the pydantic
    discriminator value when composed into a union.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
