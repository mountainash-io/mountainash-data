"""Base class for discriminated-union auth specifications."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

__all__ = ["AuthSpec"]


class AuthSpec(BaseModel):
    """Abstract tagged base for authentication specifications.

    Each concrete subclass declares its own ``kind: Literal["..."]`` field;
    this base intentionally does not declare one. When composed into a
    :class:`pydantic.Field` discriminated union, pydantic looks up ``kind``
    on each member, not on a shared base, so removing it here also closes
    Pyright's ``reportIncompatibleVariableOverride`` warnings on every
    subclass.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
