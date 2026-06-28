"""Auth adapter for MotherDuck (token → driver kwarg)."""
from __future__ import annotations
import typing as t


def token(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    """Map TokenAuthProfile → MotherDuck driver kwarg ``token``."""
    return {**base, "token": auth.TOKEN.get_secret_value()}
