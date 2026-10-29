"""Shared auth adapter for flat user/password SQL backends."""
from __future__ import annotations
import typing as t


def userpass(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}
