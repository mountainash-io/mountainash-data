"""Redshift auth adapter functions."""
from __future__ import annotations
import typing as t


def password(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}

