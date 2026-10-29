"""Databricks auth adapter functions."""
from __future__ import annotations
import typing as t


def token(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "access_token": auth.TOKEN.get_secret_value()}


def password(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "username": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}
