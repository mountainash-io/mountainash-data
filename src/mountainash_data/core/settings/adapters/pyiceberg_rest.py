"""PyIceberg REST adapters."""
from __future__ import annotations
import typing as t


def headers_compose(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.HEADERS:
        for hk, hv in profile.HEADERS.items():
            out[f"header.{hk}"] = hv
    return out


def token(auth, base):
    return {**base, "token": auth.TOKEN.get_secret_value()}
