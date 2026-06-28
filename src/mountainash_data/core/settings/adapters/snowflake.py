"""Snowflake adapters."""
from __future__ import annotations
import typing as t


def session_params(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    params: dict[str, t.Any] = {}
    if profile.QUERY_TAG is not None:
        params["QUERY_TAG"] = profile.QUERY_TAG
    if profile.TIMEZONE is not None:
        params["TIMEZONE"] = profile.TIMEZONE
    if params:
        out["session_parameters"] = params
    return out
