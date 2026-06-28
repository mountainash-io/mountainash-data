"""Redshift auth adapter functions."""
from __future__ import annotations
import typing as t


def password(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def iam(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = {**base, "iam": True}
    if auth.ROLE_ARN is not None:
        out["iam_role_arn"] = auth.ROLE_ARN
    if auth.ACCESS_KEY_ID is not None:
        out["aws_access_key_id"] = auth.ACCESS_KEY_ID
    if auth.SECRET_ACCESS_KEY is not None:
        out["aws_secret_access_key"] = auth.SECRET_ACCESS_KEY.get_secret_value()
    if auth.SESSION_TOKEN is not None:
        out["aws_session_token"] = auth.SESSION_TOKEN.get_secret_value()
    if auth.PROFILE_NAME is not None:
        out["profile_name"] = auth.PROFILE_NAME
    return out
