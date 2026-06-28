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


def password(auth, base):
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def token(auth, base):
    return {**base, "authenticator": "oauth", "token": auth.TOKEN.get_secret_value()}


def oauth2(auth, base):
    # token-only: never reads CLIENT_ID/SECRET/SERVER_URI/SCOPE (smell #1)
    return {**base, "authenticator": "oauth", "token": auth.TOKEN.get_secret_value()}


def certificate(auth, base):
    out = dict(base)
    if auth.PRIVATE_KEY is not None:
        out["private_key"] = auth.PRIVATE_KEY.get_secret_value()
    if auth.PRIVATE_KEY_PATH is not None:
        out["private_key_file"] = str(auth.PRIVATE_KEY_PATH)
    if auth.PASSPHRASE is not None:
        out["private_key_file_pwd"] = auth.PASSPHRASE.get_secret_value()
    return out
