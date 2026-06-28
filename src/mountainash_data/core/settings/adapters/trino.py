"""Trino auth adapter functions."""
from __future__ import annotations
import typing as t


def password(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    from trino.auth import BasicAuthentication
    return {**base, "user": auth.USERNAME,
            "auth": BasicAuthentication(auth.USERNAME, auth.PASSWORD.get_secret_value())}


def jwt(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    from trino.auth import JWTAuthentication
    return {**base, "auth": JWTAuthentication(auth.TOKEN.get_secret_value())}


def kerberos(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    from trino.auth import KerberosAuthentication
    return {**base, "auth": KerberosAuthentication(config=None, service_name=auth.SERVICE_NAME, principal=auth.PRINCIPAL)}
