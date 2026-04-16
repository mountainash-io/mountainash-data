"""Adapter translating AuthSpec → trino.auth.Authentication wrappers."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import (
    AuthSpec,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    PasswordAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.trino import TrinoAuthSettings


def build_driver_kwargs(profile: "TrinoAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        from trino.auth import BasicAuthentication

        kwargs["user"] = auth.username
        kwargs["auth"] = BasicAuthentication(
            auth.username, auth.password.get_secret_value()
        )
    elif isinstance(auth, JWTAuth):
        from trino.auth import JWTAuthentication

        kwargs["auth"] = JWTAuthentication(auth.token.get_secret_value())
    elif isinstance(auth, KerberosAuth):
        from trino.auth import KerberosAuthentication

        kwargs["auth"] = KerberosAuthentication(
            config=None,
            service_name=auth.service_name,
            principal=auth.principal,
        )
    elif isinstance(auth, NoAuth):
        pass
    else:
        raise ValueError(f"trino adapter does not support auth: {type(auth).__name__}")
    return kwargs
