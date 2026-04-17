"""Snowflake adapter: session_parameters, authenticator mapping, cert auth."""

from __future__ import annotations

import typing as t

from mountainash_settings.auth import (
    CertificateAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.snowflake import SnowflakeAuthSettings


def build_driver_kwargs(profile: "SnowflakeAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_kwargs()

    # Session parameters
    session_params: dict[str, t.Any] = {}
    if profile.TIMEZONE is not None:
        session_params["TIMEZONE"] = profile.TIMEZONE
    if profile.QUERY_TAG is not None:
        session_params["QUERY_TAG"] = profile.QUERY_TAG
    if session_params:
        kwargs["session_parameters"] = session_params

    # Auth dispatch
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
        if profile.AUTHENTICATOR is not None:
            kwargs["authenticator"] = str(profile.AUTHENTICATOR)
    elif isinstance(auth, TokenAuth):
        kwargs["authenticator"] = "oauth"
        kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, OAuth2Auth):
        kwargs["authenticator"] = "oauth"
        if auth.token is not None:
            kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, CertificateAuth):
        if auth.private_key is not None:
            kwargs["private_key"] = auth.private_key.get_secret_value()
        if auth.private_key_path is not None:
            kwargs["private_key_file"] = str(auth.private_key_path)
        if auth.passphrase is not None:
            kwargs["private_key_file_pwd"] = auth.passphrase.get_secret_value()
    return kwargs
