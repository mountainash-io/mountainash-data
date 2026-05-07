"""Databricks adapter: maps TokenAuth → access_token, PasswordAuth → user/pass."""

from __future__ import annotations

import typing as t

from mountainash_settings.auth import PasswordAuth, TokenAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.databricks import DatabricksAuthSettings


def build_driver_kwargs(profile: "DatabricksAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_kwargs()

    auth = profile.auth
    if isinstance(auth, TokenAuth):
        kwargs["access_token"] = auth.token.get_secret_value()
    elif isinstance(auth, PasswordAuth):
        kwargs["username"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
    return kwargs
