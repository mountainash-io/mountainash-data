"""Redshift adapter: endpoint resolution hook, IAM/password routing."""

from __future__ import annotations

import typing as t

from mountainash_data.core.settings.auth import IAMAuth, PasswordAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.redshift import RedshiftAuthSettings


def build_driver_kwargs(profile: "RedshiftAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_driver_kwargs()

    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
    elif isinstance(auth, IAMAuth):
        kwargs["iam"] = True
        if auth.role_arn is not None:
            kwargs["iam_role_arn"] = auth.role_arn
        if auth.access_key_id is not None:
            kwargs["aws_access_key_id"] = auth.access_key_id
        if auth.secret_access_key is not None:
            kwargs["aws_secret_access_key"] = auth.secret_access_key.get_secret_value()
        if auth.session_token is not None:
            kwargs["aws_session_token"] = auth.session_token.get_secret_value()
        if auth.profile_name is not None:
            kwargs["profile_name"] = auth.profile_name
    return kwargs
