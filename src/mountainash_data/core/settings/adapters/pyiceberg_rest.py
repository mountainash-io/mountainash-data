"""Adapter prefixing s3.*, rest.sigv4-*, header.* keys."""

from __future__ import annotations

import typing as t

from mountainash_settings.auth import OAuth2Auth, TokenAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.pyiceberg_rest import (
        PyIcebergRestAuthSettings,
    )


def build_driver_kwargs(profile: "PyIcebergRestAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_kwargs()

    # S3 family
    for field, key in [
        ("S3_REGION", "s3.region"),
        ("S3_ENDPOINT", "s3.endpoint"),
        ("S3_ACCESS_KEY_ID", "s3.access-key-id"),
    ]:
        val = getattr(profile, field, None)
        if val is not None:
            kwargs[key] = val
    if profile.S3_SECRET_ACCESS_KEY is not None:
        kwargs["s3.secret-access-key"] = profile.S3_SECRET_ACCESS_KEY.get_secret_value()
    if profile.S3_SESSION_TOKEN is not None:
        kwargs["s3.session-token"] = profile.S3_SESSION_TOKEN.get_secret_value()

    # SigV4
    if profile.REST_SIGV4_ENABLED is not None:
        kwargs["rest.sigv4-enabled"] = profile.REST_SIGV4_ENABLED
    if profile.REST_SIGNING_REGION is not None:
        kwargs["rest.signing-region"] = profile.REST_SIGNING_REGION
    if profile.REST_SIGNING_NAME is not None:
        kwargs["rest.signing-name"] = profile.REST_SIGNING_NAME

    # Headers (dict → header.<k> = v)
    if profile.HEADERS:
        for hk, hv in profile.HEADERS.items():
            kwargs[f"header.{hk}"] = hv

    # Auth
    auth = profile.auth
    if isinstance(auth, TokenAuth):
        kwargs["token"] = auth.token.get_secret_value()
    elif isinstance(auth, OAuth2Auth):
        if auth.token is not None:
            kwargs["token"] = auth.token.get_secret_value()
        elif auth.client_id is not None and auth.client_secret is not None:
            kwargs["credential"] = (
                f"{auth.client_id}:{auth.client_secret.get_secret_value()}"
            )
        if auth.server_uri is not None:
            kwargs["oauth2-server-uri"] = auth.server_uri
        if auth.scope is not None:
            kwargs["scope"] = auth.scope
    return kwargs
