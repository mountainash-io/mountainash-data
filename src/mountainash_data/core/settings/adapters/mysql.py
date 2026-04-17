"""Adapter that assembles mysqlclient's ssl={} dict."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.mysql import MySQLAuthSettings


def build_driver_kwargs(profile: "MySQLAuthSettings") -> dict[str, t.Any]:
    """Assemble driver kwargs, including ssl={} dict if any SSL fields are set."""
    kwargs = profile._default_driver_kwargs()
    kwargs.update(profile._auth_to_driver_kwargs())

    if profile.SSL_MODE is not None:
        kwargs["ssl_mode"] = str(profile.SSL_MODE)

    ssl: dict[str, str] = {}
    for key, val in {
        "ssl-key": profile.SSL_KEY,
        "ssl-cert": profile.SSL_CERT,
        "ssl-ca": profile.SSL_CA,
        "ssl-capath": profile.SSL_CAPATH,
        "ssl-cipher": profile.SSL_CIPHER,
    }.items():
        if val is not None:
            ssl[key] = str(val)
    if ssl:
        kwargs["ssl"] = ssl
    return kwargs
