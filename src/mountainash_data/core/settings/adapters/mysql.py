"""MySQL config-shaping adapter."""
from __future__ import annotations
import typing as t


def ssl_compose(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.SSL_MODE is not None:
        out["ssl_mode"] = str(profile.SSL_MODE)
    ssl: dict[str, str] = {}
    for key, val in {
        "ssl-key": profile.SSL_KEY, "ssl-cert": profile.SSL_CERT,
        "ssl-ca": profile.SSL_CA, "ssl-capath": profile.SSL_CAPATH,
        "ssl-cipher": profile.SSL_CIPHER,
    }.items():
        if val is not None:
            ssl[key] = str(val)
    if ssl:
        out["ssl"] = ssl
    return out
