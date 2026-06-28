"""MSSQL adapters."""
from __future__ import annotations
import typing as t


def host_fold(profile: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    out = dict(base)
    if profile.INSTANCE_NAME:
        out["host"] = f"{out['host']}\\{profile.INSTANCE_NAME}"
    if profile.ENCRYPTION is not None:
        out["encrypt"] = str(profile.ENCRYPTION)
    if profile.TRUST_SERVER_CERTIFICATE:
        out["trust_server_certificate"] = "yes"
    if profile.MARS_ENABLED:
        out["mars_connection"] = "yes"
    return out
