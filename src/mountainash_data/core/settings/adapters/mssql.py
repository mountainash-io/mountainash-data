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


# mirrors sql.userpass intentionally — mssql also uses user/password keys
def password(auth, base):
    return {**base, "user": auth.USERNAME, "password": auth.PASSWORD.get_secret_value()}


def windows(auth, base):
    out = {**base, "trusted_connection": "yes"}
    if auth.DOMAIN is not None and auth.USERNAME is not None:
        out["user"] = f"{auth.DOMAIN}\\{auth.USERNAME}"
    elif auth.USERNAME is not None:
        out["user"] = auth.USERNAME
    return out


def azure_ad(auth, base):
    out = dict(base)
    if auth.MANAGED_IDENTITY:
        out["authentication"] = "ActiveDirectoryMsi"
        if auth.MSI_ENDPOINT:
            out["msi_endpoint"] = auth.MSI_ENDPOINT
    else:
        out["authentication"] = "ActiveDirectoryServicePrincipal"
        if auth.CLIENT_ID is not None:
            out["user_id"] = auth.CLIENT_ID
        if auth.CLIENT_SECRET is not None:
            out["password"] = auth.CLIENT_SECRET.get_secret_value()
        if auth.TENANT_ID is not None:
            out["tenant_id"] = auth.TENANT_ID
    return out
