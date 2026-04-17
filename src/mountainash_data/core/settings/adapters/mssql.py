"""MSSQL adapter: auth dispatch, instance-name folding, encryption keys."""

from __future__ import annotations

import typing as t

from mountainash_settings.auth import (
    AzureADAuth,
    PasswordAuth,
    WindowsAuth,
)

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.mssql import MSSQLAuthSettings


def build_driver_kwargs(profile: "MSSQLAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_kwargs()

    # Instance name → host\instance
    if profile.INSTANCE_NAME:
        kwargs["host"] = f"{kwargs['host']}\\{profile.INSTANCE_NAME}"

    # Encryption flags
    if profile.ENCRYPTION is not None:
        kwargs["encrypt"] = str(profile.ENCRYPTION)
    if profile.TRUST_SERVER_CERTIFICATE:
        kwargs["trust_server_certificate"] = "yes"
    if profile.MARS_ENABLED:
        kwargs["mars_connection"] = "yes"

    # Auth dispatch
    auth = profile.auth
    if isinstance(auth, PasswordAuth):
        kwargs["user"] = auth.username
        kwargs["password"] = auth.password.get_secret_value()
    elif isinstance(auth, WindowsAuth):
        kwargs["trusted_connection"] = "yes"
        if auth.domain and auth.username:
            kwargs["user"] = f"{auth.domain}\\{auth.username}"
        elif auth.username:
            kwargs["user"] = auth.username
    elif isinstance(auth, AzureADAuth):
        if auth.managed_identity:
            kwargs["authentication"] = "ActiveDirectoryMsi"
            if auth.msi_endpoint:
                kwargs["msi_endpoint"] = auth.msi_endpoint
        else:
            kwargs["authentication"] = "ActiveDirectoryServicePrincipal"
            if auth.client_id:
                kwargs["user_id"] = auth.client_id
            if auth.client_secret:
                kwargs["password"] = auth.client_secret.get_secret_value()
            if auth.tenant_id:
                kwargs["tenant_id"] = auth.tenant_id
    return kwargs
