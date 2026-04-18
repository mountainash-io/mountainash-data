"""Compatibility shim — re-exports from mountainash_settings.auth.

The auth primitives (AuthSpec subclasses, auth_to_driver_kwargs) now live in
the upstream mountainash-settings package.  This module re-exports everything
so existing imports of the form::

    from mountainash_data.core.settings.auth import NoAuth

continue to work unchanged while the rest of the codebase migrates.
"""

from mountainash_settings.auth import (
    AUTH_TO_DRIVER_KWARGS,
    AuthSpec,
    AzureADAuth,
    CertificateAuth,
    IAMAuth,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    ServiceAccountAuth,
    TokenAuth,
    WindowsAuth,
    auth_to_driver_kwargs,
)

__all__ = [
    "AUTH_TO_DRIVER_KWARGS",
    "AuthSpec",
    "AzureADAuth",
    "CertificateAuth",
    "IAMAuth",
    "JWTAuth",
    "KerberosAuth",
    "NoAuth",
    "OAuth2Auth",
    "PasswordAuth",
    "ServiceAccountAuth",
    "TokenAuth",
    "WindowsAuth",
    "auth_to_driver_kwargs",
]
