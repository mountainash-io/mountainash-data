"""Discriminated-union AuthSpec members."""

from .azure import AzureADAuth, WindowsAuth
from .base import AuthSpec
from .certificate import CertificateAuth
from .iam import IAMAuth
from .kerberos import KerberosAuth
from .none import NoAuth
from .oauth2 import OAuth2Auth
from .password import PasswordAuth
from .service_account import ServiceAccountAuth
from .token import JWTAuth, TokenAuth

__all__ = [
    "AuthSpec",
    "NoAuth",
    "PasswordAuth",
    "TokenAuth",
    "JWTAuth",
    "OAuth2Auth",
    "ServiceAccountAuth",
    "IAMAuth",
    "WindowsAuth",
    "AzureADAuth",
    "KerberosAuth",
    "CertificateAuth",
]
