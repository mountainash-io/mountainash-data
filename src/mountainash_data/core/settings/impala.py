"""Impala backend settings.

Driver: https://github.com/cloudera/impyla
Ibis: ``ibis.impala.connect(host, port, database, timeout, use_ssl, ca_cert,
       user, password, auth_mechanism, kerberos_service_name, **params)``
"""

from __future__ import annotations

import typing as t
from enum import StrEnum
from pathlib import Path

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings.auth import NoAuth, PasswordAuth
from .descriptor import BackendSpec, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class ImpalaAuthMechanism(StrEnum):
    NOSASL = "NOSASL"
    PLAIN = "PLAIN"
    GSSAPI = "GSSAPI"
    LDAP = "LDAP"


IMPALA_SPEC = BackendSpec(
    name="impala",
    provider_type=CONST_DB_PROVIDER_TYPE.IMPALA,
    default_port=21050,
    connection_string_scheme="impala://",
    ibis_dialect="impala",
    auth_modes=[PasswordAuth, NoAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=21050,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=str, tier="core",
                      default="default", driver_key="database"),
        ParameterSpec(name="TIMEOUT", type=int, tier="advanced",
                      default=45, driver_key="timeout"),
        ParameterSpec(name="USE_SSL", type=bool, tier="core",
                      default=False, driver_key="use_ssl"),
        ParameterSpec(name="CA_CERT", type=t.Optional[Path], tier="advanced",
                      default=None, driver_key="ca_cert",
                      transform=lambda p: str(p)),
        ParameterSpec(name="AUTH_MECHANISM", type=ImpalaAuthMechanism,
                      tier="core", default=ImpalaAuthMechanism.NOSASL,
                      driver_key="auth_mechanism"),
        ParameterSpec(name="KERBEROS_SERVICE_NAME", type=str, tier="advanced",
                      default="impala", driver_key="kerberos_service_name"),
    ],
)


@register
class ImpalaAuthSettings(ConnectionProfile):
    __spec__ = IMPALA_SPEC
