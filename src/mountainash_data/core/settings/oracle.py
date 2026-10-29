"""Oracle backend settings.

Driver: python-oracledb (thin mode, no Oracle Instant Client required).
Ibis: ``ibis.oracle.connect(**kwargs)``.

DATABASE is plumbed as ibis's ``database`` kwarg, which the Oracle ibis
backend treats as the connection's ``service_name`` (the modern way to
address an Oracle instance/PDB). SID is kept as an optional advanced
parameter for legacy Oracle installations that are still addressed by
instance SID rather than service name; only one of DATABASE/SID should be
set (ibis raises if both service_name and database resolve non-None, and
DATABASE is what maps to service_name here — SID is passed through
independently and is safe to leave unset).
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import PasswordAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register

ORACLE_SPEC = BackendSpec(
    name="oracle",
    provider_type=CONST_DB_PROVIDER_TYPE.ORACLE,
    default_port=1521,
    connection_string_scheme="oracle://",
    ibis_dialect="oracle",
    supported_auth=(PasswordAuthProfile,),
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=1521,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SID", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="sid"),
    ],
)


@register
class OracleBackendProfile(BackendProfile):
    __spec__ = ORACLE_SPEC
