"""Materialize backend settings.

Driver: https://materialize.com/docs/integrations/python/
Ibis: ``ibis.materialize.connect(host, user, password, port, database,
       schema, autocommit, cluster, **kwargs)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register


MATERIALIZE_SPEC = BackendSpec(
    name="materialize",
    provider_type=CONST_DB_PROVIDER_TYPE.MATERIALIZE,
    default_port=6875,
    connection_string_scheme="materialize://",
    ibis_dialect="materialize",
    supported_auth=(PasswordAuthProfile, NoAuthProfile),
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=6875,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=t.Optional[str], tier="core",
                      default=None, driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="AUTOCOMMIT", type=bool, tier="core",
                      default=True, driver_key="autocommit"),
        ParameterSpec(name="CLUSTER", type=t.Optional[str], tier="core",
                      default=None, driver_key="cluster"),
    ],
)


@register
class MaterializeBackendProfile(BackendProfile):
    __spec__ = MATERIALIZE_SPEC
