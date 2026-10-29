"""Databricks backend settings.

Driver: https://docs.databricks.com/en/dev-tools/python-sql-connector.html
Ibis: ``ibis.databricks.connect(server_hostname, http_path, access_token,
       catalog, schema, ...)``
"""

from __future__ import annotations

import typing as t

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile, TokenAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register


DATABRICKS_SPEC = BackendSpec(
    name="databricks",
    provider_type=CONST_DB_PROVIDER_TYPE.DATABRICKS,
    ibis_dialect="databricks",
    supported_auth=(TokenAuthProfile, PasswordAuthProfile, NoAuthProfile),
    parameters=[
        ParameterSpec(name="SERVER_HOSTNAME", type=str, tier="core",
                      driver_key="server_hostname"),
        ParameterSpec(name="HTTP_PATH", type=str, tier="core",
                      driver_key="http_path"),
        ParameterSpec(name="CATALOG", type=t.Optional[str], tier="core",
                      default=None, driver_key="catalog"),
        ParameterSpec(name="SCHEMA", type=str, tier="core",
                      default="default", driver_key="schema"),
        ParameterSpec(name="USE_CLOUD_FETCH", type=bool, tier="advanced",
                      default=False, driver_key="use_cloud_fetch"),
    ],
)


@register
class DatabricksBackendProfile(BackendProfile):
    __spec__ = DATABRICKS_SPEC
