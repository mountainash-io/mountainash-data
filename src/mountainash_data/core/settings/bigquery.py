"""BigQuery backend settings.

Spec: ``mountainash-central/04.planning/mountainash-data/superpowers/specs/2026-04-15-settings-audit/bigquery.md``.
Ibis: ``ibis.backends.bigquery.do_connect``
"""

from __future__ import annotations

import re
import typing as t

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import NoAuthProfile, ServiceAccountAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register

__all__ = ["BigQueryBackendProfile", "BIGQUERY_SPEC"]

_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def _validate_project_id(value: str) -> str:
    if not _PROJECT_ID_RE.match(value):
        raise ValueError(
            "PROJECT_ID must be 6-30 chars, lowercase/digits/hyphens, "
            "not starting or ending with a hyphen"
        )
    return value


BIGQUERY_SPEC = BackendSpec(
    name="bigquery",
    provider_type=CONST_DB_PROVIDER_TYPE.BIGQUERY,
    connection_string_scheme="bigquery://",
    ibis_dialect="bigquery",
    supported_auth=(ServiceAccountAuthProfile, NoAuthProfile),
    parameters=[
        ParameterSpec(name="PROJECT_ID", type=str, tier="core",
                      driver_key="project_id"),
        ParameterSpec(name="DATASET_ID", type=t.Optional[str], tier="core",
                      default=None, driver_key="dataset_id"),
        ParameterSpec(name="LOCATION", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="location"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str],
                      tier="advanced", default=None,
                      driver_key="application_name"),
        ParameterSpec(name="PARTITION_COLUMN", type=str, tier="advanced",
                      default="PARTITIONTIME", driver_key="partition_column"),
        ParameterSpec(name="AUTH_LOCAL_WEBSERVER", type=bool, tier="core",
                      default=True, driver_key="auth_local_webserver"),
        ParameterSpec(name="AUTH_EXTERNAL_DATA", type=bool, tier="core",
                      default=False, driver_key="auth_external_data"),
        ParameterSpec(name="AUTH_CACHE", type=str, tier="core",
                      default="default", driver_key="auth_cache"),
    ],
)


@register
class BigQueryBackendProfile(BackendProfile):
    __spec__ = BIGQUERY_SPEC

    @field_validator("PROJECT_ID", check_fields=False)
    @classmethod
    def _pid(cls, v: str) -> str:
        return _validate_project_id(v)
