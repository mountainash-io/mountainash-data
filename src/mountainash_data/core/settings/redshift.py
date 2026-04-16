"""Redshift backend settings.

Spec: ``docs/superpowers/specs/2026-04-15-settings-audit/redshift.md``.
Driver: redshift_connector OR psycopg (via Ibis postgres). Endpoint
resolution via boto3 ``describe_clusters`` is a Phase-4 follow-up.
"""

from __future__ import annotations

import re
import typing as t
from enum import StrEnum

from pydantic import field_validator

from ..constants import CONST_DB_PROVIDER_TYPE
from .adapters import redshift as _adapter
from .auth import IAMAuth, PasswordAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register


class RedshiftSSLMode(StrEnum):
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


_REGION_RE = re.compile(r"^[a-z]{2,4}-[a-z-]+-\d{1,2}$")
_ROLE_ARN_RE = re.compile(r"^arn:aws(?:-us-gov|-cn)?:iam::\d{12}:role/.+$")


REDSHIFT_DESCRIPTOR = BackendDescriptor(
    name="redshift",
    provider_type=CONST_DB_PROVIDER_TYPE.REDSHIFT,
    default_port=5439,
    connection_string_scheme="redshift://",
    ibis_dialect="postgres",
    rides_on="postgres",
    auth_modes=[PasswordAuth, IAMAuth],
    parameters=[
        ParameterSpec(name="HOST", type=str, tier="core", driver_key="host"),
        ParameterSpec(name="PORT", type=int, tier="core", default=5439,
                      driver_key="port"),
        ParameterSpec(name="DATABASE", type=str, tier="core",
                      driver_key="database"),
        ParameterSpec(name="SCHEMA", type=t.Optional[str], tier="core",
                      default=None, driver_key="schema"),
        ParameterSpec(name="REGION", type=str, tier="core"),
        ParameterSpec(name="CLUSTER_IDENTIFIER", type=t.Optional[str],
                      tier="core", default=None),
        ParameterSpec(name="WORKGROUP_NAME", type=t.Optional[str],
                      tier="core", default=None),
        ParameterSpec(name="SERVERLESS", type=bool, tier="core", default=False),
        ParameterSpec(name="IAM_ROLE_ARN", type=t.Optional[str],
                      tier="advanced", default=None),
        ParameterSpec(name="SSL_MODE", type=RedshiftSSLMode, tier="core",
                      default=RedshiftSSLMode.VERIFY_FULL,
                      driver_key="sslmode"),
        ParameterSpec(name="CLUSTER_READ_ONLY", type=bool, tier="advanced",
                      default=False, driver_key="readonly"),
    ],
)


@register(REDSHIFT_DESCRIPTOR)
class RedshiftAuthSettings(ConnectionProfile):
    __descriptor__ = REDSHIFT_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)

    @field_validator("REGION", check_fields=False)
    @classmethod
    def _region(cls, v: str) -> str:
        if not _REGION_RE.match(v):
            raise ValueError(f"Invalid AWS region: {v}")
        return v

    @field_validator("IAM_ROLE_ARN", check_fields=False)
    @classmethod
    def _role_arn(cls, v: t.Optional[str]) -> t.Optional[str]:
        if v is not None and not _ROLE_ARN_RE.match(v):
            raise ValueError(f"Invalid IAM role ARN: {v}")
        return v
