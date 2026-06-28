"""PySpark backend settings.

Spec: audit report ``docs/superpowers/specs/2026-04-15-settings-audit/pyspark.md``.
Ibis: ``ibis.backends.pyspark.do_connect(session=None, mode='batch', **kwargs)``
where kwargs flow to ``SparkSession.builder.config(**kwargs)``.

The docstring of the prior class read 'SQLite authentication settings' — a
copy-paste from ``sqlite.py``. Corrected here.
"""

from __future__ import annotations

import typing as t
from enum import StrEnum

from ..constants import CONST_DB_PROVIDER_TYPE
from mountainash_auth_client import NoAuthProfile
from .descriptor import BackendSpec, ParameterSpec
from .profile import BackendProfile
from .registry import register

__all__ = ["PySparkBackendProfile", "PySparkMode", "PYSPARK_SPEC"]


class PySparkMode(StrEnum):
    BATCH = "batch"
    STREAMING = "streaming"


PYSPARK_SPEC = BackendSpec(
    name="pyspark",
    provider_type=CONST_DB_PROVIDER_TYPE.PYSPARK,
    connection_string_scheme=None,  # SparkSession, not URL
    ibis_dialect="pyspark",
    supported_auth=(NoAuthProfile,),
    parameters=[
        ParameterSpec(name="SESSION", type=t.Optional[t.Any], tier="core",
                      default=None, driver_key="session"),
        ParameterSpec(name="MODE", type=PySparkMode, tier="core",
                      default=PySparkMode.BATCH, driver_key="mode"),
        ParameterSpec(name="SPARK_MASTER", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="spark.master"),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="spark.app.name"),
        ParameterSpec(name="WAREHOUSE_DIR", type=t.Optional[str], tier="advanced",
                      default=None, driver_key="spark.sql.warehouse.dir"),
        ParameterSpec(name="PARTITIONS", type=t.Optional[int], tier="advanced",
                      default=None, driver_key="spark.sql.shuffle.partitions"),
    ],
)


@register
class PySparkBackendProfile(BackendProfile):
    __spec__ = PYSPARK_SPEC
