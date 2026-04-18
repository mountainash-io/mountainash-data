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
from .adapters import pyspark as _adapter
from mountainash_settings.auth import NoAuth
from .descriptor import BackendDescriptor, ParameterSpec
from .profile import ConnectionProfile
from .registry import register

__all__ = ["PySparkAuthSettings", "PySparkMode", "PYSPARK_DESCRIPTOR"]


class PySparkMode(StrEnum):
    BATCH = "batch"
    STREAMING = "streaming"


PYSPARK_DESCRIPTOR = BackendDescriptor(
    name="pyspark",
    provider_type=CONST_DB_PROVIDER_TYPE.PYSPARK,
    connection_string_scheme=None,  # SparkSession, not URL
    ibis_dialect="pyspark",
    auth_modes=[NoAuth],
    parameters=[
        ParameterSpec(name="SESSION", type=t.Optional[t.Any], tier="core",
                      default=None),
        ParameterSpec(name="MODE", type=PySparkMode, tier="core",
                      default=PySparkMode.BATCH),
        ParameterSpec(name="SPARK_MASTER", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="APPLICATION_NAME", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="WAREHOUSE_DIR", type=t.Optional[str], tier="advanced",
                      default=None),
        ParameterSpec(name="PARTITIONS", type=t.Optional[int], tier="advanced",
                      default=None),
    ],
)


@register(PYSPARK_DESCRIPTOR)
class PySparkAuthSettings(ConnectionProfile):
    __descriptor__ = PYSPARK_DESCRIPTOR
    __adapter__ = staticmethod(_adapter.build_driver_kwargs)
