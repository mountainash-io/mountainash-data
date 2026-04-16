"""Adapter emitting dotted spark.* keys from PySpark settings."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.pyspark import PySparkAuthSettings


def build_driver_kwargs(profile: "PySparkAuthSettings") -> dict[str, t.Any]:
    """Build driver kwargs from PySpark settings.

    Emits dotted spark.* keys as required by SparkSession.builder.config(**kwargs).
    """
    kwargs: dict[str, t.Any] = {}
    if profile.MODE is not None:
        # StrEnum coerces to its string value; raw str passes through unchanged.
        # Handles both pydantic-coerced and setattr-bypass construction paths
        # (see MountainAshBaseSettings.update_settings_from_dict).
        kwargs["mode"] = str(profile.MODE)
    if profile.SESSION is not None:
        kwargs["session"] = profile.SESSION
    if profile.APPLICATION_NAME is not None:
        kwargs["spark.app.name"] = profile.APPLICATION_NAME
    if profile.SPARK_MASTER is not None:
        kwargs["spark.master"] = profile.SPARK_MASTER
    if profile.WAREHOUSE_DIR is not None:
        kwargs["spark.sql.warehouse.dir"] = profile.WAREHOUSE_DIR
    if profile.PARTITIONS is not None:
        kwargs["spark.sql.shuffle.partitions"] = profile.PARTITIONS
    # NoAuth is the only accepted mode; nothing else to emit.
    return kwargs
