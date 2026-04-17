"""PySpark settings tests."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.pyspark import (
    PySparkAuthSettings,
    PySparkMode,
)


@pytest.mark.unit
class TestPySparkAuthSettings:
    def test_minimal(self):
        s = PySparkAuthSettings(auth=NoAuth())
        assert s.MODE is PySparkMode.BATCH

    def test_mode_streaming(self):
        s = PySparkAuthSettings(MODE="streaming", auth=NoAuth())
        assert s.MODE is PySparkMode.STREAMING

    def test_mode_invalid_rejected(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PySparkAuthSettings(MODE="nonsense", auth=NoAuth())

    def test_partitions_accepts_int(self):
        """Audit regression: PARTITIONS: int = {} crashed at init."""
        s = PySparkAuthSettings(PARTITIONS=200, auth=NoAuth())
        assert s.PARTITIONS == 200

    def test_partitions_none_default(self):
        s = PySparkAuthSettings(auth=NoAuth())
        assert s.PARTITIONS is None

    def test_to_driver_kwargs_emits_dotted_spark_keys(self):
        """Audit regression: previously emitted 'spark_app_name' not 'spark.app.name'."""
        s = PySparkAuthSettings(
            APPLICATION_NAME="myapp",
            SPARK_MASTER="local[2]",
            MODE="batch",
            auth=NoAuth(),
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["mode"] == "batch"
        # Adapter emits dotted Spark keys:
        assert kwargs["spark.app.name"] == "myapp"
        assert kwargs["spark.master"] == "local[2]"
