"""PySpark settings tests."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.pyspark import (
    PySparkBackendProfile,
    PySparkMode,
)


@pytest.mark.unit
class TestPySparkBackendProfile:
    def test_minimal(self):
        s = PySparkBackendProfile()
        assert s.MODE is PySparkMode.BATCH

    def test_mode_streaming(self):
        s = PySparkBackendProfile(MODE="streaming")
        assert s.MODE is PySparkMode.STREAMING

    def test_mode_invalid_rejected(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PySparkBackendProfile(MODE="nonsense")

    def test_partitions_accepts_int(self):
        """Audit regression: PARTITIONS: int = {} crashed at init."""
        s = PySparkBackendProfile(PARTITIONS=200)
        assert s.PARTITIONS == 200

    def test_partitions_none_default(self):
        s = PySparkBackendProfile()
        assert s.PARTITIONS is None

    def test_emit_emits_dotted_spark_keys(self):
        """Audit regression: previously emitted 'spark_app_name' not 'spark.app.name'."""
        s = PySparkBackendProfile(
            APPLICATION_NAME="myapp",
            SPARK_MASTER="local[2]",
            MODE="batch",
        )
        kwargs = s.emit()
        assert kwargs["mode"] == "batch"
        # Adapter emits dotted Spark keys:
        assert kwargs["spark.app.name"] == "myapp"
        assert kwargs["spark.master"] == "local[2]"

    def test_emit_emits_remote_driver_network_settings(self):
        kwargs = PySparkBackendProfile(
            DRIVER_HOST="10.10.225.2",
            DRIVER_BIND_ADDRESS="127.0.0.1",
            DRIVER_PORT=27078,
            BLOCK_MANAGER_PORT=27079,
            EXECUTOR_PYTHON="python3",
        ).emit()

        assert kwargs["spark.driver.host"] == "10.10.225.2"
        assert kwargs["spark.driver.bindAddress"] == "127.0.0.1"
        assert kwargs["spark.driver.port"] == 27078
        assert kwargs["spark.blockManager.port"] == 27079
        assert kwargs["spark.pyspark.python"] == "python3"

    def test_emit_emits_spark_connect_endpoint(self):
        kwargs = PySparkBackendProfile(
            SPARK_REMOTE="sc://127.0.0.1:25002",
        ).emit()

        assert kwargs["spark.remote"] == "sc://127.0.0.1:25002"
