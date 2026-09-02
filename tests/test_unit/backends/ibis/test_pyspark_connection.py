from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from mountainash_data.backends.ibis.dialects._registry import _build_pyspark_connection


class _SparkConf:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def setAll(self, values):
        self.values = dict(values)
        return self


class _SessionBuilder:
    def __init__(self) -> None:
        self.remote_url: str | None = None
        self.conf: _SparkConf | None = None
        self.options: dict[str, object] = {}
        self.session = object()

    def remote(self, url: str):
        self.remote_url = url
        return self

    def config(self, *args, **kwargs):
        if "conf" in kwargs:
            self.conf = kwargs["conf"]
        elif len(args) == 2:
            self.options[args[0]] = args[1]
        return self

    def getOrCreate(self):
        return self.session


def _install_spark_fakes(monkeypatch: pytest.MonkeyPatch):
    builder = _SessionBuilder()
    connect_calls: list[dict[str, object]] = []

    ibis_module = ModuleType("ibis")
    ibis_module.pyspark = SimpleNamespace(
        connect=lambda **kwargs: connect_calls.append(kwargs) or "connected"
    )
    monkeypatch.setitem(sys.modules, "ibis", ibis_module)

    pyspark_module = ModuleType("pyspark")
    pyspark_module.SparkConf = _SparkConf
    pyspark_sql_module = ModuleType("pyspark.sql")
    pyspark_sql_module.SparkSession = SimpleNamespace(builder=builder)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql_module)
    return builder, connect_calls


def test_classic_static_config_is_applied_before_ibis_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder, connect_calls = _install_spark_fakes(monkeypatch)

    result = _build_pyspark_connection(
        mode="batch",
        **{
            "spark.master": "spark://127.0.0.1:27077",
            "spark.driver.host": "10.10.225.3",
        },
    )

    assert result == "connected"
    assert builder.conf is not None
    assert builder.conf.values == {
        "spark.master": "spark://127.0.0.1:27077",
        "spark.driver.host": "10.10.225.3",
    }
    assert connect_calls == [{"session": builder.session, "mode": "batch"}]


def test_spark_connect_uses_remote_builder_without_reapplying_static_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder, connect_calls = _install_spark_fakes(monkeypatch)

    result = _build_pyspark_connection(
        mode="batch",
        **{"spark.remote": "sc://127.0.0.1:25002"},
    )

    assert result == "connected"
    assert builder.remote_url == "sc://127.0.0.1:25002"
    assert builder.options == {}
    assert connect_calls == [{"session": builder.session, "mode": "batch"}]


def test_spark_remote_and_master_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_spark_fakes(monkeypatch)

    with pytest.raises(ValueError, match="mutually exclusive"):
        _build_pyspark_connection(
            **{
                "spark.remote": "sc://127.0.0.1:25002",
                "spark.master": "spark://127.0.0.1:27077",
            }
        )
