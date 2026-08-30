from __future__ import annotations

import duckdb

from mountainash_data.resource_provider.provider import DatabaseResourceProvider
from mountainash_resource_provider import DetectedResourceFormat, ResourceRequest


def test_resource_url_provider_plans_without_exposing_credentials() -> None:
    provider = DatabaseResourceProvider.from_resource_url("postgresql://user:secret@db.example.com/sales")
    request = ResourceRequest(
        name="orders",
        locator="postgresql://user:secret@db.example.com/sales",
        detected_format=DetectedResourceFormat("postgresql", None, "postgresql", "locator"),
        encoding=None,
        dialect={},
        dialect_context={},
        schema=None,
        metadata={},
    )
    plan = provider.plan(request)
    assert "secret" not in repr(plan)
    assert plan.provider_key == "database"


def test_settings_constructors_do_not_need_connection_at_planning_time() -> None:
    provider = DatabaseResourceProvider.from_parameters("duckdb")
    default = DatabaseResourceProvider.default()
    assert provider._parameters.mode.value == "settings"
    assert default._parameters.backend == "duckdb"


def test_duckdb_resource_url_snapshots_selected_table(tmp_path) -> None:
    path = tmp_path / "orders.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute("create table orders(id integer)")
    connection.execute("insert into orders values (1)")
    connection.close()
    provider = DatabaseResourceProvider.from_resource_url(f"duckdb://{path}")
    request = ResourceRequest(
        name="orders_resource",
        locator=f"duckdb://{path}",
        detected_format=DetectedResourceFormat("duckdb", None, "duckdb", "locator"),
        encoding=None,
        dialect={"table": "orders"},
        dialect_context={},
        schema=None,
        metadata={},
    )
    assert provider.read_arrow(provider.plan(request)).table.to_pylist() == [{"id": 1}]
