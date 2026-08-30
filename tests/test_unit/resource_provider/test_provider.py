from __future__ import annotations

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
