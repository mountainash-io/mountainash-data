from __future__ import annotations

import pytest

from mountainash_data.resource_provider.provider import DatabaseResourceProvider
from mountainash_resource_provider import DetectedResourceFormat, ProviderReadError, ResourceRequest


def test_settings_mode_never_falls_back_to_direct_url() -> None:
    provider = DatabaseResourceProvider.from_parameters("duckdb")
    request = ResourceRequest(
        name="orders",
        locator="duckdb:///tmp/orders.duckdb",
        detected_format=DetectedResourceFormat("duckdb", None, "duckdb", "locator"),
        encoding=None,
        dialect={"table": "orders"},
        dialect_context={},
        schema=None,
        metadata={},
    )
    with pytest.raises(ProviderReadError, match="connection mode"):
        provider.read_arrow(provider.plan(request))
