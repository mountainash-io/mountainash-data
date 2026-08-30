from __future__ import annotations

from mountainash_data.core.settings.registry import DATABASES_REGISTRY


def test_every_registered_backend_declares_resource_read_metadata() -> None:
    for descriptor in DATABASES_REGISTRY.descriptors.values():
        assert descriptor.resource_read_disposition is not None
        if descriptor.connection_string_scheme is not None:
            assert descriptor.resource_read_disposition.value == "settings_url"
        if descriptor.name == "pyspark":
            assert descriptor.resource_read_disposition.value == "connected_identity"
        if descriptor.name == "databricks":
            assert descriptor.resource_read_disposition.value == "override_only"
            assert descriptor.resource_read_override_reason
            assert descriptor.resource_read_override_date == "2026-08-29"
