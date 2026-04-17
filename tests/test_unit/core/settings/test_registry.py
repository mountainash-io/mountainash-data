"""Tests for the DATABASES_REGISTRY wrapper + back-compat REGISTRY alias."""

import pytest

from mountainash_data.core.settings.registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    get_descriptor,
    get_settings_class,
)


@pytest.mark.unit
class TestDatabasesRegistry:
    def test_registry_is_populated_after_import(self):
        """All 12 backends register themselves at import time."""
        import mountainash_data.core.settings  # noqa: F401

        for name in ["sqlite", "duckdb", "postgresql", "mysql", "mssql",
                     "snowflake", "bigquery", "redshift", "pyspark",
                     "trino", "motherduck", "pyiceberg_rest"]:
            assert name in DATABASES_REGISTRY, f"{name} missing from registry"

    def test_get_descriptor_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        desc = get_descriptor("sqlite")
        assert desc.name == "sqlite"

    def test_get_settings_class_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        from mountainash_data.core.settings.sqlite import SQLiteAuthSettings
        assert get_settings_class("sqlite") is SQLiteAuthSettings

    def test_legacy_REGISTRY_alias_still_works(self):
        import mountainash_data.core.settings  # noqa: F401
        assert "sqlite" in REGISTRY
        assert REGISTRY["sqlite"].name == "sqlite"
        # Iterate
        names = list(REGISTRY.keys())
        assert "sqlite" in names
