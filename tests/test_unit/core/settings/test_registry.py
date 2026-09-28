"""Tests for the DATABASES_REGISTRY wrapper + back-compat REGISTRY alias."""

import pytest

from mountainash_data.core.settings.registry import (
    DATABASES_REGISTRY,
    REGISTRY,
    _reset_for_tests,
    _snapshot_for_tests,
    get_descriptor,
    get_settings_class,
)


@pytest.fixture(autouse=True)
def restore_registry():
    snapshot = _snapshot_for_tests()
    try:
        yield
    finally:
        _reset_for_tests(*snapshot)


@pytest.mark.unit
class TestDatabasesRegistry:
    def test_registry_is_populated_after_import(self):
        """Every relational backend registers itself at import time."""
        import mountainash_data.core.settings  # noqa: F401

        for name in ["sqlite", "duckdb", "postgresql", "mysql", "mssql",
                     "snowflake", "bigquery", "redshift", "pyspark",
                     "trino", "motherduck", "oracle"]:
            assert name in DATABASES_REGISTRY, f"{name} missing from registry"

    def test_get_descriptor_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        desc = get_descriptor("sqlite")
        assert desc.name == "sqlite"

    def test_get_settings_class_returns_correct_type(self):
        import mountainash_data.core.settings  # noqa: F401
        from mountainash_data.core.settings.sqlite import SQLiteBackendProfile
        assert get_settings_class("sqlite") is SQLiteBackendProfile

    def test_legacy_REGISTRY_alias_still_works(self):
        import mountainash_data.core.settings  # noqa: F401
        assert "sqlite" in REGISTRY
        assert REGISTRY["sqlite"].name == "sqlite"
        # Iterate
        names = list(REGISTRY.keys())
        assert "sqlite" in names

    def test_registry_mapping_uses_canonical_settings_specs(self):
        expected = DATABASES_REGISTRY.specs
        assert get_descriptor("sqlite") is expected["sqlite"]
        assert REGISTRY["sqlite"] is expected["sqlite"]
        assert set(iter(REGISTRY)) == set(expected)
        assert dict(REGISTRY.items()) == expected
        assert set(REGISTRY.keys()) == set(expected)
        assert list(REGISTRY.values()) == list(expected.values())
        expected.clear()
        assert "sqlite" in REGISTRY
        assert get_settings_class("sqlite") is not None

    @pytest.mark.parametrize("view", [iter, lambda r: r.items(),
                                     lambda r: r.keys(), lambda r: r.values()],
                             ids=["iteration", "items", "keys", "values"])
    def test_registry_views_match_canonical_specs(self, view):
        assert list(view(REGISTRY)) == list(view(DATABASES_REGISTRY.specs))

    @pytest.mark.parametrize("lookup", [get_descriptor, REGISTRY.__getitem__,
                                       get_settings_class])
    def test_unknown_name_raises_key_error(self, lookup):
        with pytest.raises(KeyError):
            lookup("unknown-backend")

    @pytest.mark.parametrize("name", [None, 1, [], {}])
    def test_non_string_membership_is_false(self, name):
        assert name not in REGISTRY

    def test_registry_rejects_assignment(self):
        with pytest.raises(TypeError):
            REGISTRY["sqlite"] = DATABASES_REGISTRY.specs["sqlite"]

    def test_registry_snapshot_can_restore_specs_and_classes(self):
        snapshot = _snapshot_for_tests()
        sqlite_spec = DATABASES_REGISTRY.specs["sqlite"]
        sqlite_class = get_settings_class("sqlite")
        _reset_for_tests({}, {})
        assert len(REGISTRY) == 0
        assert "sqlite" not in REGISTRY

        _reset_for_tests(*snapshot)
        assert get_descriptor("sqlite") is sqlite_spec
        assert get_settings_class("sqlite") is sqlite_class
