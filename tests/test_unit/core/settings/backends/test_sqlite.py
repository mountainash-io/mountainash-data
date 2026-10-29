"""Round-trip and audit-regression tests for SQLite settings."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.sqlite import SQLiteBackendProfile


@pytest.mark.unit
class TestSQLiteBackendProfile:
    def test_minimal_construction(self):
        s = SQLiteBackendProfile()
        assert s.DATABASE is None
        assert s.provider_type  # non-empty

    def test_database_memory(self):
        s = SQLiteBackendProfile(DATABASE=":memory:")
        assert s.DATABASE == ":memory:"

    def test_emit_memory(self):
        s = SQLiteBackendProfile(DATABASE=":memory:")
        assert s.emit() == {"database": ":memory:"}

    def test_emit_none_database_dropped(self):
        s = SQLiteBackendProfile()
        assert s.emit() == {}

    def test_type_map_optional(self):
        s = SQLiteBackendProfile(DATABASE=":memory:", TYPE_MAP={"SMALLINT": "int32"})
        assert s.emit()["type_map"] == {"SMALLINT": "int32"}
