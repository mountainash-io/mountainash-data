"""Round-trip and audit-regression tests for SQLite settings."""

from __future__ import annotations

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.sqlite import SQLiteAuthSettings


@pytest.mark.unit
class TestSQLiteAuthSettings:
    def test_minimal_construction(self):
        s = SQLiteAuthSettings(auth=NoAuth())
        assert s.DATABASE is None
        assert s.provider_type  # non-empty

    def test_database_memory(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
        assert s.DATABASE == ":memory:"

    def test_to_driver_kwargs_memory(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", auth=NoAuth())
        assert s.to_driver_kwargs() == {"database": ":memory:"}

    def test_to_driver_kwargs_none_database_dropped(self):
        s = SQLiteAuthSettings(auth=NoAuth())
        assert s.to_driver_kwargs() == {}

    def test_type_map_optional(self):
        s = SQLiteAuthSettings(DATABASE=":memory:", TYPE_MAP={"SMALLINT": "int32"},
                               auth=NoAuth())
        assert s.to_driver_kwargs()["type_map"] == {"SMALLINT": "int32"}
