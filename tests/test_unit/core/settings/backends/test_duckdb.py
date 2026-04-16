"""DuckDB settings round-trip and audit-regression tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.duckdb import DuckDBAuthSettings


@pytest.mark.unit
class TestDuckDBAuthSettings:
    def test_default_read_only_is_false(self):
        """Audit regression: previously defaulted True, mismatched Ibis."""
        s = DuckDBAuthSettings(auth=NoAuth())
        assert s.READ_ONLY is False

    def test_memory_database_default(self):
        s = DuckDBAuthSettings(auth=NoAuth())
        assert s.DATABASE is None

    def test_to_driver_kwargs_default(self):
        s = DuckDBAuthSettings(DATABASE=":memory:", auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert kwargs["database"] == ":memory:"
        assert kwargs["read_only"] is False

    def test_memory_limit_decimal_accepted(self):
        """Audit regression: regex previously rejected '1.5GB'."""
        s = DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="1.5GB",
                               auth=NoAuth())
        assert s.MEMORY_LIMIT == "1.5GB"

    def test_memory_limit_percent_accepted(self):
        s = DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="80%",
                               auth=NoAuth())
        assert s.MEMORY_LIMIT == "80%"

    def test_memory_limit_garbage_rejected(self):
        with pytest.raises(ValidationError):
            DuckDBAuthSettings(DATABASE=":memory:", MEMORY_LIMIT="lots",
                               auth=NoAuth())

    def test_extensions_passed_as_top_level_kwarg(self):
        """Audit regression: extensions was packed inside config dict."""
        s = DuckDBAuthSettings(DATABASE=":memory:", EXTENSIONS=["httpfs"],
                               auth=NoAuth())
        kwargs = s.to_driver_kwargs()
        assert kwargs["extensions"] == ["httpfs"]
        # Must NOT appear inside a nested config dict:
        assert "config" not in kwargs or "extensions" not in kwargs.get("config", {})
