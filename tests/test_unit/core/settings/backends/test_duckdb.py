"""DuckDB settings round-trip and audit-regression tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mountainash_data.core.settings.duckdb import DuckDBBackendProfile


@pytest.mark.unit
class TestDuckDBBackendProfile:
    def test_default_read_only_is_false(self):
        """Audit regression: previously defaulted True, mismatched Ibis."""
        s = DuckDBBackendProfile()
        assert s.READ_ONLY is False

    def test_memory_database_default(self):
        s = DuckDBBackendProfile()
        assert s.DATABASE is None

    def test_emit_default(self):
        s = DuckDBBackendProfile(DATABASE=":memory:")
        kwargs = s.emit()
        assert kwargs["database"] == ":memory:"
        assert kwargs["read_only"] is False

    def test_memory_limit_decimal_accepted(self):
        """Audit regression: regex previously rejected '1.5GB'."""
        s = DuckDBBackendProfile(DATABASE=":memory:", MEMORY_LIMIT="1.5GB")
        assert s.MEMORY_LIMIT == "1.5GB"

    def test_memory_limit_percent_accepted(self):
        s = DuckDBBackendProfile(DATABASE=":memory:", MEMORY_LIMIT="80%")
        assert s.MEMORY_LIMIT == "80%"

    def test_memory_limit_garbage_rejected(self):
        with pytest.raises(ValidationError):
            DuckDBBackendProfile(DATABASE=":memory:", MEMORY_LIMIT="lots")

    def test_extensions_passed_as_top_level_kwarg(self):
        """Audit regression: extensions was packed inside config dict."""
        s = DuckDBBackendProfile(DATABASE=":memory:", EXTENSIONS=["httpfs"])
        kwargs = s.emit()
        assert kwargs["extensions"] == ["httpfs"]
        # Must NOT appear inside a nested config dict:
        assert "config" not in kwargs or "extensions" not in kwargs.get("config", {})
