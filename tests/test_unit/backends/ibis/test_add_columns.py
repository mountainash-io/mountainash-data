"""Tests for dialect-agnostic add_columns (schema evolution)."""

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec


class TestDialectSpecField:
    def test_add_columns_hook_defaults_none(self):
        spec = DialectSpec(
            ibis_backend_name="duckdb",
            connection_mode="connection_string",
            connection_string_scheme="duckdb://",
        )
        assert spec.add_columns_hook is None

    def test_registered_dialects_have_no_hook_initially(self):
        # The generic path covers every dialect; none registers an override.
        assert DIALECTS["duckdb"].add_columns_hook is None
        assert DIALECTS["sqlite"].add_columns_hook is None
        assert DIALECTS["postgres"].add_columns_hook is None
