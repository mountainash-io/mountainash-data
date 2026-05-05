"""Tests for the DialectSpec dataclass — the data-driven replacement
for the per-backend connection class explosion."""

from mountainash_data.backends.ibis.dialects._registry import (
    DialectSpec,
    DIALECTS,
)


def test_dialect_spec_minimal():
    spec = DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode="DIRECT",
        connection_string_scheme="sqlite",
    )
    assert spec.ibis_backend_name == "sqlite"
    assert spec.connection_mode == "DIRECT"
    assert spec.get_index_exists_sql is None
    assert spec.get_list_indexes_sql is None


def test_dialect_spec_with_capability_hooks():
    def fake_index_sql(table_name, index_name):
        return f"SELECT 1 FROM {table_name}"

    spec = DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode="DIRECT",
        connection_string_scheme="duckdb",
        get_index_exists_sql=fake_index_sql,
    )
    assert spec.get_index_exists_sql is not None
    assert spec.get_index_exists_sql("users", "idx_users_id") == "SELECT 1 FROM users"


def test_registry_contains_all_13_backends():
    expected = {
        "sqlite", "duckdb", "motherduck", "postgres", "mysql", "mssql",
        "oracle", "snowflake", "bigquery", "redshift", "trino", "pyspark",
        "clickhouse",
    }
    assert set(DIALECTS.keys()) == expected


def test_registry_entries_are_dialect_specs():
    for name, spec in DIALECTS.items():
        assert isinstance(spec, DialectSpec), f"{name} entry is not a DialectSpec"
        assert spec.ibis_backend_name, f"{name} missing ibis_backend_name"
