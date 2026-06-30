"""The upsert_style assignment must match the spec's §7 coverage matrix."""

from mountainash_data.backends.ibis.dialects._registry import (
    DIALECTS,
    DialectSpec,
    UpsertStyle,
)

# Spec §7 coverage matrix — the single source of truth for this assertion.
EXPECTED_STYLE = {
    "sqlite": UpsertStyle.ON_CONFLICT,
    "duckdb": UpsertStyle.ON_CONFLICT,
    "motherduck": UpsertStyle.ON_CONFLICT,
    "postgres": UpsertStyle.ON_CONFLICT,
    "risingwave": UpsertStyle.ON_CONFLICT,
    "mysql": UpsertStyle.ON_DUPLICATE_KEY,
    "singlestoredb": UpsertStyle.ON_DUPLICATE_KEY,
    "snowflake": UpsertStyle.MERGE,
    "bigquery": UpsertStyle.MERGE,
    "mssql": UpsertStyle.MERGE,
    "oracle": UpsertStyle.MERGE,
    "databricks": UpsertStyle.MERGE,
    "exasol": UpsertStyle.MERGE,
    "trino": UpsertStyle.MERGE,
    "redshift": UpsertStyle.MERGE,
    "clickhouse": None,
    "impala": None,
    "materialize": None,
    "druid": None,
    "pyspark": None,
}


class TestUpsertStyleField:
    def test_field_defaults_none(self):
        spec = DialectSpec(
            ibis_backend_name="duckdb",
            connection_mode="connection_string",
            connection_string_scheme="duckdb://",
        )
        assert spec.upsert_style is None

    def test_every_registry_dialect_has_an_explicit_decision(self):
        # Iterates the live registry — a new dialect with no matrix entry fails.
        assert set(DIALECTS) == set(EXPECTED_STYLE), (
            "registry dialects and the §7 matrix have diverged"
        )

    def test_assigned_styles_match_matrix(self):
        for name, expected in EXPECTED_STYLE.items():
            assert DIALECTS[name].upsert_style == expected, name
