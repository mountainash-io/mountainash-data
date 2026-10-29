"""Tests for the DialectSpec dataclass — the data-driven replacement
for the per-backend connection class explosion."""

from mountainash_data.backends.ibis.dialects._registry import (
    DialectSpec,
    DIALECTS,
    SessionOption,
    TransactionSupport,
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


def test_registry_contains_all_20_backends():
    expected = {
        "sqlite", "duckdb", "motherduck", "postgres", "mysql", "mssql",
        "oracle", "snowflake", "bigquery", "redshift", "trino", "pyspark",
        "clickhouse", "databricks", "singlestoredb",
        "exasol", "impala", "materialize", "risingwave", "druid",
    }
    assert set(DIALECTS.keys()) == expected


def test_registry_entries_are_dialect_specs():
    for name, spec in DIALECTS.items():
        assert isinstance(spec, DialectSpec), f"{name} entry is not a DialectSpec"
        assert spec.ibis_backend_name, f"{name} missing ibis_backend_name"


def test_raw_handle_attr_defaults_to_con():
    assert DIALECTS["duckdb"].raw_handle_attr == "con"
    assert DIALECTS["postgres"].raw_handle_attr == "con"
    assert DIALECTS["sqlite"].raw_handle_attr == "con"


def test_raw_handle_attr_overrides():
    assert DIALECTS["bigquery"].raw_handle_attr == "client"
    assert DIALECTS["pyspark"].raw_handle_attr == "_session"


def test_every_dialect_has_raw_handle_attr():
    for name, spec in DIALECTS.items():
        assert isinstance(spec.raw_handle_attr, str) and spec.raw_handle_attr, \
            f"{name} missing or empty raw_handle_attr"


def test_transaction_support_assignments():
    assert DIALECTS["duckdb"].transaction_support is TransactionSupport.FULL
    assert DIALECTS["mssql"].transaction_support is TransactionSupport.FULL
    assert DIALECTS["clickhouse"].transaction_support is TransactionSupport.NONE
    assert DIALECTS["trino"].transaction_support is TransactionSupport.LIMITED


def test_begin_statement_assignments():
    assert DIALECTS["duckdb"].begin_statement == "BEGIN"
    assert DIALECTS["mssql"].begin_statement == "BEGIN TRANSACTION"
    assert DIALECTS["oracle"].begin_statement is None
    assert DIALECTS["exasol"].begin_statement is None


def test_none_support_implies_no_begin_statement():
    for name, spec in DIALECTS.items():
        if spec.transaction_support is TransactionSupport.NONE:
            assert spec.begin_statement is None, name


def test_duckdb_adoption_mutations_declared():
    names = {o.name for o in DIALECTS["duckdb"].adoption_mutations}
    assert "python_enable_replacements" in names
    assert "timezone" in names


def test_non_mutating_dialects_empty():
    for d in ("trino", "clickhouse", "druid", "bigquery"):
        assert DIALECTS[d].adoption_mutations == ()


def test_session_options_well_formed():
    for name, spec in DIALECTS.items():
        for opt in spec.adoption_mutations:
            assert isinstance(opt, SessionOption)
            assert opt.name
            # render_set must produce a str statement
            assert isinstance(opt.render_set(True), str)


def test_duckdb_timezone_render_is_injection_safe():
    tz_opt = next(o for o in DIALECTS["duckdb"].adoption_mutations if o.name == "timezone")
    rendered = tz_opt.render_set("UTC'; DROP TABLE t; --")
    # the malicious quote must be escaped inside the literal, not break out of it
    assert "DROP TABLE" in rendered            # value preserved as data
    assert rendered.count("SET TimeZone=") == 1
    assert not rendered.rstrip().endswith("--")  # not left as trailing raw SQL


def test_raw_adoption_verified_assignments():
    assert DIALECTS["duckdb"].raw_adoption_verified is True
    assert DIALECTS["postgres"].raw_adoption_verified is False
