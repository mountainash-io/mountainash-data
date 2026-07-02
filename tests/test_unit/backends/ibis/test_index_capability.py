"""IndexCapability descriptor + DropScope enum (registry capability model)."""

import dataclasses

import pytest

from mountainash_data.backends.ibis.dialects._registry import (
    DialectSpec,
    DropScope,
    IndexCapability,
)


def test_dropscope_members():
    assert DropScope.SCHEMA_GLOBAL.value == "schema_global"
    assert DropScope.TABLE_SCOPED.value == "table_scoped"


def test_index_capability_is_frozen():
    caps = IndexCapability(
        drop_scope=DropScope.SCHEMA_GLOBAL,
        partial=True,
        native_if_not_exists=True,
        native_if_exists=True,
        index_types=frozenset({"btree"}),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        caps.partial = False  # type: ignore[misc]


def test_dialectspec_index_caps_defaults_none():
    spec = DialectSpec(
        ibis_backend_name="x",
        connection_mode="kwargs",
        connection_string_scheme="x://",
    )
    assert spec.index_caps is None


from mountainash_data.backends.ibis.dialects._registry import DIALECTS

# Verified against official vendor docs 2026-06-30 (spec §4). frozenset of USING types.
# (drop_scope, partial, native_if_not_exists, native_if_exists, index_types)
_EXPECTED = {
    "sqlite":        (DropScope.SCHEMA_GLOBAL, True,  True,  True,  frozenset()),
    "duckdb":        (DropScope.SCHEMA_GLOBAL, False, True,  True,  frozenset()),
    "motherduck":    (DropScope.SCHEMA_GLOBAL, False, True,  True,  frozenset()),
    "postgres":      (DropScope.SCHEMA_GLOBAL, True,  True,  True,
                      frozenset({"btree", "hash", "gist", "gin", "brin", "spgist"})),
    "mysql":         (DropScope.TABLE_SCOPED,  False, False, False, frozenset({"btree"})),
    "singlestoredb": (DropScope.TABLE_SCOPED,  False, False, False, frozenset({"btree", "hash"})),
    "mssql":         (DropScope.TABLE_SCOPED,  True,  False, True,  frozenset()),
    "oracle":        (DropScope.SCHEMA_GLOBAL, False, False, False, frozenset()),
}

# Dialects that carry index_caps after THIS task. Task 5 appends the other 5.
_ASSIGNED_NOW = ["sqlite", "duckdb", "motherduck"]

_UNSUPPORTED = {
    "snowflake", "bigquery", "redshift", "trino", "clickhouse", "databricks",
    "exasol", "impala", "materialize", "risingwave", "druid", "pyspark",
}


@pytest.mark.parametrize("name", _ASSIGNED_NOW)
def test_index_caps_matrix(name):
    caps = DIALECTS[name].index_caps
    assert caps is not None, f"{name} must have index_caps"
    drop_scope, partial, ine, ie, types = _EXPECTED[name]
    assert caps.drop_scope is drop_scope
    assert caps.partial is partial
    assert caps.native_if_not_exists is ine
    assert caps.native_if_exists is ie
    assert caps.index_types == types


@pytest.mark.parametrize("name", sorted(_UNSUPPORTED))
def test_unsupported_dialects_have_no_index_caps(name):
    assert DIALECTS[name].index_caps is None


@pytest.mark.parametrize("name", _ASSIGNED_NOW)
def test_invariant_caps_implies_exists_sql(name):
    """Spec §3 invariant: a dialect with index_caps must also introspect indexes."""
    spec = DIALECTS[name]
    assert spec.index_caps is not None
    assert spec.get_index_exists_sql is not None


def test_no_dialect_violates_invariant():
    """Stronger guard: NO dialect may have index_caps without exists_sql — true at
    every commit, including this one (the other 5 caps are not assigned yet)."""
    for name, spec in DIALECTS.items():
        if spec.index_caps is not None:
            assert spec.get_index_exists_sql is not None, (
                f"{name}: index_caps set but get_index_exists_sql missing"
            )
