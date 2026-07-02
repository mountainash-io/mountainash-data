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
