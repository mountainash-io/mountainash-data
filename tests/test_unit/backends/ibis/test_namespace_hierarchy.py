"""Namespace hierarchy support matrix (DEBT-10, spec §10).

Live: DuckDB single-level (CREATE SCHEMA) + two-level (ATTACH).
Render-only: postgres/snowflake/bigquery — spy the raw connection to assert the
coerced Namespace renders to the correct ibis database= shape without a live DB.
DIALECTS[dialect] is import-time-safe for these three dialects — the registry's
connection_builder functions do all driver imports lazily inside the callable,
not at module import time — so no pytest.importorskip guard is needed here;
_RecordingConn stands in for the ibis connection object entirely.
Regression: the database= keyword is gone from the public surface.

Iceberg deep-namespace round-trip is deferred to DEBT-11 (see spec §10 note).
"""

from __future__ import annotations

import pytest

from mountainash_data import IbisBackend
from mountainash_data.backends.ibis.backend import IbisConnection
from mountainash_data.backends.ibis.dialects._registry import DIALECTS
from mountainash_data.core.namespace import Namespace


# --- Live: DuckDB two-level via ATTACH ------------------------------------

def test_duckdb_two_level_attach_roundtrip(tmp_path):
    """A table in an ATTACHed catalog is addressable via Namespace(catalog=...)."""
    other = tmp_path / "other.duckdb"
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        raw = backend.ibis_connection()
        raw.raw_sql(f"ATTACH '{other}' AS other_cat")
        raw.raw_sql("CREATE SCHEMA other_cat.sales")
        raw.raw_sql("CREATE TABLE other_cat.sales.orders (id INTEGER)")

        ns = Namespace(catalog="other_cat", path=("sales",))
        assert "orders" in backend.list_tables(namespace=ns)
        assert backend.table_exists("orders", namespace=ns) is True
        info = backend.inspect_table("orders", namespace=ns)
        assert info.location == ns
        assert info.qualified_name == "other_cat.sales.orders"


def test_duckdb_depth_over_one_raises():
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        with pytest.raises(ValueError, match="single namespace level"):
            backend.list_tables(namespace=("a", "b"))


# --- Render-only spies: postgres/snowflake/bigquery -----------------------

class _RecordingConn:
    """Records the database= value reaching ibis's native calls."""

    def __init__(self):
        self.calls: list[tuple[str, object]] = []

    def list_tables(self, database=None):
        self.calls.append(("list_tables", database))
        return []

    def table(self, name, database=None):
        self.calls.append(("table", database))
        raise RuntimeError("stop after recording")  # inspect not needed here


@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "bigquery"])
def test_catalog_qualified_renders_tuple_to_ibis(dialect):
    rec = _RecordingConn()
    conn = IbisConnection(rec, DIALECTS[dialect])
    conn.list_tables(namespace=Namespace(catalog="wh", path=("sales",)))
    assert rec.calls == [("list_tables", ("wh", "sales"))]


@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "bigquery"])
def test_single_level_renders_str_to_ibis(dialect):
    rec = _RecordingConn()
    conn = IbisConnection(rec, DIALECTS[dialect])
    conn.list_tables(namespace="sales")
    assert rec.calls == [("list_tables", "sales")]


# --- Rename regression (behavioral; replaces a brittle grep) --------------

@pytest.mark.parametrize(
    "call",
    [
        lambda be: be.list_tables(database="x"),
        lambda be: be.inspect_table("t", database="x"),
        lambda be: be.create_table("t", {"id": [1]}, database="x"),
        lambda be: be.drop_table("t", database="x"),
        lambda be: be.table_exists("t", database="x"),
        lambda be: be.upsert("t", {"id": [1]}, conflict_columns=["id"], database="x"),
        lambda be: be.add_columns("t", {"id": "int64"}, database="x"),
        lambda be: be.create_index("t", ["id"], database="x"),
        lambda be: be.index_exists("i", database="x"),
    ],
)
def test_database_keyword_removed_from_public_surface(call):
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        with pytest.raises(TypeError):
            call(backend)
