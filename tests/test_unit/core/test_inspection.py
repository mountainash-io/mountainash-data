"""Tests for core.inspection — the shared physical metadata model."""

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.namespace import Namespace


class TestColumnInfo:
    def test_minimal_column(self):
        col = ColumnInfo(name="id", type_name="int64", nullable=False)
        assert col.name == "id"
        assert col.type_name == "int64"
        assert col.nullable is False

    def test_column_with_metadata(self):
        col = ColumnInfo(
            name="created_at", type_name="timestamp", nullable=True,
            description="row creation time",
        )
        assert col.description == "row creation time"


class TestTableInfo:
    def test_table_with_columns(self):
        cols = [
            ColumnInfo(name="id", type_name="int64", nullable=False),
            ColumnInfo(name="name", type_name="string", nullable=True),
        ]
        table = TableInfo(name="users", columns=cols)
        assert table.name == "users"
        assert table.column_names == ["id", "name"]
        assert table.location == Namespace()

    def test_qualified_name_with_catalog(self):
        table = TableInfo(
            name="users", columns=[],
            location=Namespace(catalog="main", path=("public",)),
        )
        assert table.qualified_name == "main.public.users"

    def test_qualified_name_no_catalog(self):
        table = TableInfo(name="users", columns=[], location=Namespace(path=("public",)))
        assert table.qualified_name == "public.users"

    def test_qualified_name_deep_path_roundtrips(self):
        table = TableInfo(name="t", columns=[], location=Namespace(path=("a", "b", "c")))
        assert table.qualified_name == "a.b.c.t"

    def test_qualified_name_bare(self):
        table = TableInfo(name="users", columns=[])
        assert table.qualified_name == "users"


class TestNamespaceInfo:
    def test_namespace_with_tables(self):
        ns = NamespaceInfo(location=Namespace(path=("public",)), tables=["users", "orders"])
        assert ns.name == "public"
        assert ns.tables == ["users", "orders"]

    def test_name_is_last_segment(self):
        ns = NamespaceInfo(location=Namespace(path=("a", "b")), tables=[])
        assert ns.name == "b"

    def test_name_empty_for_default(self):
        ns = NamespaceInfo(location=Namespace(), tables=[])
        assert ns.name == ""


class TestCatalogInfo:
    def test_catalog_with_namespaces(self):
        cat = CatalogInfo(
            name="main",
            namespaces=[NamespaceInfo(location=Namespace(path=("public",)), tables=["users"])],
        )
        assert cat.name == "main"
        assert len(cat.namespaces) == 1
