"""Tests for core.inspection — the shared physical metadata model."""

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


class TestColumnInfo:
    def test_minimal_column(self):
        col = ColumnInfo(name="id", type_name="int64", nullable=False)
        assert col.name == "id"
        assert col.type_name == "int64"
        assert col.nullable is False

    def test_column_with_metadata(self):
        col = ColumnInfo(
            name="created_at",
            type_name="timestamp",
            nullable=True,
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
        assert len(table.columns) == 2
        assert table.column_names == ["id", "name"]

    def test_table_qualified_name(self):
        table = TableInfo(
            name="users",
            columns=[],
            namespace="public",
            catalog="main",
        )
        assert table.qualified_name == "main.public.users"

    def test_table_qualified_name_no_catalog(self):
        table = TableInfo(name="users", columns=[], namespace="public")
        assert table.qualified_name == "public.users"

    def test_table_qualified_name_bare(self):
        table = TableInfo(name="users", columns=[])
        assert table.qualified_name == "users"


class TestNamespaceInfo:
    def test_namespace_with_tables(self):
        ns = NamespaceInfo(name="public", tables=["users", "orders"])
        assert ns.name == "public"
        assert ns.tables == ["users", "orders"]


class TestCatalogInfo:
    def test_catalog_with_namespaces(self):
        cat = CatalogInfo(
            name="main",
            namespaces=[NamespaceInfo(name="public", tables=["users"])],
        )
        assert cat.name == "main"
        assert len(cat.namespaces) == 1
