"""Inspect SQLite objects using a backend-agnostic Namespace."""

from mountainash_data import IbisBackend, Namespace


ROWS = [
    {"id": 1, "region": "north", "total": 120},
    {"id": 2, "region": "south", "total": 95},
]


def main() -> None:
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("sales", ROWS, namespace="main")

        location = Namespace(path=("main",))
        tables = backend.list_tables(namespace=location)
        info = backend.inspect_table("sales", namespace=location)
        namespace_info = backend.inspect_namespace("main")
        assert tables == ["sales"]
        assert info.location == location
        assert [column.name for column in info.columns] == ["id", "region", "total"]
        assert namespace_info.name == "main"
        assert "sales" in namespace_info.tables
        print(f"namespace: {namespace_info.name}")
        print(f"tables: {', '.join(tables)}")
        print(f"columns: {', '.join(column.name for column in info.columns)}")


if __name__ == "__main__":
    main()
