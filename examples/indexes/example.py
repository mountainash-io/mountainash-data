"""Create, inspect, and drop an index with local SQLite."""

from mountainash_data import IbisBackend


ROWS = [
    {"id": 1, "region": "north", "total": 120},
    {"id": 2, "region": "south", "total": 95},
]


def main() -> None:
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("sales", ROWS)
        backend.create_index("sales", "region", index_name="ix_sales_region")

        indexes = backend.list_indexes("sales")
        assert len(indexes) == 1
        assert indexes[0].name == "ix_sales_region"
        assert indexes[0].columns == ("region",)
        backend.drop_index("ix_sales_region", table_name="sales")
        remaining = backend.list_indexes("sales")
        assert remaining == []
        print(f"created index: {indexes[0].name} on {', '.join(indexes[0].columns)}")
        print(f"indexes after drop: {len(remaining)}")


if __name__ == "__main__":
    main()
