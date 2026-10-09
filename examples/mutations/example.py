"""Upsert a sale and add a nullable detail column to the local table."""

from mountainash_data import IbisBackend


ROWS = [
    {"id": 1, "region": "north", "total": 120},
    {"id": 2, "region": "south", "total": 95},
]


def main() -> None:
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("sales", ROWS)
        backend.create_index("sales", "id", index_name="ux_sales_id", unique=True)
        backend.upsert(
            "sales",
            [
                {"id": 1, "region": "north", "total": 140},
                {"id": 3, "region": "west", "total": 80},
            ],
            conflict_columns="id",
        )
        backend.add_columns("sales", {"note": "string"})

        result = backend.table("sales").order_by("id").to_pyarrow().to_pylist()
        assert result == [
            {"id": 1, "region": "north", "total": 140, "note": None},
            {"id": 2, "region": "south", "total": 95, "note": None},
            {"id": 3, "region": "west", "total": 80, "note": None},
        ]
        print(f"updated north total: {result[0]['total']}")
        print(f"inserted west total: {result[2]['total']}")
        print(f"added column: note ({len(result)} null values)")


if __name__ == "__main__":
    main()
