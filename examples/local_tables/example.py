"""Create, populate, query, and evaluate a small local sales table."""

from mountainash_data import IbisBackend


ROWS = [
    {"id": 1, "region": "north", "total": 120},
    {"id": 2, "region": "south", "total": 95},
]


def main() -> None:
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("sales", ROWS)
        backend.insert("sales", [{"id": 3, "region": "north", "total": 80}])

        sales = backend.table("sales")
        result = (
            sales.filter(sales.region == "north")
            .aggregate(total=sales.total.sum())
            .to_pyarrow()
        )
        rows = backend.table("sales").order_by("id").to_pyarrow().to_pylist()
        assert result["total"].to_pylist() == [200]
        assert rows == ROWS + [{"id": 3, "region": "north", "total": 80}]
        print(f"north total: {result['total'].to_pylist()[0]}")
        print(f"sales rows: {len(rows)}")


if __name__ == "__main__":
    main()
