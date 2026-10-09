from __future__ import annotations

from mountainash_data import IbisBackend


ROWS = [(1, "north", 120), (2, "south", 95)]


def main() -> None:
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        raw = backend.raw_driver_connection()
        raw.execute("CREATE TABLE sales (id INTEGER, region TEXT, total INTEGER)")

        with backend.transaction():
            raw.executemany("INSERT INTO sales VALUES (?, ?, ?)", ROWS)
            assert backend.in_transaction() is True
            assert backend.native_transaction_open() is True
            print(f"scope active: {backend.in_transaction()}")
            print(f"native open: {backend.native_transaction_open()}")
        assert backend.in_transaction() is False
        assert backend.native_transaction_open() is False
        assert raw.execute("SELECT * FROM sales ORDER BY id").fetchall() == ROWS
        print(f"after commit - scope active: {backend.in_transaction()}")
        print(f"after commit - native open: {backend.native_transaction_open()}")
        print(f"committed rows: {raw.execute('SELECT * FROM sales ORDER BY id').fetchall()}")

        try:
            with backend.transaction():
                backend.insert("sales", [{"id": 3, "region": "west", "total": 40}])
                raise ValueError("demonstrate rollback")
        except ValueError:
            pass
        remaining = raw.execute("SELECT * FROM sales ORDER BY id").fetchall()
        assert remaining == ROWS
        print(f"after rollback: {remaining}")


if __name__ == "__main__":
    main()
