from __future__ import annotations

import duckdb

from mountainash_data import IbisBackend


def main() -> None:
    raw = duckdb.connect(":memory:")
    raw.execute("CREATE TABLE sales (id INTEGER, region TEXT, total INTEGER)")
    raw.executemany(
        "INSERT INTO sales VALUES (?, ?, ?)",
        [(1, "north", 120), (2, "south", 95)],
    )

    try:
        backend = IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=True)
        backend.connect()
        backend.close()
        # Closing the borrowed wrapper must leave this caller-owned handle alive.
        rows = raw.execute("SELECT * FROM sales ORDER BY id").fetchall()
        assert rows == [(1, "north", 120), (2, "south", 95)]
        print(f"raw usable after wrapper close: {rows}")

        raw.execute("BEGIN")
        raw.execute("INSERT INTO sales VALUES (3, 'west', 40)")
        backend = IbisBackend.from_raw_connection(raw, dialect="duckdb", preserve_session=True)
        backend.connect()
        with backend.transaction():
            raw.execute("INSERT INTO sales VALUES (4, 'east', 70)")
        assert backend.native_transaction_open() is True
        print(f"scope joined caller transaction: {backend.native_transaction_open()}")
        backend.close()

        # The wrapper scope joined the pre-existing native transaction; it did
        # not commit it. The caller remains responsible for its transaction.
        raw.rollback()
        rows = raw.execute("SELECT * FROM sales ORDER BY id").fetchall()
        assert rows == [(1, "north", 120), (2, "south", 95)]
        print(f"caller rollback rows: {rows}")
    finally:
        raw.close()


if __name__ == "__main__":
    main()
