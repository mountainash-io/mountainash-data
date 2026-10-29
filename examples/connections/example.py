from __future__ import annotations

from mountainash_data import IbisBackend
from mountainash_data.core.settings import NoAuthProfile, SQLiteBackendProfile
from mountainash_settings import SettingsParameters


def show(label: str, backend: IbisBackend) -> None:
    try:
        backend.connect(auth_profile=NoAuthProfile())
        raw = backend.raw_driver_connection()
        raw.execute("CREATE TABLE sales (id INTEGER, region TEXT, total INTEGER)")
        raw.executemany(
            "INSERT INTO sales VALUES (?, ?, ?)",
            [(1, "north", 120), (2, "south", 95)],
        )
        rows = raw.execute("SELECT id, region, total FROM sales ORDER BY id").fetchall()
        assert rows == [(1, "north", 120), (2, "south", 95)]
        print(f"{label}: {rows}")
    finally:
        backend.close()


def main() -> None:
    show("dialect", IbisBackend(dialect="sqlite", database=":memory:"))
    show("url", IbisBackend("sqlite://"))
    params = SettingsParameters.create(
        settings_class=SQLiteBackendProfile,
        DATABASE=":memory:",
    )
    show("settings", IbisBackend(params))
    print("auth: supplied separately to connect()")


if __name__ == "__main__":
    main()
