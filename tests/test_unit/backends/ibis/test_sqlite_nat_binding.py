"""Regression tests for DEBT-13 — sqlite write ops crash on null temporal
values (NaT binding).

ibis's SQLite backend stages every in-memory table via a pandas roundtrip
(``op.data.to_frame()``) before binding rows through stdlib ``sqlite3``. A
null ``date``/``timestamp`` value becomes pandas ``NaT`` during that
roundtrip, and ``sqlite3`` has no adapter for ``NaTType`` — this crashes
``create_table``, ``insert``, and ``upsert`` for any frame containing a null
temporal value. Tracked upstream as ``IB-DT-19`` in
``mountainash/registry/upstream-issues.yaml`` (status: ``needs_filing``).

See ``mountainash-central`` backlog:
``04.planning/mountainash-data/a.backlog/2026-08-18-sqlite-null-temporal-binding.md``.
"""

import datetime
import subprocess
import sys
import textwrap

import polars as pl
import pytest

from mountainash_data.backends.ibis.backend import IbisBackend


class TestCreateTableNullTemporal:
    def test_null_date_column(self):
        df = pl.DataFrame(
            {"id": [1, 2], "d": [datetime.date(2024, 1, 1), None]},
            schema={"id": pl.Int64, "d": pl.Date},
        )
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", df)
            result = backend.table("t").to_pandas()
        assert result["d"].isna().sum() == 1

    def test_null_datetime_column(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "ts": [datetime.datetime(2024, 1, 1, 12, 0, 0), None],
            },
            schema={"id": pl.Int64, "ts": pl.Datetime},
        )
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table("t", df)
            result = backend.table("t").to_pandas()
        assert result["ts"].isna().sum() == 1


class TestInsertNullTemporal:
    def test_null_datetime_column(self):
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table(
                "t", None, schema={"id": "int64", "ts": "timestamp"}
            )
            df = pl.DataFrame(
                {
                    "id": [1, 2],
                    "ts": [datetime.datetime(2024, 1, 1, 12, 0, 0), None],
                },
                schema={"id": pl.Int64, "ts": pl.Datetime},
            )
            backend.insert("t", df)
            result = backend.table("t").to_pandas()
        assert result["ts"].isna().sum() == 1


class TestUpsertNullTemporal:
    def test_null_datetime_column_update_style(self):
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table(
                "t", None, schema={"id": "int64", "ts": "timestamp"}
            )
            backend.create_index("t", ["id"], unique=True)
            df = pl.DataFrame(
                {
                    "id": [1, 2],
                    "ts": [datetime.datetime(2024, 1, 1, 12, 0, 0), None],
                },
                schema={"id": pl.Int64, "ts": pl.Datetime},
            )
            backend.upsert("t", df, conflict_columns=["id"])
            result = backend.table("t").to_pandas()
        assert result["ts"].isna().sum() == 1

    def test_null_datetime_column_nothing_style(self):
        with IbisBackend(dialect="sqlite", database=":memory:") as backend:
            backend.create_table(
                "t", None, schema={"id": "int64", "ts": "timestamp"}
            )
            backend.create_index("t", ["id"], unique=True)
            df = pl.DataFrame(
                {
                    "id": [1, 2],
                    "ts": [datetime.datetime(2024, 1, 1, 12, 0, 0), None],
                },
                schema={"id": pl.Int64, "ts": pl.Datetime},
            )
            backend.upsert(
                "t", df, conflict_columns=["id"], conflict_action="NOTHING"
            )
            result = backend.table("t").to_pandas()
        assert result["ts"].isna().sum() == 1


def test_raw_ibis_sqlite_null_temporal_upstream_bug_ib_dt_19():
    """Upstream-fix monitor (IB-DT-19), isolated from mountainash-data's own
    process-global sqlite3 adapter patch via a subprocess.

    Reproduces the raw ibis-sqlite crash directly (no mountainash-data
    involved) so this flips to a failure the moment ibis fixes the bug
    upstream — a signal to remove mountainash-data's own workaround and
    close DEBT-13/IB-DT-19 for good.
    """
    script = textwrap.dedent(
        """
        import ibis, polars as pl, datetime
        con = ibis.sqlite.connect()
        df = pl.DataFrame(
            {"id": [1, 2], "ts": [datetime.datetime(2024, 1, 1), None]},
            schema={"id": pl.Int64, "ts": pl.Datetime},
        )
        con.create_table("t", df)
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )
    if proc.returncode == 0:
        pytest.fail(
            "ibis-sqlite no longer crashes on null datetime binding — "
            "IB-DT-19 appears fixed upstream. Remove mountainash-data's "
            "_sqlite_compat workaround and close DEBT-13."
        )
    assert "NaTType" in proc.stderr and "not supported" in proc.stderr
