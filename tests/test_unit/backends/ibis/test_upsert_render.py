"""Generic upsert — ON CONFLICT family (sqlite/duckdb) + MERGE golden tests."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, UpsertStyle
from mountainash_data.backends.ibis.operations import (
    _generic_upsert,
    build_merge_sql,
    build_on_duplicate_key_sql,
)

# ibis backend name -> sqlglot dialect name (identity unless listed).
# Mirrors _IBIS_TO_SQLGLOT in test_rename_table_render.py; kept here to avoid
# a cross-test-module import (tests/ has no top-level __init__.py).
_IBIS_TO_SQLGLOT = {
    "mssql": "tsql",
    "motherduck": "duckdb",
    "singlestoredb": "singlestore",
    "impala": "hive",
    "pyspark": "spark",
}


def _seed(con):
    con.create_table("t", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))
    con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")


class TestOnConflictUpsert:
    def test_insert_and_update_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        rows = dict(con.table("t").order_by("id").execute()[["id", "v"]].itertuples(index=False))
        assert rows == {1: "a", 2: "B", 3: "c"}

    def test_do_nothing_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2], "v": ["X"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns="id",
            update_columns=None, conflict_action="NOTHING",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").filter(ibis._.id == 2).execute()["v"].iloc[0] == "b"

    def test_composite_key_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("t", pl.DataFrame({"a": [1], "b": [1], "v": ["x"]}))
        # needs a composite unique index for ON CONFLICT to detect
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (a, b)")
        _generic_upsert(
            con, "t", pl.DataFrame({"a": [1], "b": [1], "v": ["y"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["a", "b"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").execute()["v"].iloc[0] == "y"

    def test_conditional_update_only_when_newer_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "ver": [5], "v": ["old"]}))
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (id)")
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [1], "ver": [3], "v": ["stale"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=lambda inc, exi: inc.ver > exi.ver,
            database=None, schema=None,
        )
        # incoming ver(3) is NOT newer than existing(5) -> unchanged
        assert con.table("t").execute()["v"].iloc[0] == "old"

    def test_unknown_style_raises_notimplemented(self):
        con = ibis.duckdb.connect()
        _seed(con)
        with pytest.raises(NotImplementedError):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [9], "v": ["z"]}),
                style=None, conflict_columns=["id"], update_columns=None,
                conflict_action="UPDATE", update_condition=None,
                database=None, schema=None,
            )


def _sqlglot_dialect(spec: object) -> str:
    """Map a DialectSpec to its sqlglot dialect name."""
    ibis_name: str = spec.ibis_backend_name  # type: ignore[attr-defined]
    return _IBIS_TO_SQLGLOT.get(ibis_name, ibis_name)


@pytest.mark.parametrize(
    "name",
    [n for n, s in DIALECTS.items() if s.upsert_style is UpsertStyle.MERGE],
)
def test_merge_golden_per_dialect(name: str) -> None:
    """Every MERGE-family dialect renders a valid MERGE INTO statement."""
    d = _sqlglot_dialect(DIALECTS[name])
    sql = build_merge_sql(
        dialect=d,
        target=f'"{name}"',
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="UPDATE",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert sql.startswith("MERGE INTO"), f"{name}: expected MERGE INTO, got: {sql[:60]}"
    assert "WHEN MATCHED THEN UPDATE SET" in sql, f"{name}: missing WHEN MATCHED: {sql}"
    assert "WHEN NOT MATCHED THEN INSERT" in sql, f"{name}: missing WHEN NOT MATCHED: {sql}"
    # backtick-quoting MERGE-family dialects. mysql also backticks but is
    # ON_DUPLICATE_KEY (never reaches this MERGE-only parametrization), so it
    # is intentionally not listed here.
    _BACKTICK_DIALECTS = {"bigquery", "databricks"}
    uses_backtick = "`" in sql
    assert uses_backtick == (d in _BACKTICK_DIALECTS), (
        f"{name} (dialect={d}): unexpected quoting style in: {sql}"
    )


@pytest.mark.parametrize(
    "name",
    [n for n, s in DIALECTS.items() if s.upsert_style is UpsertStyle.MERGE],
)
def test_merge_golden_nothing_omits_matched(name: str) -> None:
    """MERGE with conflict_action=NOTHING omits the WHEN MATCHED clause."""
    d = _sqlglot_dialect(DIALECTS[name])
    sql = build_merge_sql(
        dialect=d,
        target=f'"{name}"',
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="NOTHING",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert sql.startswith("MERGE INTO"), f"{name}: expected MERGE INTO"
    assert "WHEN MATCHED" not in sql, f"{name}: NOTHING should omit WHEN MATCHED: {sql}"
    assert "WHEN NOT MATCHED THEN INSERT" in sql, f"{name}: missing WHEN NOT MATCHED"


# ---------------------------------------------------------------------------
# ON DUPLICATE KEY golden tests (pure builder — no live MySQL required)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    [n for n, s in DIALECTS.items() if s.upsert_style is UpsertStyle.ON_DUPLICATE_KEY],
)
def test_on_duplicate_key_golden_update(name: str) -> None:
    """Every ON_DUPLICATE_KEY dialect renders INSERT … ON DUPLICATE KEY UPDATE … VALUES(…)."""
    d = _sqlglot_dialect(DIALECTS[name])
    sql = build_on_duplicate_key_sql(
        dialect=d,
        target=f"`{name}`",
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="UPDATE",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert "ON DUPLICATE KEY UPDATE" in sql, f"{name}: missing ON DUPLICATE KEY UPDATE: {sql}"
    assert "VALUES(" in sql, f"{name}: missing VALUES(: {sql}"


@pytest.mark.parametrize(
    "name",
    [n for n, s in DIALECTS.items() if s.upsert_style is UpsertStyle.ON_DUPLICATE_KEY],
)
def test_on_duplicate_key_golden_nothing(name: str) -> None:
    """ON_DUPLICATE_KEY with conflict_action=NOTHING uses self-assign no-op."""
    d = _sqlglot_dialect(DIALECTS[name])
    sql = build_on_duplicate_key_sql(
        dialect=d,
        target=f"`{name}`",
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="NOTHING",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert "ON DUPLICATE KEY UPDATE" in sql, f"{name}: missing ON DUPLICATE KEY UPDATE: {sql}"
    # NOTHING uses self-assign: the first conflict column appears twice with =
    assert "VALUES(" not in sql, f"{name}: NOTHING should not use VALUES(): {sql}"
