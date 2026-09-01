"""Generic upsert — ON CONFLICT family (sqlite/duckdb) + MERGE golden tests."""

import re
import warnings

import ibis
import pandas as pd
import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, UpsertStyle
from mountainash_data.backends.ibis.operations import (
    _generic_upsert,
    _validate_simple_identifier,
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
            update_condition=None, namespace=None, schema=None,
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
            update_condition=None, namespace=None, schema=None,
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
            update_condition=None, namespace=None, schema=None,
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
            namespace=None, schema=None,
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
                namespace=None, schema=None,
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


def test_merge_oracle_omits_as_and_parenthesizes_on() -> None:
    """DEBT-3, live-verified against real Oracle 21c XE: Oracle's grammar
    never accepts AS before a table/subquery alias (ORA-02012, missing
    USING keyword) and requires the ON condition parenthesized
    (ORA-00969, missing ON keyword) -- both unlike every other
    MERGE-family dialect."""
    sql = build_merge_sql(
        dialect="oracle",
        target='"up_ora"',
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="UPDATE",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert "AS tgt" not in sql, f"oracle must not emit AS before the target alias: {sql}"
    assert "AS src" not in sql, f"oracle must not emit AS before the source alias: {sql}"
    assert re.search(r'ON \([^)]+\)', sql), f"oracle ON condition must be parenthesized: {sql}"
    assert 'MERGE INTO "up_ora" tgt USING' in sql, f"unexpected target/alias shape: {sql}"


def test_merge_mssql_terminates_statement() -> None:
    """SQL Server raises error 10713 when MERGE lacks its required semicolon."""
    sql = build_merge_sql(
        dialect="tsql",
        target='"up_mssql"',
        cols=["id", "v"],
        conflict=["id"],
        update=["v"],
        conflict_action="UPDATE",
        source_sql="SELECT 1 AS id, 'a' AS v",
    )

    assert sql.endswith(";")


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
    """ON_DUPLICATE_KEY with conflict_action=NOTHING self-assigns the first
    conflict column via VALUES(col), not a bare col = col — a bare
    self-assign is ambiguous on real MySQL/MariaDB because the INSERT
    source is `SELECT ... FROM (subquery) AS __src`, which always projects
    the conflict column too (verified live: MySQLdb.OperationalError 1052,
    "Column 'id' in UPDATE is ambiguous"). VALUES(col) is the documented
    MySQL idiom for an unambiguous self-reference."""
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
    assert "`id` = VALUES(`id`)" in sql, f"{name}: NOTHING should self-assign via VALUES(`id`): {sql}"
    assert "`v`" not in sql.split("ON DUPLICATE KEY UPDATE")[1], f"{name}: NOTHING must not touch non-key columns: {sql}"


class TestIdentifierValidationHardening:
    """_validate_simple_identifier is the primary gate against SQL injection in
    the MySQL preflight's string-literal interpolation (final-review finding)."""

    @pytest.mark.parametrize(
        "bad",
        [
            "x'y",                 # single quote — would break out of a literal
            "x' OR '1'='1",        # classic injection payload
            "a.b",                 # dotted (namespace) — still rejected
            "a b",                 # whitespace
            "tbl;DROP TABLE x",    # statement separator
            "tbl--",               # comment
            "1abc",                # leading digit
            "tbl`name",            # backtick
            'tbl"name',            # double quote
            "",                    # empty
        ],
    )
    def test_rejects_unsafe_identifiers(self, bad: str) -> None:
        with pytest.raises(ValueError, match="simple identifier"):
            _validate_simple_identifier(bad, kind="name")

    @pytest.mark.parametrize(
        "good", ["users", "_private", "T1", "wearables_events", "col$x", "a1_b2"]
    )
    def test_accepts_safe_identifiers(self, good: str) -> None:
        _validate_simple_identifier(good, kind="name")  # no raise


def test_upsert_rejects_catalog_qualified_namespace():
    """upsert builds engine-native SQL; a catalog-qualified namespace must raise
    a clean ValueError, never reach the SQL builders."""
    import pytest
    from mountainash_data import IbisBackend
    from mountainash_data.core.namespace import Namespace

    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("accounts", {"id": [1], "bal": [10]})
        with pytest.raises(ValueError, match="does not support catalog-qualified"):
            backend.upsert(
                "accounts", {"id": [1], "bal": [20]},
                conflict_columns=["id"],
                namespace=Namespace(catalog="wh", path=("sales",)),
            )


def _spy_raw_sql(con):
    """Wrap `con.raw_sql` to capture every executed statement while still
    calling through to the real implementation — row-state assertions that
    rely on live execution keep working; the returned list accumulates the
    exact SQL text `_generic_upsert` dispatched."""
    captured: list[str] = []
    original = con.raw_sql

    def _wrapped(sql, *args, **kwargs):
        captured.append(sql)
        return original(sql, *args, **kwargs)

    con.raw_sql = _wrapped
    return captured


class TestUpsertNoColumnsToUpdate:
    """DEBT-12: degrade to insert-or-ignore instead of raising (defect 1) or
    silently nulling existing non-key columns (defect 2, found in adversarial
    review — see spec §1)."""

    def test_target_key_only_degrades_to_insert_or_ignore(self):
        """Defect 1 (backlog headline): target table is entirely key
        columns. Must degrade to a real DO NOTHING statement — not raise,
        and not render a no-op SET, which would still fire UPDATE triggers
        and violate the insert-or-ignore contract even if row values happen
        to end up the same."""
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1, 2]}))
        con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
        captured = _spy_raw_sql(con)
        with pytest.warns(UserWarning, match="no columns to update"):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [2, 3]}),
                style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
                update_columns=None, conflict_action="UPDATE",
                update_condition=None, namespace=None, schema=None,
            )
        assert sorted(con.table("t").execute()["id"].tolist()) == [1, 2, 3]
        stmt = captured[-1].upper()
        assert "DO NOTHING" in stmt
        assert " SET " not in stmt

    def test_source_key_only_against_richer_target_preserves_existing_value(self):
        """Defect 2 (data-loss): source frame is key-only but target has a
        non-key column. Must NOT null the existing row's non-key column, and
        must render DO NOTHING, not SET value = EXCLUDED.value."""
        con = ibis.duckdb.connect()
        con.create_table(
            "t", pl.DataFrame({"id": [1, 2], "value": ["keep-me", "also-keep"]})
        )
        con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
        captured = _spy_raw_sql(con)
        with pytest.warns(UserWarning, match="no columns to update"):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [1, 3]}),  # no "value" column
                style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
                update_columns=None, conflict_action="UPDATE",
                update_condition=None, namespace=None, schema=None,
            )
        rows = dict(
            con.table("t").order_by("id").execute()[["id", "value"]]
            .itertuples(index=False)
        )
        assert rows[1] == "keep-me" and rows[2] == "also-keep"
        assert pd.isna(rows[3])  # pandas represents a missing object value as NaN, not None (verified)
        stmt = captured[-1].upper()
        assert "DO NOTHING" in stmt
        assert " SET " not in stmt

    def test_explicit_empty_update_columns_matches_implicit_all_key_case(self):
        """Resolved design fork: update_columns=[] degrades exactly like the
        implicit all-key-columns case. Compares rendered SQL with the
        auto-generated memtable identifier normalized out — each
        ibis.memtable() call mints a fresh random name for identical data
        (verified empirically: two calls on the same DataFrame produce
        different `ibis_polars_memtable_<hash>` names), so a literal string
        compare would be flaky even against a correct implementation."""
        _memtable_name = re.compile(r"ibis_polars_memtable_\w+")

        def _run(update_columns):
            con = ibis.duckdb.connect()
            con.create_table("t", pl.DataFrame({"id": [1, 2]}))
            con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
            captured = _spy_raw_sql(con)
            with pytest.warns(UserWarning, match="no columns to update"):
                _generic_upsert(
                    con, "t", pl.DataFrame({"id": [2, 3]}),
                    style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
                    update_columns=update_columns, conflict_action="UPDATE",
                    update_condition=None, namespace=None, schema=None,
                )
            rows = sorted(con.table("t").execute()["id"].tolist())
            normalized_sql = _memtable_name.sub("MEMTABLE", captured[-1])
            return rows, normalized_sql

        implicit_rows, implicit_sql = _run(None)
        explicit_rows, explicit_sql = _run([])
        assert implicit_rows == explicit_rows
        assert implicit_sql == explicit_sql

    def test_explicit_update_column_absent_from_source_raises(self):
        """The same source-not-present invariant applies to an explicit
        request, not just the default (spec §3.3.2)."""
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "value": ["a"]}))
        con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
        with pytest.raises(ValueError, match="update_columns not present in source"):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [1]}),  # source lacks "value"
                style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
                update_columns=["value"], conflict_action="UPDATE",
                update_condition=None, namespace=None, schema=None,
            )
        assert con.table("t").execute()["value"].iloc[0] == "a"  # untouched

    def test_no_warning_emitted_when_call_ultimately_raises(self):
        """Warning-ordering regression guard. Note this test is GREEN both
        before and after Task 2: pre-fix code has no downgrade warning at
        all (so it trivially emits none here), and it is exactly the
        scenario spec §8/M-1 (rev.1's own review) found broken in an
        eager-warning draft. Its job is to fail if that eager-warning bug is
        ever reintroduced, not to demonstrate a pre-fix/post-fix transition."""
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1, 2], "ver": [1, 1]}))
        con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id, ver)")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(ValueError):
                _generic_upsert(
                    con, "t", pl.DataFrame({"id": [2, 3], "ver": [1, 1]}),
                    style=UpsertStyle.ON_CONFLICT, conflict_columns=["id", "ver"],
                    update_columns=None, conflict_action="UPDATE",
                    # a Reduction inside the predicate is structurally invalid
                    update_condition=lambda inc, exi: inc.ver.sum() > 0,
                    namespace=None, schema=None,
                )
        assert caught == []

    def test_both_warnings_fire_exactly_once_when_downgraded_with_update_condition(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1, 2]}))
        con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [2, 3]}),
                style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
                update_columns=None, conflict_action="UPDATE",
                update_condition=lambda inc, exi: inc.id > 0,  # valid, but moot
                namespace=None, schema=None,
            )
        assert len(caught) == 2  # exactly one degrade warning + one ignored warning
        messages = [str(w.message) for w in caught]
        assert any("no columns to update" in m for m in messages)
        assert any("update_condition is ignored" in m for m in messages)


def test_merge_dispatch_degrades_and_omits_matched():
    """DEBT-12 dispatch-level MERGE proof: _generic_upsert's Step 8 downgrade
    must reach _render_merge as conflict_action="NOTHING" through the real
    dispatcher, not just prove the wrapper handles an already-downgraded
    value — a Step 8/10 integration bug that forwards the original "UPDATE"
    for the MERGE branch specifically would be invisible to a wrapper-only
    test. DuckDB cannot execute MERGE INTO syntax (verified against duckdb
    1.2.2: sqlglot renders it fine, but duckdb's own parser rejects it with
    ParserException), so only the final MERGE statement is skipped for real
    execution — _generic_upsert's own internal calls (list_tables,
    current_catalog, schema introspection) also route through raw_sql, some
    with a sqlglot expression rather than a string (verified: current_catalog
    passes a sqlglot Select), and must keep running for real or step 2's
    target-existence check breaks."""
    con = ibis.duckdb.connect()
    con.create_table("t", pl.DataFrame({"id": [1]}))
    con.raw_sql("CREATE UNIQUE INDEX ux_t_id ON t (id)")
    original_raw_sql = con.raw_sql
    captured: list[str] = []

    def _capture_and_skip_merge(sql, *args, **kwargs):
        captured.append(sql)
        if isinstance(sql, str) and sql.startswith("MERGE INTO"):
            return None
        return original_raw_sql(sql, *args, **kwargs)

    con.raw_sql = _capture_and_skip_merge

    with pytest.warns(UserWarning, match="no columns to update"):
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2]}),
            style=UpsertStyle.MERGE, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, namespace=None, schema=None,
        )

    stmt = captured[-1]
    assert stmt.startswith("MERGE INTO")
    assert "WHEN MATCHED" not in stmt
    assert "WHEN NOT MATCHED THEN INSERT" in stmt
