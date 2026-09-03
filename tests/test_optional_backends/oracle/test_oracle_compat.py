"""DB-free Oracle compatibility tests that require the Oracle project extra."""

from __future__ import annotations

import decimal

import oracledb
import oracledb.errors as ora_errors
import pytest

from mountainash_data.backends.ibis._oracle_compat import (
    _patch_drop_table_if_exists,
    _patch_get_schema,
    patch_oracle_connection,
)
from helpers.oracle_compat import RecordingCursor, RecordingRawConnection


def _fake_ora_error(code: int) -> oracledb.DatabaseError:
    err = ora_errors._Error(f"ORA-{code:05d}: fake", code=code)
    return oracledb.DatabaseError(err)


class _FakeDropTableCon:
    """Minimal stand-in exposing only what ``_patch_drop_table_if_exists``
    touches: a bound ``drop_table`` method it will re-wrap."""

    def __init__(self, raises: Exception | None = None):
        self.calls: list[tuple[str, dict]] = []
        self._raises = raises

    def drop_table(self, name, /, *, database=None, force=False):
        self.calls.append((name, {"database": database, "force": force}))
        if self._raises is not None:
            raise self._raises


class TestPatchDropTableIfExists:
    def test_force_true_swallows_ora_00942(self):
        con = _FakeDropTableCon(raises=_fake_ora_error(942))
        _patch_drop_table_if_exists(con)
        con.drop_table("t", force=True)  # must not raise
        assert con.calls == [("t", {"database": None, "force": False})]

    def test_force_true_other_error_still_raises(self):
        con = _FakeDropTableCon(raises=_fake_ora_error(1))
        _patch_drop_table_if_exists(con)
        with pytest.raises(oracledb.DatabaseError):
            con.drop_table("t", force=True)

    def test_force_false_ora_00942_still_raises(self):
        con = _FakeDropTableCon(raises=_fake_ora_error(942))
        _patch_drop_table_if_exists(con)
        with pytest.raises(oracledb.DatabaseError):
            con.drop_table("t", force=False)

    def test_underlying_call_never_requests_if_exists(self):
        """Regardless of the caller's `force`, the wrapped call always
        passes force=False -- IF EXISTS must never reach Oracle."""
        con = _FakeDropTableCon()
        _patch_drop_table_if_exists(con)
        con.drop_table("t", force=True)
        con.drop_table("t", force=False)
        assert all(kw["force"] is False for _, kw in con.calls)


class _FakeSafeRawSqlCon:
    """Captures the compiled statement text ``get_schema`` sends, and
    returns a canned metadata row, without touching a real database."""

    def __init__(self, rows):
        self._rows = rows
        self.captured_sql: str | None = None
        self.compiler = _Compiler()
        self.con = _FakeUsernameCon()

    def _safe_raw_sql(self, stmt):
        self.captured_sql = stmt.sql(dialect="oracle")
        return _RowsContext(self._rows)


class _Compiler:
    type_mapper = object()


class _FakeUsernameCon:
    username = "app"


class _RowsContext:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        cur = RecordingCursor()
        cur.fetchall = lambda: self._rows
        return cur

    def __exit__(self, *exc):
        return False


class TestPatchGetSchema:
    def test_emits_case_when_not_bare_boolean_predicate(self):
        """The bare `nullable = 'Y' AS nullable` form Ibis hardcodes is
        invalid pre-23ai Oracle SQL (ORA-00923); get_schema must always
        emit the CASE-WHEN form instead."""
        con = _FakeSafeRawSqlCon(rows=[("ID", "NUMBER", None, 0, 1)])
        _patch_get_schema(con)
        con.get_schema("up_ora")
        assert "nullable = 'Y' AS nullable" not in con.captured_sql
        assert "CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS nullable" in con.captured_sql

    def test_coerces_decimal_nullable_to_bool_typed_field(self):
        """oracledb returns the NUMBER(1) result as decimal.Decimal, not
        int/bool -- Ibis's dt.Int64(nullable=...) rejects anything but an
        actual bool. get_schema must coerce before constructing fields."""
        con = _FakeSafeRawSqlCon(rows=[("ID", "NUMBER", None, 0, decimal.Decimal("1"))])
        _patch_get_schema(con)
        schema = con.get_schema("up_ora")
        assert schema["ID"].nullable is True

    def test_raises_table_not_found_when_no_rows(self):
        import ibis.common.exceptions as exc

        con = _FakeSafeRawSqlCon(rows=[])
        _patch_get_schema(con)
        with pytest.raises(exc.TableNotFound):
            con.get_schema("missing")


class _FakePatchTargetCon:
    def __init__(self):
        self.drop_table = lambda *a, **k: None
        self.con = RecordingRawConnection()
        self.compiler = _Compiler()


class TestPatchOracleConnection:
    def test_idempotent_second_call_is_a_noop(self, monkeypatch):
        con = _FakePatchTargetCon()
        calls = []
        monkeypatch.setattr(
            "mountainash_data.backends.ibis._oracle_compat._patch_drop_table_if_exists",
            lambda c: calls.append(c),
        )
        patch_oracle_connection(con)
        patch_oracle_connection(con)
        assert len(calls) == 1

    def test_marks_connection_as_patched(self):
        con = _FakePatchTargetCon()
        patch_oracle_connection(con)
        assert con._mountainash_oracle_patched is True
