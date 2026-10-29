"""Driver-independent Oracle ALTER TABLE compatibility tests."""

from __future__ import annotations

from mountainash_data.backends.ibis._oracle_compat import _patch_alter_table_rename
from helpers.oracle_compat import RecordingRawConnection


class _FakeIbisCon:
    def __init__(self):
        self.con = RecordingRawConnection()


class TestPatchAlterTableRename:
    def test_strips_if_exists_from_alter_table_rename(self):
        con = _FakeIbisCon()
        _patch_alter_table_rename(con)
        cur = con.con.cursor()
        cur.execute('ALTER TABLE IF EXISTS "tmp_1" RENAME TO "final"')
        assert cur.executed == ['ALTER TABLE "tmp_1" RENAME TO "final"']

    def test_leaves_other_statements_untouched(self):
        con = _FakeIbisCon()
        _patch_alter_table_rename(con)
        cur = con.con.cursor()
        cur.execute("SELECT 1 FROM dual")
        assert cur.executed == ["SELECT 1 FROM dual"]

    def test_non_string_statement_passed_through(self):
        """`raw_sql` may pass a sqlglot Expression before it's stringified
        in some code paths -- the wrapper must not choke on non-str."""
        con = _FakeIbisCon()
        _patch_alter_table_rename(con)
        cur = con.con.cursor()
        sentinel = object()
        cur.execute(sentinel)
        assert cur.executed == [sentinel]

    def test_each_cursor_call_gets_independently_wrapped(self):
        con = _FakeIbisCon()
        _patch_alter_table_rename(con)
        cur1 = con.con.cursor()
        cur2 = con.con.cursor()
        cur1.execute("ALTER TABLE IF EXISTS x RENAME TO y")
        cur2.execute("SELECT 1")
        assert cur1.executed == ["ALTER TABLE x RENAME TO y"]
        assert cur2.executed == ["SELECT 1"]
