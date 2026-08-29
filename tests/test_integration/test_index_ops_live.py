"""Live index ops against postgres (native) and mariadb (table-scoped + emulated)."""

import polars as pl
import pytest

pytestmark = pytest.mark.integration


def _fresh_table(be, name):
    # the raw ibis connection lives on the IbisConnection, not on the backend
    conn = be._require_connected()._ibis_conn
    try:
        conn.drop_table(name, force=True)
    except Exception:  # noqa: BLE001
        pass
    conn.create_table(name, pl.DataFrame({"id": [1, 2, 3], "active": [True, False, True]}))


class TestPostgresLive:
    def test_roundtrip_and_partial(self, postgres_backend):
        be = postgres_backend
        _fresh_table(be, "ix_live")
        be.create_index("ix_live", ["id"], index_name="ix_live_id")
        assert be.index_exists("ix_live_id", table_name="ix_live") is True
        # partial (filtered) index — postgres supports WHERE
        be.create_index("ix_live", ["id"], index_name="ix_live_active",
                        where=lambda r: r.active == True)  # noqa: E712
        assert be.index_exists("ix_live_active", table_name="ix_live") is True
        be.drop_index("ix_live_id")           # schema-global: no table needed
        assert be.index_exists("ix_live_id", table_name="ix_live") is False

    def test_using_gin_index_type(self, postgres_backend):
        be = postgres_backend
        _fresh_table(be, "ix_gin")
        be.create_index("ix_gin", ["id"], index_name="ix_gin_btree", index_type="btree")
        assert be.index_exists("ix_gin_btree", table_name="ix_gin") is True

    def test_list_indexes_reports_multi_key_and_included_columns(self, postgres_backend):
        """DEBT-15 regression: list_indexes() runs its catalog query through
        conn.sql(), which round-trips the SQL through SQLGlot before
        executing against real PostgreSQL. A prior version of the SQL relied
        on a two-column `WITH ORDINALITY AS alias(col1, col2)` clause that
        SQLGlot silently truncated to one column on re-emit, breaking every
        call with `UndefinedColumn: column keypos.ord does not exist`."""
        be = postgres_backend
        _fresh_table(be, "ix_list_live")
        raw = be.raw_driver_connection()
        cur = raw.cursor()
        cur.execute("ALTER TABLE ix_list_live ADD COLUMN ver INT")
        cur.execute(
            "CREATE UNIQUE INDEX ix_list_multi ON ix_list_live (id, active)"
        )
        cur.execute(
            "CREATE UNIQUE INDEX ix_list_include ON ix_list_live (id) INCLUDE (ver)"
        )
        cur.execute("CREATE INDEX ix_list_expr ON ix_list_live ((id + 1))")
        raw.commit()

        indexes = {index.name: index for index in be.list_indexes("ix_list_live")}

        multi = indexes["ix_list_multi"]
        assert multi.unique is True
        assert multi.columns == ("id", "active")

        included = indexes["ix_list_include"]
        assert included.unique is True
        assert included.columns == ("id",)
        assert included.included_columns == ("ver",)

        expr = indexes["ix_list_expr"]
        assert expr.columns == ("((id + 1))",)



class TestMySQLLive:
    def test_table_scoped_drop_requires_table(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_my")
        be.create_index("ix_my", ["id"], index_name="ix_my_id")
        assert be.index_exists("ix_my_id", table_name="ix_my") is True
        # schema-global drop must be rejected for a TABLE_SCOPED dialect
        with pytest.raises(ValueError, match="table_name"):
            be.drop_index("ix_my_id")
        be.drop_index("ix_my_id", table_name="ix_my")
        assert be.index_exists("ix_my_id", table_name="ix_my") is False

    def test_emulated_if_not_exists_is_idempotent(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_emu")
        # mysql dialect emulates IF NOT EXISTS via precheck; double-create is a no-op
        be.create_index("ix_emu", ["id"], index_name="ix_emu_id", if_not_exists=True)
        be.create_index("ix_emu", ["id"], index_name="ix_emu_id", if_not_exists=True)
        assert be.index_exists("ix_emu_id", table_name="ix_emu") is True

    def test_emulated_if_exists_drop_absent_is_noop(self, mysql_backend):
        be = mysql_backend
        _fresh_table(be, "ix_emu2")
        be.drop_index("nope", table_name="ix_emu2", if_exists=True)  # no raise
