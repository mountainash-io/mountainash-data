"""Index render primitives: predicate compiler + pure builders + introspection SQL."""

import ibis
import pytest

from mountainash_data.backends.ibis._render import compile_index_predicate

_SCHEMA = ibis.schema({"id": "int64", "active": "boolean", "ver": "int64"})


def _pred_sql(predicate, *, table_name="t"):
    con = ibis.duckdb.connect()
    return compile_index_predicate(con, _SCHEMA, table_name, predicate)


class TestCompileIndexPredicate:
    def test_renders_unqualified_columns(self):
        sql = _pred_sql(lambda t: t.active == True)  # noqa: E712
        # the column must be UNqualified (no table/alias prefix)
        assert '"active"' in sql
        assert "." not in sql.split('"active"')[0][-3:]  # no `x.` before "active"

    def test_comparison_predicate(self):
        sql = _pred_sql(lambda t: t.ver > 5)
        assert '"ver"' in sql and "5" in sql
        assert "." not in sql

    def test_predicate_may_reference_non_indexed_column(self):
        # binding the full schema (not just indexed cols) must allow this
        sql = _pred_sql(lambda t: t.active)
        assert '"active"' in sql
        assert "." not in sql

    def test_rejects_sentinel_table_name(self):
        with pytest.raises(ValueError, match="sentinel"):
            _pred_sql(lambda t: t.id > 0, table_name="__ma_index_tbl__")

    def test_rejects_aggregate(self):
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            _pred_sql(lambda t: t.id.sum() > 0)


from mountainash_data.backends.ibis._index import (  # noqa: E402
    build_create_index_sql,
    build_drop_index_sql,
)
from mountainash_data.backends.ibis.dialects._registry import DropScope  # noqa: E402


class TestBuildCreateIndexSql:
    def test_basic(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="idx_t_id",
            cols=["id"], unique=False, index_type=None, guard="", where_sql=None,
        )
        assert sql == 'CREATE INDEX "idx_t_id" ON "t" ("id")'

    def test_unique_and_guard(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="u", cols=["a", "b"],
            unique=True, index_type=None, guard="IF NOT EXISTS ", where_sql=None,
        )
        assert sql == 'CREATE UNIQUE INDEX IF NOT EXISTS "u" ON "t" ("a", "b")'

    def test_partial_where(self):
        sql = build_create_index_sql(
            dialect="duckdb", target='"t"', index_name="p", cols=["id"],
            unique=False, index_type=None, guard="", where_sql='"active"',
        )
        assert sql.endswith('("id") WHERE "active"')

    def test_using_before_columns_postgres(self):
        sql = build_create_index_sql(
            dialect="postgres", target='"t"', index_name="g", cols=["doc"],
            unique=False, index_type="gin", guard="", where_sql=None,
        )
        assert sql == 'CREATE INDEX "g" ON "t" USING gin ("doc")'

    def test_using_before_on_mysql(self):
        # MySQL/MariaDB place USING between the index name and ON (verified:
        # dev.mysql.com 8.4 CREATE INDEX grammar `index_name [index_type] ON`).
        sql = build_create_index_sql(
            dialect="mysql", target="`t`", index_name="i", cols=["id"],
            unique=False, index_type="btree", guard="", where_sql=None,
        )
        assert sql == "CREATE INDEX `i` USING btree ON `t` (`id`)"

    def test_using_after_columns_singlestore(self):
        sql = build_create_index_sql(
            dialect="singlestore", target="`t`", index_name="i", cols=["id"],
            unique=False, index_type="hash", guard="", where_sql=None,
        )
        assert sql == "CREATE INDEX `i` ON `t` (`id`) USING hash"


class TestBuildDropIndexSql:
    def test_schema_global(self):
        sql = build_drop_index_sql(
            dialect="duckdb", drop_scope=DropScope.SCHEMA_GLOBAL,
            index_name="idx", target=None, guard="IF EXISTS ",
        )
        assert sql == 'DROP INDEX IF EXISTS "idx"'

    def test_table_scoped(self):
        sql = build_drop_index_sql(
            dialect="mysql", drop_scope=DropScope.TABLE_SCOPED,
            index_name="idx", target="`t`", guard="",
        )
        assert sql == "DROP INDEX `idx` ON `t`"


from mountainash_data.backends.ibis.operations import (  # noqa: E402
    _sql_literal,
    postgres_get_index_exists_sql,
    mysql_get_index_exists_sql,
    mssql_get_index_exists_sql,
    oracle_get_index_exists_sql,
    singlestore_get_index_exists_sql,
    sqlite_get_index_exists_sql,
)


class TestIntrospectionSql:
    def test_sql_literal_escapes_quote(self):
        assert _sql_literal("x'y") == "'x''y'"

    def test_existing_sqlite_now_escapes(self):
        sql = sqlite_get_index_exists_sql("a'b", "t", None)
        assert "'a''b'" in sql
        assert "count" in sql.lower()

    def test_postgres_shape_and_escaping(self):
        sql = postgres_get_index_exists_sql("idx", "t", "public")
        assert "pg_indexes" in sql
        assert "'idx'" in sql and "'t'" in sql and "'public'" in sql
        assert "count" in sql.lower()

    def test_mysql_is_table_scoped(self):
        sql = mysql_get_index_exists_sql("idx", "t", None)
        assert "STATISTICS" in sql.upper()
        assert "'idx'" in sql and "'t'" in sql

    def test_mssql_uses_object_id(self):
        sql = mssql_get_index_exists_sql("idx", "t", None)
        assert "sys.indexes" in sql and "OBJECT_ID" in sql.upper()

    def test_oracle_matches_exact_quoted_name(self):
        # Always-quoted create -> Oracle stores as written -> match exactly, no UPPER().
        sql = oracle_get_index_exists_sql("idx", "t", None)
        assert "user_indexes" in sql.lower()
        assert "UPPER" not in sql.upper()
        assert "'idx'" in sql

    def test_singlestore_shape(self):
        sql = singlestore_get_index_exists_sql("idx", "t", None)
        assert "STATISTICS" in sql.upper() and "'t'" in sql
        # always schema-constrained (defaults to DATABASE() when omitted) to
        # avoid cross-schema false positives
        assert "TABLE_SCHEMA = DATABASE()" in sql.upper()

    @pytest.mark.parametrize("fn", [
        postgres_get_index_exists_sql, mysql_get_index_exists_sql,
        mssql_get_index_exists_sql, oracle_get_index_exists_sql,
        singlestore_get_index_exists_sql,
    ])
    def test_injection_payload_is_escaped_not_broken(self, fn):
        # These pure SQL builders are ALLOWLIST-EXEMPT by design: the front-door
        # rejection (the primary gate) is enforced by the generic dispatcher
        # (_generic_index_exists) before any builder is called — see Task 6's
        # `test_bad_identifier_rejected`. This test asserts the SECOND layer:
        # even if a hostile value reached a builder, it is contained in an
        # escaped literal (doubled quote), not interpolated raw.
        sql = fn("x'; DROP TABLE t; --", "t", None)
        assert "''" in sql
