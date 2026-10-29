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
        assert sql == 'CREATE INDEX "p" ON "t" ("id") WHERE "active"'

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

    def test_using_placement_with_sqlglot_dialect_class(self):
        # dialect_of(ibis_conn) returns a sqlglot Dialect CLASS, not a string.
        # Regression guard: the class must normalise to its lowercase name so
        # USING placement matches (bug: str(class) never matched the frozensets).
        from sqlglot.dialects.postgres import Postgres
        from sqlglot.dialects.mysql import MySQL

        pg = build_create_index_sql(
            dialect=Postgres, target='"t"', index_name="g", cols=["doc"],
            unique=False, index_type="gin", guard="", where_sql=None,
        )
        assert pg == 'CREATE INDEX "g" ON "t" USING gin ("doc")'  # USING before columns

        my = build_create_index_sql(
            dialect=MySQL, target="`t`", index_name="i", cols=["id"],
            unique=False, index_type="btree", guard="", where_sql=None,
        )
        assert my == "CREATE INDEX `i` USING btree ON `t` (`id`)"  # USING before ON


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
        assert "TABLE_SCHEMA = DATABASE()" in sql.upper()

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


from mountainash_data.backends.ibis._index_inspection import (  # noqa: E402
    mssql_get_list_indexes_sql,
    mysql_get_list_indexes_sql,
    oracle_get_list_indexes_sql,
    postgres_get_list_indexes_sql,
    singlestore_get_list_indexes_sql,
    sqlite_get_list_indexes_sql,
)


@pytest.mark.parametrize(
    "builder",
    [
        sqlite_get_list_indexes_sql,
        postgres_get_list_indexes_sql,
        mysql_get_list_indexes_sql,
        mssql_get_list_indexes_sql,
        oracle_get_list_indexes_sql,
        singlestore_get_list_indexes_sql,
    ],
)
def test_generic_list_index_builders_expose_ten_column_contract(builder):
    sql = builder("x'y", "aux")
    aliases = [
        "index_name",
        "is_unique",
        "is_primary",
        "is_valid",
        "index_type",
        "definition",
        "col_name",
        "col_expr",
        "is_included",
        "position",
    ]
    assert all(alias in sql for alias in aliases)
    assert "x''y" in sql


def test_generic_list_index_builder_vendor_shapes():
    sqlite_sql = sqlite_get_list_indexes_sql("t", "aux")
    postgres_sql = postgres_get_list_indexes_sql("t", "public")
    mysql_sql = mysql_get_list_indexes_sql("t", "db")
    mssql_sql = mssql_get_list_indexes_sql("t", "dbo")
    oracle_sql = oracle_get_list_indexes_sql("t", None)

    assert "pragma_index_list('t', 'aux')" in sqlite_sql
    assert "pragma_index_xinfo(l.name, 'aux')" in sqlite_sql
    assert '"aux".sqlite_master' in sqlite_sql
    assert "keypos.ordinality::integer" in postgres_sql
    assert "unnest(i.indkey::smallint[]) WITH ORDINALITY" in postgres_sql
    assert "AS keypos(attnum)" in postgres_sql
    assert "INDEX_TYPE" in mysql_sql and "EXPRESSION" not in mysql_sql
    assert "i.type IN (1, 2)" in mssql_sql
    assert "ic.column_id > 0" in mssql_sql
    assert "ic.partition_ordinal > 0" in mssql_sql
    assert "exprs.column_expression" in oracle_sql
    assert "COALESCE" not in oracle_sql


def test_oracle_list_index_builder_scopes_owner():
    sql = oracle_get_list_indexes_sql("t", "OTHER_OWNER")

    assert "FROM all_indexes idx" in sql
    assert "idx.owner = 'OTHER_OWNER'" in sql
    assert "cols.index_owner = idx.owner" in sql
    assert "con.index_owner = idx.owner" in sql


def test_postgres_list_index_sql_survives_sqlglot_roundtrip():
    """DEBT-15 regression: SQLGlot's postgres dialect silently drops the
    second column of a `WITH ORDINALITY AS alias(col1, col2)` alias list on
    re-emit (confirmed against sqlglot 30.17.0) — `IbisBackend.list_indexes()`
    routes this SQL through `conn.sql()`, which parses and re-emits it via
    SQLGlot before execution, so any reintroduced two-column ordinality alias
    ships broken without a live database catching it. Assert the round-tripped
    SQL keeps every `keypos.*` reference the original resolves — a byte-exact
    round-trip isn't required, only that no column reference is orphaned."""
    import sqlglot

    sql = postgres_get_list_indexes_sql("t", "public")
    roundtripped = sqlglot.parse_one(sql, read="postgres").sql(dialect="postgres")

    assert "keypos(attnum, ord)" not in roundtripped  # the historical broken shape
    for ref in ("keypos.attnum", "keypos.ordinality"):
        assert ref in roundtripped


def test_sqlite_attachment_index_ddl_qualifies_index_not_table():
    import ibis

    from mountainash_data.backends.ibis._index import _generic_create_index
    from mountainash_data.backends.ibis.dialects._registry import DIALECTS

    con = ibis.sqlite.connect()
    con.raw_sql("ATTACH ':memory:' AS aux")
    con.raw_sql("CREATE TABLE aux.t (id INTEGER)")
    caps = DIALECTS["sqlite"].index_caps
    _generic_create_index(
        con,
        "t",
        ["id"],
        index_name="ix",
        namespace="aux",
        caps=caps,
        exists_sql_fn=DIALECTS["sqlite"].get_index_exists_sql,
    )
    assert con.raw_sql("SELECT name FROM aux.sqlite_master WHERE type = 'index'").fetchall() == [("ix",)]
