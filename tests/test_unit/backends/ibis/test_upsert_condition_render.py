"""The update_condition ibis-expression predicate compiler (§6.1)."""

import ibis
import pytest

from mountainash_data.backends.ibis._render import (
    ConditionAliases,
    compile_condition,
    dialect_of,
    validate_predicate,
)

_SCHEMA = ibis.schema({"id": "int64", "updated_at": "timestamp", "v": "string"})

# ON CONFLICT: incoming is the unquoted `excluded` pseudo-relation; existing is `tgt`.
_ONCONFLICT = ConditionAliases(incoming="excluded", existing="tgt", incoming_quoted=False)
# MERGE: both sides are normal quoted aliases.
_MERGE = ConditionAliases(incoming="src", existing="tgt")


def _render(con, predicate, aliases, *, target_name="t"):
    ast = compile_condition(con, _SCHEMA, target_name, predicate, aliases=aliases)
    return ast.sql(dialect=dialect_of(con))


class TestCompileCondition:
    def test_on_conflict_alias_mapping_unquoted_excluded(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.updated_at > exi.updated_at, _ONCONFLICT)
        # EXCLUDED is the unquoted pseudo-relation; existing is quoted "tgt"
        assert "excluded." in sql.lower() and '"EXCLUDED"' not in sql
        assert '"tgt"."updated_at"' in sql

    def test_merge_alias_mapping_duckdb(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.updated_at > exi.updated_at, _MERGE)
        assert '"src"."updated_at"' in sql and '"tgt"."updated_at"' in sql

    def test_function_predicate_renders_per_dialect(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.v.upper() != exi.v.upper(), _MERGE)
        assert "UPPER(" in sql.upper()

    def test_constant_predicate_renders(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.id > 0, _MERGE)
        assert '"src"."id"' in sql

    def test_null_check_predicate_renders(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.v.notnull(), _MERGE)
        assert "NULL" in sql.upper()
        # the alias remap must have applied: the incoming column is qualified by src
        assert '"src"."v"' in sql

    def test_rejects_target_name_colliding_with_sentinel(self):
        con = ibis.duckdb.connect()
        with pytest.raises(ValueError, match="sentinel"):
            _render(
                con,
                lambda inc, exi: inc.id > exi.id,
                _MERGE,
                target_name="__ma_incoming__",
            )

    def test_rejects_aggregate_predicate(self):
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            validate_predicate(
                ibis.table(_SCHEMA, name="x").v.count() > 0  # aggregation
            )

    def test_rejects_window_predicate(self):
        """WindowFunction op triggers the forbidden-ops check."""
        schema = ibis.schema({"id": "int64", "updated_at": "timestamp", "v": "string"})
        t = ibis.table(schema, name="x")
        # rank() is an analytic function -> a pure WindowFunction op (no
        # Reduction), so this isolates the window arm of the forbidden check.
        win_expr = t.id.rank() > 0
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            validate_predicate(win_expr)

    def test_rejects_subquery_predicate(self):
        """InSubquery op triggers the subquery rejection check."""
        schema = ibis.schema({"id": "int64", "updated_at": "timestamp", "v": "string"})
        t = ibis.table(schema, name="x")
        t2 = ibis.table(schema, name="y")
        # .isin(other_table_col) produces InSubquery op
        isin_expr = t.id.isin(t2.id)
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            validate_predicate(isin_expr)
