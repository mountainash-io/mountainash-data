"""Compatibility shims for Ibis's Oracle backend.

Ibis's Oracle backend (12.0.0, paired with sqlglot 30.17.0) emits SQL that
is invalid on any Oracle release predating Database 23ai (23c) -- i.e.
19c, 21c XE, and most on-prem/legacy deployments still in production,
which is exactly the install base DEBT-3 targets:

1. ``Backend.drop_table(force=True)`` delegates to the generic
   ``SQLBackend.drop_table``, which renders sqlglot's oracle-dialect
   ``DROP TABLE IF EXISTS "name"``. Oracle only added ``IF EXISTS`` support
   to ``DROP TABLE`` in 23ai -- ``ORA-00933: SQL command not properly
   ended`` on every earlier release.
2. ``Backend.create_table(overwrite=True)`` finalizes the replacement
   table with a hardcoded ``f"ALTER TABLE IF EXISTS {temp} RENAME TO
   {final}"`` -- even though the temp table is guaranteed to exist at that
   point. Oracle's ``ALTER TABLE`` grammar has never accepted an
   ``IF EXISTS`` guard, on any release: ``ORA-01735: invalid ALTER TABLE
   option``.
3. ``Backend.get_schema`` selects ``nullable = 'Y' AS nullable`` -- a bare
   boolean comparison used directly as a scalar SELECT-list expression.
   Oracle has never allowed that pre-23ai (which introduced a native SQL
   BOOLEAN type with wider expression support): ``ORA-00923: FROM keyword
   not found where expected``. CASE-wrapping the comparison fixes the SQL,
   but then oracledb returns the NUMBER(1) result as ``decimal.Decimal``,
   and Ibis's ``dt.Int64(nullable=...)`` strictly requires an actual
   ``bool`` -- so the query shape alone isn't enough; the Python-side
   value needs coercing too. ``get_schema`` backs ``create_table``,
   ``table()``, and ``inspect_table`` -- i.e. most schema introspection.

All three fire before any of ``create_table``, ``drop_table``, ``table()``,
or (transitively, via the shared MERGE-family upsert path) ``upsert``
reach the database -- for the entire portable write/read surface, on every
Oracle release currently in production (DEBT-3).

Verified empirically against ibis 12.0.0 + oracledb 4.0.2 + sqlglot 30.17.0
(2026-08-29) on a live Oracle Database 21c Express Edition instance.
"""
from __future__ import annotations

import typing as t

_ORA_TABLE_OR_VIEW_NOT_EXISTS = 942

# Known-bad literal SQL substring Ibis's Oracle create_table(overwrite=True)
# hardcodes -> its always-valid replacement. Deterministic -- Ibis emits
# this fragment verbatim, it does not vary per call -- so a plain literal
# substring swap is exact, not a heuristic.
_ALTER_TABLE_IF_EXISTS = ("ALTER TABLE IF EXISTS ", "ALTER TABLE ")


def _patch_drop_table_if_exists(con: t.Any) -> None:
    """Wrap ``con.drop_table`` to emulate ``force=True`` without relying on
    Oracle's version-gated ``DROP TABLE IF EXISTS`` syntax: send a plain
    ``DROP TABLE`` and suppress only the "table or view does not exist"
    error (ORA-00942) that ``IF EXISTS`` would have swallowed -- any other
    database error still propagates."""
    import oracledb

    original_drop_table = con.drop_table

    def drop_table(name: str, /, *, database: t.Any = None, force: bool = False) -> None:
        try:
            original_drop_table(name, database=database, force=False)
        except oracledb.DatabaseError as exc:
            (error,) = exc.args
            if force and getattr(error, "code", None) == _ORA_TABLE_OR_VIEW_NOT_EXISTS:
                return
            raise

    con.drop_table = drop_table


def _patch_alter_table_rename(con: t.Any) -> None:
    """Wrap cursor creation on the raw driver connection so the hardcoded
    ``ALTER TABLE IF EXISTS ... RENAME TO ...`` clause from
    ``create_table(overwrite=True)`` loses its unsupported guard before
    Oracle ever sees it. Scoped to this connection's cursors only."""
    raw_con = con.con
    original_cursor = raw_con.cursor
    bad, good = _ALTER_TABLE_IF_EXISTS

    def cursor(*args: t.Any, **kwargs: t.Any) -> t.Any:
        cur = original_cursor(*args, **kwargs)
        original_execute = cur.execute

        def execute(statement: t.Any, *a: t.Any, **kw: t.Any) -> t.Any:
            if isinstance(statement, str) and statement.startswith(bad):
                statement = good + statement[len(bad):]
            return original_execute(statement, *a, **kw)

        cur.execute = execute
        return cur

    raw_con.cursor = cursor


def _patch_get_schema(con: t.Any) -> None:
    """Replace ``con.get_schema`` with the same query, CASE-wrapping the
    ``nullable`` boolean comparison (valid on every Oracle version) and
    coercing the returned NUMBER(1) value to an actual Python ``bool``
    before it reaches Ibis's strictly-typed ``dt.Int64(nullable=...)``
    constructor. Everything else -- column selection, ordering, error
    handling -- mirrors ``ibis.backends.oracle.Backend.get_schema``
    exactly; only the ``nullable`` expression and its Python-side type
    differ."""
    import sqlglot as sg
    import sqlglot.expressions as sge
    import ibis.common.exceptions as exc
    import ibis.expr.schema as sch
    from ibis.backends.oracle import metadata_row_to_type
    from ibis.backends.sql.compilers.base import C

    type_mapper = con.compiler.type_mapper

    def get_schema(name: str, *, catalog: str | None = None, database: str | None = None) -> t.Any:
        db = database if database is not None else con.con.username.upper()
        nullable_expr = sge.Case(
            ifs=[sge.If(this=C.nullable.eq(sge.convert("Y")), true=sge.convert(1))],
            default=sge.convert(0),
        ).as_("nullable")
        stmt = (
            sg.select(C.column_name, C.data_type, C.data_precision, C.data_scale, nullable_expr)
            .from_(sg.table("all_tab_columns"))
            .where(C.table_name.eq(sge.convert(name)), C.owner.eq(sge.convert(db)))
            .order_by(C.column_id)
        )
        with con._safe_raw_sql(stmt) as cur:
            results = cur.fetchall()
        if not results:
            raise exc.TableNotFound(name)
        fields = {
            colname: metadata_row_to_type(
                type_mapper=type_mapper,
                type_string=data_type,
                precision=data_precision,
                scale=data_scale,
                nullable=bool(nullable),
            )
            for colname, data_type, data_precision, data_scale, nullable in results
        }
        return sch.Schema(fields)

    con.get_schema = get_schema


def patch_oracle_connection(con: t.Any) -> t.Any:
    """Apply every Oracle compat shim to a freshly built ibis connection.
    Idempotent: re-patching an already-patched connection is a no-op."""
    if getattr(con, "_mountainash_oracle_patched", False):
        return con
    _patch_drop_table_if_exists(con)
    _patch_alter_table_rename(con)
    _patch_get_schema(con)
    con._mountainash_oracle_patched = True
    return con
