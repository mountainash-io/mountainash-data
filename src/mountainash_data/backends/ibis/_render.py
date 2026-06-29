"""Shared sqlglot rendering primitives for dialect-agnostic write ops.

Everything renders off a *live* ibis connection's own compiler, so identifier
quoting and type rendering match what ibis emits for create_table.
"""

from __future__ import annotations

import dataclasses
import typing as t

import ibis
import ibis.expr.operations as ops
import ibis.expr.types as ir
from sqlglot import exp


def dialect_of(ibis_conn: t.Any) -> t.Any:
    """The live connection's sqlglot dialect (NOT ibis's backend name)."""
    return ibis_conn.compiler.dialect


def quote_identifier(name: str, dialect: t.Any) -> str:
    """Quote a single identifier for `dialect` via sqlglot."""
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def qualified_name(parts: list[str], dialect: t.Any) -> str:
    """Quote each part and join with '.' (e.g. database.table)."""
    return ".".join(quote_identifier(p, dialect) for p in parts)


def render_type(type_mapper: t.Any, dtype: t.Any) -> str:
    """Render an ibis dtype to SQL via the connection's type-mapper."""
    return type_mapper.to_string(dtype)


# ---------------------------------------------------------------------------
# Conditional-predicate compiler (§6.1)
# ---------------------------------------------------------------------------

INCOMING_SENTINEL = "__ma_incoming__"
EXISTING_SENTINEL = "__ma_existing__"

# Op classes whose presence makes a predicate invalid in a WHERE/WHEN MATCHED
_FORBIDDEN_OPS = (ops.Reduction, ops.WindowFunction)

# Subquery/EXISTS op classes — use getattr guard so missing attrs don't crash
# at import time if ibis version changes.  All three exist in ibis 12.
_SUBQUERY_OPS: tuple[type, ...] = tuple(
    c
    for c in (
        getattr(ops, "ExistsSubquery", None),
        getattr(ops, "InSubquery", None),
        getattr(ops, "ScalarSubquery", None),
    )
    if c is not None
)


def validate_predicate(expr: ir.BooleanValue) -> None:
    """Reject predicates that cannot live in a row-level WHERE/WHEN MATCHED.

    Raises:
        ValueError: if `expr` contains an aggregation, window function, or
            subquery/EXISTS op.
    """
    node = expr.op()
    for n in node.find(_FORBIDDEN_OPS):  # type: ignore[arg-type]
        raise ValueError(
            "update_condition must be a scalar row predicate; found "
            f"{type(n).__name__} (aggregation/window). Use the upsert_hook "
            "override for conditions outside this grammar."
        )
    # Detect subqueries/EXISTS by SPECIFIC subquery op types, NOT ops.Relation.
    # ops.Relation also matches the two allowed sentinel tables, so testing for
    # it would reject every valid predicate (Codex finding).
    for n in node.find(_SUBQUERY_OPS):  # type: ignore[arg-type]
        raise ValueError(
            "update_condition may not contain subqueries/EXISTS/third-table "
            "references; use the upsert_hook override."
        )


@dataclasses.dataclass(frozen=True)
class ConditionAliases:
    """How each side's columns are referenced in the rendered clause.

    ``incoming_quoted=False`` is used for the ON CONFLICT ``EXCLUDED``
    pseudo-relation, which must NOT be a quoted identifier (Postgres exposes it
    as the special unquoted ``excluded``; quoting it risks referencing the
    wrong object).
    """

    incoming: str  # e.g. "excluded" (on conflict) or "src" (merge)
    existing: str  # e.g. "tgt"
    incoming_quoted: bool = True
    existing_quoted: bool = True


def validate_condition(
    target_schema: t.Any,
    target_name: str,
    predicate: t.Callable[[ir.Table, ir.Table], ir.BooleanValue],
) -> None:
    """Grammar + sentinel-collision validation only (no rendering).

    Used for the unconditional §10.5 check in ``_generic_upsert``.

    Raises:
        ValueError: if *target_name* collides with a reserved sentinel, or if
            the predicate contains a forbidden op (aggregation/window/subquery).
    """
    if target_name in (INCOMING_SENTINEL, EXISTING_SENTINEL):
        raise ValueError(
            f"target table name {target_name!r} collides with a reserved sentinel."
        )
    incoming = ibis.table(target_schema, name=INCOMING_SENTINEL)
    existing = ibis.table(target_schema, name=EXISTING_SENTINEL)
    validate_predicate(predicate(incoming, existing))


def compiled_source(
    ibis_conn: t.Any, obj: t.Any, target_schema: t.Any
) -> tuple[str, list[str]]:
    """Compile `obj` to a SELECT subquery, casting each column to the target
    type and projecting in target-column order. Returns (sql, columns).

    Columns present in the target but absent from the source are omitted;
    columns present in the source but absent from the target raise ValueError.

    ``_register_in_memory_tables`` is called before ``compile`` so that
    memtable-backed expressions (the common case when `obj` is a DataFrame)
    are staged in the backend catalog.  Without this step, ``compile`` emits
    SQL referencing ``ibis_polars_memtable_<hash>`` which is not registered,
    causing a CatalogException at ``raw_sql`` time.  This matches ibis's own
    memtable-staging mechanism (``SQLBackend._register_in_memory_tables``).
    """
    src = obj if isinstance(obj, ir.Table) else ibis.memtable(obj)
    src_cols = set(src.columns)
    extra = src_cols - set(target_schema.names)
    if extra:
        raise ValueError(f"source columns absent from target: {sorted(extra)}")
    cols = [c for c in target_schema.names if c in src_cols]
    projected = src.select([src[c].cast(target_schema[c]).name(c) for c in cols])
    ibis_conn._register_in_memory_tables(projected)  # REQUIRED: stage memtables
    return ibis_conn.compile(projected), cols


def compile_condition(
    ibis_conn: t.Any,
    target_schema: t.Any,
    target_name: str,
    predicate: t.Callable[[ir.Table, ir.Table], ir.BooleanValue],
    *,
    aliases: ConditionAliases,
) -> exp.Expression:
    """Render an ``(incoming, existing) -> bool`` predicate to a sqlglot ON
    sub-AST, remapping incoming/existing columns to *aliases*.

    The mechanism (§6.1): bind two sentinel-named ibis tables, join them on
    the predicate, compile to sqlglot, extract the join ``ON`` sub-AST, then
    transform each column's table qualifier to the caller's chosen alias.

    Args:
        ibis_conn: Live ibis connection whose compiler drives rendering.
        target_schema: The ibis schema shared by both sides (e.g. the target
            table's schema).
        target_name: Real target table name — rejected if it collides with a
            reserved sentinel (spec §6.1 step 0).
        predicate: ``(incoming_table, existing_table) -> BooleanValue``.
        aliases: How to label each side in the rendered SQL.

    Returns:
        A sqlglot ``Expression`` representing the ON clause with sentinel
        names replaced by *aliases*.

    Raises:
        ValueError: on sentinel collision, or forbidden predicate grammar.
    """
    validate_condition(target_schema, target_name, predicate)

    incoming = ibis.table(target_schema, name=INCOMING_SENTINEL)
    existing = ibis.table(target_schema, name=EXISTING_SENTINEL)
    pred = predicate(incoming, existing)

    joined = existing.join(incoming, pred, how="inner")
    ast = ibis_conn.compiler.to_sqlglot(joined)
    ast = ast if isinstance(ast, exp.Expression) else ast[0]

    # Build sentinel-alias → (target_alias, quoted) map, keyed by whatever
    # alias ibis assigned to each sentinel table in the compiled AST.
    remap: dict[str, tuple[str, bool]] = {}
    for tbl in ast.find_all(exp.Table):
        if tbl.name == INCOMING_SENTINEL:
            remap[tbl.alias_or_name] = (aliases.incoming, aliases.incoming_quoted)
        elif tbl.name == EXISTING_SENTINEL:
            remap[tbl.alias_or_name] = (aliases.existing, aliases.existing_quoted)

    join = next(ast.find_all(exp.Join), None)
    if join is None or join.args.get("on") is None:
        raise ValueError("could not extract join ON predicate from compiled AST")
    on = join.args["on"].copy()

    def _remap(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column) and n.table in remap:
            alias, quoted = remap[n.table]
            n.set("table", exp.to_identifier(alias, quoted=quoted))
        return n

    return on.transform(_remap)
