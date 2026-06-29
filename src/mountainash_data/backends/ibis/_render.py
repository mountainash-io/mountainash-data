"""Shared sqlglot rendering primitives for dialect-agnostic write ops.

Everything renders off a *live* ibis connection's own compiler, so identifier
quoting and type rendering match what ibis emits for create_table.
"""

from __future__ import annotations

import typing as t

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
