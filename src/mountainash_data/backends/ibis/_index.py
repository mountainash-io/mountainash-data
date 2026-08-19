"""Generic-default index DDL: pure builders + dispatchers (spec §5).

Pure builders take pre-computed, already-validated parts so registry golden
tests render every dialect without a live connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis._render import (
    compile_index_predicate,
    dialect_of,
    qualified_name,
    quote_identifier,
)
from mountainash_data.backends.ibis.dialects._registry import DropScope, IndexCapability
from mountainash_data.backends.ibis.operations import (
    _generate_index_name,
    _normalize_columns,
    _validate_simple_identifier,
)

# USING <method> position differs across dialects (verified against official docs):
#   - Postgres:    CREATE INDEX i ON tbl USING gin (cols)   -> after ON, before columns
#   - MySQL/MariaDB: CREATE INDEX i USING btree ON tbl (cols) -> after index name, before ON
#   - SingleStore: CREATE INDEX i ON tbl (cols) USING hash   -> after columns (the default)
# sqlite/duckdb/motherduck/mssql/oracle have empty index_types -> no USING emitted.
_USING_BEFORE_ON: frozenset[str] = frozenset({"mysql"})
_USING_BEFORE_COLUMNS: frozenset[str] = frozenset({"postgres"})


def build_create_index_sql(
    *,
    dialect: t.Any,
    target: str,
    index_name: str,
    cols: list[str],
    unique: bool,
    index_type: t.Optional[str],
    guard: str,
    where_sql: t.Optional[str],
    index_ref: t.Optional[str] = None,
) -> str:
    """Render a CREATE INDEX statement from pre-validated parts.

    Args:
        dialect: sqlglot dialect string (e.g. ``dialect_of(ibis_conn)``).
        target: already-qualified, already-quoted table reference.
        index_name: unquoted index name.
        cols: unquoted column names.
        unique: emit CREATE UNIQUE INDEX.
        index_type: USING <type>, or None for no USING clause.
        guard: ``"IF NOT EXISTS "`` or ``""`` (emulation supplies idempotency).
        where_sql: rendered partial-index WHERE body, or None.
    """
    unique_sql = "UNIQUE " if unique else ""
    cols_sql = ", ".join(quote_identifier(c, dialect) for c in cols)
    name_sql = index_ref or quote_identifier(index_name, dialect)
    where = f" WHERE {where_sql}" if where_sql else ""
    name_part = f"{guard}{name_sql}"
    # dialect may be a sqlglot Dialect class (live path) or a plain string (tests/golden).
    # Normalise to the lowercase name so membership checks work in both cases.
    if isinstance(dialect, type):
        d = dialect.__name__.lower()
    elif isinstance(dialect, str):
        d = dialect.lower()
    else:
        d = str(dialect).lower()
    using = f"USING {index_type}" if index_type else None

    if using and d in _USING_BEFORE_ON:
        # MySQL/MariaDB: USING sits between the index name and ON.
        name_part = f"{name_part} {using}"
        tail = f"ON {target} ({cols_sql})"
    elif using and d in _USING_BEFORE_COLUMNS:
        # Postgres: USING sits after ON, before the column list.
        tail = f"ON {target} {using} ({cols_sql})"
    elif using:
        # SingleStore (and the general default): USING after the column list.
        tail = f"ON {target} ({cols_sql}) {using}"
    else:
        tail = f"ON {target} ({cols_sql})"

    return f"CREATE {unique_sql}INDEX {name_part} {tail}{where}"


def build_drop_index_sql(
    *,
    dialect: t.Any,
    drop_scope: DropScope,
    index_name: str,
    target: t.Optional[str],
    guard: str,
    index_ref: t.Optional[str] = None,
) -> str:
    """Render a DROP INDEX statement with an optional qualified index ref."""
    name_sql = index_ref or quote_identifier(index_name, dialect)
    if drop_scope is DropScope.TABLE_SCOPED:
        return f"DROP INDEX {guard}{name_sql} ON {target}"
    return f"DROP INDEX {guard}{name_sql}"


# ---------------------------------------------------------------------------
# Generic dispatchers (spec §5-§8)
# ---------------------------------------------------------------------------


def _generic_index_exists(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: t.Optional[str] = None,
    namespace: t.Optional[str] = None,
    exists_sql_fn: t.Any,
) -> bool:
    """Run the dialect's introspection SQL and return whether the index exists."""
    if exists_sql_fn is None:
        raise NotImplementedError("dialect has no get_index_exists_sql")
    _validate_simple_identifier(index_name, kind="index_name")
    if table_name is not None:
        _validate_simple_identifier(table_name, kind="table_name")
    if namespace is not None:
        _validate_simple_identifier(namespace, kind="namespace")
    result = ibis_conn.sql(exists_sql_fn(index_name, table_name, namespace))
    if result is None:
        return False

    # Read the single returned column BY POSITION, not by the alias name:
    # Oracle upper-cases the unquoted `count` alias ("count" -> "COUNT"), so
    # keying by "count" would KeyError. Every introspection query returns
    # exactly one column.
    first_col = result.to_pyarrow().column(0).to_pylist()
    return first_col[0] > 0

def _target_ref(ibis_conn: t.Any, table_name: str, namespace: t.Optional[str]) -> str:
    dialect = dialect_of(ibis_conn)
    dialect_name = (
        dialect.__name__.lower()
        if isinstance(dialect, type)
        else str(dialect).lower()
    )
    if dialect_name == "sqlite":
        return quote_identifier(table_name, dialect)
    parts = [namespace, table_name] if namespace else [table_name]
    return qualified_name(parts, dialect)


def _index_ref(ibis_conn: t.Any, index_name: str, namespace: t.Optional[str]) -> str:
    dialect = dialect_of(ibis_conn)
    dialect_name = (
        dialect.__name__.lower()
        if isinstance(dialect, type)
        else str(dialect).lower()
    )
    if dialect_name == "sqlite" and namespace:
        return qualified_name([namespace, index_name], dialect)
    return quote_identifier(index_name, dialect)


def _generic_create_index(
    ibis_conn: t.Any,
    table_name: str,
    columns: t.Union[list[str], str],
    *,
    index_name: t.Optional[str] = None,
    unique: bool = False,
    index_type: t.Optional[str] = None,
    where: t.Any = None,
    namespace: t.Optional[str] = None,
    if_not_exists: bool = True,
    caps: IndexCapability,
    exists_sql_fn: t.Any,
) -> None:
    """Render and execute a CREATE INDEX via the generic path (spec §5-§8).

    Emulation failure modes (TOCTOU / privilege / catalog-isolation /
    auto-commit DDL) are documented-and-accepted per spec §6: the engine's
    error is surfaced, never swallowed.
    """
    _validate_simple_identifier(table_name, kind="table_name")
    if namespace is not None:
        _validate_simple_identifier(namespace, kind="namespace")
    cols = _normalize_columns(columns)
    for c in cols:
        _validate_simple_identifier(c, kind="column")

    if index_type is not None and index_type not in caps.index_types:
        raise ValueError(
            f"index_type {index_type!r} not supported by this dialect; "
            f"valid: {sorted(caps.index_types) or 'none'}"
        )
    if where is not None and not caps.partial:
        raise ValueError("this dialect does not support partial indexes (where=)")

    if index_name is None:
        index_name = _generate_index_name(table_name, cols, unique=unique)
    _validate_simple_identifier(index_name, kind="index_name")

    # Idempotency: native guard, or emulate via precheck.
    guard = ""
    if if_not_exists:
        if caps.native_if_not_exists:
            guard = "IF NOT EXISTS "
        elif _generic_index_exists(
            ibis_conn, index_name, table_name=table_name, namespace=namespace,
            exists_sql_fn=exists_sql_fn,
        ):
            return  # emulated: already present

    where_sql = None
    if where is not None:
        schema = ibis_conn.table(table_name, database=namespace).schema()
        where_sql = compile_index_predicate(ibis_conn, schema, table_name, where)

    sql = build_create_index_sql(
        dialect=dialect_of(ibis_conn),
        target=_target_ref(ibis_conn, table_name, namespace),
        index_name=index_name,
        cols=cols,
        unique=unique,
        index_type=index_type,
        guard=guard,
        where_sql=where_sql,
        index_ref=_index_ref(ibis_conn, index_name, namespace),
    )
    ibis_conn.raw_sql(sql)


def _generic_drop_index(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: t.Optional[str] = None,
    namespace: t.Optional[str] = None,
    if_exists: bool = True,
    caps: IndexCapability,
    exists_sql_fn: t.Any,
) -> None:
    """Render and execute a DROP INDEX via the generic path (spec §5-§8).

    Emulation failure modes (TOCTOU / privilege / catalog-isolation /
    auto-commit DDL) are documented-and-accepted per spec §6: the engine's
    error is surfaced, never swallowed.
    """
    _validate_simple_identifier(index_name, kind="index_name")
    if caps.drop_scope is DropScope.TABLE_SCOPED and table_name is None:
        raise ValueError(
            "drop_index requires table_name for this dialect (DROP INDEX ... ON tbl)"
        )
    if table_name is not None:
        _validate_simple_identifier(table_name, kind="table_name")
    if namespace is not None:
        _validate_simple_identifier(namespace, kind="namespace")

    guard = ""
    if if_exists:
        if caps.native_if_exists:
            guard = "IF EXISTS "
        elif not _generic_index_exists(
            ibis_conn, index_name, table_name=table_name, namespace=namespace,
            exists_sql_fn=exists_sql_fn,
        ):
            return  # emulated: already absent

    target = _target_ref(ibis_conn, table_name, namespace) if table_name else None
    sql = build_drop_index_sql(
        dialect=dialect_of(ibis_conn),
        drop_scope=caps.drop_scope,
        index_name=index_name,
        target=target,
        guard=guard,
        index_ref=_index_ref(ibis_conn, index_name, namespace),
    )
    ibis_conn.raw_sql(sql)
