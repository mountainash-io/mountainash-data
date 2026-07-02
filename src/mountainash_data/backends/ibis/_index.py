"""Generic-default index DDL: pure builders + dispatchers (spec §5).

Pure builders take pre-computed, already-validated parts so registry golden
tests render every dialect without a live connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis._render import quote_identifier
from mountainash_data.backends.ibis.dialects._registry import DropScope

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
    name_sql = quote_identifier(index_name, dialect)
    where = f" WHERE {where_sql}" if where_sql else ""
    name_part = f"{guard}{name_sql}"
    d = str(dialect)
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
) -> str:
    """Render a DROP INDEX statement. `target` is required (already quoted) when
    `drop_scope` is TABLE_SCOPED."""
    name_sql = quote_identifier(index_name, dialect)
    if drop_scope is DropScope.TABLE_SCOPED:
        return f"DROP INDEX {guard}{name_sql} ON {target}"
    return f"DROP INDEX {guard}{name_sql}"
