"""Ibis operations module — module-level hook functions only.

Contains:
- Module-level helper functions: _generate_index_name, _format_qualified_table, _normalize_columns
- Per-dialect SQL functions: duckdb, sqlite, motherduck index SQL generators
- Standalone hook functions: duckdb_family_create_index, duckdb_family_drop_index
- Generic, dialect-agnostic write ops: _generic_rename_table, _generic_add_columns,
  _generic_upsert (with the three upsert-family renderers + MySQL preflight)
"""

import typing as t
import contextlib
import re
import warnings

import ibis

from mountainash_data.core.constants import (
    CONST_INDEX_TYPE,
)
from sqlglot import exp
from mountainash_data.backends.ibis._render import (
    ConditionAliases,
    compile_condition,
    compiled_source,
    dialect_of,
    qualified_name,
    quote_identifier,
    validate_condition,
)
from mountainash_data.backends.ibis.dialects._registry import UpsertStyle


# ===========================================================================
# MODULE-LEVEL HELPER FUNCTIONS
# ===========================================================================

def _generate_index_name(
    table_name: str,
    columns: list[str],
    *,
    unique: bool = False,
    suffix: str | None = None
) -> str:
    """Generate standardized index name."""
    sorted_cols = sorted(columns)
    prefix = "uidx" if unique else "idx"
    col_part = "_".join(sorted_cols)
    parts = [prefix, table_name, col_part]
    if suffix:
        parts.append(suffix)
    return "_".join(parts)


def _format_qualified_table(
    table_name: str,
    *,
    database: str | None = None,
    schema: str | None = None
) -> str:
    """Format fully qualified table name."""
    parts = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(table_name)
    return ".".join(parts)


def _normalize_columns(
    columns: list[str] | str
) -> list[str]:
    """Normalize column input to list."""
    if isinstance(columns, str):
        return [columns]
    if not columns:
        raise ValueError("At least one column must be specified")
    return list(columns)


def _coerce_dtype(v: t.Any) -> ibis.DataType:
    """Normalize a dtype spec to an ibis DataType.

    Accepts an ibis DataType (passthrough), an ibis type string, or a
    MountainashDtype (resolved via the canonical ibis bridge). Parametric
    MountainashDtype members (LIST/STRUCT) carry no element type and raise.
    """
    if isinstance(v, ibis.DataType):
        return v

    mountainash_dtype = None
    target_ibis = None
    try:
        from mountainash.core.dtypes.canonical import MountainashDtype as _MD
        from mountainash.core.dtypes import target_ibis as _ti

        mountainash_dtype, target_ibis = _MD, _ti
    except ImportError:  # mountainash build without the canonical dtypes bridge
        pass

    if (
        mountainash_dtype is not None
        and target_ibis is not None
        and isinstance(v, mountainash_dtype)
    ):
        # Gate parametric members explicitly via the canonical bridge's own
        # CAST_UNSUPPORTED set (currently {LIST, STRUCT}) rather than relying
        # on ibis.dtype() to reject a bare "array"/"struct" string.
        if v in target_ibis.CAST_UNSUPPORTED:
            raise ValueError(
                f"MountainashDtype.{v.name} is a parametric type with no "
                f"element types; pass an ibis DataType or use the frame form "
                f"for nested columns."
            )
        return ibis.dtype(target_ibis.SCHEMA_TYPES[v])

    return ibis.dtype(v)


def _normalize_to_schema(source: t.Any) -> ibis.Schema:
    """Resolve `source` to a candidate ibis Schema.

    A Mapping of ``{name: dtype}`` is coerced per-value; any other object is
    treated as a frame and run through Ibis's native inference (identical to
    what ``create_table`` applies).
    """
    if isinstance(source, t.Mapping):
        return ibis.schema({k: _coerce_dtype(v) for k, v in source.items()})
    return ibis.memtable(source).schema()


_SIMPLE_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")


def _validate_simple_identifier(value: str, *, kind: str) -> None:
    """Require a simple, safe SQL identifier: ``[A-Za-z_][A-Za-z0-9_$]*``.

    Two guarantees in one check:

    1. **Namespace correctness** — a dotted ``table_name``/``database`` would be
       quoted as a single literal identifier (``"a.b"``) rather than a
       namespace, silently violating the documented contract.
    2. **Injection safety** — the MySQL/MariaDB preflight builds
       ``information_schema`` queries by interpolating ``name``/``database`` into
       SQL string literals. Restricting them to this charset (no quotes,
       semicolons, whitespace, or other metacharacters) means a hostile or
       malformed identifier cannot break out of that literal context. The
       preflight ALSO renders the literals via sqlglot as defense in depth, but
       this validator is the primary gate.

    Anything outside the charset fails loudly instead of emitting unsafe SQL.
    """
    if not _SIMPLE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{kind} {value!r} must be a simple identifier (letters, digits, "
            f"underscore, $; starting with a letter or underscore); dotted, "
            f"quoted, or whitespace-bearing names are out of scope."
        )


def build_rename_sql(old_name: str, new_name: str, *, dialect: t.Any) -> str:
    """Pure builder: render a portable rename for an explicit sqlglot dialect.

    sqlglot renders ALTER TABLE … RENAME TO … for most dialects, EXEC sp_rename
    for SQL Server (tsql), and ALTER TABLE … RENAME … for MySQL/SingleStore.
    Taking `dialect` explicitly lets the registry golden test render every
    dialect without a live connection. Identifiers are built directly via
    to_identifier(quoted=True) — never pre-quoted-then-reparsed (that double-quotes).
    """
    return exp.Alter(
        this=exp.Table(this=exp.to_identifier(old_name, quoted=True)),
        kind="TABLE",
        actions=[exp.AlterRename(this=exp.to_identifier(new_name, quoted=True))],
    ).sql(dialect=dialect)


def _generic_rename_table(ibis_conn: t.Any, old_name: str, new_name: str) -> None:
    """Rename a table via the sqlglot generic default off the live connection."""
    _validate_simple_identifier(old_name, kind="old_name")
    _validate_simple_identifier(new_name, kind="new_name")
    ibis_conn.raw_sql(build_rename_sql(old_name, new_name, dialect=dialect_of(ibis_conn)))


def _generic_add_columns(
    ibis_conn: t.Any,
    table_name: str,
    source: t.Any,
    *,
    database: str | None = None,
) -> None:
    """Add columns present in `source` but missing from `table_name`.

    Additive and idempotent (single-process preflight: missing columns are
    computed once, then one ALTER is issued per column — not concurrency-safe
    and not atomic across columns on engines without transactional DDL).
    Column types render through the connection's own compiler type-mapper
    (identical to ``create_table``); a null-typed column coerces to the
    dialect string type; identifiers are quoted per dialect. One ``ALTER
    TABLE … ADD COLUMN`` is issued per new column (SQLite permits only one per
    statement).

    `table_name` and `database` must each be a simple (non-dotted) identifier;
    each is quoted as a single part. Dotted/multi-part qualified names are out
    of scope.
    """
    _validate_simple_identifier(table_name, kind="table_name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    candidate = _normalize_to_schema(source)
    existing = set(ibis_conn.table(table_name, database=database).schema().names)
    type_mapper = ibis_conn.compiler.type_mapper
    dialect = ibis_conn.compiler.dialect

    table_parts = [database, table_name] if database else [table_name]
    qualified = ".".join(quote_identifier(part, dialect) for part in table_parts)

    for col_name, dtype in candidate.items():
        if col_name in existing:
            continue
        if dtype.is_null():
            dtype = ibis.dtype("string")
        type_sql = type_mapper.to_string(dtype)
        ibis_conn.raw_sql(
            f"ALTER TABLE {qualified} ADD COLUMN {quote_identifier(col_name, dialect)} {type_sql}"
        )


# ===========================================================================
# PER-DIALECT CAPABILITY HOOK FUNCTIONS
# ===========================================================================

# --- DuckDB ---

def duckdb_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """DuckDB uses duckdb_indexes() system function."""
    where_clauses = [f"index_name = '{index_name}'"]
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE {where_sql}"


def duckdb_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """DuckDB uses duckdb_indexes() system function."""
    where_clauses = [f"table_name = '{table_name}'"]
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"""
        SELECT
            index_name as name,
            sql as definition,
            is_unique as unique
        FROM duckdb_indexes()
        WHERE {where_sql}
    """


# --- SQLite ---

def sqlite_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """SQLite uses sqlite_master system table.
    Note: database parameter is not used as SQLite doesn't support cross-database queries.
    """
    where_clauses = [
        "type = 'index'",
        f"name = '{index_name}'"
    ]
    if table_name:
        where_clauses.append(f"tbl_name = '{table_name}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM sqlite_master WHERE {where_sql}"


def sqlite_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """SQLite uses sqlite_master system table.
    Note: database parameter is not used as SQLite doesn't support cross-database queries.
    """
    return f"""
        SELECT
            name,
            sql as definition,
            CASE WHEN sql LIKE '%UNIQUE%' THEN 1 ELSE 0 END as "unique"
        FROM sqlite_master
        WHERE type = 'index'
        AND tbl_name = '{table_name}'
    """


# --- MotherDuck ---

def motherduck_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """MotherDuck uses DuckDB's duckdb_indexes() system function."""
    where_clauses = [f"index_name = '{index_name}'"]
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE {where_sql}"


def motherduck_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """MotherDuck uses DuckDB's duckdb_indexes() system function."""
    where_clauses = [f"table_name = '{table_name}'"]
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"""
        SELECT
            index_name as name,
            sql as definition,
            is_unique as unique
        FROM duckdb_indexes()
        WHERE {where_sql}
    """


# MotherDuck-specific list_tables override
def motherduck_list_tables(
    ibis_backend: t.Any,
    like: str | None = None,
    database: str | None = None,
) -> list[str]:
    """MotherDuck-specific list_tables using DuckDB backend's database parameter."""
    return ibis_backend.list_tables(like=like, database=database) if ibis_backend is not None else []


# ===========================================================================
# STANDALONE HOOK FUNCTIONS
# ===========================================================================

def duckdb_family_create_index(
    ibis_conn: t.Any,
    table_name: str,
    columns: list[str] | str,
    *,
    index_name: str | None = None,
    unique: bool = False,
    index_type: str | None = None,
    where_condition: str | None = None,
    database: str | None = None,
    if_not_exists: bool = True,
) -> None:
    """Create an index using DuckDB/SQLite syntax."""
    columns_list = _normalize_columns(columns)

    if index_name is None:
        index_name = _generate_index_name(table_name, columns_list, unique=unique)

    qualified_table = _format_qualified_table(table_name, database=database)
    columns_sql = ", ".join(columns_list)

    unique_sql = "UNIQUE " if unique else ""
    if_not_exists_sql = "IF NOT EXISTS " if if_not_exists else ""
    where_sql = f" WHERE {where_condition}" if where_condition else ""

    if index_type and index_type != CONST_INDEX_TYPE.BTREE:
        warnings.warn(
            f"Index type {index_type} not supported, using default BTREE"
        )

    create_sql = (
        f"CREATE {unique_sql}INDEX {if_not_exists_sql}{index_name} "
        f"ON {qualified_table} ({columns_sql}){where_sql}"
    )

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(create_sql)


def duckdb_family_drop_index(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: str | None = None,
    database: str | None = None,
    if_exists: bool = True,
) -> None:
    """Drop an index using DuckDB/SQLite syntax."""
    if_exists_sql = "IF EXISTS " if if_exists else ""
    drop_sql = f"DROP INDEX {if_exists_sql}{index_name}"

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(drop_sql)


# ===========================================================================
# GENERIC UPSERT — dialect-agnostic dispatcher
# ===========================================================================


def build_on_conflict_sql(
    *,
    dialect: t.Any,
    target: str,
    cols: list[str],
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    source_sql: str,
    condition_sql: str | None = None,
) -> str:
    """Pure builder: render an INSERT … ON CONFLICT statement for *dialect*.

    Takes all pre-computed parts explicitly so registry golden tests (Task 7/10)
    can render the ON CONFLICT family without a live connection.

    Args:
        dialect: sqlglot dialect (from ``dialect_of(ibis_conn)``).
        target: Fully-qualified, already-quoted target table reference.
        cols: Ordered list of source/insert column names (unquoted).
        conflict: Conflict-key column names (unquoted).
        update: Columns to update on conflict (unquoted; ignored for NOTHING).
        conflict_action: ``"UPDATE"`` or ``"NOTHING"``.
        source_sql: Compiled SELECT subquery SQL string (from ``compiled_source``).
        condition_sql: Optional rendered WHERE condition for the DO UPDATE clause.

    Returns:
        A complete ``INSERT INTO … ON CONFLICT …`` SQL string.
    """
    col_list = ", ".join(quote_identifier(c, dialect) for c in cols)
    conflict_list = ", ".join(quote_identifier(c, dialect) for c in conflict)
    # EXCLUDED is the unquoted pseudo-relation (Postgres/DuckDB/SQLite convention).
    excl = "EXCLUDED"

    if conflict_action == "NOTHING":
        action = f"ON CONFLICT ({conflict_list}) DO NOTHING"
    else:
        set_sql = ", ".join(
            f"{quote_identifier(c, dialect)} = {excl}.{quote_identifier(c, dialect)}"
            for c in update
        )
        where = f" WHERE {condition_sql}" if condition_sql else ""
        action = f"ON CONFLICT ({conflict_list}) DO UPDATE SET {set_sql}{where}"

    # ``WHERE true`` is required by SQLite to disambiguate INSERT … SELECT … ON
    # CONFLICT (its parser errors near DO without it); harmless on duckdb/postgres.
    return (
        f"INSERT INTO {target} AS tgt ({col_list}) "
        f"SELECT {col_list} FROM ({source_sql}) AS __src WHERE true {action}"
    )


def _render_on_conflict(
    ibis_conn: t.Any,
    name: str,
    obj: t.Any,
    *,
    target_schema: t.Any,
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    update_condition: t.Any,
    database: str | None,
    schema: str | None,
) -> str:
    """Thin wrapper: derive dialect/source_sql/condition_sql from the live
    connection and delegate to ``build_on_conflict_sql``."""
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)

    # update_condition only shapes the DO UPDATE arm; ON CONFLICT … DO NOTHING
    # has no WHERE, so the condition is intentionally not compiled here. The
    # caller-facing validate/warn for a condition under NOTHING lives in
    # _generic_upsert (the entry point); this branch just skips rendering it.
    condition_sql: str | None = None
    if update_condition is not None and conflict_action == "UPDATE":
        # EXCLUDED is the unquoted pseudo-relation for the incoming row; tgt is
        # the target alias used in INSERT INTO … AS tgt.
        aliases = ConditionAliases(
            incoming="EXCLUDED", existing="tgt", incoming_quoted=False
        )
        condition_sql = compile_condition(
            ibis_conn, target_schema, name, update_condition, aliases=aliases,
        ).sql(dialect=dialect)

    return build_on_conflict_sql(
        dialect=dialect,
        target=target,
        cols=cols,
        conflict=conflict,
        update=update,
        conflict_action=conflict_action,
        source_sql=source_sql,
        condition_sql=condition_sql,
    )


def build_merge_sql(
    *,
    dialect: t.Any,
    target: str,
    cols: list[str],
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    source_sql: str,
    condition_sql: str | None = None,
) -> str:
    """Pure builder: render a MERGE INTO … statement for *dialect*.

    Takes all pre-computed parts explicitly so registry golden tests can render
    any MERGE-family dialect without a live connection.

    Args:
        dialect: sqlglot dialect (from ``dialect_of(ibis_conn)``).
        target: Fully-qualified, already-quoted target table reference.
        cols: Ordered list of source/insert column names (unquoted).
        conflict: Conflict-key column names (unquoted).
        update: Columns to update on match (unquoted; ignored for NOTHING).
        conflict_action: ``"UPDATE"`` or ``"NOTHING"``.
        source_sql: Compiled SELECT subquery SQL string (from ``compiled_source``).
        condition_sql: Optional rendered condition for the WHEN MATCHED clause.

    Returns:
        A complete ``MERGE INTO … USING … ON … WHEN …`` SQL string.
    """
    q = lambda c: quote_identifier(c, dialect)  # noqa: E731
    on = " AND ".join(f"tgt.{q(c)} = src.{q(c)}" for c in conflict)
    not_matched = (
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(c) for c in cols)}) "
        f"VALUES ({', '.join(f'src.{q(c)}' for c in cols)})"
    )
    clauses: list[str] = []
    if conflict_action == "UPDATE":
        set_sql = ", ".join(f"{q(c)} = src.{q(c)}" for c in update)
        cond = f" AND {condition_sql}" if condition_sql else ""
        clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET {set_sql}")
    clauses.append(not_matched)
    return (
        f"MERGE INTO {target} AS tgt USING ({source_sql}) AS src "
        f"ON {on} " + " ".join(clauses)
    )


def _render_merge(
    ibis_conn: t.Any,
    name: str,
    obj: t.Any,
    *,
    target_schema: t.Any,
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    update_condition: t.Any,
    database: str | None,
    schema: str | None,
) -> str:
    """Thin wrapper: derive dialect/source_sql/condition_sql from the live
    connection and delegate to ``build_merge_sql``."""
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)

    condition_sql: str | None = None
    if update_condition is not None and conflict_action == "UPDATE":
        aliases = ConditionAliases(incoming="src", existing="tgt")
        condition_sql = compile_condition(
            ibis_conn, target_schema, name, update_condition, aliases=aliases,
        ).sql(dialect=dialect)

    return build_merge_sql(
        dialect=dialect,
        target=target,
        cols=cols,
        conflict=conflict,
        update=update,
        conflict_action=conflict_action,
        source_sql=source_sql,
        condition_sql=condition_sql,
    )


def _mysql_validate_conflict_key(
    ibis_conn: t.Any,
    name: str,
    conflict: list[str],
    database: str | None,
) -> None:
    """Prove the safe MySQL/MariaDB ON DUPLICATE KEY case or raise (spec §6.2).

    Fails closed on: no unique index, >1 unique index, prefix index (SUB_PART),
    functional/expression index (COLUMN_NAME IS NULL), a unique index whose
    ORDERED columns don't exactly equal conflict_columns, or any nullable
    conflict column.

    NOTE: do NOT select EXPRESSION from information_schema.STATISTICS — that
    column is MySQL-8-only; on MariaDB 12.x it errors ``Unknown column
    'EXPRESSION'``. Functional/expression index parts have a NULL COLUMN_NAME
    on both MariaDB and MySQL 8; detect them that way instead.

    NOTE: ``ibis_conn.current_database`` is a PROPERTY in ibis >=12 (no parens).
    """
    # Primary gate: name/database must be simple identifiers (charset-allowlisted
    # by _validate_simple_identifier). _generic_upsert validates them upstream;
    # re-validate here so a direct caller is equally safe.
    _validate_simple_identifier(name, kind="name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    db = database or ibis_conn.current_database
    # Defense in depth: these values go into SQL *string literals*, so render
    # them as escaped literals via sqlglot rather than bare f-string interpolation
    # (belt-and-suspenders behind the allowlist above).
    dialect = dialect_of(ibis_conn)
    name_lit = exp.Literal.string(name).sql(dialect=dialect)
    db_lit = exp.Literal.string(db).sql(dialect=dialect)
    rows = ibis_conn.raw_sql(
        "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, NON_UNIQUE "
        "FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA = {db_lit} AND TABLE_NAME = {name_lit} "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
    ).fetchall()
    uniques: dict[str, list[tuple[t.Any, t.Any]]] = {}
    for index_name, _seq, column_name, sub_part, non_unique in rows:
        if int(non_unique) == 0:
            uniques.setdefault(index_name, []).append((column_name, sub_part))
    if not uniques:
        raise ValueError(f"table {name!r} has no unique/PK index for conflict_columns")
    if len(uniques) > 1:
        raise ValueError(
            f"table {name!r} has multiple unique indexes {list(uniques)}; MySQL "
            f"ON DUPLICATE KEY detects on any of them — ambiguous for "
            f"conflict_columns={conflict}. Use the upsert_hook override."
        )
    (idx_name, parts), = uniques.items()
    if any(col is None for col, _ in parts):  # NULL COLUMN_NAME = functional/expression part
        raise ValueError(
            f"unique index {idx_name!r} is a functional/expression index; cannot "
            f"prove it matches conflict_columns={conflict}. Use the upsert_hook override."
        )
    if any(sub is not None for _, sub in parts):
        raise ValueError(
            f"unique index {idx_name!r} has a prefix (SUB_PART); it detects on a "
            f"truncated value, not the full column. Use the upsert_hook override."
        )
    if [c for c, _ in parts] != list(conflict):
        raise ValueError(
            f"unique index {idx_name!r} columns {[c for c, _ in parts]} do not "
            f"exactly match conflict_columns={list(conflict)}; refusing to guess. "
            f"Use the upsert_hook override."
        )
    # nullable check
    cols_meta = ibis_conn.raw_sql(
        "SELECT COLUMN_NAME, IS_NULLABLE FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = {db_lit} AND TABLE_NAME = {name_lit}"
    ).fetchall()
    nullable = {c for c, isn in cols_meta if isn == "YES"}
    bad = [c for c in conflict if c in nullable]
    if bad:
        raise ValueError(
            f"conflict columns {bad} are nullable; MySQL unique indexes are "
            f"NULL-distinct, so duplicates would insert instead of update. Make "
            f"them NOT NULL or use the upsert_hook override."
        )


def build_on_duplicate_key_sql(
    *,
    dialect: t.Any,
    target: str,
    cols: list[str],
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    source_sql: str,
) -> str:
    """Pure builder: render an INSERT … ON DUPLICATE KEY UPDATE statement.

    Takes all pre-computed parts explicitly so registry golden tests can render
    the ON_DUPLICATE_KEY family without a live connection.

    Args:
        dialect: sqlglot dialect (from ``dialect_of(ibis_conn)``).
        target: Fully-qualified, already-quoted target table reference.
        cols: Ordered list of source/insert column names (unquoted).
        conflict: Conflict-key column names (unquoted).  Used only for the
            NOTHING self-assign no-op (first conflict column).
        update: Columns to update on duplicate (unquoted; ignored for NOTHING).
        conflict_action: ``"UPDATE"`` or ``"NOTHING"``.
        source_sql: Compiled SELECT subquery SQL string (from ``compiled_source``).

    Returns:
        A complete ``INSERT INTO … ON DUPLICATE KEY UPDATE …`` SQL string.

    Note:
        ``VALUES(col)`` is valid on the MariaDB 12.x target.  MySQL 8.0.20+
        deprecates ``VALUES()`` in favour of a row alias; that switch is
        out-of-scope for the MariaDB-tested target here.

        For ``conflict_action="NOTHING"`` the self-assign ``k0 = k0`` is used
        as a documented no-op (it is not a true no-op on MySQL — the row is
        still "touched" — but it suppresses the update semantics with minimal
        side-effects, per spec §6.2).
    """
    q = lambda c: quote_identifier(c, dialect)  # noqa: E731
    col_list = ", ".join(q(c) for c in cols)

    if conflict_action == "NOTHING":
        k0 = q(conflict[0])
        set_sql = f"{k0} = {k0}"  # self-assign; see §6.2 (not a true no-op)
    else:
        set_sql = ", ".join(f"{q(c)} = VALUES({q(c)})" for c in update)

    return (
        f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM ({source_sql}) AS __src "
        f"ON DUPLICATE KEY UPDATE {set_sql}"
    )


def _render_on_duplicate_key(
    ibis_conn: t.Any,
    name: str,
    obj: t.Any,
    *,
    target_schema: t.Any,
    conflict: list[str],
    update: list[str],
    conflict_action: str,
    update_condition: t.Any,
    database: str | None,
    schema: str | None,
) -> str:
    """Thin wrapper: run the MySQL prove-safe preflight, then render the SQL.

    Delegates SQL construction to ``build_on_duplicate_key_sql`` so the pure
    builder is testable without a live MySQL connection.
    """
    _mysql_validate_conflict_key(ibis_conn, name, conflict, database)
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)

    return build_on_duplicate_key_sql(
        dialect=dialect,
        target=target,
        cols=cols,
        conflict=conflict,
        update=update,
        conflict_action=conflict_action,
        source_sql=source_sql,
    )


def _generic_upsert(
    ibis_conn: t.Any,
    name: str,
    obj: t.Any,
    *,
    style: t.Any,
    conflict_columns: t.Any,
    update_columns: t.Any,
    conflict_action: str,
    update_condition: t.Any,
    database: str | None,
    schema: str | None,
) -> None:
    """Dialect-agnostic upsert dispatcher.

    Validation precedence (spec §10):
      1. style (unknown → NotImplementedError)
      2. target existence
      3. identifier validation (name, database)
      4. conflict_action validity
      5. update_condition — validated UNCONDITIONALLY even under NOTHING
         (malformed predicate must error regardless of action path)
      6. updatable columns check

    Covers all three SQL families: ON_CONFLICT (DuckDB/SQLite/Postgres/RisingWave),
    MERGE (MSSQL/Oracle/Snowflake/BigQuery/Redshift/Trino/Databricks/Exasol),
    ON_DUPLICATE_KEY (MySQL/SingleStoreDB). Public ``be.upsert()`` dispatches here
    when no dialect hook is registered.
    """
    # §10.1 — style check first
    if style is None:
        raise NotImplementedError(
            f"Dialect (connection {type(ibis_conn).__name__}) does not support upsert"
        )

    # §10.2 — target existence
    _tables = ibis_conn.list_tables(database=database) if database is not None else ibis_conn.list_tables()
    if name not in _tables:
        raise ValueError(f"target table {name!r} does not exist")

    # §10.3 — identifier validation (every part that reaches qualified_name)
    _validate_simple_identifier(name, kind="name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    if schema is not None:
        _validate_simple_identifier(schema, kind="schema")

    # §10.4 — conflict_action validity
    if conflict_action not in ("UPDATE", "NOTHING"):
        raise ValueError(
            f"conflict_action must be UPDATE or NOTHING, got {conflict_action!r}"
        )

    target_schema = ibis_conn.table(name, database=database).schema()
    conflict = _normalize_columns(conflict_columns)

    # conflict column existence
    missing = [c for c in conflict if c not in target_schema.names]
    if missing:
        raise ValueError(f"conflict_columns absent from target: {missing}")

    if update_columns is None:
        update = [c for c in target_schema.names if c not in conflict]
    else:
        update = _normalize_columns(update_columns)
        missing_u = [c for c in update if c not in target_schema.names]
        if missing_u:
            raise ValueError(f"update_columns absent from target: {missing_u}")

    # §10.5 — update_condition validated UNCONDITIONALLY (even under NOTHING)
    if update_condition is not None:
        if style is UpsertStyle.ON_DUPLICATE_KEY:
            raise ValueError(
                "update_condition is not supported for the MySQL ON DUPLICATE KEY family"
            )
        validate_condition(target_schema, name, update_condition)
        if conflict_action == "NOTHING":
            warnings.warn("update_condition is ignored when conflict_action='NOTHING'")

    # §10.6 — updatable columns check
    if conflict_action == "UPDATE" and not update:
        raise ValueError(
            "no columns to update; provide update_columns or non-key columns"
        )

    # Dispatch
    if style is UpsertStyle.ON_CONFLICT:
        stmt = _render_on_conflict(
            ibis_conn, name, obj, target_schema=target_schema, conflict=conflict,
            update=update, conflict_action=conflict_action,
            update_condition=update_condition, database=database, schema=schema,
        )
    elif style is UpsertStyle.MERGE:
        stmt = _render_merge(
            ibis_conn, name, obj, target_schema=target_schema, conflict=conflict,
            update=update, conflict_action=conflict_action,
            update_condition=update_condition, database=database, schema=schema,
        )
    elif style is UpsertStyle.ON_DUPLICATE_KEY:
        stmt = _render_on_duplicate_key(
            ibis_conn, name, obj, target_schema=target_schema, conflict=conflict,
            update=update, conflict_action=conflict_action,
            update_condition=update_condition, database=database, schema=schema,
        )
    else:
        raise NotImplementedError(f"unknown upsert_style: {style!r}")

    ibis_conn.raw_sql(stmt)
