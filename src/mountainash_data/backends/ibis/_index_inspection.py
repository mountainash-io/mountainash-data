"""Index catalog inspection for generic and vendor-specific ibis backends."""

from __future__ import annotations

import re
import typing as t

from sqlglot import exp, parse_one

from mountainash_data.backends.ibis._render import _sql_literal, quote_identifier
from mountainash_data.core.inspection import IndexInfo


def _normalize_flag(value: t.Any, field: str, *, allow_none: bool = False) -> bool | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
        return bool(value)
    raise RuntimeError(f"invalid {field} flag: {value!r}")


def _normalize_text(value: t.Any, field: str, *, allow_none: bool = True) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"invalid {field} text: {value!r}")
    return value

def _positional_values(row: t.Any) -> tuple[t.Any, ...]:
    """Read Arrow rows by schema order, never by catalog alias."""
    if isinstance(row, dict):
        return tuple(row.values())
    return tuple(row)


def _normalize_position(value: t.Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RuntimeError(f"invalid position: {value!r}")
    return value


def _generic_list_indexes(
    ibis_conn: t.Any,
    table_name: str,
    namespace: str | None,
    list_sql_fn: t.Callable[[str, str | None], str],
) -> list[IndexInfo]:
    """Run a ten-column catalog query and return strict typed index metadata."""
    result = ibis_conn.sql(list_sql_fn(table_name, namespace))
    if result is None:
        return []

    groups: dict[str, dict[str, t.Any]] = {}
    for row_number, raw_row in enumerate(
        result.to_pyarrow().to_pylist(), start=1
    ):
        row = _positional_values(raw_row)
        if len(row) != 10:
            raise RuntimeError(
                f"index catalog row {row_number} has {len(row)} columns; expected 10"
            )
        (
            raw_name,
            raw_unique,
            raw_primary,
            raw_valid,
            raw_type,
            raw_definition,
            raw_col_name,
            raw_col_expr,
            raw_included,
            raw_position,
        ) = row
        name = _normalize_text(raw_name, "index_name", allow_none=False)
        unique = _normalize_flag(raw_unique, "is_unique")
        is_primary = _normalize_flag(raw_primary, "is_primary")
        is_valid = _normalize_flag(raw_valid, "is_valid", allow_none=True)
        is_included = _normalize_flag(raw_included, "is_included")
        index_type = _normalize_text(raw_type, "index_type")
        definition = _normalize_text(raw_definition, "definition")
        col_name = _normalize_text(raw_col_name, "col_name")
        col_expr = _normalize_text(raw_col_expr, "col_expr")
        position = _normalize_position(raw_position)

        metadata = (unique, is_primary, is_valid, index_type, definition)
        group = groups.setdefault(
            t.cast(str, name),
            {
                "metadata": metadata,
                "positions": set(),
                "keys": [],
                "included": [],
            },
        )
        if group["metadata"] != metadata:
            raise RuntimeError(f"conflicting index metadata for {name!r}")
        if position in group["positions"]:
            raise RuntimeError(f"duplicate index position for {name!r}: {position}")
        group["positions"].add(position)

        if is_included:
            if col_expr is not None or not isinstance(col_name, str) or not col_name:
                raise RuntimeError("invalid non-key index column")
            group["included"].append((position, col_name))
        else:
            value = col_expr if col_expr is not None else col_name
            group["keys"].append(
                (position, value if value is not None else "<expression>")
            )

    indexes: list[IndexInfo] = []
    for name, group in groups.items():
        if not group["keys"]:
            raise RuntimeError(f"index {name!r} has no key positions")
        unique, is_primary, is_valid, index_type, definition = group["metadata"]
        indexes.append(
            IndexInfo(
                name=name,
                unique=unique,
                is_primary=is_primary,
                columns=tuple(value for _, value in sorted(group["keys"])),
                index_type=index_type.lower() if index_type is not None else None,
                included_columns=tuple(
                    value for _, value in sorted(group["included"])
                ),
                is_valid=is_valid,
                definition=definition,
            )
        )
    return sorted(indexes, key=lambda index: index.name)


def sqlite_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    scope = namespace or "main"
    scope_literal = _sql_literal(scope)
    scope_identifier = quote_identifier(scope, "sqlite")
    return f"""
SELECT
    CAST(l.name AS TEXT) AS index_name,
    CAST(l."unique" AS INTEGER) AS is_unique,
    CAST(l.origin = 'pk' AS INTEGER) AS is_primary,
    CAST(NULL AS INTEGER) AS is_valid,
    CAST('btree' AS TEXT) AS index_type,
    CAST(m.sql AS TEXT) AS definition,
    CAST(i.name AS TEXT) AS col_name,
    CAST(NULL AS TEXT) AS col_expr,
    CAST(0 AS INTEGER) AS is_included,
    CAST(i.seqno + 1 AS INTEGER) AS position
FROM pragma_index_list({_sql_literal(table_name)}, {scope_literal}) l
JOIN pragma_index_xinfo(l.name, {scope_literal}) i
LEFT JOIN {scope_identifier}.sqlite_master m
       ON m.type = 'index' AND m.name = l.name
WHERE i.key = 1
"""


def postgres_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    schema = _sql_literal(namespace) if namespace else "current_schema()"
    return f"""
SELECT
    ic.relname AS index_name,
    i.indisunique AS is_unique,
    i.indisprimary AS is_primary,
    i.indisvalid AS is_valid,
    am.amname AS index_type,
    pg_get_indexdef(i.indexrelid) AS definition,
    CASE WHEN keypos.attnum = 0 THEN NULL ELSE attr.attname END AS col_name,
    CASE WHEN keypos.attnum = 0
         THEN pg_get_indexdef(i.indexrelid, keypos.ordinality::integer, false)
         ELSE NULL
    END AS col_expr,
    (keypos.ordinality > i.indnkeyatts) AS is_included,
    keypos.ordinality AS position
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace
JOIN pg_am am ON am.oid = ic.relam
CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY
                   AS keypos(attnum)
LEFT JOIN pg_attribute attr
       ON attr.attrelid = i.indrelid AND attr.attnum = keypos.attnum
WHERE tc.relname = {_sql_literal(table_name)}
  AND n.nspname = {schema}
"""


def mysql_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    schema = _sql_literal(namespace) if namespace else "DATABASE()"
    return f"""
SELECT
    INDEX_NAME AS index_name,
    (NOT NON_UNIQUE) AS is_unique,
    (INDEX_NAME = 'PRIMARY') AS is_primary,
    CAST(NULL AS SIGNED) AS is_valid,
    INDEX_TYPE AS index_type,
    CAST(NULL AS CHAR) AS definition,
    COLUMN_NAME AS col_name,
    CAST(NULL AS CHAR) AS col_expr,
    CAST(0 AS SIGNED) AS is_included,
    SEQ_IN_INDEX AS position
FROM information_schema.STATISTICS
WHERE TABLE_NAME = {_sql_literal(table_name)}
  AND TABLE_SCHEMA = {schema}
"""


def singlestore_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    return mysql_get_list_indexes_sql(table_name, namespace)


def mssql_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    target = f"{namespace}.{table_name}" if namespace else table_name
    return f"""
SELECT
    i.name AS index_name,
    i.is_unique AS is_unique,
    i.is_primary_key AS is_primary,
    CASE WHEN i.is_disabled = 0 AND i.is_hypothetical = 0
         THEN CAST(1 AS INTEGER) ELSE CAST(0 AS INTEGER) END AS is_valid,
    i.type_desc AS index_type,
    i.filter_definition AS definition,
    c.name AS col_name,
    CAST(NULL AS NVARCHAR(MAX)) AS col_expr,
    CASE
        WHEN ic.key_ordinal > 0 THEN CAST(0 AS INTEGER)
        WHEN ic.is_included_column = 1 OR ic.partition_ordinal > 0
            THEN CAST(1 AS INTEGER)
        ELSE CAST(NULL AS INTEGER)
    END AS is_included,
    ic.index_column_id AS position
FROM sys.indexes i
JOIN sys.index_columns ic
  ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c
  ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID({_sql_literal(target)})
  AND i.index_id > 0
  AND i.type IN (1, 2)
  AND ic.column_id > 0
"""


def oracle_get_list_indexes_sql(
    table_name: str, namespace: str | None
) -> str:
    owner = _sql_literal(namespace) if namespace else "USER"
    return f"""
SELECT
    idx.index_name AS index_name,
    CASE WHEN idx.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN con.constraint_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    CASE
        WHEN idx.partitioned <> 'NO' THEN NULL
        WHEN idx.status = 'UNUSABLE'
          OR idx.domidx_status = 'IDXTYP_INVLD'
          OR idx.domidx_opstatus = 'FAILED'
          OR idx.funcidx_status = 'DISABLED'
            THEN 0
        WHEN idx.partitioned = 'NO'
          AND idx.status = 'VALID'
          AND (
              (idx.domidx_status IS NULL AND idx.domidx_opstatus IS NULL)
              OR (idx.domidx_status = 'VALID' AND idx.domidx_opstatus = 'VALID')
          )
          AND (idx.funcidx_status IS NULL OR idx.funcidx_status = 'ENABLED')
            THEN 1
        ELSE NULL
    END AS is_valid,
    idx.index_type AS index_type,
    CAST(NULL AS VARCHAR2(4000)) AS definition,
    cols.column_name AS col_name,
    exprs.column_expression AS col_expr,
    0 AS is_included,
    cols.column_position AS position
FROM all_indexes idx
JOIN all_ind_columns cols
  ON cols.index_owner = idx.owner
 AND cols.index_name = idx.index_name
LEFT JOIN all_ind_expressions exprs
       ON exprs.index_owner = idx.owner
      AND exprs.index_name = idx.index_name
      AND exprs.column_position = cols.column_position
LEFT JOIN all_constraints con
       ON con.index_owner = idx.owner
      AND con.index_name = idx.index_name
      AND con.constraint_type = 'P'
WHERE idx.owner = {owner}
  AND idx.table_name = {_sql_literal(table_name)}
"""


def _extract_duckdb_index_definition(definition: str) -> tuple[tuple[str, ...], str]:
    """Extract ordered DuckDB index keys without parsing a display string."""
    try:
        statement = parse_one(definition, read="duckdb")
    except Exception as exc:
        raise RuntimeError("invalid DuckDB CREATE INDEX definition") from exc
    if not (
        isinstance(statement, exp.Create)
        and statement.args.get("kind") == "INDEX"
        and isinstance(statement.this, exp.Index)
    ):
        raise RuntimeError("DuckDB definition is not CREATE INDEX")

    params = statement.this.args.get("params")
    columns = params.args.get("columns") if params is not None else None
    if not columns:
        raise RuntimeError("DuckDB CREATE INDEX has no key columns")

    keys: list[str] = []
    for ordered in columns:
        expression = ordered.this if isinstance(ordered, exp.Ordered) else ordered
        if isinstance(expression, exp.Column):
            keys.append(expression.name)
        else:
            keys.append(expression.sql(dialect="duckdb"))

    using = params.args.get("using") if params is not None else None
    index_type = using.name.lower() if isinstance(using, exp.Var) else "art"
    return tuple(keys), index_type


def duckdb_get_indexes_sql(table_name: str, namespace: str | None) -> str:
    schema = _sql_literal(namespace) if namespace else "current_schema()"
    return f"""
SELECT index_oid, index_name, is_unique, is_primary, sql
FROM duckdb_indexes()
WHERE table_name = {_sql_literal(table_name)}
  AND schema_name = {schema}
  AND database_name = current_catalog()
"""


def duckdb_get_constraints_sql(table_name: str, namespace: str | None) -> str:
    schema = _sql_literal(namespace) if namespace else "current_schema()"
    return f"""
SELECT constraint_index, constraint_type, constraint_text, constraint_column_names
FROM duckdb_constraints()
WHERE table_name = {_sql_literal(table_name)}
  AND schema_name = {schema}
  AND database_name = current_catalog()
  AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
"""


def duckdb_list_indexes_hook(
    ibis_conn: t.Any, table_name: str, namespace: str | None
) -> list[IndexInfo]:
    """List DuckDB explicit indexes and index-backed constraints."""
    explicit_result = ibis_conn.sql(duckdb_get_indexes_sql(table_name, namespace))
    constraint_result = ibis_conn.sql(
        duckdb_get_constraints_sql(table_name, namespace)
    )
    entries: list[tuple[str, str, int, IndexInfo]] = []

    if explicit_result is not None:
        for raw_row in explicit_result.to_pyarrow().to_pylist():
            row = _positional_values(raw_row)
            if len(row) != 5:
                raise RuntimeError("DuckDB index catalog row must have five columns")
            index_oid, name, raw_unique, raw_primary, definition = row
            if not isinstance(index_oid, int) or isinstance(index_oid, bool):
                raise RuntimeError(f"invalid DuckDB index_oid: {index_oid!r}")
            name = _normalize_text(name, "index_name", allow_none=False)
            definition = _normalize_text(
                definition, "definition", allow_none=False
            )
            unique = _normalize_flag(raw_unique, "is_unique")
            is_primary = _normalize_flag(raw_primary, "is_primary")
            columns, index_type = _extract_duckdb_index_definition(definition)
            entries.append(
                (
                    name,
                    "index",
                    index_oid,
                    IndexInfo(
                        name=name,
                        unique=unique,
                        is_primary=is_primary,
                        columns=columns,
                        index_type=index_type,
                        definition=definition,
                        metadata={"source_kind": "index", "index_oid": index_oid},
                    ),
                )
            )
    if constraint_result is not None:
        for raw_row in constraint_result.to_pyarrow().to_pylist():
            row = _positional_values(raw_row)
            if len(row) != 4:
                raise RuntimeError(
                    "DuckDB constraint catalog row must have four columns"
                )
            constraint_index, raw_type, definition, raw_columns = row
            if not isinstance(constraint_index, int) or isinstance(
                constraint_index, bool
            ):
                raise RuntimeError(
                    f"invalid DuckDB constraint_index: {constraint_index!r}"
                )
            constraint_type = _normalize_text(
                raw_type, "constraint_type", allow_none=False
            )
            definition = _normalize_text(
                definition, "definition", allow_none=False
            )
            normalized_type = re.sub(r"\s+", "_", constraint_type.lower())
            if normalized_type not in {"primary_key", "unique", "foreign_key"}:
                raise RuntimeError(f"unsupported DuckDB constraint type: {raw_type!r}")
            if not isinstance(raw_columns, (list, tuple)) or not raw_columns:
                raise RuntimeError("invalid DuckDB constraint columns")
            columns = tuple(
                _normalize_text(column, "constraint column", allow_none=False)
                for column in raw_columns
            )
            is_primary = normalized_type == "primary_key"
            unique = normalized_type in {"primary_key", "unique"}
            name = f"constraint_{normalized_type}_{table_name}_{constraint_index}"
            entries.append(
                (
                    name,
                    "constraint",
                    constraint_index,
                    IndexInfo(
                        name=name,
                        unique=unique,
                        is_primary=is_primary,
                        columns=columns,
                        index_type="art",
                        definition=definition,
                        metadata={
                            "source_kind": "constraint",
                            "constraint_type": normalized_type,
                            "constraint_index": constraint_index,
                        },
                    ),
                )
            )

    return [
        info
        for _, _, _, info in sorted(
            entries, key=lambda entry: (entry[0], entry[1], entry[2])
        )
    ]
