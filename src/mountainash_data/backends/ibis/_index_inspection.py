"""Index catalog inspection for generic and vendor-specific ibis backends."""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis._render import _sql_literal, quote_identifier
from mountainash_data.core.inspection import IndexInfo


def _normalize_flag(value: t.Any, field: str, *, allow_none: bool = False) -> bool | None:
    if value is None and allow_none:
        return None
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise RuntimeError(f"invalid {field} flag: {value!r}")


def _normalize_text(value: t.Any, field: str, *, allow_none: bool = True) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"invalid {field} text: {value!r}")
    return value


def _normalize_position(value: t.Any) -> int:
    if type(value) is not int or value <= 0:
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
    for row_number, row in enumerate(result.to_pyarrow().to_pylist(), start=1):
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
         THEN pg_get_indexdef(i.indexrelid, keypos.ord::integer, false)
         ELSE NULL
    END AS col_expr,
    (keypos.ord > i.indnkeyatts) AS is_included,
    keypos.ord AS position
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace
JOIN pg_am am ON am.oid = ic.relam
CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY
                   AS keypos(attnum, ord)
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
FROM user_indexes idx
JOIN user_ind_columns cols ON cols.index_name = idx.index_name
LEFT JOIN user_ind_expressions exprs
       ON exprs.index_name = idx.index_name
      AND exprs.column_position = cols.column_position
LEFT JOIN user_constraints con
       ON con.index_name = idx.index_name
      AND con.constraint_type = 'P'
WHERE idx.table_name = {_sql_literal(table_name)}
"""
