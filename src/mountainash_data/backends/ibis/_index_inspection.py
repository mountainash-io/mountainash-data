"""Index catalog inspection for generic and vendor-specific ibis backends."""

from __future__ import annotations

import typing as t

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
