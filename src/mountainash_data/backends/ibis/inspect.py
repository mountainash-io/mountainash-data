"""Ibis -> core.inspection conversion helpers."""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


def table_to_info(
    ibis_table,
    *,
    name: str,
    namespace: str | None = None,
    catalog: str | None = None,
) -> TableInfo:
    """Convert an ibis Table object into a TableInfo."""
    schema = ibis_table.schema()
    columns = [
        ColumnInfo(
            name=col_name,
            type_name=str(col_type),
            nullable=col_type.nullable,
        )
        for col_name, col_type in zip(schema.names, schema.types)
    ]
    return TableInfo(
        name=name,
        columns=columns,
        namespace=namespace,
        catalog=catalog,
    )
