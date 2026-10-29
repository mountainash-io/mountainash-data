"""Ibis -> core.inspection conversion helpers."""

from __future__ import annotations

from mountainash_data.core.inspection import ColumnInfo, TableInfo
from mountainash_data.core.namespace import Namespace


def table_to_info(
    ibis_table,
    *,
    name: str,
    location: Namespace = Namespace(),
) -> TableInfo:
    """Convert an ibis Table object into a TableInfo."""
    schema = ibis_table.schema()
    columns = [
        ColumnInfo(name=col_name, type_name=str(col_type), nullable=col_type.nullable)
        for col_name, col_type in zip(schema.names, schema.types)
    ]
    return TableInfo(name=name, columns=columns, location=location)
