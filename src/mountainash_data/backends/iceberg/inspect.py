"""Iceberg to core.inspection conversion.

Helpers that take pyiceberg Table objects and produce TableInfo /
NamespaceInfo / CatalogInfo dataclasses from core.inspection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.namespace import Namespace


def table_to_info(
    iceberg_table,
    *,
    name: str,
    location: Namespace = Namespace(),
) -> TableInfo:
    """Convert a pyiceberg Table object into a TableInfo.

    Args:
        iceberg_table: A ``pyiceberg.table.Table`` instance.
        name: The simple table name (without namespace prefix).
        location: The table's namespace/catalog location.

    Returns:
        A ``TableInfo`` populated from the table's current schema.
    """
    columns = [
        ColumnInfo(
            name=field.name,
            type_name=str(field.field_type),
            nullable=not field.required,
        )
        for field in iceberg_table.schema().fields
    ]
    return TableInfo(name=name, columns=columns, location=location)


def namespace_to_info(
    namespace_path: t.Sequence[str],
    table_names: t.Sequence[str],
) -> NamespaceInfo:
    """Build a NamespaceInfo from a namespace path and its table names.

    Args:
        namespace_path: The namespace path segments.
        table_names: Names of tables within this namespace.

    Returns:
        A populated ``NamespaceInfo``.
    """
    return NamespaceInfo(
        location=Namespace(path=tuple(namespace_path)),
        tables=list(table_names),
    )


def catalog_to_info(
    catalog_name: str,
    namespace_infos: t.Sequence[NamespaceInfo],
) -> CatalogInfo:
    """Build a CatalogInfo from a sequence of NamespaceInfo objects.

    Args:
        catalog_name: The catalog identifier.
        namespace_infos: Pre-built NamespaceInfo objects for each namespace.

    Returns:
        A populated ``CatalogInfo``.
    """
    return CatalogInfo(
        name=catalog_name,
        namespaces=list(namespace_infos),
    )
