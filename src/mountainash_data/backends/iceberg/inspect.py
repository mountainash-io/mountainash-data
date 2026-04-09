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


def table_to_info(
    iceberg_table,
    *,
    name: str,
    namespace: t.Optional[str] = None,
    catalog: t.Optional[str] = None,
) -> TableInfo:
    """Convert a pyiceberg Table object into a TableInfo.

    Args:
        iceberg_table: A ``pyiceberg.table.Table`` instance.
        name: The simple table name (without namespace prefix).
        namespace: The namespace (schema) the table belongs to, if known.
        catalog: The catalog name, if known.

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
    return TableInfo(
        name=name,
        columns=columns,
        namespace=namespace,
        catalog=catalog,
    )


def namespace_to_info(
    namespace_name: str,
    table_names: t.Sequence[str],
    *,
    catalog: t.Optional[str] = None,
) -> NamespaceInfo:
    """Build a NamespaceInfo from a list of table names.

    Args:
        namespace_name: The namespace identifier.
        table_names: Names of tables within this namespace.
        catalog: The catalog this namespace belongs to, if known.

    Returns:
        A populated ``NamespaceInfo``.
    """
    return NamespaceInfo(
        name=namespace_name,
        tables=list(table_names),
        catalog=catalog,
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
