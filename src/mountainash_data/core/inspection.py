"""Shared physical-layer metadata model.

Both ibis and iceberg backends populate these dataclasses from their
native introspection APIs, giving consumers a uniform shape regardless
of which backend produced them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import typing as t


@dataclass(frozen=True)
class ColumnInfo:
    """Physical metadata for a single column."""

    name: str
    type_name: str
    nullable: bool
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TableInfo:
    """Physical metadata for a single table or view."""

    name: str
    columns: t.Sequence[ColumnInfo]
    namespace: t.Optional[str] = None
    catalog: t.Optional[str] = None
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def qualified_name(self) -> str:
        parts = [p for p in (self.catalog, self.namespace, self.name) if p]
        return ".".join(parts)


@dataclass(frozen=True)
class NamespaceInfo:
    """Physical metadata for a namespace (schema/database/dataset)."""

    name: str
    tables: t.Sequence[str]
    catalog: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogInfo:
    """Physical metadata for a top-level catalog or backend instance."""

    name: str
    namespaces: t.Sequence[NamespaceInfo]
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
