"""Shared physical-layer metadata model.

Both ibis and iceberg backends populate these dataclasses from their
native introspection APIs, giving consumers a uniform shape regardless
of which backend produced them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import typing as t

from mountainash_data.core.namespace import Namespace


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
    location: Namespace = field(default_factory=Namespace)
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def qualified_name(self) -> str:
        return ".".join(p for p in (self.location.dotted, self.name) if p)


@dataclass(frozen=True)
class NamespaceInfo:
    """Physical metadata for a namespace (schema/database/dataset)."""

    location: Namespace
    tables: t.Sequence[str]
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        """The last path segment (the immediate namespace name), or "" for default."""
        return self.location.path[-1] if self.location.path else ""


@dataclass(frozen=True)
class CatalogInfo:
    """Physical metadata for a top-level catalog or backend instance."""

    name: str
    namespaces: t.Sequence[NamespaceInfo]
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
