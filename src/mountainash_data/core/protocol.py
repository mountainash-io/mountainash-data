"""Backend protocol.

This is the structural contract every backend implementation must
satisfy. Implementations are plain classes — there is no inheritance.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)


@t.runtime_checkable
class Backend(t.Protocol):
    """The single handle for interacting with a backend service.

    Backends are constructed with config, connected via connect(),
    used for inspection and operations, then closed.
    """

    name: str

    def connect(self) -> t.Self: ...
    def close(self) -> t.Self: ...
    def __enter__(self) -> t.Self: ...
    def __exit__(self, *args: t.Any) -> None: ...

    def list_tables(self, namespace: str | None = None) -> list[str]: ...
    def list_namespaces(self) -> list[str]: ...

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo: ...

    def inspect_namespace(self, name: str) -> NamespaceInfo: ...
    def inspect_catalog(self) -> CatalogInfo: ...
