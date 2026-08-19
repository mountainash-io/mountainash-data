"""mountainash-data: physical access to backend data services.

Public API:
    Backend — protocol (core.protocol)
    IbisBackend — ibis-style relational backends (backends.ibis.backend)
    CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo — inspection model
"""

from mountainash_data.__version__ import __version__
from mountainash_data.core.protocol import Backend
from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    IndexInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.namespace import Namespace, NamespaceLike
from mountainash_data.backends.ibis.backend import IbisBackend

__all__ = [
    "__version__",
    "Backend",
    "CatalogInfo",
    "ColumnInfo",
    "IndexInfo",
    "NamespaceInfo",
    "TableInfo",
    "Namespace",
    "NamespaceLike",
    "IbisBackend",
]
