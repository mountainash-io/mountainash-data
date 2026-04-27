"""mountainash-data: physical access to backend data services.

Public API:
    Backend — protocol (core.protocol)
    IbisBackend — ibis-style relational backends (backends.ibis.backend)
    IcebergBackend — iceberg-style table-format catalogs (backends.iceberg.backend)
    CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo — inspection model
    DatabaseUtils — high-level facade
    ConnectionFactory, OperationsFactory, SettingsFactory — factories
    *Settings classes — see mountainash_data.core.settings
"""

from mountainash_data.__version__ import __version__
from mountainash_data.core.protocol import Backend
from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.utils import DatabaseUtils
from mountainash_data.core.factories import (
    ConnectionFactory,
    OperationsFactory,
    SettingsFactory,
)
from mountainash_data.backends.ibis.backend import IbisBackend

# IcebergBackend requires the optional pyiceberg dependency.
# It is imported lazily so that consumers without pyiceberg installed
# still get the rest of the package.
try:
    from mountainash_data.backends.iceberg.backend import IcebergBackend
except ImportError:
    IcebergBackend = None  # type: ignore[assignment,misc]

__all__ = [
    "__version__",
    "Backend",
    "CatalogInfo",
    "ColumnInfo",
    "NamespaceInfo",
    "TableInfo",
    "DatabaseUtils",
    "ConnectionFactory",
    "OperationsFactory",
    "SettingsFactory",
    "IbisBackend",
    "IcebergBackend",
]
