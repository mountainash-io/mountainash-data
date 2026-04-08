"""DEPRECATED: import from mountainash_data.backends.iceberg.catalogs.rest instead.

This shim exists during the Phase 3–6 refactor and will be removed in Phase 6.
"""

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection  # noqa: F401

# Backwards-compatibility alias.
PyIcebergRestConnection = IcebergRestConnection  # noqa: N816
