"""DEPRECATED: import from mountainash_data.backends.iceberg.catalogs.rest instead.

This shim exists during the Phase 3–6 refactor and will be removed in Phase 6.

NOTE: The Phase 3 refactor merged the REST operations into IcebergRestConnection.
PyIcebergRestOperations is aliased to IcebergRestConnection for backwards
compatibility.
"""

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection  # noqa: F401

# Backwards-compatibility alias.
PyIcebergRestOperations = IcebergRestConnection  # noqa: N816
