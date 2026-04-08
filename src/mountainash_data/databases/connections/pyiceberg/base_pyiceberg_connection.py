"""DEPRECATED: import from mountainash_data.backends.iceberg.connection instead.

This shim exists during the Phase 3–6 refactor and will be removed in Phase 6.
"""

from mountainash_data.backends.iceberg.connection import IcebergConnectionBase  # noqa: F401

# Backwards-compatibility alias: consumers that imported BasePyIcebergConnection
# will continue to work. New code should use IcebergConnectionBase.
BasePyIcebergConnection = IcebergConnectionBase  # noqa: N816
