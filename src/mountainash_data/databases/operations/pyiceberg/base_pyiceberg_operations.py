"""DEPRECATED: import from mountainash_data.backends.iceberg.connection instead.

This shim exists during the Phase 3–6 refactor and will be removed in Phase 6.

NOTE: The Phase 3 refactor moved table mutations from class methods on
BasePyIcebergOperations into module-level functions in
mountainash_data.backends.iceberg.operations. The IcebergConnectionBase class
exposes thin delegation wrappers for all mutations, so it is a drop-in
replacement for most uses of BasePyIcebergOperations.

The alias below allows code that subclassed BasePyIcebergOperations to
continue working without modification.
"""

from mountainash_data.backends.iceberg.connection import IcebergConnectionBase  # noqa: F401

# Backwards-compatibility alias.
# IcebergConnectionBase exposes all the same mutation methods (create_table,
# drop_table, insert, upsert, truncate, create_view, drop_view) as thin
# wrappers that delegate to backends.iceberg.operations.
BasePyIcebergOperations = IcebergConnectionBase  # noqa: N816
