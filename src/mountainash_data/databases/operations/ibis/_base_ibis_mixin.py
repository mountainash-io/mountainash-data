"""DEPRECATED: helpers are now module-level functions in
mountainash_data.backends.ibis.operations."""

from mountainash_data.backends.ibis.operations import (  # noqa: F401
    _generate_index_name,
    _format_qualified_table,
    _normalize_columns,
    _BaseIbisMixin,
)
