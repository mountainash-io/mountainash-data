"""Iceberg namespace interface alignment (DEBT-10; deep-path fidelity = DEBT-11).

NOTE: There is no live/in-memory iceberg catalog fixture in this repo (see
tests/fixtures/database_fixtures.py and test_backend.py), so this module is
limited to a pure structural check that needs no live catalog. Behavioral /
round-trip coverage against a real catalog is out of scope here and tracked
under DEBT-11.
"""

import pytest

pytest.importorskip("pyiceberg")

from mountainash_data.core.namespace import Namespace, NamespaceLike  # noqa: F401


def test_iceberg_connection_satisfies_widened_protocol():
    """The iceberg connection base exposes the widened discovery surface."""
    from mountainash_data.backends.iceberg.connection import IcebergConnectionBase

    for meth in ("list_tables", "list_namespaces", "list_catalogs",
                 "inspect_table", "inspect_namespace", "inspect_catalog"):
        assert hasattr(IcebergConnectionBase, meth)
