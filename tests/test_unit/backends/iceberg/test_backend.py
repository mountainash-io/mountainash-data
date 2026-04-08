"""Tests for IcebergBackend factory.

NOTE: These tests do NOT call ``.connect()`` — that would require a live
iceberg catalog and valid settings parameters. They verify construction,
protocol shape, and config storage only. End-to-end connection tests are
out of scope for this refactor (see COVERAGE_GAP.md).

pyiceberg is an optional dependency that is not installed in the default
test environment. All tests in this module are skipped when it is absent.
"""

import pytest

pyiceberg = pytest.importorskip("pyiceberg", reason="pyiceberg not installed")

from mountainash_data.backends.iceberg.backend import IcebergBackend  # noqa: E402
from mountainash_data.core.protocol import Backend  # noqa: E402


def test_iceberg_backend_satisfies_protocol():
    backend = IcebergBackend(catalog="rest", uri="http://localhost:8181")
    assert isinstance(backend, Backend)
    assert backend.name == "iceberg"


def test_unknown_catalog_raises():
    with pytest.raises(KeyError, match="Unknown iceberg catalog"):
        IcebergBackend(catalog="bogus")


def test_iceberg_backend_carries_config():
    backend = IcebergBackend(catalog="rest", uri="http://localhost:8181", token="abc")
    assert backend._config == {"uri": "http://localhost:8181", "token": "abc"}
