"""Smoke test: verify the legacy iceberg files can be imported in their new location."""

import pytest

pyiceberg = pytest.importorskip("pyiceberg", reason="pyiceberg not installed")


def test_legacy_imports():
    from mountainash_data.backends.iceberg import (
        _legacy_connection,
        _legacy_operations,
        _legacy_rest_connection,
        _legacy_rest_operations,
    )
    assert _legacy_connection is not None
    assert _legacy_operations is not None
    assert _legacy_rest_connection is not None
    assert _legacy_rest_operations is not None
