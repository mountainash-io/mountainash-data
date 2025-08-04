"""Tests for main mountainash_data package."""

import pytest
from mountainash_data import __version__
import mountainash_data


class TestPackageImports:
    """Test package-level imports and structure."""

    def test_version_import(self):
        """Test that version can be imported."""
        assert hasattr(mountainash_data, '__version__')
        assert isinstance(mountainash_data.__version__, str)
        assert len(mountainash_data.__version__) > 0

    def test_version_format(self):
        """Test version follows expected format."""
        version_parts = mountainash_data.__version__.split('.')
        assert len(version_parts) >= 2, "Version should have at least major.minor"

        # Test that major and minor are numeric
        assert version_parts[0].isdigit(), "Major version should be numeric"
        assert version_parts[1].isdigit(), "Minor version should be numeric"

    def test_core_imports_available(self):
        """Test that core classes can be imported."""
        # These imports should not raise errors
        from mountainash_data.databases.base_db_connection import BaseDBConnection

        # Check classes are properly defined
        assert BaseDBConnection is not None

    def test_database_connections_available(self):
        """Test that database connection classes can be imported."""
        from mountainash_data.databases.ibis.connections.sqlite_ibis_connection import SQLite_IbisConnection
        from mountainash_data.databases.ibis.connections.duckdb_ibis_connection import DuckDB_IbisConnection

        assert SQLite_IbisConnection is not None
        assert DuckDB_IbisConnection is not None



class TestPackageStructure:
    """Test package structure and organization."""

    def test_package_has_init(self):
        """Test that package has proper __init__.py."""
        import mountainash_data
        assert hasattr(mountainash_data, '__file__')

    def test_submodules_exist(self):
        """Test that expected submodules exist."""
        import mountainash_data.databases

        assert hasattr(mountainash_data, 'databases')

    @pytest.mark.parametrize("module_name", [
        "databases.base_db_connection",
        "databases.ibis.base_ibis_connection",
    ])
    def test_module_importable(self, module_name: str):
        """Test that core modules can be imported."""
        from importlib import import_module
        full_module = f"mountainash_data.{module_name}"

        module = import_module(full_module)
        assert module is not None
