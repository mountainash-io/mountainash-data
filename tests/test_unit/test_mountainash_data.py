"""Tests for main mountainash_data package."""

import pytest
import mountainash_data


class TestPackageImports:
    """Test package-level imports and structure."""

    def test_version_import(self):
        assert hasattr(mountainash_data, '__version__')
        assert isinstance(mountainash_data.__version__, str)
        assert len(mountainash_data.__version__) > 0

    def test_version_format(self):
        version_parts = mountainash_data.__version__.split('.')
        assert len(version_parts) >= 2
        assert version_parts[0].isdigit()
        assert version_parts[1].isdigit()

    def test_core_imports_available(self):
        from mountainash_data.core.connection import BaseDBConnection
        assert BaseDBConnection is not None


class TestPackageStructure:
    """Test package structure and organization."""

    def test_package_has_init(self):
        assert hasattr(mountainash_data, '__file__')

    def test_new_submodules_exist(self):
        import mountainash_data.core
        import mountainash_data.backends
        assert hasattr(mountainash_data, 'core')
        assert hasattr(mountainash_data, 'backends')

    def test_public_api_ibis_backend(self):
        from mountainash_data import IbisBackend
        assert IbisBackend is not None

    def test_public_api_backend_protocol(self):
        from mountainash_data import Backend
        assert Backend is not None

    def test_public_api_inspection_model(self):
        from mountainash_data import CatalogInfo, ColumnInfo, NamespaceInfo, TableInfo
        assert CatalogInfo is not None
        assert ColumnInfo is not None
        assert NamespaceInfo is not None
        assert TableInfo is not None

    def test_removed_exports_not_available(self):
        assert not hasattr(mountainash_data, 'ConnectionFactory')
        assert not hasattr(mountainash_data, 'OperationsFactory')
        assert not hasattr(mountainash_data, 'SettingsFactory')
        assert not hasattr(mountainash_data, 'DatabaseUtils')
