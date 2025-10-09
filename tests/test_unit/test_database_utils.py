"""Tests for DatabaseUtils high-level API."""

import pytest
from pathlib import Path
from mountainash_data.database_utils import DatabaseUtils
from mountainash_data.databases import BaseDBConnection, SQLite_IbisConnection, DuckDB_IbisConnection
from mountainash_data.databases.operations.ibis.base_ibis_operations import BaseIbisOperations
from mountainash_data.databases.settings import SQLiteAuthSettings, DuckDBAuthSettings
from mountainash_data.databases.constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings import SettingsParameters


@pytest.mark.unit
class TestDatabaseUtilsCreateConnection:
    """Tests for DatabaseUtils.create_connection method."""

    def test_create_connection_with_sqlite_settings(self, temp_sqlite_db):
        """Test creating SQLite connection from settings parameters."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        connection = DatabaseUtils.create_connection(settings_params)

        assert connection is not None
        assert isinstance(connection, BaseDBConnection)
        assert isinstance(connection, SQLite_IbisConnection)

    def test_create_connection_with_duckdb_settings(self):
        """Test creating DuckDB connection from settings parameters."""
        settings_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        connection = DatabaseUtils.create_connection(settings_params)

        assert connection is not None
        assert isinstance(connection, BaseDBConnection)
        assert isinstance(connection, DuckDB_IbisConnection)

    def test_created_connection_can_connect(self, temp_sqlite_db):
        """Test that created connection can actually connect."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        connection = DatabaseUtils.create_connection(settings_params)
        backend = connection.connect()

        assert backend is not None
        assert connection.is_connected()

        # Cleanup
        connection.disconnect()


@pytest.mark.unit
class TestDatabaseUtilsCreateOperations:
    """Tests for DatabaseUtils.create_operations method."""

    def test_create_operations_with_sqlite_settings(self, temp_sqlite_db):
        """Test creating operations from SQLite settings parameters."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        operations = DatabaseUtils.create_operations(settings_params)

        assert operations is not None
        assert isinstance(operations, BaseIbisOperations)

    def test_create_operations_with_duckdb_settings(self):
        """Test creating operations from DuckDB settings parameters."""
        settings_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations = DatabaseUtils.create_operations(settings_params)

        assert operations is not None
        assert isinstance(operations, BaseIbisOperations)


@pytest.mark.unit
class TestDatabaseUtilsCreateBackend:
    """Tests for DatabaseUtils.create_backend convenience method."""

    def test_create_backend_connects_immediately(self, temp_sqlite_db):
        """Test that create_backend returns connected backend."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        backend = DatabaseUtils.create_backend(settings_params)

        assert backend is not None
        # Should be able to list tables
        tables = backend.list_tables()
        assert isinstance(tables, list)
        assert 'test_table' in tables

    def test_create_backend_with_duckdb(self):
        """Test create_backend with DuckDB in-memory."""
        settings_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        backend = DatabaseUtils.create_backend(settings_params)

        assert backend is not None
        # Should be able to create and list tables
        backend.create_table('test', {'id': [1, 2, 3]})
        tables = backend.list_tables()
        assert 'test' in tables


@pytest.mark.unit
class TestDatabaseUtilsSettingsFromURL:
    """Tests for DatabaseUtils.create_settings_from_url method."""

    def test_create_settings_from_sqlite_url(self):
        """Test creating settings from SQLite URL."""
        url = "sqlite:///test.db"

        settings = DatabaseUtils.create_settings_from_url(url)

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)

    def test_create_settings_from_duckdb_url(self):
        """Test creating settings from DuckDB URL."""
        url = "duckdb:///test.duckdb"

        settings = DatabaseUtils.create_settings_from_url(url)

        assert settings is not None
        assert isinstance(settings, DuckDBAuthSettings)

    @pytest.mark.parametrize("url,expected_type", [
        ("sqlite:///:memory:", SQLiteAuthSettings),
        ("duckdb:///:memory:", DuckDBAuthSettings),
    ])
    def test_create_settings_from_url_parametrized(self, url, expected_type):
        """Test URL detection for various database types."""
        settings = DatabaseUtils.create_settings_from_url(url)

        assert settings is not None
        assert isinstance(settings, expected_type)


@pytest.mark.unit
class TestDatabaseUtilsSettingsFromBackendType:
    """Tests for DatabaseUtils.create_settings_from_backend_type method."""

    def test_create_settings_from_sqlite_backend_type(self):
        """Test creating settings from backend type enum."""
        settings = DatabaseUtils.create_settings_from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)

    def test_create_settings_from_duckdb_backend_type(self):
        """Test creating settings from DuckDB backend type."""
        settings = DatabaseUtils.create_settings_from_backend_type(
            CONST_DB_PROVIDER_TYPE.DUCKDB,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, DuckDBAuthSettings)

    @pytest.mark.parametrize("backend_type,expected_settings", [
        (CONST_DB_PROVIDER_TYPE.SQLITE, SQLiteAuthSettings),
        (CONST_DB_PROVIDER_TYPE.DUCKDB, DuckDBAuthSettings),
    ])
    def test_create_settings_from_backend_type_parametrized(self, backend_type, expected_settings):
        """Test settings creation for various backend types."""
        settings = DatabaseUtils.create_settings_from_backend_type(
            backend_type,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, expected_settings)


@pytest.mark.unit
class TestDatabaseUtilsDetectBackend:
    """Tests for DatabaseUtils.detect_backend_from_url method."""

    @pytest.mark.parametrize("url,expected_backend", [
        ("sqlite:///test.db", CONST_DB_PROVIDER_TYPE.SQLITE),
        ("sqlite:///:memory:", CONST_DB_PROVIDER_TYPE.SQLITE),
        ("duckdb:///test.duckdb", CONST_DB_PROVIDER_TYPE.DUCKDB),
        ("duckdb:///:memory:", CONST_DB_PROVIDER_TYPE.DUCKDB),
    ])
    def test_detect_backend_from_url(self, url, expected_backend):
        """Test backend detection from various URL formats."""
        detected_backend = DatabaseUtils.detect_backend_from_url(url)

        assert detected_backend == expected_backend


@pytest.mark.integration
class TestDatabaseUtilsCreateFromURL:
    """Tests for DatabaseUtils.create_from_url end-to-end method."""

    def test_create_from_sqlite_url_end_to_end(self, temp_sqlite_db):
        """Test complete workflow from URL to connected backend."""
        url = f"sqlite:///{temp_sqlite_db}"

        connection, backend = DatabaseUtils.create_from_url(url)

        assert connection is not None
        assert isinstance(connection, SQLite_IbisConnection)
        assert backend is not None
        # Verify backend is actually connected and functional
        tables = backend.list_tables()
        assert isinstance(tables, list)

        # Create a table to verify functionality
        backend.create_table('workflow_test', {'id': [1, 2, 3]}, overwrite=True)
        assert 'workflow_test' in backend.list_tables()

        # Cleanup
        connection.disconnect()

    def test_create_from_duckdb_url_end_to_end(self):
        """Test complete workflow with DuckDB in-memory."""
        url = "duckdb:///:memory:"

        connection, backend = DatabaseUtils.create_from_url(url)

        assert connection is not None
        assert isinstance(connection, DuckDB_IbisConnection)
        assert backend is not None
        # Verify we can perform operations
        backend.create_table('test_table', {'id': [1, 2, 3], 'value': [10, 20, 30]})
        tables = backend.list_tables()
        assert 'test_table' in tables

        # Cleanup
        connection.disconnect()

    def test_create_from_url_with_config_files(self, temp_sqlite_db, tmp_path):
        """Test create_from_url with additional config files."""
        url = f"sqlite:///{temp_sqlite_db}"

        connection, backend = DatabaseUtils.create_from_url(
            url,
            config_files=[]  # Empty list, just testing parameter works
        )

        assert connection is not None
        assert backend is not None

        # Cleanup
        connection.disconnect()
