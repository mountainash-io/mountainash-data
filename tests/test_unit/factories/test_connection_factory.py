"""Tests for ConnectionFactory."""

import pytest
from mountainash_data.factories.connection_factory import ConnectionFactory
from mountainash_data.databases import BaseDBConnection, SQLite_IbisConnection, DuckDB_IbisConnection
from mountainash_data.databases.settings import SQLiteAuthSettings, DuckDBAuthSettings
from mountainash_data.databases.constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings import SettingsParameters


@pytest.mark.unit
class TestConnectionFactoryGetStrategy:
    """Tests for ConnectionFactory.get_strategy method."""

    def test_get_strategy_for_sqlite(self):
        """Test getting SQLite connection class from factory."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        connection_class = ConnectionFactory.get_strategy(settings_params)

        assert connection_class is not None
        assert connection_class == SQLite_IbisConnection

    def test_get_strategy_for_duckdb(self):
        """Test getting DuckDB connection class from factory."""
        settings_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        connection_class = ConnectionFactory.get_strategy(settings_params)

        assert connection_class is not None
        assert connection_class == DuckDB_IbisConnection

    @pytest.mark.parametrize("settings_class,expected_connection", [
        (SQLiteAuthSettings, SQLite_IbisConnection),
        (DuckDBAuthSettings, DuckDB_IbisConnection),
    ])
    def test_get_strategy_parametrized(self, settings_class, expected_connection):
        """Test strategy selection for various backend types."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs={"DATABASE": ":memory:"}
        )

        connection_class = ConnectionFactory.get_strategy(settings_params)

        assert connection_class == expected_connection


@pytest.mark.unit
class TestConnectionFactoryGetConnection:
    """Tests for ConnectionFactory.get_connection convenience method."""

    def test_get_connection_returns_instance(self):
        """Test that get_connection returns a connection instance."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        connection = ConnectionFactory.get_connection(settings_params)

        assert connection is not None
        assert isinstance(connection, BaseDBConnection)
        assert isinstance(connection, SQLite_IbisConnection)

    def test_get_connection_instance_can_connect(self, temp_sqlite_db):
        """Test that returned connection instance can actually connect."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        connection = ConnectionFactory.get_connection(settings_params)
        backend = connection.connect()

        assert backend is not None
        assert connection.is_connected()

        # Cleanup
        connection.disconnect()

    def test_get_connection_with_different_backends(self):
        """Test getting connections for different backends."""
        # SQLite
        sqlite_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        sqlite_conn = ConnectionFactory.get_connection(sqlite_params)
        assert isinstance(sqlite_conn, SQLite_IbisConnection)

        # DuckDB
        duckdb_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        duckdb_conn = ConnectionFactory.get_connection(duckdb_params)
        assert isinstance(duckdb_conn, DuckDB_IbisConnection)


@pytest.mark.unit
class TestConnectionFactoryConfiguration:
    """Tests for ConnectionFactory configuration and mapping."""

    def test_factory_has_strategy_configuration(self):
        """Test that factory configures strategies."""
        # Trigger configuration by accessing class methods
        factory = ConnectionFactory()

        # Should have strategy mappings configured
        assert hasattr(ConnectionFactory, '_strategy_modules')
        assert hasattr(ConnectionFactory, '_strategy_classes')

    def test_strategy_modules_configured(self):
        """Test that strategy modules are properly configured."""
        # Access the configuration
        factory = ConnectionFactory()

        # Should have mappings for key backends
        assert ConnectionFactory._strategy_modules is not None
        assert CONST_DB_PROVIDER_TYPE.SQLITE in ConnectionFactory._strategy_modules
        assert CONST_DB_PROVIDER_TYPE.DUCKDB in ConnectionFactory._strategy_modules

    def test_strategy_classes_configured(self):
        """Test that strategy classes are properly configured."""
        factory = ConnectionFactory()

        assert ConnectionFactory._strategy_classes is not None
        assert CONST_DB_PROVIDER_TYPE.SQLITE in ConnectionFactory._strategy_classes
        assert CONST_DB_PROVIDER_TYPE.DUCKDB in ConnectionFactory._strategy_classes

    def test_sqlite_strategy_mapping(self):
        """Test SQLite strategy configuration."""
        factory = ConnectionFactory()

        module_path = ConnectionFactory._strategy_modules.get(CONST_DB_PROVIDER_TYPE.SQLITE)
        class_name = ConnectionFactory._strategy_classes.get(CONST_DB_PROVIDER_TYPE.SQLITE)

        assert module_path == "mountainash_data.databases.connections.ibis"
        assert class_name == "SQLite_IbisConnection"

    def test_duckdb_strategy_mapping(self):
        """Test DuckDB strategy configuration."""
        factory = ConnectionFactory()

        module_path = ConnectionFactory._strategy_modules.get(CONST_DB_PROVIDER_TYPE.DUCKDB)
        class_name = ConnectionFactory._strategy_classes.get(CONST_DB_PROVIDER_TYPE.DUCKDB)

        assert module_path == "mountainash_data.databases.connections.ibis"
        assert class_name == "DuckDB_IbisConnection"


@pytest.mark.unit
class TestConnectionFactoryLazyLoading:
    """Tests for lazy loading behavior of ConnectionFactory."""

    def test_factory_lazy_loads_connection_classes(self):
        """Test that connection classes are loaded lazily."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        # Getting strategy should work without pre-importing
        connection_class = ConnectionFactory.get_strategy(settings_params)

        assert connection_class is not None
        assert callable(connection_class)

    def test_multiple_calls_return_same_class(self):
        """Test that multiple calls for same backend return same class."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        class1 = ConnectionFactory.get_strategy(settings_params)
        class2 = ConnectionFactory.get_strategy(settings_params)

        assert class1 is class2  # Same class object


@pytest.mark.integration
class TestConnectionFactoryIntegration:
    """Integration tests for ConnectionFactory."""

    def test_factory_to_connection_to_backend_workflow(self, temp_sqlite_db):
        """Test complete workflow: factory → connection → backend."""
        # Create settings
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        # Get connection from factory
        connection = ConnectionFactory.get_connection(settings_params)

        # Connect to backend
        backend = connection.connect()

        # Verify backend is functional
        assert backend is not None
        tables = backend.list_tables()
        assert isinstance(tables, list)
        assert 'test_table' in tables

        # Cleanup
        connection.disconnect()

    def test_factory_works_with_multiple_concurrent_connections(self):
        """Test factory can create multiple connections concurrently."""
        sqlite_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        duckdb_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        conn1 = ConnectionFactory.get_connection(sqlite_params)
        conn2 = ConnectionFactory.get_connection(duckdb_params)
        conn3 = ConnectionFactory.get_connection(sqlite_params)

        # All should be valid instances
        assert isinstance(conn1, SQLite_IbisConnection)
        assert isinstance(conn2, DuckDB_IbisConnection)
        assert isinstance(conn3, SQLite_IbisConnection)

        # But conn1 and conn3 should be different instances
        assert conn1 is not conn3
