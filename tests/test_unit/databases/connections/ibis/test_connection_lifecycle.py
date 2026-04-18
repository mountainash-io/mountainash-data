"""Parametrized tests for Ibis connection lifecycle across backends."""

import pytest
from mountainash_data.backends.ibis.connection import (
    SQLite_IbisConnection,
    DuckDB_IbisConnection,
)
from mountainash_data.core.connection import BaseDBConnection
from mountainash_data.core.settings import (
    SQLiteAuthSettings,
    DuckDBAuthSettings,
    NoAuth,
)
from mountainash_settings import SettingsParameters


@pytest.mark.unit
@pytest.mark.parametrize("connection_class,settings_class,db_config", [
    (SQLite_IbisConnection, SQLiteAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
    (DuckDB_IbisConnection, DuckDBAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
])
class TestConnectionLifecycle:
    """Test connection lifecycle for all Ibis backends."""

    def test_connection_instantiation(self, connection_class, settings_class, db_config):
        """Test that connection can be instantiated."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)

        assert connection is not None
        assert isinstance(connection, BaseDBConnection)
        assert isinstance(connection, connection_class)

    def test_connection_not_connected_initially(self, connection_class, settings_class, db_config):
        """Test that connection is not connected initially."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)

        # Initially should not be connected
        assert not connection.is_connected()

    def test_connect_method(self, connection_class, settings_class, db_config):
        """Test that connect method works."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)
        backend = connection.connect()

        assert backend is not None
        assert connection.is_connected()

        # Cleanup
        connection.disconnect()

    def test_disconnect_method(self, connection_class, settings_class, db_config):
        """Test that disconnect method works."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)
        connection.connect()
        connection.disconnect()

        assert not connection.is_connected()

    def test_reconnect_after_disconnect(self, connection_class, settings_class, db_config):
        """Test that connection can reconnect after disconnect."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)

        # First connection
        backend1 = connection.connect()
        assert connection.is_connected()

        # Disconnect
        connection.disconnect()
        assert not connection.is_connected()

        # Reconnect
        backend2 = connection.connect()
        assert connection.is_connected()
        assert backend2 is not None

        # Cleanup
        connection.disconnect()

    def test_multiple_connect_calls(self, connection_class, settings_class, db_config):
        """Test that multiple connect calls don't cause errors."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)

        # Connect multiple times
        backend1 = connection.connect()
        backend2 = connection.connect()

        assert backend1 is not None
        assert backend2 is not None
        assert connection.is_connected()

        # Cleanup
        connection.disconnect()


@pytest.mark.integration
@pytest.mark.parametrize("connection_class,settings_class,db_config", [
    (SQLite_IbisConnection, SQLiteAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
    (DuckDB_IbisConnection, DuckDBAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
])
class TestConnectionFunctionality:
    """Test actual backend functionality for all Ibis backends."""

    def test_backend_can_create_table(self, connection_class, settings_class, db_config):
        """Test that connected backend can create tables."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)
        backend = connection.connect()

        # Create a simple table
        backend.create_table("test_table", {"id": [1, 2, 3], "value": [10, 20, 30]}, overwrite=True)

        # Verify table exists
        tables = backend.list_tables()
        assert "test_table" in tables

        # Cleanup
        connection.disconnect()

    def test_backend_can_list_tables(self, connection_class, settings_class, db_config):
        """Test that connected backend can list tables."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)
        backend = connection.connect()

        # Create some tables
        backend.create_table("table1", {"id": [1]}, overwrite=True)
        backend.create_table("table2", {"id": [2]}, overwrite=True)

        # List tables
        tables = backend.list_tables()

        assert isinstance(tables, list)
        assert "table1" in tables
        assert "table2" in tables

        # Cleanup
        connection.disconnect()

    def test_backend_can_query_table(self, connection_class, settings_class, db_config):
        """Test that connected backend can query tables."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = connection_class(db_auth_settings_parameters=settings_params)
        backend = connection.connect()

        # Create table with data
        backend.create_table("query_test", {"id": [1, 2, 3], "value": [10, 20, 30]}, overwrite=True)

        # Query table
        table = backend.table("query_test")
        assert table is not None

        # Cleanup
        connection.disconnect()


@pytest.mark.unit
@pytest.mark.parametrize("settings_class,db_config", [
    (SQLiteAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
    (DuckDBAuthSettings, {"DATABASE": ":memory:", "auth": NoAuth()}),
])
class TestConnectionWithFactory:
    """Test connections work with factory pattern."""

    def test_factory_creates_correct_connection(self, settings_class, db_config):
        """Test that factory creates correct connection type."""
        from mountainash_data.core.factories import ConnectionFactory

        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = ConnectionFactory.get_connection(settings_params)

        assert connection is not None
        assert isinstance(connection, BaseDBConnection)

    def test_factory_connection_can_connect(self, settings_class, db_config):
        """Test that factory-created connection can connect."""
        from mountainash_data.core.factories import ConnectionFactory

        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs=db_config
        )

        connection = ConnectionFactory.get_connection(settings_params)
        backend = connection.connect()

        assert backend is not None
        assert connection.is_connected()

        # Cleanup
        connection.disconnect()
