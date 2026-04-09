"""Tests for OperationsFactory."""

import pytest
from mountainash_data.core.factories.operations_factory import OperationsFactory
from mountainash_data.backends.ibis.operations import BaseIbisOperations
from mountainash_data.core.settings import SQLiteAuthSettings, DuckDBAuthSettings
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings import SettingsParameters


@pytest.mark.unit
class TestOperationsFactoryGetStrategy:
    """Tests for OperationsFactory.get_strategy method."""

    def test_get_strategy_for_sqlite(self):
        """Test getting SQLite operations class from factory."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations_class = OperationsFactory.get_strategy(settings_params)

        assert operations_class is not None
        # Should be a subclass of BaseIbisOperations
        assert issubclass(operations_class, BaseIbisOperations)

    def test_get_strategy_for_duckdb(self):
        """Test getting DuckDB operations class from factory."""
        settings_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations_class = OperationsFactory.get_strategy(settings_params)

        assert operations_class is not None
        assert issubclass(operations_class, BaseIbisOperations)

    @pytest.mark.parametrize("settings_class", [
        SQLiteAuthSettings,
        DuckDBAuthSettings,
    ])
    def test_get_strategy_parametrized(self, settings_class):
        """Test strategy selection for various backend types."""
        settings_params = SettingsParameters.create(
            settings_class=settings_class,
            kwargs={"DATABASE": ":memory:"}
        )

        operations_class = OperationsFactory.get_strategy(settings_params)

        assert operations_class is not None
        assert issubclass(operations_class, BaseIbisOperations)


@pytest.mark.unit
class TestOperationsFactoryGetOperations:
    """Tests for OperationsFactory.get_operations convenience method."""

    def test_get_operations_returns_instance(self):
        """Test that get_operations returns an operations instance."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations = OperationsFactory.get_operations(settings_params)

        assert operations is not None
        assert isinstance(operations, BaseIbisOperations)

    def test_get_operations_for_different_backends(self):
        """Test getting operations for different backends."""
        # SQLite
        sqlite_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        sqlite_ops = OperationsFactory.get_operations(sqlite_params)
        assert isinstance(sqlite_ops, BaseIbisOperations)

        # DuckDB
        duckdb_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        duckdb_ops = OperationsFactory.get_operations(duckdb_params)
        assert isinstance(duckdb_ops, BaseIbisOperations)


@pytest.mark.unit
class TestOperationsFactoryConfiguration:
    """Tests for OperationsFactory configuration and mapping."""

    def test_factory_has_strategy_configuration(self):
        """Test that factory configures strategies."""
        factory = OperationsFactory()

        assert hasattr(OperationsFactory, '_strategy_modules')
        assert hasattr(OperationsFactory, '_strategy_classes')

    def test_strategy_modules_configured(self):
        """Test that strategy modules are properly configured."""
        factory = OperationsFactory()

        assert OperationsFactory._strategy_modules is not None
        assert CONST_DB_PROVIDER_TYPE.SQLITE in OperationsFactory._strategy_modules
        assert CONST_DB_PROVIDER_TYPE.DUCKDB in OperationsFactory._strategy_modules

    def test_strategy_classes_configured(self):
        """Test that strategy classes are properly configured."""
        factory = OperationsFactory()

        assert OperationsFactory._strategy_classes is not None
        assert CONST_DB_PROVIDER_TYPE.SQLITE in OperationsFactory._strategy_classes
        assert CONST_DB_PROVIDER_TYPE.DUCKDB in OperationsFactory._strategy_classes

    def test_sqlite_strategy_mapping(self):
        """Test SQLite operations strategy configuration.

        Updated in Phase 5 Task 5.2: module path now points directly at
        backends.ibis.operations (bypassing the databases.operations.ibis shim chain).
        """
        factory = OperationsFactory()

        module_path = OperationsFactory._strategy_modules.get(CONST_DB_PROVIDER_TYPE.SQLITE)
        class_name = OperationsFactory._strategy_classes.get(CONST_DB_PROVIDER_TYPE.SQLITE)

        assert "mountainash_data.backends.ibis.operations" in module_path
        assert "Operations" in class_name

    def test_duckdb_strategy_mapping(self):
        """Test DuckDB operations strategy configuration.

        Updated in Phase 5 Task 5.2: module path now points directly at
        backends.ibis.operations (bypassing the databases.operations.ibis shim chain).
        """
        factory = OperationsFactory()

        module_path = OperationsFactory._strategy_modules.get(CONST_DB_PROVIDER_TYPE.DUCKDB)
        class_name = OperationsFactory._strategy_classes.get(CONST_DB_PROVIDER_TYPE.DUCKDB)

        assert "mountainash_data.backends.ibis.operations" in module_path
        assert "Operations" in class_name


@pytest.mark.unit
class TestOperationsFactoryLazyLoading:
    """Tests for lazy loading behavior of OperationsFactory."""

    def test_factory_lazy_loads_operations_classes(self):
        """Test that operations classes are loaded lazily."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations_class = OperationsFactory.get_strategy(settings_params)

        assert operations_class is not None
        assert callable(operations_class)

    def test_multiple_calls_return_same_class(self):
        """Test that multiple calls for same backend return same class."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        class1 = OperationsFactory.get_strategy(settings_params)
        class2 = OperationsFactory.get_strategy(settings_params)

        assert class1 is class2


@pytest.mark.unit
class TestOperationsFactoryMatchesConnectionFactory:
    """Tests to ensure operations factory aligns with connection factory."""

    def test_operations_backend_matches_connection_backend(self):
        """Test that operations backend type matches connection backend type."""
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        operations = OperationsFactory.get_operations(settings_params)

        # Operations should have correct backend type
        assert hasattr(operations, 'db_backend_name')


@pytest.mark.integration
class TestOperationsFactoryIntegration:
    """Integration tests for OperationsFactory."""

    def test_operations_work_with_actual_backend(self, temp_sqlite_db):
        """Test that factory-created operations work with real backend."""
        from mountainash_data.core.utils import DatabaseUtils

        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        # Get operations from factory
        operations = OperationsFactory.get_operations(settings_params)

        # Get connected backend
        backend = DatabaseUtils.create_backend(settings_params)

        # Test operations with backend
        tables = operations.list_tables(backend)
        assert isinstance(tables, list)
        assert 'test_table' in tables

    def test_factory_creates_multiple_operations_instances(self):
        """Test factory can create multiple operations instances."""
        sqlite_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        duckdb_params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )

        ops1 = OperationsFactory.get_operations(sqlite_params)
        ops2 = OperationsFactory.get_operations(duckdb_params)
        ops3 = OperationsFactory.get_operations(sqlite_params)

        # All should be valid instances
        assert isinstance(ops1, BaseIbisOperations)
        assert isinstance(ops2, BaseIbisOperations)
        assert isinstance(ops3, BaseIbisOperations)

        # ops1 and ops3 should be different instances
        assert ops1 is not ops3
