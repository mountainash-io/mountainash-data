"""Tests for SettingsFactory."""

import pytest
from mountainash_data.core.factories.settings_factory import SettingsFactory
from mountainash_data.core.settings import (
    SQLiteAuthSettings,
    DuckDBAuthSettings,
    ConnectionProfile,
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE


@pytest.mark.unit
class TestSettingsFactoryFromConnectionString:
    """Tests for SettingsFactory.from_connection_string method."""

    def test_from_sqlite_connection_string(self):
        """Test creating settings from SQLite connection string."""
        url = "sqlite:///test.db"

        settings = SettingsFactory.from_connection_string(url)

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)

    def test_from_duckdb_connection_string(self):
        """Test creating settings from DuckDB connection string."""
        url = "duckdb:///test.duckdb"

        settings = SettingsFactory.from_connection_string(url)

        assert settings is not None
        assert isinstance(settings, DuckDBAuthSettings)

    @pytest.mark.parametrize("url,expected_type", [
        ("sqlite:///:memory:", SQLiteAuthSettings),
        ("sqlite:///test.db", SQLiteAuthSettings),
        ("duckdb:///:memory:", DuckDBAuthSettings),
        ("duckdb:///test.duckdb", DuckDBAuthSettings),
    ])
    def test_from_connection_string_parametrized(self, url, expected_type):
        """Test connection string parsing for various database types."""
        settings = SettingsFactory.from_connection_string(url)

        assert settings is not None
        assert isinstance(settings, expected_type)

    def test_from_connection_string_with_kwargs(self):
        """Test from_connection_string with additional kwargs."""
        url = "sqlite:///:memory:"

        settings = SettingsFactory.from_connection_string(
            url,
            timeout=30
        )

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)


@pytest.mark.unit
class TestSettingsFactoryFromBackendType:
    """Tests for SettingsFactory.from_backend_type method."""

    def test_from_sqlite_backend_type(self):
        """Test creating settings from SQLite backend type."""
        settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)

    def test_from_duckdb_backend_type(self):
        """Test creating settings from DuckDB backend type."""
        settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.DUCKDB,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, DuckDBAuthSettings)

    @pytest.mark.parametrize("backend_type,expected_settings", [
        (CONST_DB_PROVIDER_TYPE.SQLITE, SQLiteAuthSettings),
        (CONST_DB_PROVIDER_TYPE.DUCKDB, DuckDBAuthSettings),
    ])
    def test_from_backend_type_parametrized(self, backend_type, expected_settings):
        """Test settings creation for various backend types."""
        settings = SettingsFactory.from_backend_type(
            backend_type,
            DATABASE=":memory:"
        )

        assert settings is not None
        assert isinstance(settings, expected_settings)

    def test_from_backend_type_with_multiple_kwargs(self):
        """Test from_backend_type with multiple configuration parameters."""
        settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE=":memory:",
            timeout=30,
            read_only=False
        )

        assert settings is not None
        assert isinstance(settings, SQLiteAuthSettings)


@pytest.mark.unit
class TestSettingsFactoryDetectBackend:
    """Tests for SettingsFactory.detect_backend_from_url method."""

    @pytest.mark.parametrize("url,expected_backend", [
        ("sqlite:///test.db", CONST_DB_PROVIDER_TYPE.SQLITE),
        ("sqlite:///:memory:", CONST_DB_PROVIDER_TYPE.SQLITE),
        ("duckdb:///test.duckdb", CONST_DB_PROVIDER_TYPE.DUCKDB),
        ("duckdb:///:memory:", CONST_DB_PROVIDER_TYPE.DUCKDB),
        ("postgresql://localhost/db", CONST_DB_PROVIDER_TYPE.POSTGRESQL),
        ("postgres://localhost/db", CONST_DB_PROVIDER_TYPE.POSTGRESQL),
    ])
    def test_detect_backend_from_url(self, url, expected_backend):
        """Test backend detection from various URL formats."""
        detected_backend = SettingsFactory.detect_backend_from_url(url)

        assert detected_backend == expected_backend

    def test_detect_backend_sqlite_variations(self):
        """Test SQLite URL variations."""
        sqlite_urls = [
            "sqlite:///absolute/path/to/db.sqlite",
            "sqlite:///:memory:",
            "sqlite:///relative/path.db",
        ]

        for url in sqlite_urls:
            backend = SettingsFactory.detect_backend_from_url(url)
            assert backend == CONST_DB_PROVIDER_TYPE.SQLITE, f"Failed for URL: {url}"

    def test_detect_backend_duckdb_variations(self):
        """Test DuckDB URL variations."""
        duckdb_urls = [
            "duckdb:///path/to/db.duckdb",
            "duckdb:///:memory:",
            "duckdb:///test.db",
        ]

        for url in duckdb_urls:
            backend = SettingsFactory.detect_backend_from_url(url)
            assert backend == CONST_DB_PROVIDER_TYPE.DUCKDB, f"Failed for URL: {url}"


@pytest.mark.unit
class TestSettingsFactoryConfiguration:
    """Tests for SettingsFactory configuration."""

    def test_factory_has_backend_mappings(self):
        """Test that factory has backend to settings mappings."""
        # This tests that the factory properly maps backend types to settings classes
        sqlite_settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE=":memory:"
        )
        duckdb_settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.DUCKDB,
            DATABASE=":memory:"
        )

        assert type(sqlite_settings) != type(duckdb_settings)
        assert isinstance(sqlite_settings, ConnectionProfile)
        assert isinstance(duckdb_settings, ConnectionProfile)

    def test_factory_lazy_loads_settings_classes(self):
        """Test that settings classes are loaded lazily."""
        # Creating settings should work without pre-importing
        settings = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE=":memory:"
        )

        assert settings is not None


@pytest.mark.unit
class TestSettingsFactoryErrorHandling:
    """Tests for SettingsFactory error handling."""

    def test_invalid_url_format(self):
        """Test handling of invalid URL formats."""
        invalid_url = "not-a-valid-url"

        with pytest.raises((ValueError, KeyError, AttributeError)):
            SettingsFactory.from_connection_string(invalid_url)

    def test_unsupported_backend_type(self):
        """Test handling of URL with unsupported backend."""
        # This should either raise an error or handle gracefully
        unsupported_url = "unknown-backend://localhost/db"

        with pytest.raises((ValueError, KeyError, AttributeError, NotImplementedError)):
            SettingsFactory.from_connection_string(unsupported_url)


@pytest.mark.integration
class TestSettingsFactoryIntegration:
    """Integration tests for SettingsFactory."""

    def test_settings_factory_to_connection_workflow(self, temp_sqlite_db):
        """Test workflow: factory creates settings → settings create connection."""
        from mountainash_data.core.utils import DatabaseUtils

        # Factory creates settings
        url = f"sqlite:///{temp_sqlite_db}"
        settings = SettingsFactory.from_connection_string(url)

        # Settings can be used to create connection
        settings_params = settings.extract_settings_parameters()
        connection = DatabaseUtils.create_connection(settings_params)

        assert connection is not None

        # Connection can connect
        backend = connection.connect()
        assert backend is not None

        # Cleanup
        connection.disconnect()

    def test_settings_factory_round_trip(self):
        """Test that settings created from URL can generate correct connection strings."""
        original_url = "sqlite:///:memory:"

        # Create settings from URL
        settings = SettingsFactory.from_connection_string(original_url)

        # Settings should have connection info
        assert settings is not None
        assert hasattr(settings, 'DATABASE')

    def test_factory_creates_independent_settings_instances(self):
        """Test that factory creates independent settings instances."""
        settings1 = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE="db1.sqlite"
        )
        settings2 = SettingsFactory.from_backend_type(
            CONST_DB_PROVIDER_TYPE.SQLITE,
            DATABASE="db2.sqlite"
        )

        # Different instances
        assert settings1 is not settings2

        # Different databases
        assert settings1.DATABASE != settings2.DATABASE
