"""Settings fixtures for all backend types."""

import pytest
from mountainash_data.core.settings import (
    SQLiteAuthSettings,
    DuckDBAuthSettings,
    PostgreSQLAuthSettings,
    BaseDBAuthSettings
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE
from mountainash_settings import SettingsParameters


@pytest.fixture
def sqlite_settings_params(temp_sqlite_db):
    """Create SQLite settings parameters for testing."""
    return SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        kwargs={"DATABASE": str(temp_sqlite_db)}
    )


@pytest.fixture
def sqlite_memory_settings_params():
    """Create SQLite in-memory settings parameters for testing."""
    return SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        kwargs={"DATABASE": ":memory:"}
    )


@pytest.fixture
def duckdb_settings_params():
    """Create DuckDB settings parameters for testing."""
    return SettingsParameters.create(
        settings_class=DuckDBAuthSettings,
        kwargs={"DATABASE": ":memory:"}
    )


@pytest.fixture
def duckdb_file_settings_params(temp_duckdb_db):
    """Create DuckDB file-based settings parameters for testing."""
    return SettingsParameters.create(
        settings_class=DuckDBAuthSettings,
        kwargs={"DATABASE": str(temp_duckdb_db)}
    )


@pytest.fixture(params=[
    SQLiteAuthSettings,
    DuckDBAuthSettings,
])
def backend_settings_class(request):
    """Parametrized fixture providing all available backend settings classes."""
    return request.param


@pytest.fixture(params=[
    (CONST_DB_PROVIDER_TYPE.SQLITE, SQLiteAuthSettings, ":memory:"),
    (CONST_DB_PROVIDER_TYPE.DUCKDB, DuckDBAuthSettings, ":memory:"),
])
def backend_config(request):
    """Parametrized fixture providing backend configuration tuples.

    Returns:
        tuple: (backend_type, settings_class, database_path)
    """
    return request.param


@pytest.fixture
def settings_factory_helper():
    """Helper fixture for creating settings with various configurations."""
    def _create_settings(backend_type, **kwargs):
        """Create settings for a given backend type."""
        settings_map = {
            CONST_DB_PROVIDER_TYPE.SQLITE: SQLiteAuthSettings,
            CONST_DB_PROVIDER_TYPE.DUCKDB: DuckDBAuthSettings,
        }

        settings_class = settings_map.get(backend_type)
        if not settings_class:
            raise ValueError(f"Unsupported backend type: {backend_type}")

        # Set default DATABASE if not provided
        if "DATABASE" not in kwargs:
            kwargs["DATABASE"] = ":memory:"

        return SettingsParameters.create(
            settings_class=settings_class,
            kwargs=kwargs
        )

    return _create_settings


@pytest.fixture
def connection_url_samples():
    """Provide sample connection URLs for various backends."""
    return {
        "sqlite_memory": "sqlite:///:memory:",
        "sqlite_file": "sqlite:///test.db",
        "duckdb_memory": "duckdb:///:memory:",
        "duckdb_file": "duckdb:///test.duckdb",
        "postgresql_local": "postgresql://localhost/testdb",
        "postgresql_full": "postgresql://user:pass@localhost:5432/testdb",
    }


@pytest.fixture(params=[
    ("sqlite:///:memory:", CONST_DB_PROVIDER_TYPE.SQLITE),
    ("duckdb:///:memory:", CONST_DB_PROVIDER_TYPE.DUCKDB),
    ("postgresql://localhost/db", CONST_DB_PROVIDER_TYPE.POSTGRESQL),
])
def url_backend_mapping(request):
    """Parametrized fixture mapping URLs to expected backend types.

    Returns:
        tuple: (connection_url, expected_backend_type)
    """
    return request.param
