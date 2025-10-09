"""Shared test fixtures and configuration for mountainash-data tests.

This module imports and consolidates fixtures from the fixtures package,
making them available to all tests.
"""

import pytest
from pathlib import Path

# Import all fixtures from consolidated fixture modules
from fixtures.database_fixtures import (
    temp_sqlite_db,
    temp_duckdb_db,
    ibis_sqlite_backend,
    ibis_duckdb_backend,
    ibis_polars_backend,
    sample_table_data,
    sample_table_schemas
)

from fixtures.settings_fixtures import (
    sqlite_settings_params,
    sqlite_memory_settings_params,
    duckdb_settings_params,
    duckdb_file_settings_params,
    backend_settings_class,
    backend_config,
    settings_factory_helper,
    connection_url_samples,
    url_backend_mapping
)

from fixtures.dataframe_fixtures import (
    sample_data_dict,
    sample_data_list,
    sample_polars_df,
    sample_pandas_df,
    sample_pyarrow_table,
    simple_polars_df,
    simple_pandas_df,
    column_mapping_config,
    dataframe_builder,
    dataframe_type,
    dataframe_samples
)


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Create and provide path to test data directory."""
    test_dir = Path(__file__).parent / "fixtures"
    test_dir.mkdir(exist_ok=True)
    return test_dir


# Configure pytest
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (slower, may require external dependencies)"
    )
    config.addinivalue_line(
        "markers", "performance: Performance and benchmark tests"
    )
    config.addinivalue_line(
        "markers", "slow: Slow-running tests"
    )
    config.addinivalue_line(
        "markers", "requires_postgres: Tests requiring PostgreSQL"
    )
    config.addinivalue_line(
        "markers", "requires_duckdb: Tests requiring DuckDB"
    )
    config.addinivalue_line(
        "markers", "requires_snowflake: Tests requiring Snowflake"
    )
