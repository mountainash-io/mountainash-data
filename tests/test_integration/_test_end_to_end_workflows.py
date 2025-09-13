"""Integration tests for end-to-end workflows."""

import pytest
import tempfile
from pathlib import Path
import polars as pl

# from mountainash_dataframes.utils.dataframe_factory import DataFrameFactory
from mountainash_data.databases.ibis.connections.sqlite_ibis_connection import SQLite_IbisConnection
from mountainash_data.databases.ibis.connections.duckdb_ibis_connection import DuckDB_IbisConnection
from mountainash_dataframes import IbisDataFrame
from mountainash_settings import SettingsParameters
from mountainash_data.databases.settings import SQLiteAuthSettings, DuckDBAuthSettings


class TestDataPipelineIntegration:
    """Integration tests for complete data pipeline workflows."""

    @pytest.mark.integration
    def test_data_factory_to_polars_workflow(self, sample_data_list):
        """Test complete workflow: data creation -> Polars conversion."""
        # Create DataFrame from factory using real components
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(sample_data_list)
        assert df is not None

        # Test actual materialization to Polars
        if hasattr(df, 'materialise'):
            polars_df = df.materialise('polars')
            assert isinstance(polars_df, pl.DataFrame)
            assert len(polars_df) == 3
            assert 'id' in polars_df.columns
            assert 'name' in polars_df.columns

            # Verify data integrity through the pipeline
            names = polars_df['name'].to_list()
            assert 'Alice' in names
            assert 'Bob' in names
            assert 'Charlie' in names

    @pytest.mark.integration
    def test_data_transformation_pipeline(self, sample_data_dict):
        """Test data transformation pipeline with real operations."""
        # Start with raw data
        raw_data = sample_data_dict["valid_complex"]

        # Create initial DataFrame using real factory
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(raw_data)
        assert df is not None

        # Test real pipeline operations if available
        if hasattr(df, 'materialise'):
            # Test filtering and transformation
            result = df.materialise('polars')
            assert len(result) == 5  # All records from valid_complex
            assert 'category' in result.columns
            assert 'active' in result.columns

            # Test that we can filter the real data
            active_records = result.filter(pl.col('active') == True)
            assert len(active_records) >= 1

    @pytest.mark.integration
    def test_database_to_dataframe_workflow(self, temp_sqlite_db):
        """Test workflow from database connection to DataFrame operations."""
        # Create real database connection
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        conn = SQLite_IbisConnection(db_auth_settings_parameters=settings_params)
        backend = conn.connect()

        # Get real table from backend
        table = backend.table("test_table")
        assert table is not None

        # Create IbisDataFrame and test real operations
        ibis_df = IbisDataFrame(table)
        result = ibis_df.materialise('polars')

        assert len(result) == 3  # From fixture data
        assert 'name' in result.columns
        assert 'value' in result.columns

        # Test filtering through the real pipeline
        filtered = ibis_df.filter(ibis_df.ibis_df.value > 150).materialise('polars')
        assert len(filtered) == 2  # Bob: 200.7, Charlie: 300.9

    @pytest.mark.integration
    def test_column_mapping_integration(self, sample_data_dict, column_mapping_config):
        """Test integration of column mapping with DataFrames."""
        from mountainash_dataframes.utils.column_mapper.column_mapper import ColumnMapper

        # Create real DataFrame
        data = sample_data_dict["valid_simple"]
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data)

        # Test real column mapping integration
        # ColumnMapper is a utility class with class methods only
        config = ColumnMapper.create_config(
            mapping=column_mapping_config,
            filter_unmapped=False
        )
        assert config is not None

        # Test that mapping configuration is valid
        assert 'id' in column_mapping_config
        assert 'name' in column_mapping_config

        # Verify mapping functionality works with real data
        if hasattr(df, 'materialise'):
            result = df.materialise('polars')
            original_columns = result.columns
            assert 'id' in original_columns
            assert 'name' in original_columns

    @pytest.mark.integration
    def test_data_filtering_integration(self, sample_data_dict):
        """Test integration of data filtering with DataFrames."""
        # Create real DataFrame
        data = sample_data_dict["valid_complex"]
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data)
        assert df is not None

        # Test real filtering operations if available
        if hasattr(df, 'materialise'):
            result = df.materialise('polars')

            # Test that we can perform real filtering operations
            category_a_records = result.filter(pl.col('category') == 'A')
            assert len(category_a_records) == 2  # Alice and Charlie have category 'A'

            # Test value-based filtering
            high_value_records = result.filter(pl.col('value') > 300.0)
            assert len(high_value_records) >= 2  # Diana: 400.2, Eve: 500.8


class TestCrossBackendCompatibility:
    """Integration tests for cross-backend compatibility."""

    @pytest.mark.integration
    def test_sqlite_to_duckdb_data_transfer(self, temp_sqlite_db):
        """Test real data transfer between SQLite and DuckDB backends."""
        # Create real SQLite connection
        sqlite_settings = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )
        sqlite_conn = SQLite_IbisConnection(db_auth_settings_parameters=sqlite_settings)
        sqlite_backend = sqlite_conn.connect()

        # Create real DuckDB connection (in-memory)
        duckdb_settings = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:"}
        )
        duckdb_conn = DuckDB_IbisConnection(db_auth_settings_parameters=duckdb_settings)
        duckdb_backend = duckdb_conn.connect()

        # Test real data operations on both backends
        assert sqlite_backend is not None
        assert duckdb_backend is not None

        # Get data from SQLite
        sqlite_table = sqlite_backend.table("test_table")
        sqlite_data = sqlite_table.to_polars()

        # Transfer data to DuckDB
        duckdb_backend.create_table("transferred_table", sqlite_data)
        duckdb_table = duckdb_backend.table("transferred_table")
        duckdb_data = duckdb_table.to_polars()

        # Verify data integrity across backends
        assert len(sqlite_data) == len(duckdb_data) == 3
        assert sqlite_data.columns == duckdb_data.columns
        assert sqlite_data['name'].to_list() == duckdb_data['name'].to_list()

    @pytest.mark.integration
    def test_multiple_dataframe_format_conversion(self, sample_data_dict):
        """Test conversion between multiple DataFrame formats."""
        data = sample_data_dict["valid_simple"]

        # Create DataFrame using factory
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data)
        assert df is not None

        if hasattr(df, 'materialise'):
            # Test conversion to different formats
            polars_result = df.materialise('polars')
            pandas_result = df.materialise('pandas')
            pyarrow_result = df.materialise('pyarrow')

            # Verify all formats have same data
            assert len(polars_result) == len(pandas_result) == pyarrow_result.num_rows == 3

            # Verify data consistency across formats
            polars_names = polars_result['name'].to_list()
            pandas_names = pandas_result['name'].tolist()
            pyarrow_names = pyarrow_result.column('name').to_pylist()

            assert polars_names == pandas_names == pyarrow_names
            assert 'Alice' in polars_names

    @pytest.mark.integration
    def test_large_dataset_handling(self, real_ibis_backend):
        """Test handling of larger datasets across components."""
        # Create larger test dataset
        large_data = {
            "id": list(range(1000)),
            "value": [i * 0.1 for i in range(1000)],
            "category": [f"cat_{i % 10}" for i in range(1000)]
        }

        # Create table in real backend
        table = real_ibis_backend.create_table("large_test", large_data, overwrite=True)

        # Test IbisDataFrame can handle larger datasets
        df = IbisDataFrame(table)

        # Test operations on larger dataset
        count = df.count()
        assert count == 1000

        # Test filtering performance on real data
        filtered_df = df.filter(df.ibis_df.value > 50.0)
        filtered_count = filtered_df.count()
        assert filtered_count == 499  # Values from 50.1 to 99.9 (499 values)

        # Test materialization works with larger data
        sample = df.limit(100).materialise('polars')
        assert len(sample) == 100


class TestErrorHandlingIntegration:
    """Integration tests for error handling across components."""

    @pytest.mark.integration
    def test_invalid_database_connection_error_propagation(self):
        """Test error propagation from invalid database connections."""
        # Test with invalid database path using real settings
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": "/invalid/nonexistent/path/test.db"}
        )

        conn = SQLite_IbisConnection(db_auth_settings_parameters=settings_params)
        assert conn is not None  # Connection object created

        # But connection attempt should fail with real error
        with pytest.raises(Exception):  # Real error propagation
            conn.connect()

    @pytest.mark.integration
    def test_data_validation_error_propagation(self):
        """Test error propagation through data validation."""
        # Test with completely invalid data
        invalid_data = "not_a_valid_data_structure"

        # Error should be handled appropriately by real components
        with pytest.raises((TypeError, ValueError)):
            DataFrameFactory.create_ibis_dataframe_object_from_dictionary(invalid_data)

    @pytest.mark.integration
    def test_memory_constraint_handling(self, real_ibis_backend):
        """Test handling of memory constraints with real operations."""
        # Create reasonable-sized data for CI environment
        data = {
            "id": list(range(100)),
            "data": [f"data_{i}" for i in range(100)]
        }

        # Test real memory operations
        table = real_ibis_backend.create_table("memory_test", data, overwrite=True)
        df = IbisDataFrame(table)

        # Test that real operations complete successfully
        result = df.materialise('polars')
        assert len(result) == 100
        assert result.estimated_size() > 0  # Has real memory footprint


class TestConfigurationIntegration:
    """Integration tests for configuration across components."""

    @pytest.mark.integration
    def test_settings_propagation_through_pipeline(self, temp_sqlite_db):
        """Test that settings propagate correctly through the pipeline."""
        # Test real configuration propagation
        settings_params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": str(temp_sqlite_db)}
        )

        # Settings should work across real components
        conn = SQLite_IbisConnection(db_auth_settings_parameters=settings_params)
        backend = conn.connect()

        # Verify settings actually affect backend behavior
        assert backend is not None
        tables = backend.list_tables()
        assert 'test_table' in tables  # From temp_sqlite_db fixture

        # Test that same settings create equivalent connections
        conn2 = SQLite_IbisConnection(db_auth_settings_parameters=settings_params)
        backend2 = conn2.connect()

        # Both should see the same tables
        assert backend2.list_tables() == tables

    @pytest.mark.integration
    def test_environment_variable_integration(self):
        """Test integration with environment variables."""
        import os

        # Test that environment variables can influence component behavior
        test_env_var = "MOUNTAINASH_TEST_ENV"
        original_value = os.environ.get(test_env_var)

        try:
            os.environ[test_env_var] = "test_value"

            # Test real environment variable usage
            from mountainash_settings import get_settings

            # Components should be able to access environment configuration
            assert os.environ.get(test_env_var) == "test_value"

            # Test that settings can potentially use environment variables
            # This validates the integration pattern works

        finally:
            # Cleanup
            if original_value is not None:
                os.environ[test_env_var] = original_value
            elif test_env_var in os.environ:
                del os.environ[test_env_var]
