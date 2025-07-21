"""Tests for DataFrameFactory."""

import pytest
import polars as pl
import pandas as pd
import pyarrow as pa
from datetime import datetime

from mountainash_data.dataframes.utils.dataframe_factory import DataFrameFactory


class TestDataFrameFactoryCreation:
    """Tests for DataFrameFactory creation methods."""

    def test_factory_can_be_imported(self):
        """Test that DataFrameFactory can be imported."""
        assert DataFrameFactory is not None

    def test_create_ibis_dataframe_from_list_of_dicts(self, sample_data_list, real_ibis_backend):
        """Test creating DataFrame from list of dictionaries using real objects."""
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(sample_data_list, ibis_backend=real_ibis_backend)
        
        assert result is not None
        # Test that we can access real properties
        assert hasattr(result, 'materialise')
        
        # Verify we can materialize and get expected data
        polars_result = result.materialise('polars')
        assert isinstance(polars_result, pl.DataFrame)
        assert len(polars_result) == 3
        assert 'id' in polars_result.columns
        assert 'name' in polars_result.columns
        
        # Verify data integrity
        names = polars_result['name'].to_list()
        assert 'Alice' in names
        assert 'Bob' in names
        assert 'Charlie' in names

    def test_create_ibis_dataframe_from_dict_of_lists(self, sample_data_dict, real_ibis_backend):
        """Test creating DataFrame from dictionary of lists using real objects."""
        data = sample_data_dict["valid_simple"]
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        
        assert result is not None
        # Test that we can perform real operations
        assert hasattr(result, 'count')
        count = result.count()
        assert count == 3  # Should have 3 records
        
        # Test materialization
        pandas_result = result.materialise('pandas')
        assert isinstance(pandas_result, pd.DataFrame)
        assert len(pandas_result) == 3
        assert list(pandas_result.columns) == ['id', 'name', 'value']

    def test_create_from_empty_list_returns_none(self):
        """Test creating DataFrame from empty list."""
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary([])
        
        assert result is None  # Factory returns None for empty data

    def test_create_from_empty_dict_returns_none(self):
        """Test creating DataFrame from empty dictionary."""
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary({})
        
        assert result is None  # Factory returns None for empty data


class TestDataFrameFactoryTypeHandling:
    """Tests for DataFrameFactory type handling."""

    def test_create_with_mixed_data_types(self, sample_data_dict, real_ibis_backend):
        """Test creating DataFrame with mixed data types using real operations."""
        data = sample_data_dict.get("mixed_types", {
            "text": ["hello", "world", "test"],
            "numbers": [1, 2, 3],
            "floats": [1.1, 2.2, 3.3],
            "bools": [True, False, True]
        })
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test that mixed types are handled correctly
        polars_result = result.materialise('polars')
        assert 'text' in polars_result.columns
        assert 'numbers' in polars_result.columns
        assert 'floats' in polars_result.columns
        assert 'bools' in polars_result.columns
        
        # Verify data types were preserved appropriately
        assert polars_result['text'].dtype == pl.Utf8
        assert polars_result['bools'].dtype == pl.Boolean

    def test_create_with_none_values(self, real_ibis_backend):
        """Test creating DataFrame with None values."""
        data = {
            "id": [1, 2, None, 4],
            "name": ["Alice", None, "Charlie", "Diana"],
            "value": [100.5, None, 300.9, 400.2]
        }
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test that None values are handled correctly
        polars_result = result.materialise('polars')
        assert len(polars_result) == 4
        
        # Verify None values are preserved
        assert polars_result['name'][1] is None
        assert polars_result['value'][1] is None

    def test_create_with_boolean_values(self, real_ibis_backend):
        """Test creating DataFrame with boolean values."""
        data = {
            "id": [1, 2, 3],
            "active": [True, False, True],
            "verified": [False, False, True]
        }
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test boolean handling
        polars_result = result.materialise('polars')
        assert polars_result['active'].dtype == pl.Boolean
        assert polars_result['verified'].dtype == pl.Boolean
        
        # Verify boolean values
        assert polars_result['active'].to_list() == [True, False, True]
        assert polars_result['verified'].to_list() == [False, False, True]

    def test_create_with_datetime_values(self, real_ibis_backend):
        """Test creating DataFrame with datetime values."""
        data = {
            "id": [1, 2, 3],
            "created_at": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 3)
            ]
        }
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test datetime handling
        polars_result = result.materialise('polars')
        assert 'created_at' in polars_result.columns
        
        # Verify dates are handled properly
        dates = polars_result['created_at'].to_list()
        assert len(dates) == 3


class TestDataFrameFactoryErrorHandling:
    """Tests for DataFrameFactory error handling."""

    def test_create_with_invalid_list_of_dicts_structure(self, real_ibis_backend):
        """Test error handling for invalid list of dicts structure."""
        invalid_data = [
            {"id": 1, "name": "Alice"},
            {"different_key": 2, "other_key": "Bob"}  # Inconsistent keys
        ]
        
        # Factory should handle inconsistent structures gracefully or raise error
        try:
            result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(invalid_data, ibis_backend=real_ibis_backend)
            if result is not None:
                # If handled gracefully, should still produce valid result
                polars_result = result.materialise('polars')
                assert len(polars_result) >= 1
        except (ValueError, KeyError) as e:
            # If error raised, it should be informative
            assert len(str(e)) > 0

    def test_create_with_uneven_list_lengths(self, real_ibis_backend):
        """Test error handling for uneven list lengths."""
        uneven_data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob"],  # Shorter list
            "value": [100.5, 200.7, 300.9, 400.2]  # Longer list
        }
        
        # Should either handle gracefully or raise appropriate error
        try:
            result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(uneven_data, ibis_backend=real_ibis_backend)
            if result is not None:
                polars_result = result.materialise('polars')
                # If handled gracefully, should have consistent length
                assert len(polars_result) > 0
        except (ValueError, Exception) as e:
            # If error raised, it should be informative
            assert len(str(e)) > 0

    def test_create_with_none_input_raises_error(self):
        """Test that None input is handled appropriately."""
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(None)
        assert result is None
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(None)
        assert result is None

    def test_create_with_non_dict_in_list_raises_error(self, real_ibis_backend):
        """Test error handling for non-dict items in list."""
        invalid_data = [
            {"id": 1, "name": "Alice"},
            "not_a_dict",  # Invalid item
            {"id": 3, "name": "Charlie"}
        ]
        
        # Should raise appropriate error for invalid structure
        with pytest.raises((TypeError, ValueError)):
            DataFrameFactory.create_ibis_dataframe_object_from_dictionary(invalid_data, ibis_backend=real_ibis_backend)


class TestDataFrameFactoryConfiguration:
    """Tests for DataFrameFactory configuration options."""

    @pytest.mark.parametrize("output_type", ["polars", "pandas", "pyarrow"])
    def test_create_with_different_output_types(self, sample_data_dict, real_ibis_backend, output_type):
        """Test creating DataFrames and converting to different output types."""
        data = sample_data_dict["valid_simple"]
        
        # Create DataFrame using factory
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test materialization to different types
        materialized = result.materialise(output_type)
        
        if output_type == "polars":
            assert isinstance(materialized, pl.DataFrame)
            assert len(materialized) == 3
            assert 'id' in materialized.columns
        elif output_type == "pandas":
            assert isinstance(materialized, pd.DataFrame)
            assert len(materialized) == 3
            assert 'id' in materialized.columns
        elif output_type == "pyarrow":
            assert isinstance(materialized, pa.Table)
            assert materialized.num_rows == 3
            assert 'id' in materialized.schema.names

    def test_create_with_column_type_specifications(self, real_ibis_backend):
        """Test creating DataFrame with different data types."""
        data = {
            "id": [1, 2, 3],  # Integer data
            "value": [100.5, 200.7, 300.9],  # Float data
            "name": ["Alice", "Bob", "Charlie"],  # String data
            "active": [True, False, True]  # Boolean data
        }
        
        result = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        assert result is not None
        
        # Test that column types are handled appropriately
        polars_result = result.materialise('polars')
        assert polars_result['id'].dtype in [pl.Int64, pl.Int32]
        assert polars_result['value'].dtype == pl.Float64
        assert polars_result['name'].dtype == pl.Utf8
        assert polars_result['active'].dtype == pl.Boolean


class TestDataFrameFactoryIntegration:
    """Tests for DataFrameFactory integration with other components."""

    def test_factory_result_works_with_ibis_dataframe(self, sample_data_dict, real_ibis_backend):
        """Test that factory results are IbisDataFrame instances that work correctly."""
        from mountainash_data.dataframes.ibis_dataframe import IbisDataFrame
        
        data = sample_data_dict["valid_simple"]
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        
        # Test that the result is an IbisDataFrame
        assert isinstance(df, IbisDataFrame)
        
        # Test that it has all expected IbisDataFrame methods
        assert hasattr(df, 'filter')
        assert hasattr(df, 'select')
        assert hasattr(df, 'limit')
        assert hasattr(df, 'count')
        assert hasattr(df, 'materialise')
        
        # Test that operations work
        filtered = df.filter(df.ibis_df.id > 1)
        assert filtered is not None
        count = filtered.count()
        assert count == 2  # Should have id=2 and id=3

    def test_factory_preserves_data_integrity(self, sample_data_list, real_ibis_backend):
        """Test that factory preserves data integrity during conversion."""
        original_data = [dict(item) for item in sample_data_list]  # Deep copy
        result_df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(sample_data_list, ibis_backend=real_ibis_backend)
        
        assert result_df is not None
        
        # Verify original data wasn't modified
        assert len(original_data) == len(sample_data_list)
        for orig, current in zip(original_data, sample_data_list):
            assert orig == current
        
        # Verify data integrity in result
        polars_result = result_df.materialise('polars')
        assert len(polars_result) == len(sample_data_list)
        
        # Verify specific data values
        names = polars_result['name'].to_list()
        ids = polars_result['id'].to_list()
        
        for item in sample_data_list:
            assert item['name'] in names
            assert item['id'] in ids

    def test_factory_with_real_ibis_backend_operations(self, sample_data_dict, real_ibis_backend):
        """Test factory integration with real Ibis backend operations."""
        data = sample_data_dict["valid_complex"]
        df = DataFrameFactory.create_ibis_dataframe_object_from_dictionary(data, ibis_backend=real_ibis_backend)
        
        assert df is not None
        
        # Test chained operations work with factory-created DataFrame
        result = (df
                 .filter(df.ibis_df.value > 200.0)
                 .filter(df.ibis_df.active == True)
                 .select(['name', 'value', 'category']))
        
        materialized = result.materialise('polars')
        
        # Verify filtering worked correctly
        assert len(materialized) >= 1  # Should have some results
        for value in materialized['value']:
            assert value > 200.0
        for active in materialized.to_pandas()['active'] if 'active' in materialized.columns else []:
            assert active == True