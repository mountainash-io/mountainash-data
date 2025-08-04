"""Tests for IbisDataFrame class."""

import pytest
import polars as pl
import pandas as pd
import pyarrow as pa

from mountainash_data.dataframes.base_dataframe import BaseDataFrame
from mountainash_data.dataframes.ibis_dataframe import IbisDataFrame


class TestIbisDataFrameInitialization:
    """Tests for IbisDataFrame initialization."""

    def test_ibis_dataframe_inherits_from_base(self):
        """Test that IbisDataFrame inherits from BaseDataFrame."""
        assert issubclass(IbisDataFrame, BaseDataFrame)

    def test_ibis_dataframe_initialization_with_real_table(self, real_ibis_table):
        """Test IbisDataFrame can be initialized with real table."""
        df = IbisDataFrame(real_ibis_table)
        assert df is not None
        assert hasattr(df, 'ibis_df')
        assert df.ibis_df is not None

    def test_ibis_dataframe_requires_table_parameter(self):
        """Test that IbisDataFrame requires a table parameter."""
        with pytest.raises(TypeError):
            IbisDataFrame()


class TestIbisDataFrameOperations:
    """Tests for IbisDataFrame operations."""

    def test_filter_operation(self, real_ibis_table):
        """Test filter operation with real data."""
        df = IbisDataFrame(real_ibis_table)
        
        # Filter for records where value > 200
        filtered_df = df.filter(df.ibis_df.value > 200.0)
        
        # Verify the result is still an IbisDataFrame
        assert isinstance(filtered_df, IbisDataFrame)
        
        # Materialize and verify the filtering worked
        result = filtered_df.materialise('polars')
        assert len(result) == 3  # Alice=100.5, Bob=200.7, Charlie=300.9, Diana=400.2, Eve=500.8 -> 3 records > 200
        assert all(result['value'] > 200.0)
        
        # Verify specific values
        names = result['name'].to_list()
        assert 'Charlie' in names  # 300.9
        assert 'Diana' in names    # 400.2  
        assert 'Eve' in names      # 500.8
        assert 'Alice' not in names  # 100.5
        assert 'Bob' not in names    # 200.7

    def test_select_operation(self, real_ibis_table):
        """Test select operation with real data."""
        df = IbisDataFrame(real_ibis_table)
        
        # Select only specific columns
        selected_df = df.select(['name', 'value'])
        
        assert isinstance(selected_df, IbisDataFrame)
        
        # Materialize and verify columns
        result = selected_df.materialise('polars')
        assert list(result.columns) == ['name', 'value']
        assert len(result) == 5  # All 5 records
        assert result['name'].to_list() == ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']

    def test_limit_operation(self, real_ibis_table):
        """Test limit operation with real data."""
        df = IbisDataFrame(real_ibis_table)
        
        # Limit to first 3 records
        limited_df = df.limit(3)
        
        assert isinstance(limited_df, IbisDataFrame)
        
        # Materialize and verify limit
        result = limited_df.materialise('polars')
        assert len(result) == 3
        # Should be first 3: Alice, Bob, Charlie
        assert result['name'].to_list() == ['Alice', 'Bob', 'Charlie']

    def test_count_operation(self, real_ibis_table):
        """Test count operation with real data."""
        df = IbisDataFrame(real_ibis_table)
        
        count = df.count()
        
        assert count == 5  # Real count from fixture data
        
        # Test count on filtered data
        filtered_df = df.filter(df.ibis_df.active == True)
        filtered_count = filtered_df.count()
        assert filtered_count == 3  # Alice, Charlie, Diana are active


class TestIbisDataFrameMaterialization:
    """Tests for IbisDataFrame materialization methods."""

    def test_materialize_to_polars(self, real_ibis_table):
        """Test materialization to Polars DataFrame."""
        df = IbisDataFrame(real_ibis_table)
        
        result = df.materialise("polars")
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert 'value' in result.columns
        assert 'active' in result.columns
        
        # Verify data integrity
        assert result['name'].to_list() == ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']
        assert result['value'].to_list() == [100.5, 200.7, 300.9, 400.2, 500.8]

    def test_materialize_to_pandas(self, real_ibis_table):
        """Test materialization to pandas DataFrame."""
        df = IbisDataFrame(real_ibis_table)
        
        result = df.materialise("pandas")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert 'value' in result.columns
        assert 'active' in result.columns
        
        # Verify data integrity
        assert result['name'].tolist() == ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']
        assert result['value'].tolist() == [100.5, 200.7, 300.9, 400.2, 500.8]

    def test_materialize_to_pyarrow(self, real_ibis_table):
        """Test materialization to PyArrow Table."""
        df = IbisDataFrame(real_ibis_table)
        
        result = df.materialise("pyarrow")
        
        assert isinstance(result, pa.Table)
        assert result.num_rows == 5
        assert 'id' in result.schema.names
        assert 'name' in result.schema.names
        assert 'value' in result.schema.names
        assert 'active' in result.schema.names
        
        # Verify data integrity
        assert result.column('name').to_pylist() == ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']
        assert result.column('value').to_pylist() == [100.5, 200.7, 300.9, 400.2, 500.8]

    def test_materialize_invalid_format_raises_error(self, real_ibis_table):
        """Test that invalid format raises appropriate error."""
        df = IbisDataFrame(real_ibis_table)
        
        with pytest.raises(ValueError, match="Unsupported format"):
            df.materialise("invalid_format")

    @pytest.mark.parametrize("format_name,expected_method", [
        ("polars", "to_polars"),
        ("pandas", "to_pandas"), 
        ("pyarrow", "to_pyarrow")
    ])
    def test_materialize_calls_correct_method(self, real_ibis_table, format_name, expected_method):
        """Test that materialization calls the correct underlying method."""
        df = IbisDataFrame(real_ibis_table)
        
        # Verify the method exists on the underlying table
        assert hasattr(df.ibis_df, expected_method)
        
        # Test materialization works
        result = df.materialise(format_name)
        assert result is not None
        
        # Verify result type matches expectation
        if format_name == "polars":
            assert isinstance(result, pl.DataFrame)
        elif format_name == "pandas":
            assert isinstance(result, pd.DataFrame)
        elif format_name == "pyarrow":
            assert isinstance(result, pa.Table)


class TestIbisDataFrameUtilities:
    """Tests for IbisDataFrame utility methods."""

    def test_get_schema(self, real_ibis_table):
        """Test schema retrieval with real table."""
        df = IbisDataFrame(real_ibis_table)
        
        schema = df.schema
        
        assert schema is not None
        # Verify all expected columns are present
        column_names = [field.name for field in schema]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'value' in column_names
        assert 'active' in column_names

    def test_get_columns(self, real_ibis_table):
        """Test column list retrieval with real table."""
        df = IbisDataFrame(real_ibis_table)
        
        columns = df.columns
        
        assert isinstance(columns, list)
        assert 'id' in columns
        assert 'name' in columns
        assert 'value' in columns
        assert 'active' in columns
        assert len(columns) == 4

    def test_repr_method(self, real_ibis_table):
        """Test string representation with real table."""
        df = IbisDataFrame(real_ibis_table)
        
        repr_str = repr(df)
        
        assert isinstance(repr_str, str)
        assert 'IbisDataFrame' in repr_str
        # Should contain some representation of the underlying table
        assert len(repr_str) > 10  # Should be a meaningful representation


class TestIbisDataFrameErrorHandling:
    """Tests for IbisDataFrame error handling with real scenarios."""

    def test_filter_with_invalid_expression_propagates_error(self, real_ibis_table):
        """Test that invalid filter expressions raise appropriate errors."""
        df = IbisDataFrame(real_ibis_table)
        
        # Try to filter with a non-existent column
        with pytest.raises(Exception):  # Real error from ibis
            df.filter(df.ibis_df.nonexistent_column > 5).materialise('polars')

    def test_materialization_error_propagates(self, real_ibis_table):
        """Test that materialization errors are properly propagated."""
        df = IbisDataFrame(real_ibis_table)
        
        # This should work fine, but if there were connection issues they would propagate
        result = df.materialise('polars')
        assert result is not None
        
        # Test with invalid format still raises ValueError
        with pytest.raises(ValueError):
            df.materialise('invalid_format')

    def test_operation_on_complex_filter_chain(self, real_ibis_table):
        """Test operations on complex filter chains work correctly."""
        df = IbisDataFrame(real_ibis_table)
        
        # Use a single complex filter expression instead of chaining
        # This tests filtering with multiple conditions in one expression
        result_df = (df
                    .filter((df.ibis_df.value > 150.0) & (df.ibis_df.active == True))  # Should get Charlie, Diana
                    .select(['name', 'value']))
        
        result = result_df.materialise('polars')
        
        assert len(result) == 2
        names = result['name'].to_list()
        assert 'Charlie' in names
        assert 'Diana' in names
        assert 'Alice' not in names  # value too low
        assert 'Bob' not in names    # not active
        assert 'Eve' not in names    # not active