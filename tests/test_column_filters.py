import pytest
import pandas as pd
import polars as pl
import pyarrow as pa
import ibis
from ibis import _
import numpy as np
from datetime import datetime, date

from mountainash_data.dataframes.utils import DataFrameUtils
from mountainash_data.dataframes.utils.dataframe_filters import FilterCondition

# Helper function to create test data in different formats
def create_test_data(df_type):
    data = {
        'id': [1, 2, 3, 4, 5],
        'value_a': [10, 20, 30, 40, 50],
        'value_b': [15, 25, 30, 35, 45],
        'category_x': ['A', 'B', 'C', 'A', 'B'],
        'category_y': ['X', 'Y', 'Z', 'X', 'Y'],
        'date_1': [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1), date(2023, 4, 1), date(2023, 5, 1)],
        'date_2': [date(2023, 1, 15), date(2023, 2, 15), date(2023, 3, 1), date(2023, 4, 15), date(2023, 5, 15)],
        'nullable_1': [1, 2, None, 4, 5],
        'nullable_2': [1, None, 3, None, 5]
    }
    
    if df_type == 'pandas':
        return pd.DataFrame(data)
    elif df_type == 'polars':
        return pl.DataFrame(data)
    elif df_type == 'polars_lazy':
        return pl.LazyFrame(data)
    elif df_type == 'pyarrow':
        return pa.Table.from_pydict(data)
    elif df_type == 'pyarrow_recordbatch':
        df_temp = pa.Table.from_pydict(data)
        return DataFrameUtils.cast_dataframe_to_pyarrow_recordbatch(df_temp, 1)
    elif df_type == 'ibis':
        return ibis.memtable(data)
    else:
        raise ValueError(f"Unsupported dataframe type: {df_type}")

# Fixture for dataframe types
@pytest.fixture(params=[
    'pandas',
    'polars', 
    'polars_lazy', 
    'pyarrow', 
    'pyarrow_recordbatch', 
    'ibis'
])
def df_type(request):
    return request.param

# Fixture for test data
@pytest.fixture
def test_df(df_type):
    return create_test_data(df_type)

# Fixture for DataFrameUtils
@pytest.fixture
def df_utils():
    return DataFrameUtils()

# Fixture for FilterCondition
@pytest.fixture
def filter_condition():
    return FilterCondition

# Test cases for column comparisons
def test_column_equals(test_df, df_utils, filter_condition):
    condition = filter_condition.column_equals('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 1  # Only the row where value_a == value_b (30 == 30)

def test_column_not_equals(test_df, df_utils, filter_condition):
    condition = filter_condition.column_not_equals('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 4  # All rows except where value_a == value_b

def test_column_greater_than(test_df, df_utils, filter_condition):
    condition = filter_condition.column_greater_than('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2  # Rows where value_a > value_b (40 > 35 and 50 > 45)

def test_column_less_than(test_df, df_utils, filter_condition):
    condition = filter_condition.column_less_than('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2  # Rows where value_a < value_b (10 < 15 and 20 < 25)

def test_column_greater_than_or_equal(test_df, df_utils, filter_condition):
    condition = filter_condition.column_greater_than_or_equal('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 3  # Rows where value_a >= value_b (30 >= 30, 40 >= 35, and 50 >= 45)

def test_column_less_than_or_equal(test_df, df_utils, filter_condition):
    condition = filter_condition.column_less_than_or_equal('value_a', 'value_b')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 3  # Rows where value_a <= value_b (10 <= 15, 20 <= 25, and 30 <= 30)

def test_column_comparison_with_dates(test_df, df_utils, filter_condition):
    condition = filter_condition.column_less_than('date_1', 'date_2')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 4  # All rows except where date_1 == date_2 (2023-03-01)

def test_column_comparison_with_categories(test_df, df_utils, filter_condition):
    condition = filter_condition.column_equals('category_x', 'category_y')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 0  # No rows where category_x == category_y

def test_complex_column_comparison(test_df, df_utils, filter_condition):
    condition = filter_condition.and_(
        filter_condition.column_greater_than('value_a', 'value_b'),
        filter_condition.eq('category_y', 'X')
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 1  # Only one row satisfies both conditions (id = 4)

def test_column_comparison_with_nulls(test_df, df_utils, filter_condition):
    condition = filter_condition.column_equals('nullable_1', 'nullable_2')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2  # Rows where nullable_1 == nullable_2 (1 == 1 and 5 == 5)

def test_mixed_column_and_value_comparison(test_df, df_utils, filter_condition):
    condition = filter_condition.and_(
        filter_condition.column_greater_than('value_a', 'value_b'),
        filter_condition.equals('category_x', 'A')
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 1  # Only one row satisfies both conditions (id = 4)