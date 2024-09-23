import pytest
import pandas as pd
import polars as pl
import pyarrow as pa
import ibis
from ibis import _
import numpy as np
from datetime import datetime, date

from mountainash_data import DataFrameUtils
from mountainash_data.dataframes.utils.filter import FilterCondition

# Helper function to create test data in different formats
def create_test_data(df_type):
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000, 60000, None, 80000, 90000],
        'department': ['Sales', 'Marketing', 'Engineering', 'Sales', 'Marketing'],
        'join_date': [date(2020, 1, 1), date(2019, 6, 1), date(2021, 3, 15), date(2018, 9, 1), date(2022, 2, 28)],
        'last_login': [datetime(2023, 5, 1, 9, 0), datetime(2023, 5, 2, 10, 30), None, datetime(2023, 5, 3, 14, 15), datetime(2023, 5, 4, 16, 45)]
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
        # table = pa.Table.from_pydict(data)
        return DataFrameUtils.cast_dataframe_to_pyarrow_recordbatch(pl.DataFrame(data), 1)
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
    # 'pyarrow_recordbatch', 
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

# Test cases
def test_equals_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.equals('department', 'Sales')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

def test_not_equals_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.not_equals('department', 'Sales')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 3

def test_greater_than_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.greater_than('age', 35)
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

def test_less_than_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.less_than('age', 35)
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

def test_between_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.between('age', 30, 40)
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 3

def test_in_list_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.in_list('department', ['Sales', 'Marketing'])
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 4


def test_is_null_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.is_null('salary')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 1

def test_not_null_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.not_null('salary')
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 4

def test_and_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.and_(
        filter_condition.greater_than('age', 30),
        filter_condition.equals('department', 'Sales')
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 1

def test_or_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.or_(
        filter_condition.equals('department', 'Sales'),
        filter_condition.equals('department', 'Marketing')
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 4

def test_not_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.not_(
        filter_condition.equals('department', 'Sales')
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 3

def test_complex_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.and_(
        filter_condition.greater_than('age', 30),
        filter_condition.or_(
            filter_condition.equals('department', 'Sales'),
            filter_condition.equals('department', 'Marketing')
        ),
        filter_condition.not_(filter_condition.is_null('salary'))
    )
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

def test_date_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.greater_than('join_date', date(2020, 1, 1))
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

def test_datetime_filter(test_df, df_utils, filter_condition):
    condition = filter_condition.less_than('last_login', datetime(2023, 5, 3, 0, 0))
    result = df_utils.filter(test_df, condition)
    assert df_utils.count(result) == 2

# Add more test cases as needed