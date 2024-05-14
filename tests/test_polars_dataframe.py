import pytest
from pytest_check import check
from ibis import _

import polars as pl

from mountainash_data import IbisDataFrame

# Create fixture for initializing IbisDataFrame instance with sample data
@pytest.fixture
def sample_polars_df():
    data = {'A': [1, 2, 3], 
            'B': ['a', 'b', 'c']}
    df = pl.DataFrame(data)
    return IbisDataFrame(df)



# Test is_materialised method
# def test_is_materialised(sample_polars_df):
#     assert sample_polars_df.is_materialised() == True





# Parameterized test for select_columns method
@pytest.mark.parametrize("columns, expected_shape", [
    (["A"], (3, 1)),
    (["A", "B"], (3, 2))
])
def test_select_columns(sample_polars_df: IbisDataFrame, columns: list[str], expected_shape):

    selected_df = sample_polars_df.select(columns)
    assert selected_df.materialise().shape == expected_shape




@pytest.mark.parametrize("column, expected_values", [
    ("A", [1, 2, 3]),
    ("B", ['a', 'b', 'c'])
])
def test_get_column_as_list(sample_polars_df: IbisDataFrame, column, expected_values):


    selected_values = sample_polars_df.get_column_as_list(column)


    assert selected_values == expected_values



# # Parameterized test to check create_filter_expression method
# @pytest.mark.parametrize("column, operator, value, expected_expression", [
#     ("A", ">", 2, pl.col("A") > 2),
#     ("B", "==", 'b', pl.col("B") == 'b'),
#     ("A", "isin", [1, 3], pl.col("A").is_in([1, 3]))
#      ])
# def test_create_filter_expression(sample_polars_df, column, operator, value, expected_expression):

#     result = sample_polars_df.create_filter_expression(column, operator, value)
#     assert str(result) == str(expected_expression)



# Test filter method with a specific filter expression
def test_filter_method(sample_polars_df: IbisDataFrame):

    expr = _.A == 3
    filtered_df = sample_polars_df.filter(expr)

    with check:
        assert isinstance(filtered_df, IbisDataFrame)
        assert filtered_df.materialise().shape == (1,2)

# Parameterized test for head method
@pytest.mark.parametrize("n, expected_shape", [
    (0, 0),
    (1, 1),
    (2, 2),
    (3, 3)
])
def test_head_method(sample_polars_df: IbisDataFrame, n, expected_shape):
    result_df = sample_polars_df.head(n)

    with check:
        assert isinstance(result_df, IbisDataFrame)
        assert result_df.count() == expected_shape

# Test case for as_dict method
def test_as_dict_method(sample_polars_df: IbisDataFrame):
    result_dict = sample_polars_df.as_dict()
    assert isinstance(result_dict, dict)

# Test case for get_first_row_as_dict method
def test_get_first_row_as_dict_method(sample_polars_df: IbisDataFrame):

    result_first_row_dict = sample_polars_df.get_first_row_as_dict()
    print(result_first_row_dict)
    with check:    
        assert isinstance(result_first_row_dict, dict)
        assert result_first_row_dict == {'A': 1, 'B': 'a'}

# Test case for get_row_count method
def test_get_row_count_method(sample_polars_df: IbisDataFrame):

    row_count = sample_polars_df.count()
    
    with check:
        assert row_count == 3
