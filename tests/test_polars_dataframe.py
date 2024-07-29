import pytest
from pytest_check import check
from ibis import _

import polars as pl

from mountainash_data import IbisDataFrame
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from mountainash_data import DataFrameUtils

# Create fixture for initializing IbisDataFrame instance with sample data
@pytest.fixture
def sample_polars_df():
    data = {'A': [1, 2, 3], 
            'B': ['a', 'b', 'c']}
    df = pl.DataFrame(data)
    return IbisDataFrame(df)


data_dict = {
    "A": [1, 2, 3],
    "B": [4, 5, 6],
    "C": ["A", "B", "C"]
}

dfPandas = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, data_dict=data_dict)
dfPolars = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, data_dict=data_dict)

# Test is_materialised method
# def test_is_materialised(sample_polars_df):
#     assert sample_polars_df.is_materialised() == True

#Test creation, same backend schema
@pytest.mark.parametrize("df, backend, expected", [
    (dfPandas, "pandas", "pandas"),
    (dfPolars, "polars", "polars")
])
def test_creation_same_backend(df, backend, expected):
    ibis_df = IbisDataFrame(df, ibis_backend_schema=backend)
    assert ibis_df.is_materialised() == True
    assert ibis_df.ibis_backend_schema == expected
    assert ibis_df.materialise().shape == (3, 3)
    assert list(ibis_df.materialise().columns) == ["column1", "column2", "column3"]
    assert list(ibis_df.execute()["column1"]) == [1, 2, 3]

#Test creation, different backend schema
@pytest.mark.parametrize("df, backend, expected", [
    (dfPandas, "polars", "polars"),
    (dfPolars, "pandas", "pandas"),
    (dfPandas, "sqlite", "sqlite"),
    (dfPolars, "sqlite", "sqlite"),
    (dfPandas, "duckdb", "duckdb"),
    (dfPolars, "duckdb", "duckdb")
])
def test_creation_same_backend(df, backend, expected):
    ibis_df = IbisDataFrame(df, ibis_backend_schema=backend)
    assert ibis_df.is_materialised() == True
    assert ibis_df.ibis_backend_schema == expected
    assert ibis_df.materialise().shape == (3, 3)
    assert list(ibis_df.materialise().columns) == ["A", "B", "C"]
    assert list(ibis_df.execute()["A"]) == [1, 2, 3]

#Test creation, incorrect backend schema
@pytest.mark.parametrize("df, backend", [
    (dfPandas, "TESTEST"),
    (dfPolars, "TESTEST"),
    (dfPolars, 4),
    (dfPolars, 4)
])
def test_creation_incorrec_backend(df, backend):
    with pytest.raises(ValueError):
        ibis_df = IbisDataFrame(df, ibis_backend_schema=backend)
    

# Parameterized test for select_columns method, with same backend schema
@pytest.mark.parametrize("columns, expected_shape, baseDF", [
    (["A"], (3, 1), dfPandas),
    (["A", "B"], (3, 2), dfPandas),
    (["A"], (3, 1), dfPolars),
    (["A", "B"], (3, 2), dfPolars)
])
def test_select_columns(columns: list[str], expected_shape, baseDF):
    ibis_df = IbisDataFrame(baseDF)
    selected_df = ibis_df.select(columns)
    assert selected_df.materialise().shape == expected_shape




@pytest.mark.parametrize("column, expected_values, baseDF", [
    ("A", [1, 2, 3], dfPandas),
    ("C", ['A', 'B', 'C'], dfPandas),
    ("A", [1, 2, 3], dfPolars),
    ("C", ['A', 'B', 'C'], dfPolars)
])
def test_get_column_as_list(column, expected_values, baseDF):
    ibis_df = IbisDataFrame(baseDF)
    selected_df = ibis_df.select(column)
    selected_values = selected_df.get_column_as_list(column)
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
@pytest.mark.parametrize("baseDF", [
    (dfPandas),
    (dfPolars),
])
def test_filter_method(baseDF):
    ibis_df = IbisDataFrame(baseDF)
    expr = _.A == 3
    filtered_df = ibis_df.filter(expr)

    with check:
        assert isinstance(filtered_df, IbisDataFrame)
        assert filtered_df.materialise().shape == (1,3)

# Parameterized test for head method
@pytest.mark.parametrize("n, expected_shape, baseDF", [
    (0, 0, dfPandas),
    (1, 1, dfPandas),
    (2, 2, dfPandas),
    (3, 3, dfPandas),
    (0, 0, dfPolars),
    (1, 1, dfPolars),
    (2, 2, dfPolars),
    (3, 3, dfPolars)
])
def test_head_method(baseDF, n, expected_shape):
    ibis_df = IbisDataFrame(baseDF)
    result_df = ibis_df.head(n)

    with check:
        assert isinstance(result_df, IbisDataFrame)
        assert result_df.count() == expected_shape

# Test case for as_dict method
@pytest.mark.parametrize("baseDF", [
    (dfPandas),
    (dfPolars),
])
def test_as_dict_method(baseDF):
    ibis_df = IbisDataFrame(baseDF)
    result_dict = ibis_df.as_dict()
    assert isinstance(result_dict, dict)

# Test case for get_first_row_as_dict method
@pytest.mark.parametrize("baseDF", [
    (dfPandas),
    (dfPolars),
])
def test_get_first_row_as_dict_method(baseDF):

    ibis_df = IbisDataFrame(baseDF)
    result_first_row_dict = ibis_df.get_first_row_as_dict()
    print(result_first_row_dict)
    with check:    
        assert isinstance(result_first_row_dict, dict)
        assert result_first_row_dict == {'A': 1, 'B': 4, 'C': 'A'}

# Test case for get_row_count method
@pytest.mark.parametrize("baseDF", [
    (dfPandas),
    (dfPolars),
])
def test_get_row_count_method(baseDF):
    ibis_df = IbisDataFrame(baseDF)
    row_count = ibis_df.count()
    
    with check:
        assert row_count == 3
