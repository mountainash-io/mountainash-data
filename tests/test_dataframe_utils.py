import pytest
import pandas as pd
import polars as pl
from mountainash_data import DataFrameUtils


def test_cast_dataframe_to_pandas():
    # Test using a Polars DataFrame
    df_polars = pl.DataFrame({"a": [1, 2, 3]})

    result = DataFrameUtils.cast_dataframe_to_pandas(df_polars)

    assert isinstance(result, pd.DataFrame)
    assert result.equals(pd.DataFrame({"a": [1, 2, 3]}))

    # Test using a Pandas DataFrame
    df_pandas = pd.DataFrame({"a": [1, 2, 3]})
    result = DataFrameUtils.cast_dataframe_to_pandas(df_pandas)

    assert isinstance(result, pd.DataFrame)
    assert result.equals(df_pandas)


def test_cast_dataframe_to_polars():
    # Test using a Pandas DataFrame
    df_pandas = pd.DataFrame({"a": [1, 2, 3]})

    result = DataFrameUtils.cast_dataframe_to_polars(df_pandas)

    assert isinstance(result, pl.DataFrame)
    assert result.shape == (3, 1)


@pytest.mark.parametrize(
    "input_df, expected_exception",
    [
        (123, TypeError),
        ("random_string", TypeError),
    ],
)
def test_cast_dataframe_to_pandas_exceptions(input_df, expected_exception):
    with pytest.raises(expected_exception):
        DataFrameUtils.cast_dataframe_to_pandas(input_df)


@pytest.mark.parametrize(
    "input_df, expected_exception",
    [
        (123, TypeError),
        ("random_string", TypeError),
    ],
)
def test_cast_dataframe_to_polars_exceptions(input_df, expected_exception):
    with pytest.raises(expected_exception):
        DataFrameUtils.cast_dataframe_to_polars(input_df)
