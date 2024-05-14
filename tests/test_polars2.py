import pytest
from pytest_check import check
from ibis import _

import polars as pl
import pandas as pd

from mountainash_data import IbisDataFrame, DataFrameFactory


# Fixture to initialize a sample IbisDataFrame object for testing
@pytest.fixture
def sample_polars_dataframe():
    # Create a sample Polars DataFrame for testing
    data = {'A': [1, 2, 3], 'B': [4, 5, 6]}
    pdf = pd.DataFrame(data)
    return IbisDataFrame(df=pdf)


def test_initialization(sample_polars_dataframe):
    assert isinstance(sample_polars_dataframe, IbisDataFrame)


def test_materialization(sample_polars_dataframe):
    
    materialized_df = sample_polars_dataframe.materialise()
    assert isinstance(materialized_df, pd.DataFrame)


def test_column_operations(sample_polars_dataframe):
    selected_df = sample_polars_dataframe.select("A")
    assert "A" in selected_df.get_column_names()

    dropped_df = sample_polars_dataframe.drop("B")
    assert "B" not in dropped_df.get_column_names()

    # mutated_df = sample_polars_dataframe.mutate([col("C") + col("D")])
    # assert "C+D" in mutated_df.get_column_names()

    # aggregated_df = sample_polars_dataframe.aggregate(mean_col_A=col("A").mean())
    # assert "mean_col_A" in aggregated_df.get_column_names()

    # pivot_wider_df = sample_polars_dataframe.pivot_wider(pivot_column="category", value_column="value")
    # assert "category" in pivot_wider_df.get_column_names()

    # pivot_longer_df = sample_polars_dataframe.pivot_longer(columns=["B", "C"])
    # assert "variable" in pivot_longer_df.get_column_names()


def test_joins(sample_polars_dataframe):
    right_df = sample_polars_dataframe
    joined_df = sample_polars_dataframe.left_join(right_df, predicates=["A"])
    assert "A" in joined_df.get_column_names()


def test_row_selection(sample_polars_dataframe):
    filtered_df = sample_polars_dataframe.filter(_.A > 1)
    assert filtered_df.count() == 2

    head_df = sample_polars_dataframe.head(2)
    assert head_df.count() == 2


def test_aggregates(sample_polars_dataframe):
    assert sample_polars_dataframe.count() == 3


# def test_query(sample_polars_dataframe):
#     query_df = sample_polars_dataframe.sql("SELECT * FROM {_} WHERE A > 1")
#     assert "A" in query_df.get_column_names()


# def test_ibis_connections(sample_polars_dataframe):
#     sample_polars_dataframe.set_ibis_backend()
#     assert ibis.set_backend() == "polars"
#     sample_polars_dataframe.init_ibis_connection()
#     assert sample_polars_dataframe.ibis_backend is not None
