import pytest
import pandas as pd
import polars as pl
import ibis
from mountainash_data import DataFrameUtils
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK

#Data Dicts
dataDictExample1 = {
    "column1": [1, 2, 3],
    "column2": [4, 5, 6],
    "column3": ["A", "B", "C"]
}

dataDictExampleEmpty = {
    "column1": [7,8,9],
    "column2": [2,4,6],
    "column3": None
}

dataDictExampleDirty = {
    "column1": 4,
    "column2": [4.7, None, "90"],
    "column3": ["A", 5, [5,9.7]]
}

dataDictExampleUneven = {
    "column1": [2, 7],
    "column2": [4.7, 8.9, 0.002],
    "column3": ["A", "B", "C", "D", "E"]
}

# Column dictionary for renaming columns
columnDictExample1 = {
    "column1": "col1",
    "column2": "col2",
    "column3": "col3"
}

columnDictExampleEmpty = {
    "column1": None,
    "column2": [],
    "column3": "col3"
}

columnDictExampleDirty = {
    "column1": "col1",
    "column2": " ",
    "column3": 9
}

columnDictExampleUneven = {
    "column1": "col1",
    "column2": "col2"
}

# Def create_dataframe tests
def test_create_dataframe_pandas_simple():
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExample1)
    assert isinstance(pdDataframe, pd.DataFrame)
    assert "col3" in pdDataframe
    assert pdDataframe["col1"][0] == 1
    assert pdDataframe["col2"][1] == 5
    assert pdDataframe["col3"][2] == "C"

def test_create_dataframe_polas_simple():
    plDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExample1)
    assert isinstance(plDataFrame, pl.DataFrame)
    assert "col3" in plDataFrame
    assert plDataFrame["col3"][2] == "C"
    assert plDataFrame["col1"][0] == 1
    assert plDataFrame["col2"][1] == 5

def test_create_dataframe_ibis_simple():
    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExample1)
    #Expected column names
    expCol = list(columnDictExample1.values())
    assert ibisDataFrame.columns == expCol
    #Expected col 1
    expOne = dataDictExample1["column1"]
    valuesColOne = list(ibisDataFrame.execute()["col1"])
    assert valuesColOne == expOne


def test_create_dataframe_pandas_empty():
    #Pandas Empty Column Names
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleEmpty)
    assert isinstance(pdDataframe, pd.DataFrame)
    with pytest.raises(TypeError):
        print(pdDataframe)#Should not be able to be created if this breaks, which it does currently
    #assert pdDataframe["col3"] == ["A", "B", "C"] 
    #assert pdDataframe["column1"] == [1, 2, 3] 

    #Allows for dataframe with an empty list and None to be column names. Crashes when ever attempts to use are made


     #Pandas Empty Data
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExampleEmpty, columnDictExample1)
    print(pdDataframe)
    with pytest.raises(ValueError): #Either the dataframe creation should be stopped before this or this should fail
        assert pdDataframe["col1"] == [7,8,9] #Allows for creation of an unaccesable dataframe
        assert pdDataframe["col3"] == None  


def test_create_dataframe_polars_empty():

    with pytest.raises(Exception): #Actual Polars function raises error, may be worthwhile testing in the data utils function and raising specific exception
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleEmpty)
    

    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExampleEmpty, columnDictExample1)
    print(plDataframe) #Same issue as pandas, allows for creation of unaccessable dataframes with None columns
    assert plDataframe["col1"] == [7,8,9] 
    assert plDataframe["col3"] == None  


"""

TODO: Write empty tests for ibis dataframes

"""

def test_create_dataframe_ibis_empty():
    #Ibis
    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExample1)
    #Expected column names
    expCol = list(columnDictExample1.values())
    assert ibisDataFrame.columns == expCol
    #Expected col 1
    expOne = dataDictExample1["column1"]
    valuesColOne = list(ibisDataFrame.execute()["col1"])
    assert valuesColOne == expOne

















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
