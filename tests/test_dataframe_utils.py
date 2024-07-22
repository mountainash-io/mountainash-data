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
    "column3": ["A", "B", "C", "D", "E"],
    "column4": [["a","b"],["c"]]
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
    "column1": ["col1"],
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

    with pytest.raises(Exception):
        print(pdDataframe)#Should not be able to be created if this breaks, which it does currently
        assert pdDataframe[None] == ["A", "B", "C"] 

    #Allows for dataframe with an empty list and None to be column names. Crashes when ever attempts to use are made


    #Pandas Empty Data
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExampleEmpty, columnDictExample1)
    print(pdDataframe)
    with pytest.raises(ValueError): #Either the dataframe creation should be stopped before this or this should fail
        assert pdDataframe["col1"] == [7,8,9] #Allows for creation of an unaccesable dataframe
        assert pdDataframe["col3"] == None 


def test_create_dataframe_polars_empty():
    #Polars Empty Column Names
    with pytest.raises(Exception): #Actual Polars function raises error, may be worthwhile testing in the data utils function and raising specific exception
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleEmpty)
    
    #Polars Empty Data
    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExampleEmpty, columnDictExample1)
    print(plDataframe) #Same issue as pandas, allows for creation of unaccessable dataframes with None columns
    with pytest.raises(Exception):
        assert plDataframe["col3"] == [7,8,9] 
        assert plDataframe["col3"] == None  


def test_create_dataframe_ibis_empty():
    #Ibis Empty Column Names
    with pytest.raises(Exception): 
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleEmpty)

    #Ibis Empty Data
    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExampleEmpty, columnDictExample1)
    print(ibisDataFrame)

    #Expected column names
    expCol = list(columnDictExample1.values())
    assert ibisDataFrame.columns == expCol
    valuesColOne = list(ibisDataFrame.execute()["col3"])
    assert valuesColOne == [None] * 3

    #Ibis actually works after being created with an empty value for one of the columns, needs to be restricted if we want it
    #to stay in line with Polars and Pandas.
    #Still need to raise value error when created with empty column names in the creation function


def test_create_dataframe_pandas_dirty():
    #Pandas Dirty Column Names
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleDirty)
    assert isinstance(pdDataframe, pd.DataFrame)
    with pytest.raises(Exception):
        print(pdDataframe) #Allows creation with lists as column values. Crashes
    assert " " in pdDataframe
    assert pdDataframe[9][0] == "A"
    #Empty space strings and integers work as column names


     #Pandas Dirty Data
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExampleDirty, columnDictExample1)
    print(pdDataframe)
    assert pdDataframe["col1"][0] == 4
    with pytest.raises(Exception):
        assert pdDataframe["col1"] == 4 #When only one value is inputed Pandas breaks trying to find the entire column
    assert pdDataframe["col2"][2] == "90" 
    assert pdDataframe["col3"][1] == 5 
    assert pdDataframe["col3"][2][1] == 9.7 

    #Need to ensure that inputed data is in a list form. Could just put the value in a list and multiple by row number


def test_create_dataframe_polars_dirty():
    #Polars Dirty Column Names
    with pytest.raises(Exception):
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleDirty)

        assert isinstance(plDataFrame, pl.DataFrame)
        assert "col1" in plDataFrame
        assert " " in plDataFrame
        assert 9 in plDataFrame
        assert plDataFrame["col1"][2] == "1"
        assert plDataFrame[" "][0] == 1
        assert plDataFrame[9][1] == "B"

    #Cannot create dataframe with a list as column name, error raised not in function so not that helpful for debugging later

    #Polars Dirty Data
    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExampleDirty, columnDictExample1)
    #Same issue as pandas, allows for creation of unaccessable dataframes with None columns
    assert plDataframe["col1"][0] == 4
    with pytest.raises(Exception):
        assert plDataframe["col1"] == 4 #Does same as Pandas
    #Intrestling Polars removes the differing data types, leaves Null for everthing except strings    
    assert plDataframe["col2"][0] == None
    assert plDataframe["col2"][2] == "90"
    assert plDataframe["col3"][2] == None 
    assert plDataframe["col3"][0] == "A"

    #Dirty no strings
    tempDict = {}
    tempDict["column1"] = [4, 4, 4]
    tempDict["column2"] = [1, 2.4, 3]
    tempDict["column3"] = [1.2, 2.3, "3.4"]

    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, tempDict, columnDictExample1)   
    assert plDataframe["col1"][0] == 4
    assert plDataframe["col2"][0] == 1
    assert plDataframe["col2"][1] == 2.4
    assert plDataframe["col3"][2] == "3.4" 
    assert plDataframe["col3"][0] == 1.2

    #Polars always chooses the string as the data type when a column has more than one, this is different than pandas
    #Need to validate that all inputed values are the same type in creation
    #If integers and floats are inputted it changes the integers into float values


def test_create_dataframe_ibis_dirty():
    with pytest.raises(Exception):
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleDirty)
    #Cannot create column names with lists, need to move exception raising to function

    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExampleDirty, columnDictExample1)
    valuesColOne = list(ibisDataFrame.execute()["col1"])
    valuesColTwo = list(ibisDataFrame.execute()["col2"])
    valuesColThree = list(ibisDataFrame.execute()["col3"])
    assert valuesColOne == [4] * 3 #Ibis actually works with a single value inputted, creates a list so each row has the value
    assert valuesColTwo == [None, None, "90"]
    assert valuesColThree == ["A", None, None]
    #Does the same as Polars, if multiple data type are inputted it defaults to strings

    #Dirty no strings
    tempDict = {}
    tempDict["column1"] = [4, 4, 4]
    tempDict["column2"] = [1, 2.4, 3]
    tempDict["column3"] = [1.2, 2.3, "3.4"]

    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, tempDict, columnDictExample1)
    valuesColOne = list(ibisDataFrame.execute()["col1"])
    valuesColTwo = list(ibisDataFrame.execute()["col2"])
    valuesColThree = list(ibisDataFrame.execute()["col3"])

    assert valuesColOne == [4] * 3 
    assert valuesColTwo == [1, 2.4, 3]
    assert valuesColThree == [None, None, "3.4"]
    #Same as Polars




























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
