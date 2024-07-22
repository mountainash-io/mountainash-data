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
def test_create_dataframe_fake_dataframes():
    with pytest.raises(ValueError) as erro:
        fkDataframe = DataFrameUtils.create_dataframe(None, dataDictExample1, columnDictExample1)
    assert "dataframe_framework must be specified" in str(erro.value)

    with pytest.raises(ValueError)as erro:
        fkDataframe = DataFrameUtils.create_dataframe(2, dataDictExample1, columnDictExample1)
    assert "Unsupported dataframe framework: 2" in str(erro.value)

    with pytest.raises(ValueError)as erro:
        fkDataframe = DataFrameUtils.create_dataframe("AAAAAA", dataDictExample1, columnDictExample1)
    assert "Unsupported dataframe framework: AAAAAA" in str(erro.value)

    with pytest.raises(ValueError)as erro:
        fkDataframe = DataFrameUtils.create_dataframe([CONST_DATAFRAME_FRAMEWORK.PANDAS.value], dataDictExample1, columnDictExample1)
    assert "Unsupported dataframe framework: ['pandas']" in str(erro.value)
    

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
    with pytest.raises(ValueError):
        pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleEmpty)
    #Should raise a value error because there is a None in the column names

    #Testing empty list
    tempDict = dict(columnDictExampleEmpty)
    tempDict["column1"] = "col1"

    with pytest.raises(TypeError):
        pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, tempDict)
    #Should raise a type error because there is a list in the column names

    #Pandas Empty Data
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExampleEmpty, columnDictExample1)
    print(pdDataframe)
    with pytest.raises(ValueError): #Either the dataframe creation should be stopped before this or this should fail
        assert pdDataframe["col1"] == [7,8,9] #Allows for creation of an unaccesable dataframe
        assert pdDataframe["col3"] == None 


def test_create_dataframe_polars_empty():
    #Polars Empty Column Names
    with pytest.raises(ValueError):
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleEmpty)
    #Should raise errors when any of the column values is none

    tempDict = dict(columnDictExampleEmpty)
    tempDict["column1"] = "col1"
    
    with pytest.raises(TypeError):
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, tempDict)
    #Should raise a type error because there is a list in the column names


    #Polars Empty Data
    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExampleEmpty, columnDictExample1)
    print(plDataframe) #Same issue as pandas, allows for creation of unaccessable dataframes with None columns
    with pytest.raises(Exception):
        assert plDataframe["col3"] == [7,8,9] 
        assert plDataframe["col3"] == None  


def test_create_dataframe_ibis_empty():
    #Ibis Empty Column Names
    with pytest.raises(ValueError): 
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleEmpty)
    #Should raise errors when any of the column values is none

    tempDict = dict(columnDictExampleEmpty)
    tempDict["column1"] = "col1"
    
    with pytest.raises(TypeError):
        ibisDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, tempDict)
    #Should raise a type error because there is a list in the column names

    
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
    with pytest.raises(TypeError):
        pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleDirty)
    #Should create a type error because there is a list in the column names

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
    with pytest.raises(TypeError):
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleDirty)
    #Should create a type error because there is a list in the column names

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
    with pytest.raises(TypeError):
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleDirty)
    #Should create a type error because there is a list in the column names

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


def test_create_dataframe_pandas_uneven():
    #Pandas Uneven Column Names
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleUneven)
    assert "col1" in pdDataframe
    assert "column3" in pdDataframe
    assert not "col3" in pdDataframe
    assert pdDataframe["col2"][0] == 4
    assert pdDataframe["column3"][0] == "A"

    columnDictExampleUneven2 = {
	"column1": "col1",
	"column2": "col2",
    "column3": "col3",
    "column4": "col4"
    }
    pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExample1, columnDictExampleUneven2)
    assert "col1" in pdDataframe
    assert "col2" in pdDataframe
    assert "col3" in pdDataframe
    assert not "col4" in pdDataframe

    #If there are not enough column names then the original names will be kept, if there are too many than only the \
    #correct assiociations will be kept. Works properly

    #Pandas Uneven Data
    with pytest.raises(ValueError):
        pdDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, dataDictExampleUneven, columnDictExample1)
    #Creates a value error, does not raise it in the function tho
    #Should test this before it gets to the actual pandas library code


def test_create_dataframe_polars_uneven():
    #Polars Uneven Column Names
    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleUneven)
    assert "col1" in plDataframe
    assert "col2" in plDataframe
    assert "column3" in plDataframe

    columnDictExampleUneven2 = {
	"column1": "col1",
	"column2": "col2",
    "column3": "col3",
    "column4": "col4"
    }
    plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExample1, columnDictExampleUneven2)
    assert "col1" in plDataframe
    assert "col2" in plDataframe
    assert "col3" in plDataframe
    assert not "col4" in plDataframe
    #Seems to work similar to Pandas

    #Polars Uneven Data
    with pytest.raises(Exception):
        plDataframe = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, dataDictExampleUneven, columnDictExample1)
    #Raises an error however it is a ShapeError and is not in the function 


def test_create_dataframe_ibis_uneven():
    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleUneven)
    assert ibisDataFrame.columns == ["col1", "col2", "column3"]

    columnDictExampleUneven2 = {
	"column1": "col1",
	"column2": "col2",
    "column3": "col3",
    "column4": "col4"
    }
    ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExample1, columnDictExampleUneven2)
    assert ibisDataFrame.columns == ["col1", "col2", "col3"]

    with pytest.raises(Exception):
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExampleUneven, columnDictExample1)
    #Raises exact same error as the polars

    dataDictExampleUneven2 = {
	"column1": [2],
	"column2": [4.7, 8.9, 0.002],
	"column3": ["A", "B", "C"]
    }
    with pytest.raises(Exception): 
        ibisDataFrame = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.IBIS.value, dataDictExampleUneven2, columnDictExample1)
    #Intrestingly dataframes can be created with single values (str,int,float) inputed into the creation but lists cannot




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
