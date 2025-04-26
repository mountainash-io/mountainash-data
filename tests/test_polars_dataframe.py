import pytest
from pytest_check import check
from ibis import _
import ibis
import polars as pl
from typing import Any

from functools import lru_cache
from itertools import product

from mountainash_data import IbisDataFrame
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from mountainash_data.dataframes.utils import DataFrameUtils

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

data_dict2 = {
    "A": [1, 2, 3, None],
    "D": [11, 12, 13, 14], #pandas converts this to a float
    "E": ["A", "R", "R", "R"]
}

num_cols = data_dict.__len__()
num_rows = data_dict["A"].__len__()
num_cols_2 = data_dict2.__len__()
num_rows_2 = data_dict2["A"].__len__()

# data_null = {
#     "A": [1, 2, 3],
#     "B": [4, 5, 6],
#     "C": ["A", "B", None]
# }


lru_cache(maxsize=None)
def create_ibis_dataframe(df, ibis_backend_schema) -> IbisDataFrame:
    return IbisDataFrame(df, ibis_backend_schema=ibis_backend_schema)

lru_cache(maxsize=None)
def create_native_dataframe(dataframe_framework, data_dict) -> Any:
    return DataFrameUtils.create_dataframe(dataframe_framework=dataframe_framework, data_dict=data_dict)


dfPandas = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, data_dict=data_dict)
dfPolars = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, data_dict=data_dict)

dfPandas2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, data_dict=data_dict2)
dfPolars2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, data_dict=data_dict2)

native_dataframes = [dfPandas, dfPolars]
native_dataframes2 = [dfPandas2, dfPolars2]


dataframe_frameworks = [CONST_DATAFRAME_FRAMEWORK.PANDAS.value, 
                        CONST_DATAFRAME_FRAMEWORK.POLARS.value
                        ]
ibis_backend_schemas = ["polars", 
                        "duckdb", 
                        "sqlite"
                        ]


# ibis_dataframes=   [(IbisDataFrame(df=dataframe, ibis_backend_schema=backend), backend) for dataframe, backend in product(native_dataframes, ibis_backend_schemas)]
# ibis_dataframes_2= [(IbisDataFrame(df=dataframe, ibis_backend_schema=backend), backend) for dataframe, backend in product(native_dataframes2, ibis_backend_schemas)]

testcases_df_framework_backend =  [(data_dict, dataframe_framework, ibis_backend_schema) for dataframe_framework, ibis_backend_schema in product(dataframe_frameworks, ibis_backend_schemas)]
testcases_df_framework_backend_2 = [(data_dict2, dataframe_framework, ibis_backend_schema) for dataframe_framework, ibis_backend_schema in product(dataframe_frameworks, ibis_backend_schemas)]

# ibis_dataframes= [(case, version[0],version[1] ) for case, version in product(native_dataframes, ibis_backend_schemas)]


#Test creation, different backend schema
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_creation(data_dict, framework, ibis_backend_schema):
    
    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    with check:
        assert ibis_df.ibis_backend_schema == ibis_backend_schema
    with check:
        assert ibis_df.materialise().shape == (num_rows, num_cols)
    with check:
        assert ibis_df.get_column_names().sort() == ["A", "B", "C"].sort()
    with check:
        assert ibis_df.get_column_as_list(column="A") == [1, 2, 3]    

@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend_2)
def test_creation_2(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    print(ibis_df.ibis_df)


    with check:
        assert ibis_df.ibis_backend_schema == ibis_backend_schema
    with check:
        assert ibis_df.materialise().shape == (num_rows_2, num_cols_2)
    with check:
        assert ibis_df.get_column_names().sort() == ["A", "D", "E"].sort()
    with check:
        assert ibis_df.get_column_as_list(column="A") == [1, 2, 3, None]   
    with check:
       assert ibis_df.get_column_as_list(column="D") == [11, 12, 13, 14]  

"""
# Parameterized test for select_columns method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("columns, expected_shape", [
    (["A"],      (num_rows, 1)),
    (["A", "B"], (num_rows, 2)),
])
def test_select_columns(data_dict, framework, ibis_backend_schema, 
                        columns: list[str], expected_shape):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    selected_df = ibis_df.select(columns)
    assert selected_df.to_pandas().shape == expected_shape




@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("column, expected_values", [
    ("A", data_dict["A"]),
    ("B", data_dict["B"]),
    ("C", data_dict["C"])
])
def test_get_column_as_list(data_dict, framework, ibis_backend_schema,
                            column, expected_values):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    selected_df = ibis_df.select(column)
    selected_values = selected_df.get_column_as_list(column=column)
    assert selected_values == expected_values

# Problem
# TODO: This fails only when Pandas is used as the schema, see below for more specific test 


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
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_filter_method(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    expr = _.A == 3
    filtered_df = ibis_df.filter(expr)

    with check:
        assert isinstance(filtered_df, IbisDataFrame)
        assert filtered_df.materialise().shape == (1,3)


# Parameterized test for head method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("n", [
    (0),
    (1),
    (2),
    (3)
])
def test_head_method(data_dict, framework, ibis_backend_schema, 
                     n):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)


    result_df = ibis_df.head(n)

    with check:
        assert isinstance(result_df, IbisDataFrame)
    with check:
        assert result_df.count() == n

# Problem
# TODO: Same as above


# Test case for as_dict method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_as_dict_method(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    result_dict = ibis_df.as_dict()
    assert isinstance(result_dict, dict)

# Test case for get_first_row_as_dict method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_get_first_row_as_dict_method(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    result_first_row_dict = ibis_df.get_first_row_as_dict()
    print(result_first_row_dict)
    with check:    
        assert isinstance(result_first_row_dict, dict)
        assert result_first_row_dict == {'A': 1, 'B': 4, 'C': 'A'}


# Test case for get_row_count method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_get_row_count_method(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    row_count = ibis_df.count()
    
    with check:
        assert row_count == 3


# Test case for rename method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)

@pytest.mark.parametrize("rename_map, expected_columns", [
    ({'new_A': 'A', 'new_B': 'B', 'new_C': 'C'}, ['new_A', 'new_B', 'new_C']),
    ({'new_A': 'A'},                             ['new_A', 'B', 'C']),
    # ({'new_A': 'A', 'new_B': 'D', 'new_C': 'C'}, ['new_A', 'B', 'new_C'])
])
def test_rename_method(data_dict, framework, ibis_backend_schema, 
                       rename_map, expected_columns):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)


    #Issue created: https://github.com/mountainash-io/mountainash-data/issues/22
    # Mapping will need to be validated, similarly to in 
    renamed_df = ibis_df.rename(**rename_map)

    # assert list(renamed_df.materialise().columns) == expected_columns
    # NR: Use get_column_names instead of materialise and pandas column attribute. We want to avoid non-ibis DF framework specific logic.
    assert renamed_df.get_column_names() == expected_columns



# Test case for mutate method
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
def test_mutate_method(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)


    #Don't materialize the dataframe mid-expression. This defeats the purpose of using expressions.    
    # mutate_df = ibis_df.mutate(D = (ibis_df.execute()["A"] + 10))
    # assert list(mutate_df.materialize().columns) == ["A", "B", "C", "D"]
    # result_df = mutate_df.execute()
    # print(result_df)
    # assert list(result_df["D"]) == [11, 12, 13]

    mutate_df = ibis_df.mutate(D = (ibis._["A"] + 10))

    with check:
        assert mutate_df.get_column_names() == ["A", "B", "C", "D"]

    with check:
        assert mutate_df.get_column_as_list("D") == [11, 12, 13]



# Problem
# TODO: Having trouble with actually getting mutate to work properly. Same issues as above with pandas though


#Attempts to Cause Problems:

@pytest.mark.parametrize("data_dict", [data_dict])
@pytest.mark.parametrize("framework", ["Nonsense", None, 4, "Pandas"])
@pytest.mark.parametrize("ibis_backend_schema", ibis_backend_schemas)
#Test creation, incorrect backend schema
def test_creation_incorrect_framework(data_dict, framework, ibis_backend_schema):


    with pytest.raises(ValueError):
        native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
        create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)


@pytest.mark.parametrize("data_dict", [data_dict])
@pytest.mark.parametrize("framework", dataframe_frameworks)
@pytest.mark.parametrize("ibis_backend_schema", ["Nonsense", 4, "Pandas"])
#Test creation, incorrect backend schema
def test_creation_incorrect_backend(data_dict, framework, ibis_backend_schema):

    with pytest.raises(ValueError):
        native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
        create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

@pytest.mark.parametrize("data_dict", [data_dict])
@pytest.mark.parametrize("framework", dataframe_frameworks)
@pytest.mark.parametrize("ibis_backend_schema", [None])
#Test creation, incorrect backend schema
def test_creation_none_backend(data_dict, framework, ibis_backend_schema):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    assert isinstance(ibis_df, IbisDataFrame)
"""

"""
# Attempts to break SELECT method
@pytest.mark.parametrize("values", [
    (dfPandas),
    (dfPolars),
    ("TEST"),
    ([4,"break"]),
    (4)
])
def test_break_select_method_polars(values):
    ibis_df = IbisDataFrame(dfPolars)
    # with pytest.raises(Exception):                      #Problem
    #     selected_df = ibis_df.select(values)            #Doesn't raise exceptions for dfpandas and 4. Should raise something like:
                                                        #Unknown column. Please provide a valid column name or list of column names.

    #What does this return? Should it return an empty dataframe, matching columns only, or raise an exception?
    selected_df = ibis_df.select(values)        
    print(selected_df)
@pytest.mark.parametrize("values", [
    (dfPandas),
    (dfPolars),
    ("TEST"),
    ([4,"break"]),
    (4)
])
def test_break_select_method_pandas(values):        
    ibis_df = IbisDataFrame(dfPandas)
    with pytest.raises(Exception):
        selected_df = ibis_df.select(values)            #Same as above, but for dfPandas as starting df

    selected_df = ibis_df.select(values)            #Same as above, but for dfPandas as starting df
    print(selected_df)

"""


# Parameterized test for select_columns method

# @pytest.mark.parametrize("columns, expected_shape", [
#     (["A"], (3, 1)),
#     (["A", "B"], (3, 2)),
#     (["A"], (3, 1)),
#     (["A", "B"], (3, 2))
# ])
# def test_select_columns_specifics(columns: list[str], expected_shape):
#     with check:
#         selected_df = ibisPolarsPolars.select(columns)
#         assert selected_df.materialise().shape == expected_shape

#         selected_df = ibisPolarsDuckDB.select(columns)
#         assert selected_df.materialise().shape == expected_shape

#         selected_df = ibisPolarsSqlite.select(columns)
#         assert selected_df.materialise().shape == expected_shape

#         selected_df = ibisPandasPolars.select(columns)
#         assert selected_df.materialise().shape == expected_shape

#         selected_df = ibisPandasDuckDB.select(columns)
#         assert selected_df.materialise().shape == expected_shape

#         selected_df = ibisPandasSqlite.select(columns)
#         assert selected_df.materialise().shape == expected_shape

    # with pytest.raises(Exception):
    #     selected_df = ibisPandasPandas.select(columns)
    #     assert selected_df.materialise().shape == expected_shape

    # with pytest.raises(Exception):
    #     selected_df = ibisPolarsPandas.select(columns)
    #     assert selected_df.materialise().shape == expected_shape



#Union and Join Tests

#We seem to have a problem with joins involving memtables and sql backends
# https://github.com/mountainash-io/mountainash-data/issues/23

#Alternative data sets
# data_dict2 = {
#     "A": [1, 2, 3, 4],
#     "D": [11, 12, 13, 14],
#     "E": ["A", "R", "R", "R"]
# }

# dfPandas2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, data_dict=data_dict2)
# dfPolars2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, data_dict=data_dict2)


# # Different Variations of IbisDataFrame
# # ibisPandasPandas2 = IbisDataFrame(dfPandas2, ibis_backend_schema="pandas")
# ibisPandasPolars2 = IbisDataFrame(dfPandas2, ibis_backend_schema="polars")
# ibisPandasDuckDB2 = IbisDataFrame(dfPandas2, ibis_backend_schema="duckdb")
# ibisPandasSqlite2 = IbisDataFrame(dfPandas2, ibis_backend_schema="sqlite")
# ibisPolarsPolars2 = IbisDataFrame(dfPolars2, ibis_backend_schema="polars")
# # ibisPolarsPandas2 = IbisDataFrame(dfPolars2, ibis_backend_schema="pandas")
# ibisPolarsDuckDB2 = IbisDataFrame(dfPolars2, ibis_backend_schema="duckdb")
# ibisPolarsSqlite2 = IbisDataFrame(dfPolars2, ibis_backend_schema="sqlite")



# Test inner join
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_inner_join_one(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on ):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)


    joined_df = ibis_df.inner_join(ibis_df_2, predicates=["A"], execute_on=execute_on)

    with check: 
        assert joined_df.materialise().shape == (3, 5)
    with check: 
        #assert column names are same, rgardless of order
        assert joined_df.get_column_names().sort() == ["A", "B", "C", "D", "E"].sort()
    with check: 
        assert joined_df.get_column_as_list(column="A") == [1, 2, 3]
    with check: 
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6]
    with check: 
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13]
    with check: 
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R"]


@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_inner_join_two(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on ):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)


    joined_df = ibis_df_2.inner_join(ibis_df, predicates=["A"], execute_on=execute_on)

    with check: 
        assert joined_df.materialise().shape == (3, 5)
    with check: 
        #assert column names are same, rgardless of order
        assert joined_df.get_column_names().sort() == ["A", "B", "C", "D", "E"].sort()
    with check: 
        assert joined_df.get_column_as_list(column="A") == [1, 2, 3]
    with check: 
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6]
    with check: 
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13]
    with check: 
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R"]



#Left Joins
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_left_join_one(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on ):

    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)


    joined_df = ibis_df.left_join(ibis_df_2, predicates=["A"], execute_on=execute_on)

    with check: 
        assert joined_df.materialise().shape == (3, 6)
    with check: 
        #assert column names are same, rgardless of order
        assert joined_df.get_column_names().sort() == ["A", "B", "C", "D", "E", "A_right"].sort()
    with check: 
        assert joined_df.get_column_as_list(column="A") == [1, 2, 3]
    with check: 
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6]
    with check: 
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13]
    with check: 
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R"]



@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_left_join_two(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on 
                        ):
    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)


    joined_df = ibis_df_2.outer_join(right=ibis_df, predicates=["A"], execute_on=execute_on)


    with check:
        assert joined_df.materialise().shape == (4, 6)
    with check:
        assert joined_df.get_column_names().sort() == ["A", "B", "C", "D", "E", "A_right",].sort()       
    
    with check:
        assert joined_df.get_column_as_list(column="A") ==  [1, 2, 3, None]
    with check:
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6, None]
    with check:
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13, 14]
    with check:
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R", "R"]




#Test Outer Join
@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_outer_join_one(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on 
                        ):
    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)

    print(f"ibis_df: {ibis_df.as_dict()}")
    print(f"ibis_df_2: {ibis_df_2.as_dict()}")
    print(f"ibis_df_2: {ibis_df_2}")


    joined_df = ibis_df.outer_join(right=ibis_df_2, predicates=["A"], execute_on=execute_on)
    print(f"joined_df: {joined_df.as_dict()}")


    with check:
        assert joined_df.materialise().shape == (4, 6)
    with check:
        assert joined_df.get_column_names().sort() == ["A", "B", "C","A_right", "D", "E"].sort()       
    
    with check:
        assert joined_df.get_column_as_list(column="A") ==  [1, 2, 3, None]
    with check:
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6, None]
    with check:
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13, 14]
    with check:
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R", "R"]

@pytest.mark.parametrize("data_dict, framework, ibis_backend_schema", testcases_df_framework_backend)
@pytest.mark.parametrize("data_dict_2, framework_2, ibis_backend_schema_2", testcases_df_framework_backend_2)
@pytest.mark.parametrize("execute_on", ["left", "right", None])
def test_outer_join_two(data_dict, framework, ibis_backend_schema, 
                        data_dict_2, framework_2, ibis_backend_schema_2, 
                        execute_on 
                        ):
    native_df = create_native_dataframe(dataframe_framework=framework, data_dict=data_dict)
    ibis_df = create_ibis_dataframe(df=native_df, ibis_backend_schema=ibis_backend_schema)

    native_df_2 = create_native_dataframe(dataframe_framework=framework_2, data_dict=data_dict_2)
    ibis_df_2 = create_ibis_dataframe(df=native_df_2, ibis_backend_schema=ibis_backend_schema_2)


    print(f"ibis_df: {ibis_df.as_dict()}")
    print(f"ibis_df_2: {ibis_df_2.as_dict()}")
    print(f"ibis_df_2: {ibis_df_2}")

    joined_df = ibis_df_2.outer_join(right=ibis_df, predicates=["A"], execute_on=execute_on)
    print(f"joined_df: {joined_df.as_dict()}")

    with check:
        assert joined_df.materialise().shape == (4, 6)
    with check:
        assert joined_df.get_column_names().sort() == ["A", "B", "C","A_right", "D", "E"].sort()       
    
    with check:
        assert joined_df.get_column_as_list(column="A") ==  [1, 2, 3, None]
    with check:
        assert joined_df.get_column_as_list(column="B") == [4, 5, 6, None]
    with check:
        assert joined_df.get_column_as_list(column="D") == [11, 12, 13, 14]
    with check:
        assert joined_df.get_column_as_list(column="E") == ["A", "R", "R", "R"]
