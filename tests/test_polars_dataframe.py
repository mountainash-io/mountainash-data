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


# Different Variations of IbisDataFrame
ibisPandasPandas = IbisDataFrame(dfPandas, ibis_backend_schema="pandas")
ibisPandasPolars = IbisDataFrame(dfPandas, ibis_backend_schema="polars")
ibisPandasDuckDB = IbisDataFrame(dfPandas, ibis_backend_schema="duckdb")
ibisPandasSqlite = IbisDataFrame(dfPandas, ibis_backend_schema="sqlite")
ibisPolarsPolars = IbisDataFrame(dfPolars, ibis_backend_schema="polars")
ibisPolarsPandas = IbisDataFrame(dfPolars, ibis_backend_schema="pandas")
ibisPolarsDuckDB = IbisDataFrame(dfPolars, ibis_backend_schema="duckdb")
ibisPolarsSqlite = IbisDataFrame(dfPolars, ibis_backend_schema="sqlite")

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

# Parameterized test for select_columns method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("columns, expected_shape", [
    (["A"], (3, 1)),
    (["A", "B"], (3, 2)),
    (["A"], (3, 1)),
    (["A", "B"], (3, 2))
])
def test_select_columns(columns: list[str], expected_shape, ibis_df):
    selected_df = ibis_df.select(columns)
    assert selected_df.materialise().shape == expected_shape
"""
Problem
TODO: This fails only when Pandas is used as the schema, see below for more specific test

"""


@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("column, expected_values", [
    ("A", [1, 2, 3]),
    ("C", ['A', 'B', 'C']),
    ("A", [1, 2, 3]),
    ("C", ['A', 'B', 'C'])
])
def test_get_column_as_list(column, expected_values, ibis_df):
    selected_df = ibis_df.select(column)
    selected_values = selected_df.get_column_as_list(column)
    assert selected_values == expected_values

"""
Problem
TODO: This fails only when Pandas is used as the schema, see below for more specific test 

"""

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
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
def test_filter_method(ibis_df):
    expr = _.A == 3
    filtered_df = ibis_df.filter(expr)

    with check:
        assert isinstance(filtered_df, IbisDataFrame)
        assert filtered_df.materialise().shape == (1,3)
"""
Problem
TODO: Same as above

"""
# Parameterized test for head method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("n, expected_shape", [
    (0, 0),
    (1, 1),
    (2, 2),
    (3, 3),
    (0, 0),
    (1, 1),
    (2, 2),
    (3, 3)
])
def test_head_method(ibis_df, n, expected_shape):
    result_df = ibis_df.head(n)

    with check:
        assert isinstance(result_df, IbisDataFrame)
        assert result_df.count() == expected_shape
"""
Problem
TODO: Same as above

"""


# Test case for as_dict method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
def test_as_dict_method(ibis_df):
    result_dict = ibis_df.as_dict()
    assert isinstance(result_dict, dict)

# Test case for get_first_row_as_dict method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
def test_get_first_row_as_dict_method(ibis_df):
    result_first_row_dict = ibis_df.get_first_row_as_dict()
    print(result_first_row_dict)
    with check:    
        assert isinstance(result_first_row_dict, dict)
        assert result_first_row_dict == {'A': 1, 'B': 4, 'C': 'A'}

"""
Problem
TODO: Same as above

"""

# Test case for get_row_count method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
def test_get_row_count_method(ibis_df):
    #ibis_df = IbisDataFrame(baseDF)
    row_count = ibis_df.count()
    
    with check:
        assert row_count == 3


# Test case for rename method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("rename_map, expected_columns", [
    ({'new_A': 'A', 'new_B': 'B', 'new_C': 'C'}, ['new_A', 'new_B', 'new_C']),
    ({'new_A': 'A'}, ['new_A', 'B', 'C']),
    ({'new_A': 'A', 'new_B': 'D', 'new_C': 'C'}, ['new_A', 'B', 'new_C'])
])
def test_rename_method(ibis_df, rename_map, expected_columns):
    renamed_df = ibis_df.rename(**rename_map)
    assert list(renamed_df.materialise().columns) == expected_columns

"""
Problem
TODO: Same pandas schema issue, also throws an error if the column name is not in the dataframe. Should just ignore the column name if it doesn't exist


# Test case for mutate method
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
def test_mutate_method(ibis_df):
    mutate_df = ibis_df.mutate(D = (ibis_df.execute()["A"] + 10))
    assert list(mutate_df.materialize().columns) == ["A", "B", "C", "D"]
    result_df = mutate_df.execute()
    print(result_df)
    assert list(result_df["D"]) == [11, 12, 13]


Problem
TODO: Having trouble with actually getting mutate to work properly. Same issues as above with pandas though
"""




#Attempts to Cause Problems:

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
    with pytest.raises(Exception):                      #Problem
        selected_df = ibis_df.select(values)            #Doesn't raise exceptions for dfpandas and 4. Should raise something like:
                                                        #Unknown column. Please provide a valid column name or list of column names.
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


# Parameterized test for select_columns method

@pytest.mark.parametrize("columns, expected_shape", [
    (["A"], (3, 1)),
    (["A", "B"], (3, 2)),
    (["A"], (3, 1)),
    (["A", "B"], (3, 2))
])
def test_select_columns_specifics(columns: list[str], expected_shape):
    with check:
        selected_df = ibisPolarsPolars.select(columns)
        assert selected_df.materialise().shape == expected_shape

        selected_df = ibisPolarsDuckDB.select(columns)
        assert selected_df.materialise().shape == expected_shape

        selected_df = ibisPolarsSqlite.select(columns)
        assert selected_df.materialise().shape == expected_shape

        selected_df = ibisPandasPolars.select(columns)
        assert selected_df.materialise().shape == expected_shape

        selected_df = ibisPandasDuckDB.select(columns)
        assert selected_df.materialise().shape == expected_shape

        selected_df = ibisPandasSqlite.select(columns)
        assert selected_df.materialise().shape == expected_shape

    with pytest.raises(Exception):
        selected_df = ibisPandasPandas.select(columns)
        assert selected_df.materialise().shape == expected_shape

    with pytest.raises(Exception):
        selected_df = ibisPolarsPandas.select(columns)
        assert selected_df.materialise().shape == expected_shape






#Union and Join Tests

#Alternative data sets
data_dict2 = {
    "A": [1, 2, 3, 4],
    "D": [11, 12, 13, 14],
    "E": ["A", "R", "R", "R"]
}

dfPandas2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.PANDAS.value, data_dict=data_dict2)
dfPolars2 = DataFrameUtils.create_dataframe(CONST_DATAFRAME_FRAMEWORK.POLARS.value, data_dict=data_dict2)


# Different Variations of IbisDataFrame
ibisPandasPandas2 = IbisDataFrame(dfPandas2, ibis_backend_schema="pandas")
ibisPandasPolars2 = IbisDataFrame(dfPandas2, ibis_backend_schema="polars")
ibisPandasDuckDB2 = IbisDataFrame(dfPandas2, ibis_backend_schema="duckdb")
ibisPandasSqlite2 = IbisDataFrame(dfPandas2, ibis_backend_schema="sqlite")
ibisPolarsPolars2 = IbisDataFrame(dfPolars2, ibis_backend_schema="polars")
ibisPolarsPandas2 = IbisDataFrame(dfPolars2, ibis_backend_schema="pandas")
ibisPolarsDuckDB2 = IbisDataFrame(dfPolars2, ibis_backend_schema="duckdb")
ibisPolarsSqlite2 = IbisDataFrame(dfPolars2, ibis_backend_schema="sqlite")


# Test inner join
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("ibis_df_two", [
    (ibisPandasPandas2),
    (ibisPandasPolars2),
    (ibisPandasDuckDB2),
    (ibisPandasSqlite2),
    (ibisPolarsPolars2),
    (ibisPolarsPandas2),
    (ibisPolarsDuckDB2),
    (ibisPolarsSqlite2),
])
def test_inner_join_one(ibis_df, ibis_df_two):
    joined_df = ibis_df.inner_join(ibis_df_two, predicates=["A"])
    assert joined_df.materialise().shape == (3, 5)
    assert list(joined_df.materialise().columns) == ["A", "B", "C", "D", "E"]
    
    result = joined_df.execute()
    assert list(result["A"]) == [1, 2, 3]
    assert list(result["B"]) == [4, 5, 6]
    assert list(result["D"]) == [11, 12, 13]
    assert list(result["E"]) == ["A", "R", "R"]


@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("ibis_df_two", [
    (ibisPandasPandas2),
    (ibisPandasPolars2),
    (ibisPandasDuckDB2),
    (ibisPandasSqlite2),
    (ibisPolarsPolars2),
    (ibisPolarsPandas2),
    (ibisPolarsDuckDB2),
    (ibisPolarsSqlite2),
])
def test_inner_join_two(ibis_df, ibis_df_two):
    joined_df = ibis_df_two.inner_join(ibis_df, predicates=["A"])
    #assert joined_df.materialise().shape == (3, 5)
    #assert list(joined_df.materialise().columns) == ["A", "D", "E", "B", "C"]
    
    result = joined_df.execute()
    assert list(result["A"]) == [1, 2, 3]
    assert list(result["B"]) == [4, 5, 6]
    assert list(result["D"]) == [11, 12, 13]
    assert list(result["E"]) == ["A", "R", "R"]


#Problem Combinations: This ignores the pandas schemas as they all fail no matter the combo. Error actually happens whenever it attempts to execute or materiralise
"""




"""
@pytest.mark.parametrize("ibis_df, ibis_df_two", [
    (ibisPandasDuckDB, ibisPandasDuckDB2),
    (ibisPandasDuckDB, ibisPolarsDuckDB2),
    (ibisPandasSqlite, ibisPandasSqlite2),
    (ibisPandasSqlite, ibisPolarsSqlite2),
    (ibisPolarsDuckDB, ibisPandasDuckDB2),
    (ibisPolarsDuckDB, ibisPolarsDuckDB2),
    (ibisPolarsSqlite, ibisPandasSqlite2),
    (ibisPolarsSqlite, ibisPolarsSqlite2)
])

def test_inner_join_issues_one(ibis_df, ibis_df_two):
    with pytest.raises(Exception):
        joined_df = ibis_df.inner_join(ibis_df_two, predicates=["A"])
        print(joined_df.execute())


#Test Outer Join
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("ibis_df_two", [
    (ibisPandasPandas2),
    (ibisPandasPolars2),
    (ibisPandasDuckDB2),
    (ibisPandasSqlite2),
    (ibisPolarsPolars2),
    (ibisPolarsPandas2),
    (ibisPolarsDuckDB2),
    (ibisPolarsSqlite2),
])
def test_outer_join_one(ibis_df, ibis_df_two):
    joined_df = ibis_df.outer_join(ibis_df_two, predicates=["A"])
    with check:
        assert joined_df.materialise().shape == (4, 6)
        assert list(joined_df.materialise().columns) == ["A", "B", "C","A_right", "D", "E"]
        
        result = joined_df.execute()
        print(result)
        assert list(result["A"]) == [1, 2, 3, None]
        assert list(result["B"]) == [4.0, 5.0, 6.0, None]
        assert list(result["D"]) == [11, 12, 13, 14]
        assert list(result["E"]) == ["A", "R", "R", "R"]
"""
Problem
TODO: Outer join returns different results for different combos.
for pa_pl + pa_duck, pa_pl + pa_sql, pa_pl + pl_duck, pa_pl + pl_sql, pa_duck + pa_duck, pa_duck + pl_pl, pa_duck + pl_sql, pa_sql + pa_duck, pa_sql + pl_pl,
pa_sql + pl_duck, pl_pl + pa_pl, pl_pl + pa_duck, pl_pl + pl_pl, pl_pl + pl_sql, pl_duck + pa_duck, pl_duck + pl_pl 

it returned:

    A    B     C  A_right   D  E
0  1.0  4.0     A        1  11  A
1  2.0  5.0     B        2  12  R
2  3.0  6.0     C        3  13  R
3  NaN  NaN  None        4  14  R

While for the rest of the combinations, it returned:
shape: (4, 6)
┌──────┬──────┬──────┬─────────┬─────┬─────┐
│ A    ┆ B    ┆ C    ┆ A_right ┆ D   ┆ E   │
│ ---  ┆ ---  ┆ ---  ┆ ---     ┆ --- ┆ --- │
│ i64  ┆ i64  ┆ str  ┆ i64     ┆ i64 ┆ str │
╞══════╪══════╪══════╪═════════╪═════╪═════╡
│ 1    ┆ 4    ┆ A    ┆ 1       ┆ 11  ┆ A   │
│ 2    ┆ 5    ┆ B    ┆ 2       ┆ 12  ┆ R   │
│ 3    ┆ 6    ┆ C    ┆ 3       ┆ 13  ┆ R   │
│ null ┆ null ┆ null ┆ 4       ┆ 14  ┆ R   │
└──────┴──────┴──────┴─────────┴─────┴─────┘


And once more there seems to be an issue with the pandas schema. It breaks on what feels like everything.
"""

#Noisy tests that will only become useful once the above issues are resolved
"""
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPandas),
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsPandas),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("ibis_df_two", [
    (ibisPandasPandas2),
    (ibisPandasPolars2),
    (ibisPandasDuckDB2),
    (ibisPandasSqlite2),
    (ibisPolarsPolars2),
    (ibisPolarsPandas2),
    (ibisPolarsDuckDB2),
    (ibisPolarsSqlite2),
])
def test_outer_join_two(ibis_df, ibis_df_two):
    joined_df = ibis_df_two.outer_join(ibis_df, predicates=["A"])
    assert joined_df.materialise().shape == (4, 6)
    assert list(joined_df.materialise().columns) == ["A", "D", "E","A_right", "B", "C"]
    
    result = joined_df.execute()
    assert list(result["A"]) == [1, 2, 3, 4]
    assert list(result["B"]) == [4, 5, 6, None]
    assert list(result["D"]) == [11, 12, 13, 14]
    assert list(result["E"]) == ["A", "R", "R", "R"]


#Problem Combinations: This ignores the pandas schemas as they all fail no matter the combo 
@pytest.mark.parametrize("ibis_df, ibis_df_two", [
    (ibisPandasDuckDB, ibisPandasDuckDB2),
    (ibisPandasDuckDB, ibisPolarsDuckDB2),
    (ibisPandasSqlite, ibisPandasSqlite2),
    (ibisPandasSqlite, ibisPolarsSqlite2),
    (ibisPolarsDuckDB, ibisPandasDuckDB2),
    (ibisPolarsDuckDB, ibisPolarsDuckDB2),
    (ibisPolarsSqlite, ibisPandasSqlite2),
    (ibisPolarsSqlite, ibisPolarsSqlite2)
])

def test_inner_join_issues_one(ibis_df, ibis_df_two):
    with pytest.raises(Exception):
        joined_df = ibis_df.outer_join(ibis_df_two, predicates=["A"])
        print(joined_df.execute())



#Prints out all the working outer joins
@pytest.mark.parametrize("ibis_df", [
    (ibisPandasPolars),
    (ibisPandasDuckDB),
    (ibisPandasSqlite),
    (ibisPolarsPolars),
    (ibisPolarsDuckDB),
    (ibisPolarsSqlite)
])
@pytest.mark.parametrize("ibis_df_two", [
    (ibisPandasPolars2),
    (ibisPandasDuckDB2),
    (ibisPandasSqlite2),
    (ibisPolarsPolars2),
    (ibisPolarsDuckDB2),
    (ibisPolarsSqlite2),
])
def test_outer_join_one(ibis_df, ibis_df_two):
    joined_df = ibis_df.outer_join(ibis_df_two, predicates=["A"])        
    result = joined_df.execute()
    print("<?><")
    print(result)
    print("><?>")
    assert True == False
"""