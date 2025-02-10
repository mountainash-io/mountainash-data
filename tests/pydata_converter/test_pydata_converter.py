from dataclasses import dataclass
from typing import List, Optional
from pytest_check import check
import pytest
import polars as pl
from pydantic import BaseModel

from mountainash_data.dataframes.utils.pydata_converter import (
    BasePyDataConverter,
    InputType,
    PyDataConverterFactory,
    PyDataConverterDataclass,
    PyDataConverterPydantic,
    PyDataConverterPydict,
    PyDataConverterPylist
)

@dataclass
class Person:
    name: str
    age: int
    email: Optional[str] = None

class PersonPydantic(BaseModel):
    name: str
    age: int
    email: Optional[str] = None

def test_converter_factory():
    # Test factory returns correct converter for each type
    dict_data = {"a": [1, 3], "b": [2, 4]}
    list_data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    dataclass_data = Person(name="John", age=30)
    pydantic_data = PersonPydantic(name="John", age=30)

    with check:
        assert isinstance(PyDataConverterFactory.get_strategy(dict_data), PyDataConverterPydict)
        assert isinstance(PyDataConverterFactory.get_strategy(list_data), PyDataConverterPylist)
        assert isinstance(PyDataConverterFactory.get_strategy(dataclass_data), PyDataConverterDataclass)
        assert isinstance(PyDataConverterFactory.get_strategy(pydantic_data), PyDataConverterPydantic)

def test_pydict_converter():
    converter = PyDataConverterPydict()
    data = {"a": [1, 3], "b": [2, 4]}
    
    # Test can_handle
    with check:
        assert converter.can_handle(data) is True
        assert converter.can_handle([1, 2, 3]) is False

    # Test validation
    with check:
        converter.validate(data)  # Should not raise
        with pytest.raises(ValueError):
            converter.validate({"a": [1, 2], "b": [1]})  # Uneven lengths

    # Test conversion
    df = converter.convert(data)
    with check:
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 2)
        assert df.columns == ["a", "b"]
        assert df.to_dict(as_series=False) == data

def test_pylist_converter():
    converter = PyDataConverterPylist()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    
    # Test can_handle
    with check:
        assert converter.can_handle(data) is True
        assert converter.can_handle({"a": [1, 2]}) is False

    # Test validation
    with check:
        converter.validate(data)  # Should not raise
        with pytest.raises(ValueError):
            converter.validate([{"a": 1, "b": 2}, {"a": 3}])  # Inconsistent keys

    # Test conversion
    df = converter.convert(data)
    with check:
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 2)
        assert df.columns == ["a", "b"]
        assert df.to_dicts() == data

def test_dataclass_converter():
    converter = PyDataConverterDataclass()
    single_data = Person(name="John", age=30)
    list_data = [
        Person(name="John", age=30),
        Person(name="Jane", age=25, email="jane@example.com")
    ]
    
    # Test can_handle
    with check:
        assert converter.can_handle(single_data) is True
        assert converter.can_handle(list_data) is True
        assert converter.can_handle({"name": "John"}) is False

    # Test validation
    with check:
        converter.validate(single_data)  # Should not raise
        converter.validate(list_data)    # Should not raise
        with pytest.raises(ValueError):
            # Mix of different types
            converter.validate([single_data, PersonPydantic(name="Jane", age=25)])

    # Test conversion - single instance
    df1 = converter.convert(single_data)
    with check:
        assert isinstance(df1, pl.DataFrame)
        assert df1.shape == (1, 3)
        assert set(df1.columns) == {"name", "age", "email"}

    # Test conversion - list of instances
    df2 = converter.convert(list_data)
    with check:
        assert isinstance(df2, pl.DataFrame)
        assert df2.shape == (2, 3)
        assert set(df2.columns) == {"name", "age", "email"}

def test_pydantic_converter():
    converter = PyDataConverterPydantic()
    single_data = PersonPydantic(name="John", age=30)
    list_data = [
        PersonPydantic(name="John", age=30),
        PersonPydantic(name="Jane", age=25, email="jane@example.com")
    ]
    
    # Test can_handle
    with check:
        assert converter.can_handle(single_data) is True
        assert converter.can_handle(list_data) is True
        assert converter.can_handle({"name": "John"}) is False

    # Test validation
    with check:
        converter.validate(single_data)  # Should not raise
        converter.validate(list_data)    # Should not raise
        with pytest.raises(ValueError):
            # Mix of different types
            converter.validate([single_data, Person(name="Jane", age=25)])

    # Test conversion - single instance
    df1 = converter.convert(single_data)
    with check:
        assert isinstance(df1, pl.DataFrame)
        assert df1.shape == (1, 3)
        assert set(df1.columns) == {"name", "age", "email"}

    # Test conversion - list of instances
    df2 = converter.convert(list_data)
    with check:
        assert isinstance(df2, pl.DataFrame)
        assert df2.shape == (2, 3)
        assert set(df2.columns) == {"name", "age", "email"}

def test_column_mapping():
    # Test column mapping with each converter type
    column_mapping = {"a": "column_a", "b": "column_b"}
    
    # Dictionary of lists
    dict_data = {"a": [1, 3], "b": [2, 4]}
    df1 = PyDataConverterPydict().convert(dict_data, column_mapping)
    with check:
        assert df1.columns == ["column_a", "column_b"]

    # List of dictionaries
    list_data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    df2 = PyDataConverterPylist().convert(list_data, column_mapping)
    with check:
        assert df2.columns == ["column_a", "column_b"]

    # Dataclass with mapping
    dataclass_mapping = {"name": "full_name", "age": "years"}
    people = [
        Person(name="John", age=30),
        Person(name="Jane", age=25, email="jane@example.com")
    ]
    df3 = PyDataConverterDataclass().convert(people, dataclass_mapping)
    with check:
        assert "full_name" in df3.columns
        assert "years" in df3.columns

    # Pydantic with mapping
    pydantic_people = [
        PersonPydantic(name="John", age=30),
        PersonPydantic(name="Jane", age=25, email="jane@example.com")
    ]
    df4 = PyDataConverterPydantic().convert(pydantic_people, dataclass_mapping)
    with check:
        assert "full_name" in df4.columns
        assert "years" in df4.columns

def test_invalid_inputs():
    # Test factory with invalid input
    with pytest.raises(ValueError):
        PyDataConverterFactory.get_strategy([1, 2, 3])  # Plain list

    # Test converters with None
    converters = [
        PyDataConverterPydict(),
        PyDataConverterPylist(),
        PyDataConverterDataclass(),
        PyDataConverterPydantic()
    ]
    
