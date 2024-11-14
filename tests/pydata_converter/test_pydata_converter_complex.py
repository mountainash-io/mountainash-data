from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from pytest_check import check
import pytest
import polars as pl
from pydantic import BaseModel, Field

from mountainash_data.dataframes.utils.pydata_converter import (
    BasePyDataConverter,
    InputType,
    PyDataConverterFactory,
    PyDataConverterDataclass,
    PyDataConverterPydantic,
    PyDataConverterPydict,
    PyDataConverterPylist
)

# Complex test data structures
class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"

@dataclass
class Address:
    street: str
    city: str
    country: str
    postal_code: str

@dataclass
class Transaction:
    id: str
    amount: Decimal
    date: datetime
    status: Status
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ComplexPerson:
    name: str
    age: int
    address: Address
    transactions: List[Transaction]
    active: bool
    created_at: datetime
    tags: List[str] = field(default_factory=list)
    preferences: Dict[str, Any] = field(default_factory=dict)

class AddressModel(BaseModel):
    street: str
    city: str
    country: str
    postal_code: str

class TransactionModel(BaseModel):
    id: str
    amount: Decimal
    date: datetime
    status: Status
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ComplexPersonModel(BaseModel):
    name: str
    age: int
    address: AddressModel
    transactions: List[TransactionModel]
    active: bool
    created_at: datetime
    tags: List[str] = Field(default_factory=list)
    preferences: Dict[str, Any] = Field(default_factory=dict)

def test_complex_nested_dataclass():
    converter = PyDataConverterDataclass()
    
    # Create complex nested data
    address = Address("123 Main St", "City", "Country", "12345")
    transaction = Transaction(
        id="tx1",
        amount=Decimal("100.50"),
        date=datetime.now(),
        status=Status.ACTIVE,
        metadata={"key": "value"}
    )
    
    person = ComplexPerson(
        name="John",
        age=30,
        address=address,
        transactions=[transaction],
        active=True,
        created_at=datetime.now(),
        tags=["tag1", "tag2"],
        preferences={"theme": "dark"}
    )

    # Test single instance
    df = converter.convert(person)
    with check:
        assert isinstance(df, pl.DataFrame)
        assert "address" in df.columns
        assert "transactions" in df.columns
        assert df.shape[0] == 1

    # Test list of instances
    people = [person, person]
    df_list = converter.convert(people)
    with check:
        assert isinstance(df_list, pl.DataFrame)
        assert df_list.shape[0] == 2
        assert all(col in df_list.columns for col in [
            "name", "age", "address", "transactions", "active", 
            "created_at", "tags", "preferences"
        ])

def test_edge_cases():
    # Test empty collections
    dict_converter = PyDataConverterPydict()
    list_converter = PyDataConverterPylist()
    
    # Empty dict of lists
    empty_dict = {"a": [], "b": []}
    df_empty_dict = dict_converter.convert(empty_dict)
    with check:
        assert isinstance(df_empty_dict, pl.DataFrame)
        assert df_empty_dict.shape == (0, 2)

    # Empty list of dicts
    df_empty_list = list_converter.convert([])
    with check:
        assert isinstance(df_empty_list, pl.DataFrame)
        assert df_empty_list.shape == (0, 0)


    # None values in collections
    dict_with_nones = {"a": [1, None, 3], "b": [None, 2, None]}
    df_with_nones = dict_converter.convert(dict_with_nones)
    with check:
        assert isinstance(df_with_nones, pl.DataFrame)
        assert df_with_nones.shape == (3, 2)

def test_type_conversions():
    # Test various data types
    data = {
        "integers": [1, 2, 3],
        "floats": [1.1, 2.2, 3.3],
        "strings": ["a", "b", "c"],
        "booleans": [True, False, True],
        "dates": [date.today(), date.today(), date.today()],
        "datetimes": [datetime.now(), datetime.now(), datetime.now()],
        "decimals": [Decimal("1.1"), Decimal("2.2"), Decimal("3.3")],
        "nullable": [1, None, 3]
    }

    converter = PyDataConverterPydict()
    df = converter.convert(data)
    
    with check:
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (3, 8)
        for col in df.columns:
            assert df[col].dtype in [
                pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, 
                pl.Date, pl.Datetime, pl.Decimal
            ]

def test_column_mapping_edge_cases():
    converter = PyDataConverterPydict()
    data = {"a": [1, 2], "b": [3, 4], "c": [5, 6]}

    # Test partial mapping
    partial_mapping = {"a": "col_a", "b": "col_b"}
    df_partial = converter.convert(data, partial_mapping)
    with check:
        assert "col_a" in df_partial
        assert "col_b" in df_partial
        assert "c" in df_partial

    # Test mapping with extra fields
    extra_mapping = {"a": "col_a", "b": "col_b", "d": "col_d"}
    df_extra = converter.convert(data, extra_mapping)
    with check:
        assert "col_a" in df_extra.columns
        assert "col_b" in df_extra.columns
        assert "c" in df_extra.columns
        assert "col_d" not in df_extra.columns

def test_invalid_column_mappings():
    converter = PyDataConverterPydict()
    data = {"a": [1, 2], "b": [3, 4]}

    # Test duplicate target names
    with pytest.raises(ValueError):
        converter.convert(data, {"a": "new_col", "b": "new_col"})
        # Should raise error for duplicate target names

    # Test invalid mapping types
    with pytest.raises((ValueError, TypeError)):
        converter.convert(data, {"a": 1, "b": "col_b"})

def test_mixed_data_types():
    # Test handling of mixed data types within columns
    data = {
        "mixed": [1, "2", 3.0],
        "mixed_nullable": [1, None, "3"]
    }
    
    converter = PyDataConverterPydict()
    df = converter.convert(data)
    
    with check:
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (3, 2)
        # Polars should coerce to common type or string
        assert df["mixed"].dtype in [pl.Utf8, pl.Float64]
