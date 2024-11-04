
from enum import Enum, auto
from typing import Type
import polars as pl
from datetime import datetime, date

class NullStrategy(Enum):
    """Strategy for handling null values in columns."""
    ALLOW = auto()          # Allow nulls to remain as nulls
    REJECT = auto()         # Raise error if nulls are found
    DEFAULT = auto()        # Replace nulls with a default value
    DROP_ROW = auto()       # Drop rows containing nulls

class DataType(Enum):
    """Supported data types with their Polars equivalents."""
    STRING = pl.Utf8
    INTEGER = pl.Int64
    FLOAT = pl.Float64
    BOOLEAN = pl.Boolean
    DATE = pl.Date
    DATETIME = pl.Datetime

    @classmethod
    def from_python_type(cls, py_type: Type) -> 'DataType':
        """Convert Python type to DataType enum."""
        type_mapping = {
            str: cls.STRING,
            int: cls.INTEGER,
            float: cls.FLOAT,
            bool: cls.BOOLEAN,
            date: cls.DATE,
            datetime: cls.DATETIME
        }
        return type_mapping.get(py_type, cls.STRING)
