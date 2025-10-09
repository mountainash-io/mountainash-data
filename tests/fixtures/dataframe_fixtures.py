"""DataFrame fixtures for testing."""

import pytest
from typing import Dict, Any, List
import polars as pl
import pandas as pd
import pyarrow as pa


@pytest.fixture(scope="session")
def sample_data_dict() -> Dict[str, Any]:
    """Provide consistent test data as dictionary."""
    return {
        "valid_simple": {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.5, 200.7, 300.9]
        },
        "valid_complex": {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "category": ["A", "B", "A", "C", "B"],
            "value": [100.5, 200.7, 300.9, 400.2, 500.8],
            "active": [True, False, True, True, False]
        },
        "empty": {},
        "single_column": {"id": [1, 2, 3]},
        "mixed_types": {
            "text": ["hello", "world", "test"],
            "numbers": [1, 2, 3],
            "floats": [1.1, 2.2, 3.3],
            "bools": [True, False, True]
        }
    }


@pytest.fixture(scope="session")
def sample_data_list() -> List[Dict[str, Any]]:
    """Provide consistent test data as list of dictionaries."""
    return [
        {"id": 1, "name": "Alice", "value": 100.5, "active": True},
        {"id": 2, "name": "Bob", "value": 200.7, "active": False},
        {"id": 3, "name": "Charlie", "value": 300.9, "active": True}
    ]


@pytest.fixture
def sample_polars_df() -> pl.DataFrame:
    """Create a sample Polars DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "category": ["A", "B", "A", "C", "B"],
        "value": [100.5, 200.7, 300.9, 400.2, 500.8],
        "active": [True, False, True, True, False]
    })


@pytest.fixture
def sample_pandas_df() -> pd.DataFrame:
    """Create a sample pandas DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "category": ["A", "B", "A", "C", "B"],
        "value": [100.5, 200.7, 300.9, 400.2, 500.8],
        "active": [True, False, True, True, False]
    })


@pytest.fixture
def sample_pyarrow_table() -> pa.Table:
    """Create a sample PyArrow Table for testing."""
    return pa.table({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "category": ["A", "B", "A", "C", "B"],
        "value": [100.5, 200.7, 300.9, 400.2, 500.8],
        "active": [True, False, True, True, False]
    })


@pytest.fixture
def simple_polars_df() -> pl.DataFrame:
    """Create a simple Polars DataFrame for basic testing."""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100.5, 200.7, 300.9]
    })


@pytest.fixture
def simple_pandas_df() -> pd.DataFrame:
    """Create a simple pandas DataFrame for basic testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100.5, 200.7, 300.9]
    })


@pytest.fixture
def column_mapping_config() -> Dict[str, str]:
    """Provide sample column mapping configuration."""
    return {
        "id": "identifier",
        "name": "full_name",
        "value": "amount",
        "active": "is_active"
    }


@pytest.fixture
def dataframe_builder():
    """Factory fixture for building DataFrames with custom data."""
    class DataFrameBuilder:
        """Builder for creating test DataFrames."""

        @staticmethod
        def build_polars(data: Dict[str, list]) -> pl.DataFrame:
            """Build Polars DataFrame from dict."""
            return pl.DataFrame(data)

        @staticmethod
        def build_pandas(data: Dict[str, list]) -> pd.DataFrame:
            """Build pandas DataFrame from dict."""
            return pd.DataFrame(data)

        @staticmethod
        def build_pyarrow(data: Dict[str, list]) -> pa.Table:
            """Build PyArrow Table from dict."""
            return pa.table(data)

        @staticmethod
        def simple_table(rows: int = 3) -> Dict[str, list]:
            """Generate simple table data."""
            return {
                "id": list(range(1, rows + 1)),
                "value": [i * 100 for i in range(1, rows + 1)]
            }

        @staticmethod
        def complex_table(rows: int = 5) -> Dict[str, list]:
            """Generate complex table data with various types."""
            names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
            categories = ["A", "B", "C"]

            return {
                "id": list(range(1, rows + 1)),
                "name": [names[i % len(names)] for i in range(rows)],
                "category": [categories[i % len(categories)] for i in range(rows)],
                "value": [round((i + 1) * 100.5, 2) for i in range(rows)],
                "active": [bool(i % 2) for i in range(rows)]
            }

    return DataFrameBuilder()


@pytest.fixture(params=[pl.DataFrame, pd.DataFrame, pa.table])
def dataframe_type(request):
    """Parametrized fixture providing different DataFrame types."""
    return request.param


@pytest.fixture
def dataframe_samples():
    """Provide various pre-built DataFrame samples."""
    base_data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100.5, 200.7, 300.9]
    }

    return {
        "polars": pl.DataFrame(base_data),
        "pandas": pd.DataFrame(base_data),
        "pyarrow": pa.table(base_data),
        "dict": base_data
    }
