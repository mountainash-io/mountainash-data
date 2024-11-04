from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
import pytest
import polars as pl
from pytest_check import check
from pydantic import BaseModel

from mountainash_data.dataframes.utils.column_mapper import (
    ColumnMapper, 
    ColumnMapConfig,
    TypeConfig,
    DataType,
    NullStrategy
)

# Test Data Structures
@dataclass
class Person:
    name: str
    age: int
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    tags: List[str] = None
    metadata: Dict[str, Any] = None

class PersonPydantic(BaseModel):
    name: str
    age: int
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    tags: List[str] = []
    metadata: Dict[str, Any] = {}

# Basic Column Mapping Tests
def test_column_mapper_basic():
    """Test basic column mapping functionality."""
    df = pl.DataFrame({
        "old_name": ["John", "Jane"],
        "age": [30, 25],
        "email": ["john@example.com", "jane@example.com"]
    })
    
    config = ColumnMapper.create_config(
        mapping={"old_name": "full_name", "age": "years"},
        filter_unmapped=False
    )
    
    result = ColumnMapper.apply_mapping(df, config)
    
    with check:
        assert result.columns == ["full_name", "years", "email"]
        assert result.shape == (2, 3)
        assert result["full_name"].to_list() == ["John", "Jane"]
        assert result["years"].to_list() == [30, 25]

def test_column_mapper_with_filter():
    """Test column mapping with filtered columns."""
    df = pl.DataFrame({
        "old_name": ["John", "Jane"],
        "age": [30, 25],
        "email": ["john@example.com", "jane@example.com"],
        "extra": [1, 2]
    })
    
    config = ColumnMapper.create_config(
        mapping={"old_name": "full_name", "age": "years"},
        filter_unmapped=True
    )
    
    result = ColumnMapper.apply_mapping(df, config)
    
    with check:
        assert result.columns == ["full_name", "years"]
        assert result.shape == (2, 2)
        assert "email" not in result.columns
        assert "extra" not in result.columns



# def test_column_mapper_with_type_enforcement():
#     """Test column mapping with type enforcement."""
#     df = pl.DataFrame({
#         "age_str": ["30", "25"],
#         "active_str": ["true", "false"],
#         "value_str": ["10.5", "20.5"]
#     })
    
#     config = ColumnMapper.create_config(
#         mapping={
#             "age_str": {
#                 "target_name": "age",
#                 "data_type": DataType.INTEGER,
#                 "null_strategy": NullStrategy.REJECT
#             },
#             "active_str": {
#                 "target_name": "active",
#                 "data_type": DataType.BOOLEAN
#             },
#             "value_str": {
#                 "target_name": "value",
#                 "data_type": DataType.FLOAT
#             }
#         }
#     )
    
#     result = ColumnMapper.apply_mapping(df, config)
    
#     with check:
#         assert result.columns == ["age", "active", "value"]
#         assert result["age"].dtype == pl.Int64
#         assert result["active"].dtype == pl.Boolean
#         assert result["value"].dtype == pl.Float64

# def test_column_mapper_with_null_handling():
#     """Test column mapping with different null handling strategies."""
#     df = pl.DataFrame({
#         "required": [1, None, 3],
#         "optional": [4, None, 6],
#         "default": [7, None, 9]
#     })
    
#     config = ColumnMapper.create_config(
#         mapping={
#             "required": {
#                 "target_name": "required_field",
#                 "null_strategy": NullStrategy.REJECT
#             },
#             "optional": {
#                 "target_name": "optional_field",
#                 "null_strategy": NullStrategy.ALLOW
#             },
#             "default": {
#                 "target_name": "default_field",
#                 "null_strategy": NullStrategy.DEFAULT,
#                 "default_value": -1
#             }
#         }
#     )
    
#     with pytest.raises(ValueError):
#         ColumnMapper.apply_mapping(df, config)  # Should fail due to NULL in required field
    
#     # Test with default values
#     result = ColumnMapper.apply_mapping(
#         df.filter(pl.col("required").is_not_null()), 
#         config
#     )
    
#     with check:
#         assert result["optional_field"].null_count() > 0
#         assert result["default_field"].null_count() == 0
#         assert -1 in result["default_field"].to_list()

# def test_column_mapper_complex_types():
#     """Test column mapping with complex data types."""
#     df = pl.DataFrame({
#         "dates": ["2023-01-01", "2023-01-02"],
#         "json_data": ['{"key": "value"}', '{"key": "value2"}'],
#         "arrays": ["[1,2,3]", "[4,5,6]"]
#     })
    
#     config = ColumnMapper.create_config(
#         mapping={
#             "dates": {
#                 "target_name": "date_field",
#                 "data_type": DataType.DATE
#             }
#         }
#     )
    
#     result = ColumnMapper.apply_mapping(df, config)
    
#     with check:
#         assert result["date_field"].dtype == pl.Date
#         assert len(result.columns) == 3

def test_column_mapper_validation():
    """Test various validation scenarios."""
    
    # Test duplicate target names
    with pytest.raises(ValueError):
        ColumnMapper.create_config(
            mapping={
                "name": "full_name",
                "nombre": "full_name"  # Duplicate target
            }
        )


    # Test invalid data type
    with pytest.raises(ValueError):
        ColumnMapper.create_config(
            mapping={
                "age": {
                    "target_name": "years",
                    "data_type": "invalid_type"
                }
            }
        )
    
    # # Test missing source columns
    # df = pl.DataFrame({"name": ["John"]})
    # config = ColumnMapper.create_config(mapping={"missing": "new"})
    
    # with pytest.raises(ValueError):
    #     ColumnMapper.apply_mapping(df, config)

def test_column_mapper_bulk_operations():
    """Test mapping multiple columns simultaneously."""
    df = pl.DataFrame({
        f"col_{i}": range(5) for i in range(10)
    })
    
    # Create mapping for all columns
    mapping = {
        f"col_{i}": f"new_col_{i}" 
        for i in range(10)
    }
    
    config = ColumnMapper.create_config(mapping=mapping)
    result = ColumnMapper.apply_mapping(df, config)
    
    with check:
        assert all(f"new_col_{i}" in result.columns for i in range(10))
        assert result.shape == df.shape

def test_get_target_columns():
    """Test getting target column names."""
    source_columns = ["name", "age", "email", "extra"]
    
    # Without filter
    config1 = ColumnMapper.create_config(
        mapping={"name": "full_name", "age": "years"},
        filter_unmapped=False
    )
    result1 = ColumnMapper.get_target_columns(source_columns, config1)
    with check:
        assert "full_name" in result1
        assert "years" in result1
        assert "email" in result1
        assert "extra" in result1
    
    # With filter
    config2 = ColumnMapper.create_config(
        mapping={"name": "full_name", "age": "years"},
        filter_unmapped=True
    )
    result2 = ColumnMapper.get_target_columns(source_columns, config2)
    with check:
        assert result2 == ["full_name", "years"]

def test_column_mapper_edge_cases():
    """Test edge cases and corner scenarios."""
    
    
    # Single column DataFrame
    df_single = pl.DataFrame({"single": [1]})
    config = ColumnMapper.create_config(
        mapping={"single": "renamed"},
        filter_unmapped=False
    )
    result = ColumnMapper.apply_mapping(df_single, config)
    with check:
        assert result.columns == ["renamed"]
    
    # No mappings
    config = ColumnMapper.create_config(mapping={})
    result = ColumnMapper.apply_mapping(df_single, config)
    with check:
        assert result.columns == ["single"]

def test_column_mapper_performance():
    """Test performance with larger datasets."""
    # Create a larger DataFrame
    df = pl.DataFrame({
        f"col_{i}": range(10000) for i in range(50)
    })
    
    # Create mapping for alternating columns
    mapping = {
        f"col_{i}": f"new_col_{i}"
        for i in range(0, 50, 2)
    }
    
    config = ColumnMapper.create_config(
        mapping=mapping,
        filter_unmapped=False
    )
    
    result = ColumnMapper.apply_mapping(df, config)
    
    with check:
        assert result.shape == df.shape
        assert all(f"new_col_{i}" in result.columns for i in range(0, 50, 2))