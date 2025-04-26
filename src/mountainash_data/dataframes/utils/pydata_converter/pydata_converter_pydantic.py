from typing import Any, Dict, Optional

import polars as pl
from pydantic import BaseModel
from .base_pydata_converter import BasePyDataConverter
from ..column_mapper import ColumnMapper

class PyDataConverterPydantic(BasePyDataConverter):
    """Strategy for handling Pydantic models."""
    
    def can_handle(self, data: Any) -> bool:
        return (isinstance(data, BaseModel) or 
                (isinstance(data, list) and 
                 len(data) > 0 and 
                 isinstance(data[0], BaseModel)))

    def validate(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None) -> None:
        if isinstance(data, list):
            if not data:
                return

            # Check all items are the same model type
            model_type = type(data[0])
            for idx, item in enumerate(data[1:], start=1):
                if not isinstance(item, model_type):
                    raise ValueError(
                        f"Inconsistent types at index {idx}. "
                        f"Expected {model_type}, got {type(item)}"
                    )
        
        if column_mapping:
            # Get available fields
            model = data[0] if isinstance(data, list) else data
            available_fields = set(model.model_fields.keys())
            missing_fields = set(column_mapping.keys()) - available_fields
            if missing_fields:
                raise ValueError(f"Mapped columns not found in model: {missing_fields}")

    def convert(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None,
                filter_unmapped: Optional[bool] = False) -> pl.DataFrame:
        # Convert to list of dicts
        if isinstance(data, list):
            data_dicts = [item.model_dump() for item in data]
        else:
            data_dicts = [data.model_dump()]

        # Create DataFrame
        df = pl.DataFrame(data_dicts, strict=False)

        if column_mapping:
            # Create ColumnMapConfig
            map_config = ColumnMapper.create_config(
                mapping=column_mapping,
                filter_unmapped=filter_unmapped  # Keep unmapped columns by default
            )
            # Apply the mapping using ColumnMapper
            df = ColumnMapper.apply_mapping(df, map_config)

        return df