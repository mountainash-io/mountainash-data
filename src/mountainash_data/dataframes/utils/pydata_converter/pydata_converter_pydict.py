from typing import Any, Dict, Optional, Sequence

import polars as pl
from .base_pydata_converter import BasePyDataConverter
from ..column_mapper import ColumnMapper


class PyDataConverterPydict(BasePyDataConverter):
    """Strategy for handling dictionary of lists."""
    
    def can_handle(self, data: Any) -> bool:
        return (isinstance(data, dict) and 
                all(isinstance(v, (list, tuple, Sequence)) 
                    for v in data.values()))

    def validate(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None
                
                ) -> None:
        if not data:
            return

        # Check length consistency
        lengths = {len(v) for v in data.values()}
        if len(lengths) > 1:
            raise ValueError("All lists must have the same length")

        if column_mapping:
            missing_fields = set(column_mapping.keys()) - set(data.keys())
            if missing_fields:
                raise ValueError(f"Mapped columns not found in data: {missing_fields}")

    def convert(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None,
                filter_unmapped: Optional[bool] = False) -> pl.DataFrame:

        #TODO: Handle type casting...
        # # Handle mixed data types by converting to strings if needed
        # processed_data = {} 
        # for col, values in data.items():
        #     # Check if column has mixed types
        #     non_none_values = [v for v in values if v is not None]
        #     if not non_none_values:
        #         # All None values, use nullable Int64 as default
        #         processed_data[col] = values
        #     else:
        #         value_types = {type(v) for v in non_none_values}
        #         if len(value_types) > 1:
        #             # Mixed types, convert all to strings
        #             processed_data[col] = [str(v) if v is not None else None for v in values]
        #         else:
        #             # Single type, keep as is
        #             processed_data[col] = values

        df = pl.DataFrame(data, strict=False)

        if column_mapping:
            # Create ColumnMapConfig
            map_config = ColumnMapper.create_config(
                mapping=column_mapping,
                filter_unmapped=filter_unmapped  # Keep unmapped columns by default
            )
            # Apply the mapping using ColumnMapper
            df = ColumnMapper.apply_mapping(df, map_config)
        return df
