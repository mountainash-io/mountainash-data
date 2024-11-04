from typing import Dict, List, Optional, Union, Set
from dataclasses import dataclass
import polars as pl
from .column_config import ColumnConfig, ColumnMapConfig
from .column_mapper_validator import ColumnMappingValidator
from .constants import DataType, NullStrategy
from ..dataframe_handlers import DataFrameStrategyFactory


class ColumnMapper:
    """
    Handles column mapping operations for DataFrames and raw data structures.
    Provides consistent column renaming and filtering behavior.
    """
    
    @classmethod
    def apply_mapping(cls,
                     df: pl.DataFrame,
                     config: ColumnMapConfig) -> pl.DataFrame:
        """
        Apply column mapping to a Polars DataFrame.
        
        Args:
            df: Input DataFrame
            config: Column mapping configuration
            
        Returns:
            pl.DataFrame: DataFrame with applied column mapping
            
        Example:
            >>> config = ColumnMapConfig(
            ...     mapping={"old_name": "new_name", "keep_same": "keep_same"},
            ...     filter_unmapped=True
            ... )
            >>> df = pl.DataFrame({"old_name": [1, 2], "keep_same": [3, 4], "drop_me": [5, 6]})
            >>> result = ColumnMapper.apply_mapping(df, config)
        """
        # Validate the mapping
        ColumnMappingValidator.validate_mapping(df.columns, config.mapping)
        

        df_strategy = DataFrameStrategyFactory._get_strategy(df)

        # Get the columns we want to keep
        existing_columns = df_strategy.get_column_names(df)
        
        columns_to_keep = cls._get_columns_to_keep(
            existing_columns=existing_columns,
            config=config
        )
        
        # Build expressions for select operation
        select_exprs = []
        
        for col in columns_to_keep:
            if col in config.mapping:
                # Column needs to be renamed
                select_exprs.append(
                    pl.col(col).alias(config.mapping[col])
                )
            else:
                # Keep column with same name
                select_exprs.append(pl.col(col))
        
        # Apply the mapping
        return df_strategy.select(select_exprs)

    @classmethod
    def get_target_columns(cls,
                          source_columns: List[str],
                          config: ColumnMapConfig) -> List[str]:
        """
        Get the final column names after applying mapping.
        
        Args:
            source_columns: Original column names
            config: Column mapping configuration
            
        Returns:
            List[str]: Final column names after mapping
        """
        columns_to_keep = cls._get_columns_to_keep(
            existing_columns=source_columns,
            config=config
        )
        
        return [
            config.mapping.get(col, col)
            for col in columns_to_keep
        ]


    @classmethod
    def _get_columns_to_keep(cls,
                            existing_columns: List[str],
                            config: ColumnMapConfig) -> List[str]:
        """
        Determine which columns should be kept based on mapping and filter settings.
        
        Args:
            existing_columns: List of existing column names
            mapping: Column mapping dictionary
            filter_unmapped: Whether to exclude unmapped columns
            
        Returns:
            List[str]: List of columns to keep
        """

        if config.filter_unmapped:

            if config.mapping is None:
                return existing_columns

            # Only keep columns that are in the mapping
            return [col for col in existing_columns if col in config.mapping]
        else:
            # Keep all existing columns
            return existing_columns

    @classmethod
    def create_config(cls,
                     mapping: Optional[Dict[str, str]] = None,
                     filter_unmapped: Optional[bool] = False) -> Optional[ColumnMapConfig]:
        """
        Create a ColumnMapConfig instance with validation.
        
        Args:
            mapping: Column mapping dictionary
            filter_unmapped: Whether to exclude unmapped columns
            
        Returns:
            ColumnMapConfig: Validated configuration object
        """
        # Validate mapping structure
        if mapping is None:
            return None

        ColumnMappingValidator.validate_mapping_structure(mapping=mapping)
        ColumnMappingValidator.validate_no_duplicate_targets(mapping=mapping)


        return ColumnMapConfig(
            mapping=mapping,
            filter_unmapped=filter_unmapped
        )
    
    @classmethod
    def _validate_mapping(cls,
                        #  existing_columns: List[str],
                         mapping: Dict[str, str]) -> None:
        """
        Validate the column mapping configuration.
        
        Args:
            existing_columns: List of existing column names
            mapping: Column mapping dictionary
            
        Raises:
            ValueError: If mapping is invalid
        """

        if mapping and not isinstance(mapping, dict):
            raise ValueError("Mapping must be a dictionary")
        
        if not all(isinstance(k, str) and isinstance(v, str) 
                  for k, v in mapping.items()):
            raise ValueError("All mapping keys and values must be strings")


        # Check for duplicate target names
        target_names = list(mapping.values())
        duplicates = {name for name in target_names if target_names.count(name) > 1}
        if duplicates:
            raise ValueError(f"Duplicate target column names found: {duplicates}")
        

    #### Typing 

    @classmethod
    def apply_types(cls,
                   df: pl.DataFrame,
                   column_configs: Dict[str, ColumnConfig],
                   drop_invalid: bool = False) -> pl.DataFrame:
        """
        Apply type enforcement to DataFrame columns.
        
        Args:
            df: Input DataFrame
            column_configs: Mapping of source column names to their configurations
            drop_invalid: Whether to drop rows with invalid values that can't be coerced
            
        Returns:
            pl.DataFrame: DataFrame with enforced types
            
        Raises:
            ValueError: If type enforcement fails and drop_invalid is False
        """
        exprs = []
        drop_mask = None
        
        for source_col, config in column_configs.items():
            if source_col not in df.columns:
                continue
                
            # Create expression for this column
            expr = cls._create_column_expression(
                source_col=source_col,
                config=config,
                drop_invalid=drop_invalid
            )
            
            # Handle null strategy
            if config.type_config.null_strategy == NullStrategy.REJECT or config.type_config.null_strategy == NullStrategy.DROP_ROW:
                null_check = pl.col(source_col).is_null()
                if drop_mask is None:
                    drop_mask = ~null_check
                else:
                    drop_mask = drop_mask & ~null_check
                

            elif config.type_config.null_strategy == NullStrategy.DEFAULT:
                expr = expr.fill_null(config.type_config.default_value)
            
            exprs.append(expr.alias(config.target_name))
        
        # Add unchanged columns
        unchanged_cols = [
            col for col in df.columns 
            if col not in column_configs
        ]
        exprs.extend([pl.col(col) for col in unchanged_cols])
        
        # Apply expressions
        result = df.select(exprs)
        
        # Apply drop mask if needed
        if drop_mask is not None:
            result = result.filter(drop_mask)
            
        return result
    
    @classmethod
    def _create_column_expression(cls,
                                source_col: str,
                                config: ColumnConfig,
                                drop_invalid: bool) -> pl.Expr:
        """Create a Polars expression for type enforcement on a column."""
        expr = pl.col(source_col)
        
        # Create cast expression based on type
        if config.type_config.data_type == DataType.STRING:
            expr = expr.cast(pl.Utf8, strict=not drop_invalid)
        elif config.type_config.data_type == DataType.INTEGER:
            expr = expr.cast(pl.Int64, strict=not drop_invalid)
        elif config.type_config.data_type == DataType.FLOAT:
            expr = expr.cast(pl.Float64, strict=not drop_invalid)
        elif config.type_config.data_type == DataType.BOOLEAN:
            expr = expr.cast(pl.Boolean, strict=not drop_invalid)
        elif config.type_config.data_type == DataType.DATE:
            expr = cls._handle_date_cast(expr, drop_invalid)
        elif config.type_config.data_type == DataType.DATETIME:
            expr = cls._handle_datetime_cast(expr, drop_invalid)
            
        return expr
    
    @staticmethod
    def _handle_date_cast(expr: pl.Expr, drop_invalid: bool) -> pl.Expr:
        """Handle casting to date type with various input formats."""
        return (
            pl.when(expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False).is_not_null())
            .then(expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False))
            .when(expr.str.strptime(pl.Date, "%d/%m/%Y", strict=False).is_not_null())
            .then(expr.str.strptime(pl.Date, "%d/%m/%Y", strict=False))
            .when(expr.str.strptime(pl.Date, "%m/%d/%Y", strict=False).is_not_null())
            .then(expr.str.strptime(pl.Date, "%m/%d/%Y", strict=False))
            .otherwise(None if drop_invalid else expr)
        )
    
    @staticmethod
    def _handle_datetime_cast(expr: pl.Expr, drop_invalid: bool) -> pl.Expr:
        """Handle casting to datetime type with various input formats."""
        return (
            pl.when(expr.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).is_not_null())
            .then(expr.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False))
            .when(expr.str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False).is_not_null())
            .then(expr.str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False))
            .otherwise(None if drop_invalid else expr)
        )

