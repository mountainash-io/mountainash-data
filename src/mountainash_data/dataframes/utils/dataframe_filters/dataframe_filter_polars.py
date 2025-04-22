import polars as pl
from typing import  Callable

from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition


class PolarsFilterVisitor(FilterVisitor):

    COLUMN_OPS = {
        "==": lambda col1, col2: col1 == col2,
        "!=": lambda col1, col2: col1 != col2,
        ">": lambda col1, col2: col1 > col2,
        "<": lambda col1, col2: col1 < col2,
        ">=": lambda col1, col2: col1 >= col2,
        "<=": lambda col1, col2: col1 <= col2,
        "in": lambda col1, col2: col1.is_in(col2),
        "is null": lambda col1, _: col1.is_null(),
        "is not null": lambda col1, _: col1.is_not_null(),
    }

    def visit_column_condition(self, condition: ColumnCondition) -> Callable:
        if condition.operator not in self.COLUMN_OPS:
            raise ValueError(f"Unsupported operator: {condition.operator}")
            
        op_func = self.COLUMN_OPS[condition.operator]
        
        if condition.compare_column:
            # Column to column comparison
            return lambda df: op_func(pl.col(condition.column), pl.col(condition.compare_column))
        else:

            if condition.operator == "in":
               print(f"PolarsFilterVisitor: {condition.value}")

               return  lambda df: pl.col(condition.column).is_in(condition.value)              
            else:
                # Column to value comparison 
                return lambda df: op_func(pl.col(condition.column), condition.value)

    # def visit_column_condition(self, condition: ColumnCondition) -> Callable:

    #     if condition.compare_column:
    #         if condition.operator == "==":
    #             return lambda table: pl.col(condition.column) == pl.col(condition.compare_column)
    #         elif condition.operator == "!=":
    #             return lambda table: pl.col(condition.column) != pl.col(condition.compare_column)
    #         elif condition.operator == ">":
    #             return lambda table: pl.col(condition.column) > pl.col(condition.compare_column)
    #         elif condition.operator == "<":
    #             return lambda table: pl.col(condition.column) < pl.col(condition.compare_column)
    #         elif condition.operator == ">=":
    #             return lambda table: pl.col(condition.column) >= pl.col(condition.compare_column)
    #         elif condition.operator == "<=":
    #             return lambda table: pl.col(condition.column) <= pl.col(condition.compare_column)
    #         else:
    #             raise ValueError(f"Unsupported operator for column comparison: {condition.operator}")
    #     else:        
    #         if condition.operator == "==":
    #             return lambda df: pl.col(condition.column) == condition.value
    #         elif condition.operator == "!=":
    #             return lambda df: pl.col(condition.column) != condition.value
    #         elif condition.operator == ">":
    #             return lambda df: pl.col(condition.column) > condition.value
    #         elif condition.operator == "<":
    #             return lambda df: pl.col(condition.column) < condition.value
    #         elif condition.operator == ">=":
    #             return lambda df: pl.col(condition.column) >= condition.value
    #         elif condition.operator == "<=":
    #             return lambda df: pl.col(condition.column) <= condition.value
    #         elif condition.operator == "in":
    #             return lambda df: pl.col(condition.column).is_in(condition.value)
    #         elif condition.operator == "is null":
    #             return lambda df: pl.col(condition.column).is_null()
    #         elif condition.operator == "is not null":
    #             return lambda df: pl.col(condition.column).is_not_null()
    #         else:
    #             raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_logical_condition(self, condition: LogicalCondition):
        if condition.operator == "and":
            return lambda df: pl.all_horizontal([operand.accept(self)(df) for operand in condition.operands])
        elif condition.operator == "or":
            return lambda df: pl.any_horizontal([operand.accept(self)(df) for operand in condition.operands])
        elif condition.operator == "not":
            return lambda df: ~condition.operands[0].accept(self)(df)
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")

