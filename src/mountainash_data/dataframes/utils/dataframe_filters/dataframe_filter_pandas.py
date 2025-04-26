from typing import Callable
import pandas as pd
from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition


class PandasFilterVisitor(FilterVisitor):


    # Mapping of operators to their corresponding pandas operations  
    COLUMN_OPS = {
        "==": lambda col1, col2: col1 == col2,
        "!=": lambda col1, col2: col1 != col2,
        ">": lambda col1, col2: col1 > col2,
        "<": lambda col1, col2: col1 < col2,
        ">=": lambda col1, col2: col1 >= col2,
        "<=": lambda col1, col2: col1 <= col2,
        "in": lambda col1, col2: col1.isin(col2),
        "is null": lambda col1, _: col1.isnull(),
        "is not null": lambda col1, _: col1.notnull(),
    }

    def visit_column_condition(self, condition: ColumnCondition) -> Callable:
        if condition.operator not in self.COLUMN_OPS:
            raise ValueError(f"Unsupported operator: {condition.operator}")
            
        op_func = self.COLUMN_OPS[condition.operator]
        
        if condition.compare_column:
            # Column to column comparison
            return lambda df: op_func(df[condition.column], df[condition.compare_column])
        else:

            if condition.operator == "in":

               values_list = list(condition.value) if not isinstance(condition.value, list) else condition.value # Ensure it's a list

               return  lambda df: df[condition.column].isin(values_list)          
            else:            
                # Column to value comparison
                return lambda df: op_func(df[condition.column], condition.value)


    # def visit_column_condition(self, condition: ColumnCondition) -> Callable:

    #     if condition.compare_column:
    #         if condition.operator == "==":
    #             return lambda df: df[condition.column] == df[condition.compare_column]
    #         elif condition.operator == "!=":
    #             return lambda df: df[condition.column] != df[condition.compare_column]
    #         elif condition.operator == ">":
    #             return lambda df: df[condition.column] > df[condition.compare_column]
    #         elif condition.operator == "<":
    #             return lambda df: df[condition.column] < df[condition.compare_column]
    #         elif condition.operator == ">=":
    #             return lambda df: df[condition.column] >= df[condition.compare_column]
    #         elif condition.operator == "<=":
    #             return lambda df: df[condition.column] <= df[condition.compare_column]
    #         else:
    #             raise ValueError(f"Unsupported operator for column comparison: {condition.operator}")
    #     else:
    #         if condition.operator == "==":
    #             return lambda df: df[condition.column] == condition.value
    #         elif condition.operator == "!=":
    #             return lambda df: df[condition.column] != condition.value
    #         elif condition.operator == ">":
    #             return lambda df: df[condition.column] > condition.value
    #         elif condition.operator == "<":
    #             return lambda df: df[condition.column] < condition.value
    #         elif condition.operator == ">=":
    #             return lambda df: df[condition.column] >= condition.value
    #         elif condition.operator == "<=":
    #             return lambda df: df[condition.column] <= condition.value
    #         elif condition.operator == "in":
    #             return lambda df: df[condition.column].isin(condition.value)
    #         elif condition.operator == "is null":
    #             return lambda df: df[condition.column].isnull()
    #         elif condition.operator == "is not null":
    #             return lambda df: df[condition.column].notnull()
    #         else:
    #             raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_logical_condition(self, condition: LogicalCondition) -> Callable:
        if condition.operator == LogicalCondition.ALWAYS_TRUE_OP:
            return lambda df: pd.Series(True, index=df.index)
        elif condition.operator == LogicalCondition.ALWAYS_FALSE_OP:
            return lambda df: pd.Series(False, index=df.index)
        elif condition.operator == "and":
            return lambda df: pd.concat([operand.accept(self)(df) for operand in condition.operands], axis=1).all(axis=1)
        elif condition.operator == "or":
            return lambda df: pd.concat([operand.accept(self)(df) for operand in condition.operands], axis=1).any(axis=1)
        elif condition.operator == "not":
            return lambda df: ~condition.operands[0].accept(self)(df)
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")

