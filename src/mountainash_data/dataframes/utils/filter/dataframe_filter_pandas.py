from typing import Callable
import pandas as pd
from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition


class PandasFilterVisitor(FilterVisitor):
    def visit_column_condition(self, condition: ColumnCondition) -> Callable:


        if condition.operator == "==":
            return lambda df: df[condition.column] == condition.value
        elif condition.operator == "!=":
            return lambda df: df[condition.column] != condition.value
        elif condition.operator == ">":
            return lambda df: df[condition.column] > condition.value
        elif condition.operator == "<":
            return lambda df: df[condition.column] < condition.value
        elif condition.operator == ">=":
            return lambda df: df[condition.column] >= condition.value
        elif condition.operator == "<=":
            return lambda df: df[condition.column] <= condition.value
        elif condition.operator == "in":
            return lambda df: df[condition.column].isin(condition.value)
        elif condition.operator == "is null":
            return lambda df: df[condition.column].isnull()
        elif condition.operator == "is not null":
            return lambda df: df[condition.column].notnull()
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_logical_condition(self, condition: LogicalCondition) -> Callable:
        if condition.operator == "and":
            return lambda df: pd.concat([operand.accept(self)(df) for operand in condition.operands], axis=1).all(axis=1)
        elif condition.operator == "or":
            return lambda df: pd.concat([operand.accept(self)(df) for operand in condition.operands], axis=1).any(axis=1)
        elif condition.operator == "not":
            return lambda df: ~condition.operands[0].accept(self)(df)
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")
