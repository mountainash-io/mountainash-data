import pyarrow as pa
import pyarrow.compute as pc

from typing import Callable

from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition


class PyArrowFilterVisitor(FilterVisitor):
    def visit_column_condition(self, condition: ColumnCondition) -> Callable:
        
        if condition.operator == "==":
            return lambda table: pc.field(condition.column) == pc.scalar(condition.value)
        elif condition.operator == "!=":
            return lambda table: pc.field(condition.column) != pc.scalar(condition.value)
        elif condition.operator == ">":
            return lambda table: pc.field(condition.column) > pc.scalar(condition.value)
        elif condition.operator == "<":
            return lambda table: pc.field(condition.column) < pc.scalar(condition.value)
        elif condition.operator == ">=":
            return lambda table: pc.field(condition.column) >= pc.scalar(condition.value)
        elif condition.operator == "<=":
            return lambda table: pc.field(condition.column) <= pc.scalar(condition.value)
        elif condition.operator == "in":
            return lambda table: pc.field(condition.column).isin(pa.array(condition.value))
         
        elif condition.operator == "is null":
            return lambda table: pc.field(condition.column).is_null()
        elif condition.operator == "is not null":
            return lambda table: pc.field(condition.column).is_valid()
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_logical_condition(self, condition: LogicalCondition):
        if condition.operator == "and":
            return lambda table: self._combine_conditions(table, condition.operands, pc.and_kleene)
        elif condition.operator == "or":
            return lambda table: self._combine_conditions(table, condition.operands, pc.or_kleene)
        elif condition.operator == "not":
            return lambda table: pc.invert(condition.operands[0].accept(self)(table))
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")

    def _combine_conditions(self, table, operands, combine_func):
        conditions = [operand.accept(self)(table) for operand in operands]
        result = conditions[0]
        for condition in conditions[1:]:
            result = combine_func(result, condition)
        return result

