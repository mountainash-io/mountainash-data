import pyarrow as pa
import pyarrow.compute as pc

from typing import Callable

from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition


class PyArrowFilterVisitor(FilterVisitor):

   # Mapping of operators to their corresponding pyarrow operations
    COLUMN_OPS = {
        "==": lambda col1, col2: pc.equal(col1, col2),
        "!=": lambda col1, col2: pc.not_equal(col1, col2),
        ">": lambda col1, col2: pc.greater(col1, col2),
        "<": lambda col1, col2: pc.less(col1, col2),
        ">=": lambda col1, col2: pc.greater_equal(col1, col2),
        "<=": lambda col1, col2: pc.less_equal(col1, col2),
        "in": lambda col1, col2: pc.isin(col1, col2),
        "is null": lambda col1, _: pc.is_null(col1),
        "is not null": lambda col1, _: pc.is_valid(col1),
    }

    def visit_column_condition(self, condition: ColumnCondition) -> Callable:
        if condition.operator not in self.COLUMN_OPS:
            raise ValueError(f"Unsupported operator: {condition.operator}")
            
        op_func = self.COLUMN_OPS[condition.operator]
        
        if condition.compare_column:

                # Column to column comparison
                return lambda table: op_func(pc.field(condition.column), pc.field(condition.compare_column))
        else:
            
            if condition.operator == "in":
               print(f"PyArrowFilterVisitor: {condition.value}")

               return  lambda table: pc.field(condition.column).isin(pc.array(condition.value))                
            #    return  lambda table: pc.field(condition.column).isin(condition.value)                
            else:
                # Column to value comparison
                return lambda table: op_func(pc.field(condition.column), pc.scalar(condition.value))



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

