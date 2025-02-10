import ibis

from typing import Callable

from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition

class IbisFilterVisitor(FilterVisitor):
    
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
            return lambda table: op_func(table[condition.column], table[condition.compare_column])
        else:

            if condition.operator == "in":
               return  lambda table: table[condition.column].isin(ibis.literal(condition.value))       
            else:               
                # Column to value comparison
                return lambda table: op_func(table[condition.column], ibis.literal(condition.value))

    # def visit_column_condition(self, condition: ColumnCondition) -> Callable:

    #     if condition.compare_column:
    #         if condition.operator == "==":
    #             return lambda table: table[condition.column] == table[condition.compare_column]
    #         elif condition.operator == "!=":
    #             return lambda table: table[condition.column] != table[condition.compare_column]
    #         elif condition.operator == ">":
    #             return lambda table: table[condition.column] > table[condition.compare_column]
    #         elif condition.operator == "<":
    #             return lambda table: table[condition.column] < table[condition.compare_column]
    #         elif condition.operator == ">=":
    #             return lambda table: table[condition.column] >= table[condition.compare_column]
    #         elif condition.operator == "<=":
    #             return lambda table: table[condition.column] <= table[condition.compare_column]

    #     else:

    #         if condition.operator == "==":
    #             return lambda table: table[condition.column] == ibis.literal(condition.value)
    #         elif condition.operator == "!=":
    #             return lambda table: table[condition.column] != ibis.literal(condition.value)
    #         elif condition.operator == ">":
    #             return lambda table: table[condition.column] > ibis.literal(condition.value)
    #         elif condition.operator == "<":
    #             return lambda table: table[condition.column] < ibis.literal(condition.value)
    #         elif condition.operator == ">=":
    #             return lambda table: table[condition.column] >= ibis.literal(condition.value)
    #         elif condition.operator == "<=":
    #             return lambda table: table[condition.column] <= ibis.literal(condition.value)
    #         elif condition.operator == "in":
    #             return lambda table: table[condition.column].isin(ibis.literal(condition.value))
    #         elif condition.operator == "is null":
    #             return lambda table: table[condition.column].isnull()
    #         elif condition.operator == "is not null":
    #             return lambda table: table[condition.column].notnull()
    #         else:
    #             raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_logical_condition(self, condition: LogicalCondition) -> Callable:
        if condition.operator == LogicalCondition.ALWAYS_TRUE_OP:
            return lambda table: ibis.literal(True)
        elif condition.operator == LogicalCondition.ALWAYS_FALSE_OP:
            return lambda table: ibis.literal(False)
        elif condition.operator == "and":
            return lambda table: self._combine_conditions(table, condition.operands, lambda x, y: x & y)
        elif condition.operator == "or":
            return lambda table: self._combine_conditions(table, condition.operands, lambda x, y: x | y)
        elif condition.operator == "not":
            return lambda table: ~condition.operands[0].accept(self)(table)
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")

    def _combine_conditions(self, table, operands, combine_func):
        conditions = [operand.accept(self)(table) for operand in operands]
        result = conditions[0]
        for condition in conditions[1:]:
            result = combine_func(result, condition)
        return result
