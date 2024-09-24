import ibis

from typing import Callable

from .dataframe_filter import FilterVisitor, ColumnCondition, LogicalCondition

class IbisFilterVisitor(FilterVisitor):
    
    def visit_column_condition(self, condition: ColumnCondition) -> Callable:

        if condition.compare_column:
            if condition.operator == "==":
                return lambda table: table[condition.column] == table[condition.compare_column]
            elif condition.operator == "!=":
                return lambda table: table[condition.column] != table[condition.compare_column]
            elif condition.operator == ">":
                return lambda table: table[condition.column] > table[condition.compare_column]
            elif condition.operator == "<":
                return lambda table: table[condition.column] < table[condition.compare_column]
            elif condition.operator == ">=":
                return lambda table: table[condition.column] >= table[condition.compare_column]
            elif condition.operator == "<=":
                return lambda table: table[condition.column] <= table[condition.compare_column]

        else:

            if condition.operator == "==":
                return lambda table: table[condition.column] == ibis.literal(condition.value)
            elif condition.operator == "!=":
                return lambda table: table[condition.column] != ibis.literal(condition.value)
            elif condition.operator == ">":
                return lambda table: table[condition.column] > ibis.literal(condition.value)
            elif condition.operator == "<":
                return lambda table: table[condition.column] < ibis.literal(condition.value)
            elif condition.operator == ">=":
                return lambda table: table[condition.column] >= ibis.literal(condition.value)
            elif condition.operator == "<=":
                return lambda table: table[condition.column] <= ibis.literal(condition.value)
            elif condition.operator == "in":
                return lambda table: table[condition.column].isin(ibis.literal(condition.value))
            elif condition.operator == "is null":
                return lambda table: table[condition.column].isnull()
            elif condition.operator == "is not null":
                return lambda table: table[condition.column].notnull()
            else:
                raise ValueError(f"Unsupported operator: {condition.operator}")


    def visit_logical_condition(self, condition: LogicalCondition):
        if condition.operator == "and":
            return lambda table: self._combine_conditions(table,condition.operands, lambda x, y: x & y)
        elif condition.operator == "or":
            return lambda table: self._combine_conditions(table, condition.operands, lambda x, y: x | y)
        elif condition.operator == "not":
            return lambda table:  ~condition.operands[0].accept(self)(table)
        else:
            raise ValueError(f"Unsupported logical operator: {condition.operator}")

    def _combine_conditions(self, table, operands, combine_func):
        conditions = [operand.accept(self)(table) for operand in operands]
        result = conditions[0]
        for condition in conditions[1:]:
            result = combine_func(result, condition)
        return result


    # def visit_logical_condition(self, condition: LogicalCondition) -> Callable:
    #     if condition.operator == "and":
    #         return lambda table: ibis.and_([operand.accept(self)(table) for operand in condition.operands])
    #     elif condition.operator == "or":
    #         return lambda table: ibis.or_([operand.accept(self)(table) for operand in condition.operands])
    #     elif condition.operator == "not":
    #         return lambda table: ~condition.operands[0].accept(self)(table)
    #     else:
    #         raise ValueError(f"Unsupported logical operator: {condition.operator}")        

    # def visit_logical_condition(self, condition: LogicalCondition):

        
    #     if condition.operator == "and":
    #         return lambda table: ibis.and_([operand.accept(self)(table) for operand in condition.operands])
    #     elif condition.operator == "or":
    #         return lambda table: ibis.or_([operand.accept(self)(table) for operand in condition.operands])
    #     elif condition.operator == "not":
    #         return lambda table: ~condition.operands[0].accept(self)(table)
    #     else:
    #         raise ValueError(f"Unsupported logical operator: {condition.operator}")
