from abc import ABC, abstractmethod
from typing import Any, List, Union, Callable

class FilterNode(ABC):
    @abstractmethod
    def accept(self, visitor: 'FilterVisitor') -> Callable:
        pass

class ColumnCondition(FilterNode):
    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

    def accept(self, visitor: 'FilterVisitor') -> Callable:
        return visitor.visit_column_condition(self)

class LogicalCondition(FilterNode):
    def __init__(self, operator: str, operands: List[FilterNode]):
        self.operator = operator
        self.operands = operands

    def accept(self, visitor: 'FilterVisitor') -> Callable:
        return visitor.visit_logical_condition(self)

class FilterVisitor(ABC):
    @abstractmethod
    def visit_column_condition(self, condition: ColumnCondition) -> Callable:
        pass

    @abstractmethod
    def visit_logical_condition(self, condition: LogicalCondition) -> Callable:
        pass

class FilterCondition:
    @staticmethod
    def equals(column: str, value: Any) -> ColumnCondition:
        return ColumnCondition(column, "==", value)

    @staticmethod
    def not_equals(column: str, value: Any) -> ColumnCondition:
        return ColumnCondition(column, "!=", value)

    @staticmethod
    def greater_than(column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, ">", value)

    @staticmethod
    def less_than(column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, "<", value)

    @staticmethod
    def greater_than_or_equal(column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, ">=", value)

    @staticmethod
    def less_than_or_equal(column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, "<=", value)


    @staticmethod
    def between(column: str, lower: Union[int, float], upper: Union[int, float]) -> LogicalCondition:
        return LogicalCondition("and", [
            ColumnCondition(column, ">=", lower),
            ColumnCondition(column, "<=", upper)
        ])

    @staticmethod
    def in_list(column: str, values: List[Any]) -> ColumnCondition:
        return ColumnCondition(column, "in", values)


    @staticmethod
    def is_null(column: str) -> ColumnCondition:
        return ColumnCondition(column, "is null", None)

    @staticmethod
    def not_null(column: str) -> ColumnCondition:
        return ColumnCondition(column, "is not null", None)

    # Logical operators on multiple ColumnConditions
    @staticmethod
    def and_(*conditions: FilterNode) -> LogicalCondition:
        return LogicalCondition("and", list(conditions))

    @staticmethod
    def or_(*conditions: FilterNode) -> LogicalCondition:
        return LogicalCondition("or", list(conditions))

    @staticmethod
    def not_(condition: FilterNode) -> LogicalCondition:
        return LogicalCondition("not", [condition])