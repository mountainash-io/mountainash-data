from abc import ABC, abstractmethod
from typing import Any, List, Union, Callable, Optional

class FilterNode(ABC):
    @abstractmethod
    def accept(self, visitor: 'FilterVisitor') -> Callable:
        pass


class ColumnCondition(FilterNode):
    def __init__(self, column: str, operator: str, value: Any, compare_column: Optional[str] = None):
        self.column = column
        self.operator = operator
        self.value = value
        self.compare_column = compare_column

    def accept(self, visitor: 'FilterVisitor') -> Callable:
        return visitor.visit_column_condition(self)


class LogicalCondition(FilterNode):
    ALWAYS_TRUE_OP = "__always_true__"
    ALWAYS_FALSE_OP = "__always_false__"

    def __init__(self, operator: str, operands: List[FilterNode]):
        self.operator = operator
        self.operands = operands

    @classmethod
    def always_true(cls) -> 'LogicalCondition':
        """Creates a logical condition that always evaluates to True."""
        return cls(operator=cls.ALWAYS_TRUE_OP, operands=[])

    @classmethod
    def always_false(cls) -> 'LogicalCondition':
        """Creates a logical condition that always evaluates to False."""
        return cls(operator=cls.ALWAYS_FALSE_OP, operands=[])

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

    #############
    # Shortcut Aliases - Values
    @classmethod
    def eq(cls, column: str, value: Any) -> ColumnCondition:
        return cls.equals(column, value)

    @classmethod
    def ne(cls, column: str, value: Any) -> ColumnCondition:
        return cls.not_equals(column, value)

    @classmethod
    def gt(cls, column: str, value: Any) -> ColumnCondition:
        return cls.greater_than(column, value)

    @classmethod
    def lt(cls, column: str, value: Any) -> ColumnCondition:
        return cls.less_than(column, value)

    @classmethod
    def ge(cls, column: str, value: Any) -> ColumnCondition:
        return cls.greater_than_or_equal(column, value)

    @classmethod
    def le(cls, column: str, value: Any) -> ColumnCondition:
        return cls.less_than_or_equal(column, value)

    #############
    # Shortcut Aliases - Column to Column
    @classmethod
    def col_eq(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_equals(column1, column2)

    @classmethod
    def col_ne(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_not_equals(column1, column2)

    @classmethod
    def col_gt(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_greater_than(column1, column2)

    @classmethod
    def col_lt(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_less_than(column1, column2)

    @classmethod
    def col_ge(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_greater_than_or_equal(column1, column2)

    @classmethod
    def col_le(cls, column1: str, column2: str) -> ColumnCondition:
        return cls.column_less_than_or_equal(column1, column2)



    #####  Actual Implementations ####

    @classmethod
    def always_true(cls) -> ColumnCondition:
        return LogicalCondition.always_true()

    @classmethod
    def always_false(cls) -> ColumnCondition:
        return LogicalCondition.always_false()

    # Column to Value comparison
    @classmethod
    def equals(cls, column: str, value: Any) -> ColumnCondition:
        return ColumnCondition(column, "==", value)

    @classmethod
    def not_equals(cls, column: str, value: Any) -> ColumnCondition:
        return ColumnCondition(column, "!=", value)

    @classmethod
    def greater_than(cls, column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, ">", value)

    @classmethod
    def less_than(cls, column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, "<", value)

    @classmethod
    def greater_than_or_equal(cls, column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, ">=", value)

    @classmethod
    def less_than_or_equal(cls, column: str, value: Union[int, float]) -> ColumnCondition:
        return ColumnCondition(column, "<=", value)


    @classmethod
    def between(cls, column: str, lower: Union[int, float], upper: Union[int, float]) -> LogicalCondition:
        return LogicalCondition("and", [
            ColumnCondition(column, ">=", lower),
            ColumnCondition(column, "<=", upper)
        ])

    @classmethod
    def in_list(cls, column: str, values: List[Any]) -> ColumnCondition:
        return ColumnCondition(column, "in", values)


    @classmethod
    def is_null(cls, column: str) -> ColumnCondition:
        return ColumnCondition(column, "is null", None)

    @classmethod
    def not_null(cls, column: str) -> ColumnCondition:
        return ColumnCondition(column, "is not null", None)

    # Column to Column comparison
    @classmethod
    def column_equals(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, "==", None, compare_column=column2)

    @classmethod
    def column_not_equals(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, "!=", None, compare_column=column2)

    @classmethod
    def column_greater_than(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, ">", None, compare_column=column2)

    @classmethod
    def column_less_than(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, "<", None, compare_column=column2)

    @classmethod
    def column_greater_than_or_equal(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, ">=", None, compare_column=column2)

    @classmethod
    def column_less_than_or_equal(cls, column1: str, column2: str) -> ColumnCondition:
        return ColumnCondition(column1, "<=", None, compare_column=column2)



    # Logical operators on multiple ColumnConditions
    @classmethod
    def and_(cls, *conditions: FilterNode) -> LogicalCondition:
        return LogicalCondition("and", list(conditions))

    @classmethod
    def or_(cls, *conditions: FilterNode) -> LogicalCondition:
        return LogicalCondition("or", list(conditions))

    @classmethod
    def not_(cls, condition: FilterNode) -> LogicalCondition:
        return LogicalCondition("not", [condition]) 