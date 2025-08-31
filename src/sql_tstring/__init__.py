from __future__ import annotations

import typing
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import auto, Enum, unique
from types import TracebackType

from sql_tstring.parser import (
    Clause,
    Element,
    Expression,
    ExpressionGroup,
    Function,
    Group,
    Literal,
    Operator,
    parse,
    Part,
    Placeholder,
    PlaceholderType,
    Statement,
)
from sql_tstring.t import t, Template as TTemplate

try:
    from string.templatelib import Template
except ImportError:

    class Template:  # type: ignore[no-redef]
        pass


@unique
class RewritingValue(Enum):
    ABSENT = auto()
    IS_NULL = auto()
    IS_NOT_NULL = auto()


type AbsentType = typing.Literal[RewritingValue.ABSENT]
Absent: AbsentType = RewritingValue.ABSENT
IsNull = RewritingValue.IS_NULL
IsNotNull = RewritingValue.IS_NOT_NULL


@dataclass
class Context:
    columns: set[str] = field(default_factory=set)
    dialect: typing.Literal["asyncpg", "sql"] = "sql"
    tables: set[str] = field(default_factory=set)


_context_var: ContextVar[Context] = ContextVar("sql_tstring_context")


def get_context() -> Context:
    try:
        return _context_var.get()
    except LookupError:
        context = Context()
        _context_var.set(context)
        return context


def set_context(context: Context) -> None:
    _context_var.set(context)


def sql_context(
    columns: set[typing.LiteralString] | None = None,
    dialect: typing.Literal["asyncpg", "sql"] | None = None,
    tables: set[typing.LiteralString] | None = None,
) -> _ContextManager:
    ctx = get_context()
    ctx_manager = _ContextManager(ctx)
    if columns is not None:
        ctx_manager._context.columns = columns
    if dialect is not None:
        ctx_manager._context.dialect = dialect
    if tables is not None:
        ctx_manager._context.tables = tables
    return ctx_manager


def sql(
    query_or_template: str | Template | TTemplate, values: dict[str, typing.Any] | None = None
) -> tuple[str, list]:
    template: Template | TTemplate
    if isinstance(query_or_template, (Template, TTemplate)) and values is None:
        template = query_or_template
    elif isinstance(query_or_template, str) and values is not None:
        template = t(query_or_template, values)
    else:
        raise ValueError("Must call with a template, or a query string and values")

    parsed_queries = parse(template)
    result_str = ""
    result_values: list[typing.Any] = []
    ctx = get_context()
    for raw_parsed_query in parsed_queries:
        parsed_query = deepcopy(raw_parsed_query)
        new_values = _replace_placeholders(parsed_query, 0)
        result_str += _print_node(parsed_query, [None] * len(result_values), ctx.dialect)
        result_values.extend(new_values)

    return result_str, result_values


class _ContextManager:
    def __init__(self, context: Context) -> None:
        self._context = replace(context)

    def __enter__(self) -> Context:
        self._original_context = get_context()
        set_context(self._context)
        return self._context

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        set_context(self._original_context)


def _check_valid(
    value: object,
    *,
    case_sensitive: set[str] | None = None,
    case_insensitive: set[str] | None = None,
    value_type: type = str,
) -> None:
    if case_sensitive is None:
        case_sensitive = set()
    if case_insensitive is None:
        case_insensitive = set()
    if not isinstance(value, value_type):
        raise ValueError(f"{value} is not valid, must be {value_type}")
    if isinstance(value, str) and (
        value not in case_sensitive and value.lower() not in case_insensitive
    ):
        raise ValueError(
            f"{value} is not valid, must be one of {case_sensitive} or {case_insensitive}"
        )


def _print_node(
    node: Element,
    placeholders: list | None = None,
    dialect: str = "sql",
) -> str:
    if placeholders is None:
        placeholders = []

    match node:
        case Statement():
            result = " ".join(_print_node(clause, placeholders, dialect) for clause in node.clauses)
        case Clause() | ExpressionGroup():
            result = ""

            for expression in node.expressions:
                addition = _print_node(expression, placeholders, dialect)
                separator = ""
                if result != "":
                    separator = expression.separator
                if addition != "":
                    result += f" {separator} {addition}"

            result = result.strip()

            if isinstance(node, ExpressionGroup):
                if result != "":
                    result = f"({result})"
            else:
                if node.removed:
                    result = ""
                elif result == "" and not node.properties.allow_empty:
                    result = ""
                else:
                    result = f"{node.text} {result}"
        case Expression():
            if not node.removed:
                result = " ".join(_print_node(part, placeholders, dialect) for part in node.parts)
            else:
                result = ""
        case Function():
            arguments = " ".join(_print_node(part, placeholders, dialect) for part in node.parts)
            result = f"{node.name}({arguments})"
        case Group():
            result = (
                f"({" ".join(_print_node(part, placeholders, dialect) for part in node.parts)})"
            )
        case Operator():
            result = node.text
        case Part():
            result = node.text
        case Placeholder():
            placeholders.append(None)
            result = f"${len(placeholders)}" if dialect == "asyncpg" else "?"
        case Literal():
            value = "".join(_print_node(part, placeholders, dialect) for part in node.parts)
            result = f"'{value}'"

    return result.strip()


def _replace_placeholders(
    node: Element,
    index: int,
) -> list[typing.Any]:
    result = []
    match node:
        case Statement():
            for clause_ in node.clauses:
                result.extend(_replace_placeholders(clause_, 0))
        case Clause() | ExpressionGroup():
            for index, expression_ in enumerate(node.expressions):
                result.extend(_replace_placeholders(expression_, index))
        case Expression() | Function() | Group() | Literal():
            for index, part in enumerate(node.parts):
                result.extend(_replace_placeholders(part, index))
        case Placeholder():
            result.extend(_replace_placeholder(node, index))

    return result


def _replace_placeholder(
    node: Placeholder,
    index: int,
) -> list[typing.Any]:
    result = []
    ctx = get_context()

    clause_or_function = node.parent
    while not isinstance(clause_or_function, (Clause, Function)):
        clause_or_function = clause_or_function.parent  # type: ignore

    clause: Clause | None = None
    placeholder_type = PlaceholderType.VARIABLE
    if isinstance(clause_or_function, Clause):
        clause = clause_or_function
        placeholder_type = clause_or_function.properties.placeholder_type

    value = node.value
    new_node: Part | Placeholder
    if value is RewritingValue.ABSENT:
        if placeholder_type == PlaceholderType.VARIABLE_DEFAULT:
            new_node = Part(text="DEFAULT", parent=node.parent)
            node.parent.parts[index] = new_node
        elif placeholder_type == PlaceholderType.LOCK:
            if clause is not None:
                clause.removed = True
        else:
            expression = node.parent
            while not isinstance(expression, Expression):
                expression = expression.parent

            expression.removed = True
    elif isinstance(node.parent, Literal):
        if not isinstance(value, str):
            raise RuntimeError("Invalid placeholder usage")
        else:
            for position, part in enumerate(node.parent.parent.parts):
                if part is node.parent:
                    value = ""
                    for bit in part.parts:  # type: ignore[union-attr]
                        if isinstance(bit, Placeholder):
                            value += str(bit.value)
                        else:
                            value += bit.text  # type: ignore[union-attr]
                    node.parent.parent.parts[position] = Placeholder(
                        parent=part.parent, value=value  # type: ignore[arg-type]
                    )
                    result.append(value)
    else:
        match placeholder_type:
            case PlaceholderType.COLUMN:
                _check_valid(value, case_sensitive=ctx.columns)
                new_node = Part(text=typing.cast(str, value), parent=node.parent)
            case PlaceholderType.FRAME:
                _check_valid(value, value_type=int)
                new_node = Part(text=str(value), parent=node.parent)
            case PlaceholderType.LOCK:
                _check_valid(value, case_insensitive={"", "nowait", "skip locked"})
                new_node = Part(text=typing.cast(str, value), parent=node.parent)
            case PlaceholderType.SORT:
                _check_valid(
                    value,
                    case_sensitive=ctx.columns,
                    case_insensitive={"asc", "ascending", "desc", "descending"},
                )
                new_node = Part(text=typing.cast(str, value), parent=node.parent)
            case PlaceholderType.TABLE:
                _check_valid(value, case_sensitive=ctx.tables)
                new_node = Part(text=typing.cast(str, value), parent=node.parent)
            case _:
                if (
                    value is RewritingValue.IS_NULL or value is RewritingValue.IS_NOT_NULL
                ) and placeholder_type == PlaceholderType.VARIABLE_CONDITION:
                    for part in node.parent.parts:
                        if isinstance(part, Operator):
                            if value is RewritingValue.IS_NULL:
                                part.text = "IS"
                            else:
                                part.text = "IS NOT"
                    new_node = Part(text="NULL", parent=node.parent)
                else:
                    new_node = node
                    result.append(value)

        if isinstance(node.parent, (Expression, ExpressionGroup, Function, Group)):
            node.parent.parts[index] = new_node
        else:
            raise RuntimeError("Invalid placeholder")

    return result
