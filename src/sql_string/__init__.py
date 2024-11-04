from __future__ import annotations

from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Any, Literal

from sql_string.parser import (
    Clause,
    ClausePlaceholderType,
    Expression,
    Group,
    parse_raw,
    Part,
    Placeholder,
    Statement,
)


@dataclass
class Context:
    columns: set[str] = field(default_factory=set)
    dialect: Literal["asyncpg", "sql"] = "sql"
    tables: set[str] = field(default_factory=set)


_context_var: ContextVar[Context] = ContextVar("sql_string_context")


def get_context() -> Context:
    try:
        return _context_var.get()
    except LookupError:
        context = Context()
        _context_var.set(context)
        return context


def set_context(context: Context) -> None:
    _context_var.set(context)


def sql_context(**kwargs: Any) -> _ContextManager:
    ctx = get_context()
    ctx_manager = _ContextManager(ctx)
    for key, value in kwargs.items():
        setattr(ctx_manager._context, key, value)
    return ctx_manager


class Absent:
    pass


def sql(query: str, values: dict[str, Any]) -> tuple[str, list]:
    parsed_queries = parse_raw(query)
    result_str = ""
    result_values = []
    for raw_parsed_query in parsed_queries:
        parsed_query = deepcopy(raw_parsed_query)
        result_values.extend(_replace_placeholders(parsed_query, 0, values))
        result_str += _print_node(parsed_query, [None] * len(result_values))

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


def _check_valid(value: str, valid_options: set[str]) -> None:
    if value not in valid_options:
        raise ValueError(f"{value} is not valid, must be one of {valid_options}")


def _print_node(
    node: Clause | Expression | Group | Part | Placeholder | Statement,
    placeholders: list | None = None,
    dialect: str = "sql",
) -> str:
    if placeholders is None:
        placeholders = []

    match node:
        case Statement():
            result = " ".join(_print_node(clause, placeholders, dialect) for clause in node.clauses)
        case Clause():
            if not node.removed:
                expressions = " ".join(
                    _print_node(expression, placeholders, dialect)
                    for expression in node.expressions
                ).strip()
                for suffix in node.separators:
                    expressions = expressions.removesuffix(suffix).removesuffix(suffix.upper())
                result = f"{node.text} {expressions}"
            else:
                result = ""
        case Expression():
            if not node.removed:
                result = " ".join(_print_node(part, placeholders, dialect) for part in node.parts)
            else:
                result = ""
        case Group():
            result = (
                f"({" ".join(_print_node(part, placeholders, dialect) for part in node.parts)})"
            )
        case Part():
            result = node.text
        case Placeholder():
            placeholders.append(None)
            result = f"${len(placeholders)}" if dialect == "asyncpg" else "?"

    return result.strip()


def _replace_placeholders(
    node: Clause | Expression | Group | Part | Placeholder | Statement,
    index: int,
    values: dict[str, Any],
) -> list[Any]:
    result = []
    ctx = get_context()
    match node:
        case Statement():
            for clause_ in node.clauses:
                result.extend(_replace_placeholders(clause_, 0, values))
        case Clause():
            for index, expression_ in enumerate(node.expressions):
                result.extend(_replace_placeholders(expression_, index, values))
        case Expression() | Group():
            for index, part in enumerate(node.parts):
                result.extend(_replace_placeholders(part, index, values))
        case Placeholder():
            clause = node.parent
            while not isinstance(clause, Clause):
                clause = clause.parent  # type: ignore

            value = values[node.name]
            new_node: Part | Placeholder
            if value is Absent or isinstance(value, Absent):
                if clause.placeholder_type == ClausePlaceholderType.VARIABLE_DEFAULT:
                    new_node = Part(text="default", parent=node.parent)
                    node.parent.parts[index] = new_node
                elif clause.placeholder_type == ClausePlaceholderType.LOCK:
                    clause.removed = True
                else:
                    expression = node.parent
                    while not isinstance(expression, Expression):
                        expression = expression.parent

                    expression.removed = True
            else:
                if clause.text == "order by":
                    _check_valid(value, ctx.columns | {"ASC", "DESC"})
                    new_node = Part(text=value, parent=node.parent)
                elif clause.placeholder_type == ClausePlaceholderType.COLUMN:
                    _check_valid(value, ctx.columns)
                    new_node = Part(text=value, parent=node.parent)
                elif clause.placeholder_type == ClausePlaceholderType.TABLE:
                    _check_valid(value, ctx.tables)
                    new_node = Part(text=value, parent=node.parent)
                elif clause.placeholder_type == ClausePlaceholderType.LOCK:
                    _check_valid(value, {"", "NOWAIT", "SKIP LOCKED"})
                    new_node = Part(text=value, parent=node.parent)
                else:
                    if value is None:
                        for part in node.parent.parts:
                            if isinstance(part, Part):
                                if part.text == "=":
                                    part.text = "is"
                                elif part.text in {"!=", "<>"}:
                                    part.text = "is not"
                        new_node = Part(text="null", parent=node.parent)
                    else:
                        new_node = node
                        result.append(value)

                if isinstance(node.parent, (Expression, Group)):
                    node.parent.parts[index] = new_node
                else:
                    raise RuntimeError("Invalid placeholder")

    return result
