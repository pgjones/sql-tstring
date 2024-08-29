import re
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Any, Container

from sqlglot import exp, parse, parse_one, to_identifier
from sqlglot._typing import E

BRACES_RE = re.compile(r"(\{.*?\}\}?)")


@dataclass
class Context:
    columns: Container[str] = field(default_factory=list)
    tables: Container[str] = field(default_factory=list)


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


def sql_context(**kwargs: Any) -> _ContextManager:
    ctx = get_context()
    ctx_manager = _ContextManager(ctx)
    for key, value in kwargs.items():
        setattr(ctx_manager._context, key, value)
    return ctx_manager


class Absent:
    pass


def sql(query: str, values: dict[str, Any]) -> tuple[str, list[Any]]:
    result_query = ""
    result_values = []
    ctx = get_context()

    for expressions in parse(_braces_to_placeholders(query)):
        for node in expressions.dfs():
            match node:
                case exp.Placeholder():
                    identifier = node.this
                    value = values[identifier]
                    context = _context(node)
                    if isinstance(value, Absent):
                        match context:
                            case exp.Values():
                                new_node = parse_one("DEFAULT")
                                node.parent.set(node.arg_key, new_node, node.index)
                            case exp.Update() | exp.Where():
                                _remove_node(node)
                            case exp.Ordered():
                                _remove_node(node)
                    else:
                        if isinstance(context, exp.Ordered):
                            if value not in ctx.columns:
                                raise ValueError(f"{value} is not a valid column")
                            new_node = to_identifier(value)
                        else:
                            new_node = parse_one("?")
                            result_values.append(value)
                        node.parent.set(node.arg_key, new_node, node.index)
        result_query += str(expressions)
    return str(result_query), result_values


def _braces_to_placeholders(raw: str) -> str:
    result = ""
    last_position = 0
    for match_ in BRACES_RE.finditer(raw):
        result += raw[last_position : match_.start()]
        content = match_.group()
        if content.startswith("{{") and content.endswith("}}"):
            result += content[1:-1]
        elif content[1:-1].isidentifier():
            result += f":{content[1:-1]}"
        else:
            raise SyntaxError(f"Value '{content[1:-1]}' must be an identifier")
        last_position = match_.end()
    result += raw[last_position:]
    return result


def _context(node: E) -> exp.Expression | None:
    match node:
        case exp.Ordered() | exp.Update() | exp.Values() | exp.Where():
            return node
        case None:
            return None
        case _:
            return _context(node.parent)


def _remove_node(node: E) -> None:
    match node:
        case exp.Connector():
            if node.left is not None and node.right is not None:
                return
            elif node.left is not None:
                node.parent.set(node.arg_key, node.left, node.index)
            else:  # node.right is not None
                node.parent.set(node.arg_key, node.right, node.index)
        case exp.Ordered() | exp.Update() | exp.Where():
            if node.is_leaf():
                node.parent.set(node.arg_key, None, node.index)
            return
        case _:
            node.parent.set(node.arg_key, None, node.index)
            _remove_node(node.parent)
