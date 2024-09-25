import re
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Any, Container, Literal

SPLIT_RE = re.compile(r"([ ,()])")


@dataclass
class Context:
    columns: Container[str] = field(default_factory=list)
    dialect: Literal["asyncpg", "sql"] = "sql"
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


def sql(query: str, values: dict[str, Any]) -> tuple[str, list]:
    ctx = get_context()
    root_node = _Node(text="", parent=None)
    current_node = root_node
    result_values = []
    for part in _tokenise(query):
        if part.lower() in {"insert", "group", "order"}:
            node = _Node(text=part.lower(), parent=root_node)
            current_node = node
            root_node.children.append(node)
        elif part.lower() == "update":
            if current_node.text == "for":
                current_node.text = "for update"
            else:
                node = _Node(text="update", parent=root_node)
                current_node = node
                root_node.children.append(node)
        elif part.lower() in {"on", "for", "from", "select", "set", "values", "with", "where"}:
            node = _Node(text=part.lower(), parent=root_node)
            current_node = node
            root_node.children.append(node)
        elif part.lower() in {"by", "conflict", "do", "into"}:
            current_node.parent.children[-1].text += f" {part.lower()}"
        elif part.lower() in {"and", "any", "or"}:
            node = _Node(text=part.lower(), parent=current_node)
            current_node.children.append(node)
        else:
            if part.startswith("{{") and part.endswith("}}"):
                node = _Node(text=part[1:-1], parent=current_node)
            elif part[0] == "{" and part[-1] == "}":
                value = values[part[1:-1]]
                if value is Absent or isinstance(value, Absent):
                    if current_node.text == "values":
                        node = _Node(text="default", parent=current_node)
                    else:
                        node = None
                elif current_node.text in {"order by", "select"}:
                    _check_valid(value.lower(), ctx.columns)
                    node = _Node(text=value.lower(), parent=current_node)
                elif current_node.text in {"from", "update"}:
                    _check_valid(value.lower(), ctx.tables)
                    node = _Node(text=value.lower(), parent=current_node)
                elif current_node.text == "for update":
                    _check_valid(value.lower(), {"", "nowait", "skip locked"})
                    node = _Node(text=value.lower(), parent=current_node)
                elif current_node.text in {"set", "values", "where"}:
                    result_values.append(value)
                    if ctx.dialect == "asyncpg":
                        placeholder = f"${len(result_values)}"
                    else:
                        placeholder = "?"
                    node = _Node(text=placeholder, parent=current_node)
            else:
                node = _Node(text=part.replace("{{", "{").replace("}}", "}"), parent=current_node)

            current_node.children.append(node)

    _clean_tree(root_node)
    return str(root_node), result_values


@dataclass
class _Node:
    text: str
    parent: "_Node | None"
    children: list["_Node | None"] = field(default_factory=list)

    def __str__(self) -> str:
        child_str = " ".join(str(node) for node in self.children)
        return f"{self.text} {child_str}".strip()


def _tokenise(raw: str) -> list[str]:
    return [part.strip() for part in SPLIT_RE.split(raw) if part.strip() != ""]


def _check_valid(value: str, valid_options: Container[str]) -> None:
    if value not in valid_options:
        raise ValueError(f"{value} is not valid, must be one of {valid_options}")


def _clean_tree(root: _Node) -> None:
    new_children = []
    for node in root.children:
        if node.text in {"order by", "set"}:
            _clean_node(node, groupings={","})
        elif node.text == "where":
            _clean_node(node, groupings={"and", "or"})
        else:
            _clean_node(node, groupings=set())

        if len(node.children) > 0 or node.text in {"on conflict do", "update"}:
            new_children.append(node)
    root.children = new_children


def _clean_node(node: _Node, groupings: set[str]) -> None:
    new_children = []
    group = []
    for child in node.children:
        group.append(child)
        if child is not None and child.text in groupings:
            if None not in group:
                new_children.extend(group)
            group = []
    if None not in group:
        new_children.extend(group)
    if len(new_children) > 0:
        if new_children[-1].text in groupings:
            del new_children[-1]
        if new_children[0].text in groupings:
            del new_children[0]
    node.children = new_children
