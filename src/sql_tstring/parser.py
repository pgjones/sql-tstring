from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import auto, Enum, unique
from typing import cast

from sql_tstring.t import Interpolation as TInterpolation, Template as TTemplate

try:
    from string.templatelib import Interpolation, Template
except ImportError:

    class Interpolation:  # type: ignore[no-redef]
        pass

    class Template:  # type: ignore[no-redef]
        pass


SPLIT_RE = re.compile(r"([^\s'(]+\(|\(|'+|[ ',;)\n\t])")


@unique
class PlaceholderType(Enum):
    COLUMN = auto()
    DISALLOWED = auto()
    FRAME = auto()
    LOCK = auto()
    SORT = auto()
    TABLE = auto()
    VARIABLE = auto()
    VARIABLE_CONDITION = auto()
    VARIABLE_DEFAULT = auto()


@dataclass
class ClauseProperties:
    allow_empty: bool
    placeholder_type: PlaceholderType
    separators: set[str]


type ClauseDictionary = dict[str, "ClauseDictionary" | ClauseProperties]

_JOIN_CLAUSE = ClauseProperties(
    allow_empty=False, placeholder_type=PlaceholderType.TABLE, separators=set()
)

CLAUSES: ClauseDictionary = {
    "delete": {
        "from": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=PlaceholderType.TABLE, separators=set()
            ),
        },
    },
    "default": {
        "values": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=PlaceholderType.DISALLOWED,
                separators=set(),
            ),
        },
    },
    "for": {
        "update": {
            "": ClauseProperties(
                allow_empty=True, placeholder_type=PlaceholderType.LOCK, separators=set()
            )
        },
    },
    "full": {
        "join": {
            "": _JOIN_CLAUSE,
        },
        "outer": {
            "join": {
                "": _JOIN_CLAUSE,
            },
        },
    },
    "group": {
        "by": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=PlaceholderType.COLUMN, separators={","}
            )
        },
    },
    "inner": {
        "join": {
            "": _JOIN_CLAUSE,
        },
    },
    "insert": {
        "into": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=PlaceholderType.DISALLOWED,
                separators=set(),
            )
        },
    },
    "left": {
        "join": {
            "": _JOIN_CLAUSE,
        },
        "outer": {
            "join": {
                "": _JOIN_CLAUSE,
            },
        },
    },
    "on": {
        "conflict": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=PlaceholderType.DISALLOWED,
                separators=set(),
            )
        },
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.VARIABLE, separators={","}
        ),
    },
    "order": {
        "by": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=PlaceholderType.SORT, separators={","}
            )
        },
    },
    "partition": {
        "by": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=PlaceholderType.COLUMN, separators={","}
            )
        },
    },
    "right": {
        "join": {
            "": _JOIN_CLAUSE,
        },
        "outer": {
            "join": {
                "": _JOIN_CLAUSE,
            },
        },
    },
    "do": {
        "update": {
            "set": {
                "": ClauseProperties(
                    allow_empty=False,
                    placeholder_type=PlaceholderType.VARIABLE,
                    separators={","},
                ),
            },
        },
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.DISALLOWED, separators=set()
        ),
    },
    "from": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.TABLE, separators=set()
        )
    },
    "groups": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.FRAME, separators=set()
        )
    },
    "having": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=PlaceholderType.VARIABLE_CONDITION,
            separators={"and", "or"},
        )
    },
    "join": {
        "": _JOIN_CLAUSE,
    },
    "limit": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.VARIABLE, separators=set()
        )
    },
    "offset": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.VARIABLE, separators=set()
        )
    },
    "range": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.FRAME, separators=set()
        )
    },
    "returning": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.DISALLOWED, separators={","}
        )
    },
    "rows": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.FRAME, separators=set()
        )
    },
    "select": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.COLUMN, separators={","}
        )
    },
    "set": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.VARIABLE, separators={","}
        )
    },
    "union": {
        "": ClauseProperties(
            allow_empty=True, placeholder_type=PlaceholderType.DISALLOWED, separators=set()
        ),
        "all": {
            "": ClauseProperties(
                allow_empty=True, placeholder_type=PlaceholderType.DISALLOWED, separators=set()
            )
        },
    },
    "update": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.DISALLOWED, separators=set()
        )
    },
    "values": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=PlaceholderType.VARIABLE_DEFAULT,
            separators={","},
        )
    },
    "with": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.DISALLOWED, separators=set()
        )
    },
    "where": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=PlaceholderType.VARIABLE_CONDITION,
            separators={"and", "or"},
        )
    },
}

OPERATORS: dict[str, dict] = {
    "=": {},
    "<>": {},
    "!=": {},
    ">": {},
    "<": {},
    ">=": {},
    "<=": {},
    "between": {},
    "ilike": {},
    "in": {},
    "is": {
        "not": {
            "null": {},
            "true": {},
        },
        "null": {},
        "true": {},
    },
    "like": {},
    "not": {
        "between": {},
        "ilike": {},
        "in": {},
        "like": {},
    },
}


@dataclass
class Statement:
    clauses: list[Clause | Group] = field(default_factory=list)
    parent: ExpressionGroup | Function | Group | None = None


@dataclass
class Clause:
    parent: Statement
    properties: ClauseProperties
    text: str
    expressions: list[Expression] = field(init=False)
    removed: bool = False

    def __post_init__(self) -> None:
        self.expressions = [Expression(self)]


@dataclass
class Expression:
    parent: Clause | ExpressionGroup
    parts: list[
        ExpressionGroup | Function | Group | Operator | Part | Placeholder | Statement | Literal
    ] = field(default_factory=list)
    removed: bool = False
    separator: str = ""


@dataclass
class Part:
    parent: Expression | Function | Group | Literal
    text: str


@dataclass
class Placeholder:
    parent: Expression | Function | Group | Literal
    value: object


@dataclass
class Group:
    parent: Expression | Function | Group | Statement
    parts: list[Function | Group | Literal | Operator | Part | Placeholder | Statement] = field(
        default_factory=list
    )


@dataclass
class ExpressionGroup:
    parent: Expression
    expressions: list[Expression] = field(init=False)

    def __post_init__(self) -> None:
        self.expressions = [Expression(self)]


@dataclass
class Function:
    name: str
    parent: Expression | Function | Group
    parts: list[Function | Group | Literal | Operator | Part | Placeholder | Statement] = field(
        default_factory=list
    )


@dataclass
class Literal:
    parent: Expression | Function | Group
    parts: list[Operator | Part | Placeholder] = field(default_factory=list)


@dataclass
class Operator:
    parent: Expression | Function | Group | Literal
    text: str


type ParentNode = Clause | Expression | ExpressionGroup | Function | Group
type Node = ParentNode | Literal | Statement
type Element = Node | Operator | Part | Placeholder


def parse(template: Template | TTemplate) -> list[Statement]:
    statements = [Statement()]
    current_node: Node = statements[0]
    _parse_template(template, current_node, statements)
    return statements


def _parse_template(
    template: Template | TTemplate, current_node: Node, statements: list[Statement]
) -> None:
    for item in template:
        match item:
            case Interpolation(value, _, _, _):
                if isinstance(value, (Template, TTemplate)):
                    _parse_template(value, current_node, statements)
                else:
                    _parse_placeholder(current_node, value)
            case TInterpolation(value, _, _, _):
                if isinstance(value, (Template, TTemplate)):
                    _parse_template(value, current_node, statements)
                else:
                    _parse_placeholder(current_node, value)
            case str() as raw:
                current_node = _parse_string(raw, current_node, statements)


def _parse_placeholder(
    current_node: Node,
    value: object,
) -> None:
    if isinstance(current_node, (Expression, Function, Group, Literal)):
        parent = current_node
    elif isinstance(current_node, Statement):
        raise ValueError("Invalid syntax")
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]
    placeholder = Placeholder(parent=parent, value=value)
    parent.parts.append(placeholder)


def _parse_string(
    raw: str,
    current_node: Node,
    statements: list[Statement],
) -> Node:
    tokens = [part.strip() for part in SPLIT_RE.split(raw) if part.strip() != ""]
    index = 0
    while index < len(tokens):
        raw_current_token = tokens[index]
        current_token = raw_current_token.lower()

        consumed = 1
        if isinstance(current_node, Literal):
            if current_token == "'":
                current_node = _find_node(  # type: ignore[assignment]
                    current_node.parent, (Clause, ExpressionGroup, Function, Group)
                )
            else:
                current_node.parts.append(Part(parent=current_node, text=raw_current_token))
        elif isinstance(current_node, (Function, Group)):
            if current_token == ")":
                group_or_function = _find_node(current_node, (Function, Group))
                current_node = _find_node(  # type: ignore[assignment]
                    group_or_function.parent, (Clause, ExpressionGroup, Function, Group)
                )
            else:
                current_node, consumed = _parse_token(
                    current_node, raw_current_token, current_token, tokens[index:], statements
                )
        elif isinstance(current_node, ExpressionGroup):
            if current_token == ")":
                group = _find_node(current_node, ExpressionGroup)
                current_node = _find_node(  # type: ignore[assignment]
                    group.parent, (Clause, ExpressionGroup)
                )
            else:
                clause = _find_node(current_node, Clause)
                if current_token in clause.properties.separators:
                    current_node.expressions.append(
                        Expression(parent=current_node, separator=raw_current_token)
                    )
                else:
                    current_node, consumed = _parse_token(
                        current_node, raw_current_token, current_token, tokens[index:], statements
                    )
        elif isinstance(current_node, Clause):
            if current_token in current_node.properties.separators:
                current_node.expressions.append(
                    Expression(parent=current_node, separator=raw_current_token)
                )
            else:
                current_node, consumed = _parse_token(
                    current_node, raw_current_token, current_token, tokens[index:], statements
                )
        elif isinstance(current_node, Expression):
            clause = _find_node(current_node, Clause)
            if current_token in clause.properties.separators:
                parent_group = cast(
                    Clause | ExpressionGroup, _find_node(current_node, (Clause, ExpressionGroup))
                )
                current_node = Expression(parent=parent_group, separator=raw_current_token)
                parent_group.expressions.append(current_node)
            else:
                current_node, consumed = _parse_token(
                    current_node, raw_current_token, current_token, tokens[index:], statements
                )
        else:  # Statement
            current_node, consumed = _parse_token(
                current_node, raw_current_token, current_token, tokens[index:], statements
            )

        index += consumed

    return current_node


def _parse_token(
    current_node: ParentNode | Statement,
    raw_current_token: str,
    current_token: str,
    tokens: list[str],
    statements: list[Statement],
) -> tuple[Node, int]:
    if current_token in CLAUSES:
        return _parse_clause(current_node, tokens)
    elif current_token == ";":
        statements.append(Statement())
        return statements[-1], 1
    elif current_token == "(":
        return _parse_group(current_node)
    elif not isinstance(current_node, Statement):
        if current_token in OPERATORS:
            return _parse_operator(current_node, tokens)
        elif current_token == "'":
            return _parse_literal(current_node)
        elif current_token == "(":
            return _parse_group(current_node)
        elif current_token.endswith("("):
            return _parse_function(current_node, raw_current_token[:-1])
        elif current_token == ")":
            current_node = _find_node(  # type: ignore[assignment]
                current_node, (ExpressionGroup, Function, Group)
            )
            return current_node.parent, 1
        else:
            return _parse_part(current_node, raw_current_token)
    else:
        raise ValueError("Invalid syntax")


def _parse_clause(
    current_node: ParentNode | Statement,
    tokens: list[str],
) -> tuple[Clause, int]:
    index = 0
    clause_entry = CLAUSES
    text = ""
    while index < len(tokens) and tokens[index].lower() in clause_entry:
        clause_entry = cast(ClauseDictionary, clause_entry[tokens[index].lower()])
        text = f"{text} {tokens[index]}".strip()
        index += 1

    if isinstance(current_node, (Function, Group)):
        statement = Statement(parent=current_node)
        current_node.parts.append(statement)
        current_node = statement
    elif isinstance(current_node, ExpressionGroup):
        statement = Statement(parent=current_node)
        current_node.expressions[-1].parts.append(statement)
        current_node = statement
    else:  # Clause | Expression | Statement
        current_node = _find_node(current_node, Statement)

    clause_properties = cast(ClauseProperties, clause_entry[""])
    clause = Clause(
        parent=current_node,
        properties=clause_properties,
        text=text,
    )
    current_node.clauses.append(clause)
    return clause, index


def _parse_operator[T: ParentNode](
    current_node: T,
    tokens: list[str],
) -> tuple[T, int]:
    index = 0
    operator_entry = OPERATORS
    text = ""
    while index < len(tokens) and tokens[index].lower() in operator_entry:
        operator_entry = operator_entry[tokens[index].lower()]
        text = f"{text} {tokens[index]}".strip()
        index += 1
    if isinstance(current_node, (Expression, Function, Group)):
        parent = current_node
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]  # type: ignore[assignment]
    parent.parts.append(Operator(parent=parent, text=text))
    return current_node, index


def _parse_group(
    current_node: ParentNode | Statement,
) -> tuple[ExpressionGroup | Group, int]:
    group: ExpressionGroup | Group
    if isinstance(current_node, (Expression, Function, Group)):
        group = Group(parent=current_node)
        current_node.parts.append(group)
        return group, 1
    elif isinstance(current_node, Statement):
        group = Group(parent=current_node)
        current_node.clauses.append(group)
        return group, 1
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]
        if len(parent.parts) == 0:
            group = ExpressionGroup(parent=parent)
        else:
            group = Group(parent=parent)
        parent.parts.append(group)
        return group, 1


def _parse_function(
    current_node: ParentNode,
    name: str,
) -> tuple[Function, int]:
    if isinstance(current_node, (Expression, Function, Group)):
        parent = current_node
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]
    func = Function(name=name, parent=parent)
    parent.parts.append(func)
    return func, 1


def _parse_literal(current_node: ParentNode) -> tuple[Literal, int]:
    if isinstance(current_node, (Expression, Function, Group)):
        value = Literal(parent=current_node)
        current_node.parts.append(value)
        return value, 1
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]
        value = Literal(parent=parent)
        parent.parts.append(value)
        return value, 1


def _parse_part[T: ParentNode](
    current_node: T,
    text: str,
) -> tuple[T, int]:
    if isinstance(current_node, (Expression, Function, Group)):
        parent = current_node
    else:  # Clause | ExpressionGroup
        parent = current_node.expressions[-1]  # type: ignore[assignment]
    parent.parts.append(Part(parent=parent, text=text))
    return current_node, 1


def _find_node[T: Element](current_node: Element, target: type[T] | tuple[type[T], ...]) -> T:
    while not isinstance(current_node, target):
        if current_node is None:
            raise ValueError("Parsing Error")
        current_node = current_node.parent
    return cast(T, current_node)
