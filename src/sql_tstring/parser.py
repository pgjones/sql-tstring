from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import auto, Enum, unique
from typing import cast

try:
    from string.templatelib import Interpolation, Template  # type: ignore[import-untyped]
except ImportError:
    from sql_tstring.t import Interpolation, Template

SPLIT_RE = re.compile(r"([^\s(]+\(|\(|'+|[ ',;)])")


@unique
class PlaceholderType(Enum):
    COLUMN = auto()
    DISALLOWED = auto()
    LOCK = auto()
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
    "returning": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=PlaceholderType.DISALLOWED, separators={","}
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


@dataclass
class Statement:
    clauses: list[Clause] = field(default_factory=list)
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
    parts: list[ExpressionGroup | Function | Group | Part | Placeholder | Statement | Value] = (
        field(default_factory=list)
    )
    removed: bool = False
    separator: str = ""


@dataclass
class Part:
    parent: Expression | Function | Group | Value
    text: str


@dataclass
class Placeholder:
    parent: Expression | Function | Group | Value
    value: object


@dataclass
class Group:
    parent: Expression | Function | Group
    parts: list[Function | Group | Part | Placeholder | Statement] = field(default_factory=list)


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
    parts: list[Function | Group | Part | Placeholder | Statement] = field(default_factory=list)


@dataclass
class Value:
    parent: Expression
    parts: list[Part | Placeholder] = field(default_factory=list)


def parse_template(template: Template) -> list[Statement]:
    statements = [Statement()]
    current_node: Clause | ExpressionGroup | Function | Group | Statement = statements[0]

    for item in template:
        match item:
            case Interpolation(value, _, _, _):  # type: ignore[misc]
                _parse_placeholder(value, current_node)  # type: ignore
            case str() as raw:
                current_node = _parse_string(raw, current_node, statements)

    return statements


def _parse_string(
    raw: str,
    current_node: Clause | ExpressionGroup | Function | Group | Statement,
    statements: list[Statement],
) -> Clause | ExpressionGroup | Function | Group | Statement:
    tokens = [part.strip() for part in SPLIT_RE.split(raw) if part.strip() != ""]
    index = 0
    while index < len(tokens):
        raw_current_token = tokens[index]
        current_token = raw_current_token.lower()
        if current_token in CLAUSES:
            current_node, consumed = _parse_clause(tokens[index:], current_node)
            index += consumed
        else:
            if current_token == ";":
                current_node = Statement()
                statements.append(current_node)
            elif current_token == "(":
                if isinstance(current_node, Statement):
                    raise ValueError(f"Syntax error in '{raw}'")
                current_node = _parse_group(current_node)
            elif current_token.endswith("("):
                if isinstance(current_node, Statement):
                    raise ValueError(f"Syntax error in '{raw}'")
                current_node = _parse_function(raw_current_token[:-1], current_node)
            elif current_token == ")":
                while not isinstance(current_node, (ExpressionGroup, Function, Group)):
                    current_node = current_node.parent

                current_node = current_node.parent  # type: ignore[assignment]
                while not isinstance(current_node, (Clause, ExpressionGroup, Function, Group)):
                    current_node = current_node.parent
            elif current_token == "''":
                pass
            elif current_token == "'":
                if isinstance(current_node, Value):
                    current_node = current_node.parent  # type: ignore[assignment]
                elif isinstance(current_node, (Clause, ExpressionGroup)):
                    parent = current_node.expressions[-1]
                    value = Value(parent=parent)
                    parent.parts.append(value)
                    current_node = value  # type: ignore[assignment]
                else:
                    raise ValueError(f"Syntax error in '{raw}'")
            else:
                if isinstance(current_node, Statement):
                    raise ValueError(f"Syntax error in '{raw}'")
                _parse_part(raw_current_token, current_node)

            index += 1

    return current_node


def _parse_clause(
    tokens: list[str],
    current_node: Clause | ExpressionGroup | Function | Group | Statement,
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

    while not isinstance(current_node, Statement):
        current_node = current_node.parent

    clause_properties = cast(ClauseProperties, clause_entry[""])
    clause = Clause(
        parent=current_node,
        properties=clause_properties,
        text=text,
    )
    current_node.clauses.append(clause)
    current_node = clause
    return current_node, index


def _parse_group(
    current_node: Clause | ExpressionGroup | Function | Group,
) -> ExpressionGroup | Group:
    group: ExpressionGroup | Group
    if isinstance(current_node, (Function, Group)):
        group = Group(parent=current_node)
        current_node.parts.append(group)
        return group
    else:
        parent = current_node.expressions[-1]
        if len(parent.parts) == 0:
            group = ExpressionGroup(parent=parent)
        else:
            group = Group(parent=parent)
        parent.parts.append(group)
        return group


def _parse_function(
    name: str,
    current_node: Clause | ExpressionGroup | Function | Group,
) -> Function:
    parent: Expression | Function | Group
    if isinstance(current_node, (Function, Group)):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    func = Function(name=name, parent=parent)
    parent.parts.append(func)
    return func


def _parse_placeholder(
    value: object,
    current_node: Clause | ExpressionGroup | Function | Group | Value,
) -> None:
    parent: Expression | Function | Group | Value
    if isinstance(current_node, (Function, Group, Value)):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    placeholder = Placeholder(parent=parent, value=value)
    parent.parts.append(placeholder)


def _parse_part(
    text: str,
    current_node: Clause | ExpressionGroup | Function | Group | Value,
) -> None:
    parent: Expression | Function | Group | Value
    if isinstance(current_node, (Function, Group, Value)):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    part = Part(parent=parent, text=text)
    if isinstance(current_node, (Clause, ExpressionGroup)):
        clause = current_node
        while not isinstance(clause, Clause):
            clause = clause.parent  # type: ignore

        if text.lower() in clause.properties.separators:
            current_node.expressions.append(Expression(parent=current_node, separator=text))
        else:
            parent.parts.append(part)
    else:
        parent.parts.append(part)
