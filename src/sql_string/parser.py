from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import auto, Enum, unique
from functools import lru_cache
from typing import cast

SPLIT_RE = re.compile(r"([ ,;()])")
PLACEHOLDER_RE = re.compile(r"(?<=(?<!\{)\{)[^{}]*(?=\}(?!\}))")


@unique
class ClausePlaceholderType(Enum):
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
    placeholder_type: ClausePlaceholderType
    separators: set[str]


type ClauseDictionary = dict[str, "ClauseDictionary" | ClauseProperties]

CLAUSES: ClauseDictionary = {
    "delete": {
        "from": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=ClausePlaceholderType.TABLE, separators=set()
            ),
        },
    },
    "default": {
        "values": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=ClausePlaceholderType.DISALLOWED,
                separators=set(),
            ),
        },
    },
    "for": {
        "update": {
            "": ClauseProperties(
                allow_empty=True, placeholder_type=ClausePlaceholderType.LOCK, separators=set()
            )
        },
    },
    "group": {
        "by": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=ClausePlaceholderType.COLUMN, separators={","}
            )
        },
    },
    "insert": {
        "into": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=ClausePlaceholderType.DISALLOWED,
                separators=set(),
            )
        },
    },
    "on": {
        "conflict": {
            "": ClauseProperties(
                allow_empty=True,
                placeholder_type=ClausePlaceholderType.DISALLOWED,
                separators=set(),
            )
        },
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.COLUMN, separators={","}
        ),
    },
    "order": {
        "by": {
            "": ClauseProperties(
                allow_empty=False, placeholder_type=ClausePlaceholderType.COLUMN, separators={","}
            )
        },
    },
    "do": {
        "update": {
            "set": {
                "": ClauseProperties(
                    allow_empty=False,
                    placeholder_type=ClausePlaceholderType.VARIABLE,
                    separators={","},
                ),
            },
        },
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.DISALLOWED, separators=set()
        ),
    },
    "from": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.TABLE, separators=set()
        )
    },
    "having": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=ClausePlaceholderType.VARIABLE_CONDITION,
            separators={"and", "or"},
        )
    },
    "join": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.TABLE, separators=set()
        )
    },
    "returning": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.DISALLOWED, separators={","}
        )
    },
    "select": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.COLUMN, separators={","}
        )
    },
    "set": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.VARIABLE, separators={","}
        )
    },
    "update": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.DISALLOWED, separators=set()
        )
    },
    "values": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=ClausePlaceholderType.VARIABLE_DEFAULT,
            separators={","},
        )
    },
    "with": {
        "": ClauseProperties(
            allow_empty=False, placeholder_type=ClausePlaceholderType.DISALLOWED, separators=set()
        )
    },
    "where": {
        "": ClauseProperties(
            allow_empty=False,
            placeholder_type=ClausePlaceholderType.VARIABLE_CONDITION,
            separators={"and", "or"},
        )
    },
}


@dataclass
class Statement:
    clauses: list[Clause] = field(default_factory=list)
    parent: Group | None = None


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
    parent: Clause
    parts: list[Group | Part | Placeholder] = field(default_factory=list)
    removed: bool = False


@dataclass
class Part:
    parent: Expression | Group
    text: str


@dataclass
class Placeholder:
    name: str
    parent: Expression | Group


@dataclass
class Group:
    parent: Expression | Group
    parts: list[Group | Part | Placeholder | Statement] = field(default_factory=list)


@lru_cache
def parse_raw(raw: str) -> list[Statement]:
    statements = [Statement()]
    current_node: Clause | Group | Statement = statements[0]
    tokens = _tokenise(raw)
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
            elif current_token == ")":
                while not isinstance(current_node, Group):
                    current_node = current_node.parent

                current_node = current_node.parent  # type: ignore[assignment]
                while not isinstance(current_node, (Clause, Group)):
                    current_node = current_node.parent
            elif (match_ := PLACEHOLDER_RE.search(raw_current_token)) is not None:
                if isinstance(current_node, Statement):
                    raise ValueError(f"Syntax error in '{raw}'")
                _parse_placeholder(match_.group(0), current_node)
            else:
                if isinstance(current_node, Statement):
                    raise ValueError(f"Syntax error in '{raw}'")
                _parse_part(raw_current_token.replace("{{", "{").replace("}}", "}"), current_node)

            index += 1

    return statements


def _tokenise(raw: str) -> list[str]:
    return [part.strip() for part in SPLIT_RE.split(raw) if part.strip() != ""]


def _parse_clause(
    tokens: list[str],
    current_node: Clause | Group | Statement,
) -> tuple[Clause, int]:
    index = 0
    clause_entry = CLAUSES
    text = ""
    while tokens[index].lower() in clause_entry:
        clause_entry = cast(ClauseDictionary, clause_entry[tokens[index].lower()])
        text = f"{text} {tokens[index]}".lower().strip()
        index += 1

    if isinstance(current_node, Group):
        statement = Statement(parent=current_node)
        current_node.parts.append(statement)
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
    current_node: Clause | Group,
) -> Group:
    parent: Expression | Group
    if isinstance(current_node, Group):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    group = Group(parent=parent)
    parent.parts.append(group)
    return group


def _parse_placeholder(
    name: str,
    current_node: Clause | Group,
) -> None:
    parent: Expression | Group
    if isinstance(current_node, Group):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    placeholder = Placeholder(name=name, parent=parent)
    parent.parts.append(placeholder)


def _parse_part(
    text: str,
    current_node: Clause | Group,
) -> None:
    parent: Expression | Group
    if isinstance(current_node, Group):
        parent = current_node
    else:
        parent = current_node.expressions[-1]
    part = Part(parent=parent, text=text)
    parent.parts.append(part)

    if isinstance(current_node, Clause):
        if text.lower() in current_node.properties.separators:
            current_node.expressions.append(Expression(parent=current_node))
