import re
from typing import Any, Iterator

PLACEHOLDER_RE = re.compile(r"(?<=(?<!\{)\{)[^{}]*(?=\}(?!\}))")


class Interpolation:
    __match_args__ = ("value", "expr", "conv", "format_spec")

    def __init__(self, value: object) -> None:
        self.value = value
        self.expr = ""
        self.conv = None
        self.format_spec = ""


class Template:
    def __init__(self, values: list[str | Interpolation]) -> None:
        self._values = values

    def __iter__(self) -> Iterator[str | Interpolation]:
        return iter(self._values)


def t(raw: str, values: dict[str, Any]) -> Template:
    parts: list[str | Interpolation] = []
    position = 0
    for match_ in PLACEHOLDER_RE.finditer(raw):
        end = match_.start() - 1
        if position != end:
            parts.append(raw[position:end].replace("{{", "{").replace("}}", "}"))
        position = match_.end() + 1
        parts.append(Interpolation(value=values[match_.group(0)]))

    if position != len(raw):
        parts.append(raw[position:].replace("{{", "{").replace("}}", "}"))

    return Template(parts)
