import pytest

from sql_string import _braces_to_placeholders


def test__braces_to_placeholders() -> None:
    result = _braces_to_placeholders(
        "SELECT a FROM b WHERE c = {c} AND d = ANY({{f}}) ORDER BY {d} {e}"
    )
    assert result == "SELECT a FROM b WHERE c = :c AND d = ANY({f}) ORDER BY :d :e"


def test__braces_to_placeholders_syntax_error() -> None:
    with pytest.raises(SyntaxError):
        _braces_to_placeholders("{a - 2}")
