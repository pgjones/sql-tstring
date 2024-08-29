from typing import Any

import pytest

from sql_string import Absent, sql


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE a = {a} AND (b = {b} OR c = 1)",
            "SELECT x FROM y WHERE (b = ? OR c = 1)",
            [2],
        ),
        (
            "SELECT x FROM y WHERE a = ANY({a}) AND b = ANY({b})",
            "SELECT x FROM y WHERE b = ANY(?)",
            [2],
        ),
    ],
)
def test_select(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = Absent()
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "UPDATE x SET a = {a}, b = {b}, c = 1",
            "UPDATE x SET b = ?, c = 1",
            [2],
        ),
    ],
)
def test_update(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = Absent()
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "INSERT INTO x (a, b) VALUES ({a}, {b})",
            "INSERT INTO x (a, b) VALUES (DEFAULT, ?)",
            [2],
        ),
    ],
)
def test_insert(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = Absent()
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())
