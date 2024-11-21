from typing import Any

import pytest

from sql_tstring import RewritingValue, sql

TZ = "uk"


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
        (
            "SELECT x FROM y WHERE b = {b} AND a = {a}",
            "SELECT x FROM y WHERE b = ?",
            [2],
        ),
        (
            "SELECT x FROM y WHERE a = {a}",
            "SELECT x FROM y",
            [],
        ),
        (
            "SELECT x FROM y WHERE DATE(b) <= {b} AND DATE(a) >= {a}",
            "SELECT x FROM y WHERE DATE(b) <= ?",
            [2],
        ),
        (
            "SELECT x FROM y WHERE c = {c} OR c != {c}",
            "SELECT x FROM y WHERE c = ? OR c != ?",
            [None, None],
        ),
        (
            "SELECT x FROM y WHERE d = {d} OR d != {d}",
            "SELECT x FROM y WHERE d IS NULL OR d IS NULL",
            [],
        ),
        (
            "SELECT x FROM y JOIN z ON a = {a}",
            "SELECT x FROM y JOIN z",
            [],
        ),
        (
            "SELECT x FROM y WHERE DATE(b AT TIME ZONE {TZ}) >= {b}",
            "SELECT x FROM y WHERE DATE(b AT TIME ZONE ?) >= ?",
            ["uk", 2],
        ),
        (
            "SELECT x FROM y LIMIT {b} OFFSET {b}",
            "SELECT x FROM y LIMIT ? OFFSET ?",
            [2, 2],
        ),
        (
            "SELECT x FROM y ORDER BY ARRAY_POSITION({b}, x)",
            "SELECT x FROM y ORDER BY ARRAY_POSITION(? , x)",
            [2],
        ),
        (
            "UPDATE x SET c = {c}",
            "UPDATE x SET c = ?",
            [None],
        ),
    ],
)
def test_select(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = RewritingValue.ABSENT
    b = 2
    c = None
    d = RewritingValue.IS_NULL
    assert (expected_query, expected_values) == sql(query, locals() | globals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "UPDATE x SET a = {a}, b = {b}, c = 1",
            "UPDATE x SET b = ? , c = 1",
            [2],
        ),
    ],
)
def test_update(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = RewritingValue.ABSENT
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "INSERT INTO x (a, b) VALUES ({a}, {b})",
            "INSERT INTO x (a , b) VALUES (DEFAULT , ?)",
            [2],
        ),
        (
            "INSERT INTO x (b) VALUES ({b}) ON CONFLICT DO UPDATE SET b = {b}",
            "INSERT INTO x (b) VALUES (?) ON CONFLICT DO UPDATE SET b = ?",
            [2, 2],
        ),
        (
            "INSERT INTO x (a) VALUES ('{{}}')",
            "INSERT INTO x (a) VALUES ('{}')",
            [],
        ),
    ],
)
def test_insert(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = RewritingValue.ABSENT
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())
