from typing import Any

import pytest

from sql_tstring import RewritingValue, sql, sql_context

TZ = "uk"


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE x = {val}",
            "SELECT x FROM y WHERE x = ?",
            [2],
        ),
        (
            "SELECT x FROM y WHERE DATE(x AT TIME ZONE {TZ}) >= {val}",
            "SELECT x FROM y WHERE DATE(x AT TIME ZONE ?) >= ?",
            ["uk", 2],
        ),
        (
            "SELECT x FROM y WHERE x = ANY({val})",
            "SELECT x FROM y WHERE x = ANY(?)",
            [2],
        ),
        (
            "UPDATE x SET x = {val}",
            "UPDATE x SET x = ?",
            [2],
        ),
        (
            "SELECT {col} FROM {tbl}",
            "SELECT col FROM tbl",
            [],
        ),
        (
            "SELECT x FROM y LIMIT {val} OFFSET {val}",
            "SELECT x FROM y LIMIT ? OFFSET ?",
            [2, 2],
        ),
        (
            "SELECT x FROM y ORDER BY ARRAY_POSITION({val}, x)",
            "SELECT x FROM y ORDER BY ARRAY_POSITION(? , x)",
            [2],
        ),
        (
            "INSERT INTO y (x) VALUES (2) ON CONFLICT DO UPDATE SET x = {val}",
            "INSERT INTO y (x) VALUES (2) ON CONFLICT DO UPDATE SET x = ?",
            [2],
        ),
    ],
)
def test_placeholders(query: str, expected_query: str, expected_values: list[Any]) -> None:
    col = "col"
    tbl = "tbl"
    val = 2
    with sql_context(columns={"col"}, tables={"tbl"}):
        assert (expected_query, expected_values) == sql(query, locals() | globals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE x = {val}",
            "SELECT x FROM y",
            [],
        ),
        (
            "SELECT x FROM y WHERE x = 2 AND z = ANY({val})",
            "SELECT x FROM y WHERE x = 2",
            [],
        ),
        (
            "SELECT x FROM y WHERE x = 2 AND (u = {val} OR v = 1)",
            "SELECT x FROM y WHERE x = 2 AND (v = 1)",
            [],
        ),
        (
            "SELECT x FROM y WHERE x = 2 AND (v = 1 OR u = {val})",
            "SELECT x FROM y WHERE x = 2 AND (v = 1)",
            [],
        ),
        (
            "SELECT x FROM y WHERE x = {val} AND (v = 1 OR u = 2)",
            "SELECT x FROM y WHERE (v = 1 OR u = 2)",
            [],
        ),
        (
            "SELECT x FROM y WHERE x = 2 AND (v = {val} OR u = {val})",
            "SELECT x FROM y WHERE x = 2",
            [],
        ),
        (
            "SELECT x FROM y JOIN z ON u = {val}",
            "SELECT x FROM y JOIN z",
            [],
        ),
        (
            "SELECT x FROM y LIMIT {val} OFFSET {val}",
            "SELECT x FROM y",
            [],
        ),
        (
            "SELECT x FROM y ORDER BY ARRAY_POSITION({val}, x)",
            "SELECT x FROM y",
            [],
        ),
        (
            "UPDATE y SET x = {val}, u = 2",
            "UPDATE y SET u = 2",
            [],
        ),
        (
            "INSERT INTO y (x) VALUES ({val})",
            "INSERT INTO y (x) VALUES (DEFAULT)",
            [],
        ),
    ],
)
def test_absent(query: str, expected_query: str, expected_values: list[Any]) -> None:
    val = RewritingValue.ABSENT
    assert (expected_query, expected_values) == sql(query, locals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE x = {val}",
            "SELECT x FROM y WHERE x IS NULL",
            [],
        ),
        (
            "SELECT x FROM y WHERE x != {val}",
            "SELECT x FROM y WHERE x IS NULL",
            [],
        ),
    ],
)
def test_is_null(query: str, expected_query: str, expected_values: list[Any]) -> None:
    val = RewritingValue.IS_NULL
    assert (expected_query, expected_values) == sql(query, locals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE x = {val}",
            "SELECT x FROM y WHERE x IS NOT NULL",
            [],
        ),
        (
            "SELECT x FROM y WHERE x != {val}",
            "SELECT x FROM y WHERE x IS NOT NULL",
            [],
        ),
    ],
)
def test_is_not_null(query: str, expected_query: str, expected_values: list[Any]) -> None:
    val = RewritingValue.IS_NOT_NULL
    assert (expected_query, expected_values) == sql(query, locals())
