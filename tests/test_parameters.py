from typing import Any

import pytest

from sql_tstring import Absent, sql

TZ = "uk"


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "SELECT x FROM y WHERE a = {a} AND (b = {b} OR c = 1)",
            "select x from y where (b = ? OR c = 1)",
            [2],
        ),
        (
            "SELECT x FROM y WHERE a = ANY({a}) AND b = ANY({b})",
            "select x from y where b = ANY (?)",
            [2],
        ),
        (
            "SELECT x FROM y WHERE b = {b} AND a = {a}",
            "select x from y where b = ?",
            [2],
        ),
        (
            "SELECT x FROM y WHERE a = {a}",
            "select x from y",
            [],
        ),
        (
            "SELECT x FROM y WHERE DATE(b) <= {b} AND DATE(a) >= {a}",
            "select x from y where DATE (b) <= ?",
            [2],
        ),
        (
            "SELECT x FROM y WHERE c = {c} OR c != {c}",
            "select x from y where c is null OR c is not null",
            [],
        ),
        (
            "SELECT x FROM y JOIN z ON a = {a}",
            "select x from y join z",
            [],
        ),
        (
            "SELECT x FROM y WHERE DATE(b AT TIME ZONE {TZ}) >= {b}",
            "select x from y where DATE (b AT TIME ZONE ?) >= ?",
            ["uk", 2],
        ),
        (
            "SELECT x FROM y LIMIT {b} OFFSET {b}",
            "select x from y limit ? offset ?",
            [2, 2],
        ),
        (
            "UPDATE x SET c = {c}",
            "update x set c = ?",
            [None],
        ),
    ],
)
def test_select(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = Absent()
    b = 2
    c = None
    assert (expected_query, expected_values) == sql(query, locals() | globals())


@pytest.mark.parametrize(
    "query, expected_query, expected_values",
    [
        (
            "UPDATE x SET a = {a}, b = {b}, c = 1",
            "update x set b = ? , c = 1",
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
            "insert into x (a , b) values (default , ?)",
            [2],
        ),
        (
            "INSERT INTO x (b) VALUES ({b}) ON CONFLICT DO UPDATE SET b = {b}",
            "insert into x (b) values (?) on conflict do update set b = ?",
            [2, 2],
        ),
        (
            "INSERT INTO x (a) VALUES ('{{}}')",
            "insert into x (a) values ('{}')",
            [],
        ),
    ],
)
def test_insert(query: str, expected_query: str, expected_values: list[Any]) -> None:
    a = Absent()
    b = 2
    assert (expected_query, expected_values) == sql(query, locals())
