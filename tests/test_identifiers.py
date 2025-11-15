import pytest

from sql_tstring import RewritingValue, sql, sql_context


def test_order_by() -> None:
    a = RewritingValue.ABSENT
    b = "x"
    with sql_context(columns={"x"}):
        assert ("SELECT x FROM y ORDER BY x", []) == sql(
            "SELECT x FROM y ORDER BY {a}, {b}", locals()
        )


@pytest.mark.parametrize(
    "a",
    ["ASC", "ASCENDING", "asc", "ascending"],
)
def test_order_by_direction(a: str) -> None:
    b = "x"
    with sql_context(columns={"x"}):
        assert (f"SELECT x FROM y ORDER BY x {a}", []) == sql(
            "SELECT x FROM y ORDER BY {b} {a}", locals()
        )


def test_order_by_invalid_column() -> None:
    a = RewritingValue.ABSENT
    b = "x"
    with pytest.raises(ValueError):
        sql("SELECT x FROM y ORDER BY {a}, {b}", locals())


def test_partition_by() -> None:
    a = RewritingValue.ABSENT
    b = "x"
    with sql_context(columns={"x"}):
        assert ("SELECT x OVER(PARTITION BY x) FROM y", []) == sql(
            "SELECT x OVER(PARTITION BY {b}) FROM y", locals()
        )


def test_partition_by_invalid_column() -> None:
    a = RewritingValue.ABSENT
    b = "x"
    with pytest.raises(ValueError):
        sql("SELECT x OVER(PARTITION BY {b}) FROM y", locals())


@pytest.mark.parametrize(
    "frame_clause",
    ["GROUPS", "RANGE", "ROWS"],
)
def test_frame_clause_int(frame_clause: str) -> None:
    a = RewritingValue.ABSENT
    b = 2
    assert (f"SELECT x OVER(PARTITION BY x {frame_clause} 2 PRECEDING) FROM y", []) == sql(
        f"SELECT x OVER(PARTITION BY x {frame_clause} {{b}} PRECEDING) FROM y", locals()
    )


@pytest.mark.parametrize(
    "frame_clause",
    ["GROUPS", "RANGE", "ROWS"],
)
def test_frame_clause_invalid(frame_clause: str) -> None:
    a = RewritingValue.ABSENT
    b = "INVALID"
    with pytest.raises(ValueError):
        sql(f"SELECT x OVER(PARTITION BY x {frame_clause} {{b}} PRECEDING) FROM y", locals())


@pytest.mark.xfail
def test_over_clause() -> None:
    a = 2
    assert ("SELECT AVG(x) OVER y - (2 / 365) FROM x", []) == sql(
        "SELECT AVG(x) OVER y - ({a} / 365) FROM x", locals()
    )


@pytest.mark.parametrize(
    "lock_type, expected",
    (
        ("", "SELECT x FROM y FOR UPDATE"),
        ("NOWAIT", "SELECT x FROM y FOR UPDATE NOWAIT"),
        ("SKIP LOCKED", "SELECT x FROM y FOR UPDATE SKIP LOCKED"),
    ),
)
def test_lock(lock_type: str, expected: str) -> None:
    assert (expected, []) == sql("SELECT x FROM y FOR UPDATE {lock_type}", locals())


def test_absent_lock() -> None:
    a = RewritingValue.ABSENT
    assert ("SELECT x FROM y", []) == sql("SELECT x FROM y FOR UPDATE {a}", locals())
