import pytest

from sql_string import Absent, sql, sql_context


def test_order_by() -> None:
    a = Absent()
    b = "x"
    with sql_context(columns={"x"}):
        assert ("select x from y order by x", []) == sql(
            "SELECT x FROM y ORDER BY {a}, {b}", locals()
        )


def test_order_by_direction() -> None:
    a = "ASC"
    b = "x"
    with sql_context(columns={"x"}):
        assert ("select x from y order by x ASC", []) == sql(
            "SELECT x FROM y ORDER BY {b} {a}", locals()
        )


def test_order_by_invalid_column() -> None:
    a = Absent()
    b = "x"
    with pytest.raises(ValueError):
        sql("SELECT x FROM y ORDER BY {a}, {b}", locals())


@pytest.mark.parametrize(
    "lock_type, expected",
    (
        ("", "select x from y for update"),
        ("NOWAIT", "select x from y for update NOWAIT"),
        ("SKIP LOCKED", "select x from y for update SKIP LOCKED"),
    ),
)
def test_lock(lock_type: str, expected: str) -> None:
    assert (expected, []) == sql("SELECT x FROM y FOR UPDATE {lock_type}", locals())


def test_absent_lock() -> None:
    a = Absent
    assert ("select x from y", []) == sql("SELECT x FROM y FOR UPDATE {a}", locals())
