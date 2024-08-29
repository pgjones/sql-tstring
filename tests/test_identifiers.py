import pytest

from sql_string import Absent, sql, sql_context


def test_order_by() -> None:
    a = Absent()
    b = "x"
    with sql_context(columns=["x"]):
        assert ("SELECT x FROM y ORDER BY x", []) == sql(
            "SELECT x FROM y ORDER BY {a}, {b}", locals()
        )


@pytest.mark.xfail
def test_order_by_direction() -> None:
    a = "ASC"
    b = "x"
    with sql_context(columns=["x"]):
        assert ("SELECT x FROM y ORDER BY x ASC", []) == sql(
            "SELECT x FROM y ORDER BY {b} {a}", locals()
        )


def test_order_by_invalid_column() -> None:
    a = Absent()
    b = "x"
    with pytest.raises(ValueError):
        sql("SELECT x FROM y ORDER BY {a}, {b}", locals())
