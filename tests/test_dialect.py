from sql_string import Absent, sql, sql_context


def test_asyncpg() -> None:
    a = 1
    b = Absent()
    c = 2
    with sql_context(dialect="asyncpg"):
        assert ("select x from y where a = $1 AND  c = $2", [1, 2]) == sql(
            "SELECT x FROM y WHERE a = {a} AND b = {b} AND c = {c}", locals()
        )
