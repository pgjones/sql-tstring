from sql_tstring import RewritingValue, sql, sql_context


def test_asyncpg() -> None:
    a = 1
    b = RewritingValue.ABSENT
    c = 2
    with sql_context(dialect="asyncpg"):
        assert ("SELECT x FROM y WHERE a = $1 AND c = $2", [1, 2]) == sql(
            "SELECT x FROM y WHERE a = {a} AND b = {b} AND c = {c}", locals()
        )
