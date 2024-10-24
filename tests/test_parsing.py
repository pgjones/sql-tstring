from sql_string import sql


def test_literals() -> None:
    query, _ = sql("SELECT x FROM y WHERE x = 'NONE'", locals())
    assert query == "select x from y where x = 'NONE'"


def test_cte() -> None:
    query, _ = sql(
        """WITH cte AS (SELECT DISTINCT x FROM y)
         SELECT DISTINCT x
           FROM z
          WHERE x NOT IN (SELECT a FROM b)""",
        locals(),
    )
    assert (
        query
        == """with cte AS (select DISTINCT x from y) select DISTINCT x from z where x NOT IN (select a from b)"""  # noqa: E501
    )


def test_with_conflict() -> None:
    a = "A"
    b = "B"
    query, _ = sql(
        """INSERT INTO x (a, b)
                VALUES ({a}, {b})
           ON CONFLICT (a) DO UPDATE SET b = {b}
             RETURNING a, b""",
        locals(),
    )
    assert (
        query
        == "insert into x (a , b) values (? , ?) on conflict (a) do update set b = ? returning a , b"  # noqa: E501
    )
