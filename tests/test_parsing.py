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
        == """with cte AS ( select DISTINCT x from y ) select DISTINCT x from z where x NOT IN ( select a from b )"""  # noqa: E501
    )
