from sql_tstring import sql, t


def test_literals() -> None:
    query, _ = sql("SELECT x FROM y WHERE x = 'NONE' AND y = 'FUNC_LIKE('", locals())
    assert query == "SELECT x FROM y WHERE x = 'NONE' AND y = 'FUNC_LIKE('"


def test_function_literals() -> None:
    query, _ = sql("SELECT COALESCE(x, 'a') FROM y WHERE x = 'NONE'", locals())
    assert query == "SELECT COALESCE(x , 'a') FROM y WHERE x = 'NONE'"


def test_literal_quote() -> None:
    query, _ = sql("SELECT x FROM y WHERE x = 'AB''C'", locals())
    assert query == "SELECT x FROM y WHERE x = 'AB''C'"


def test_quoted() -> None:
    query, _ = sql('SELECT "x" FROM "y"', locals())
    assert query == 'SELECT "x" FROM "y"'


def test_whitespace() -> None:
    query, _ = sql(
        """SELECT
"x" FROM	"y" """,
        locals(),
    )
    assert query == 'SELECT "x" FROM "y"'


def test_delete_from() -> None:
    query, _ = sql("DELETE FROM y WHERE x = 'NONE'", locals())
    assert query == "DELETE FROM y WHERE x = 'NONE'"


def test_function() -> None:
    query, _ = sql("SELECT COALESCE(x, now())", locals())
    assert query == "SELECT COALESCE(x , now())"


def test_lowercase() -> None:
    query, _ = sql("select x from y where x = 2", locals())
    assert query == "select x from y where x = 2"


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
        == """WITH cte AS (SELECT DISTINCT x FROM y) SELECT DISTINCT x FROM z WHERE x NOT IN (SELECT a FROM b)"""  # noqa: E501
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
        == "INSERT INTO x (a , b) VALUES (? , ?) ON CONFLICT (a) DO UPDATE SET b = ? RETURNING a , b"  # noqa: E501
    )


def test_default_insert() -> None:
    query, _ = sql("INSERT INTO tbl DEFAULT VALUES RETURNING id", locals())
    assert query == "INSERT INTO tbl DEFAULT VALUES RETURNING id"


def test_grouping() -> None:
    query, _ = sql("SELECT x FROM y WHERE (DATE(x) = 1 OR x = 2) AND y = 3", locals())
    assert query == "SELECT x FROM y WHERE (DATE(x) = 1 OR x = 2) AND y = 3"


def test_subquery() -> None:
    query, _ = sql(
        "SELECT x FROM y JOIN (SELECT z FROM a) ON z = x",
        locals(),
    )
    assert query == "SELECT x FROM y JOIN (SELECT z FROM a) ON z = x"


def test_functional_subquery() -> None:
    query, _ = sql(
        "SELECT x, ARRAY(SELECT a FROM b) FROM y",
        locals(),
    )
    assert query == "SELECT x , ARRAY(SELECT a FROM b) FROM y"


def test_nested() -> None:
    inner = t("x = 'a'", locals())
    query, _ = sql("SELECT x FROM y WHERE {inner}", locals())
    assert query == "SELECT x FROM y WHERE x = 'a'"


def test_opening_parenthesis() -> None:
    query, _ = sql("(SELECT x FROM y) UNION ALL (SELECT z FROM y)", locals())
    assert query == "(SELECT x FROM y) UNION ALL (SELECT z FROM y)"
