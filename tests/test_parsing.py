from sql_string import sql


def test_literals() -> None:
    query, _ = sql("SELECT x FROM y WHERE x = 'NONE'", locals())
    assert query == "select x from y where x = 'NONE'"
