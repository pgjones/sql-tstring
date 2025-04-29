SQL-tString
===========

|Build Status| |pypi| |python| |license|

SQL-tString allows for t-string based construction of sql queries
without allowing for SQL injection. The basic usage is as follows,

.. code-block:: python

    from sql_tstring import sql

    a = 1

    query, values = sql(
        t"""SELECT a, b, c
              FROM tbl
             WHERE a = {a}""",
    )

The ``query`` is a ``str`` and ``values`` a ``list[Any]``, both are
then typically passed to a DB connection. Note the parameters can only
be identifiers that identify variables (in the above example in the
locals()) e.g. ``{a - 1}`` is not valid.

SQL-tString will convert parameters to SQL placeholders where
appropriate. In other locations SQL-tString will allow pre defined
column or table names to be used,

.. code-block:: python

    from sql_tstring import sql, sql_context

    col = "a"
    table = "tbl"

    with sql_context(columns={"a"}, tables={"tbl"}):
        query, values = sql(
            t"SELECT {col} FROM {table}",
        )

If the value of ``col`` or ``table`` does not match the valid values
given to the ``sql_context`` function an error will be raised.

Rewriting values
----------------

SQL-tString will also remove parameters if they are set to the special
value of ``Absent`` (or ``RewritingValue.Absent``). This is most
useful for optional updates, or conditionals,

.. code-block:: python

    from sql_tstring import Absent, sql

    a = Absent
    b = Absent

    query, values = sql(
        t"""UPDATE tbl
               SET a = {a},
                   b = 1
             WHERE b = {b}""",
    )

As both ``a`` and ``b`` are ``Absent`` the above ``query`` will be
``UPDATE tbl SET b =1``.

In addition for conditionals the values ``IsNull`` (or
``RewritingValue.IS_NULL``) and ``IsNotNull`` (or
``RewritingValue.IS_NOT_NULL``) can be used to rewrite the conditional
as expected. This is useful as ``x = NULL`` is always false in SQL.

Pre Python 3.14 usage
---------------------

t-strings were introduced in Python 3.14 via, `PEP 750
<https://peps.python.org/pep-0750/>`_, however this library can be
used with Python 3.12 and 3.13 as follows,

.. code-block:: python

    from sql_tstring import sql

    a = 1

    query, values = sql(
        """SELECT a, b, c
             FROM tbl
            WHERE a = {a}""",
        locals(),
    )

Please note though that only simple variable identifiers can be placed
within the braces.

.. |Build Status| image:: https://github.com/pgjones/sql-tstring/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/pgjones/sql-tstring/commits/main

.. |pypi| image:: https://img.shields.io/pypi/v/sql-tstring.svg
   :target: https://pypi.python.org/pypi/Sql-Tstring/

.. |python| image:: https://img.shields.io/pypi/pyversions/sql-tstring.svg
   :target: https://pypi.python.org/pypi/Sql-Tstring/

.. |license| image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://github.com/pgjones/sql-tstring/blob/main/LICENSE
