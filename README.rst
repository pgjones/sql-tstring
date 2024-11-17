SQL-tString
===========

|Build Status| |pypi| |python| |license|

SQL-tString allows for f-string like construction of sql queries
without allowing for SQL injection. The basic usage is as follows,

.. code-block:: python

    from sql_tstring import sql

    a = 1

    query, values = sql(
        """SELECT a, b, c
             FROM tbl
            WHERE a = {a}""",
        locals(),
    )

The resultant query is a ``str`` and values a ``list[Any]``, both are
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
            "SELECT {col} FROM {table}",
            locals(),
        )

If the value of ``col`` or ``table`` does not match the valid values
given to the ``sql_context`` function an error will be raised.

SQL-tString will also remove parameters if they are set to the special
value of ``Absent``. This is most useful for optional updates, or
conditionals,

.. code-block:: python

    from sql_tstring import Absent, sql

    a = Absent
    b = Absent

    query, values = sql(
        """UPDATE tbl
              SET a = {a},
                  b = 1
            WHERE b = {b}""",
        locals(),
    )

As both ``a`` and ``b`` are ``Absent`` the above ``query`` will be
``UPDATE tbl SET b =1``.

t-string (PEP 750)
------------------

If, hopefully, `PEP 750 <https://peps.python.org/pep-0750/>`_ is
accepted the usage of this library will change to,

.. code-block:: python

    from sql_tstring import sql

    a = 1

    query, values = sql(
        t"""SELECT a, b, c
              FROM tbl
             WHERE a = {a}""",
    )

.. |Build Status| image:: https://github.com/pgjones/sql-sttring/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/pgjones/sql-tstring/commits/main

.. |pypi| image:: https://img.shields.io/pypi/v/sql-tstring.svg
   :target: https://pypi.python.org/pypi/Sql-Tstring/

.. |python| image:: https://img.shields.io/pypi/pyversions/sql-tstring.svg
   :target: https://pypi.python.org/pypi/Sql-Tstring/

.. |license| image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://github.com/pgjones/sql-tstring/blob/main/LICENSE
