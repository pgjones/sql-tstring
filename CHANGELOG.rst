0.4.0 2025-11-06
----------------

* Support frame clauses.
* Support 3.14 as released.
* Support Clause groups and statement separators.
* Bugfix correctly ensure separators are identified in expressions.
* Bugfix find correct current_node on closure.

0.3.0 2025-05-05
----------------

* Better support expression groups.
* Handle subqueries in expressions.
* Treat ON clause placeholders as variables.
* Add the various types of sql join.
* Switch to a Template, t-string placeholder (better support Python <
  3.14).
* Parse Value (single quoted text).
* Support t-strings as defined in PEP 750.
* Change Value -> Literal.
* Allow for nested templates.
* Bugfix absent expression groups.
* Bugfix recognise line breaks and tabs as whitepace to be split on


0.2.0 2024-11-21
----------------

* Introduce RewritingValues, IsNull, IsNotNull, and Absent.
* Stop lowercasing, keeping the original casing.
* BugFix tokenisation for opening brackets followed by a function.

0.1.0 2024-11-17
----------------

* Basic initial release.
