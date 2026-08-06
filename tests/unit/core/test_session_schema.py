# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The result schema a session reports belongs to the statement it just ran.

The schema is built from the first morsel of a result, and used to be built only
when the session did not already have one - so a session reused for a second
query kept reporting the columns of the first. A caller reading `column_names`
or `description` off it was told, with no error to notice, that its result had
columns it does not have.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def test_column_names_of_a_read_result():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT name, gravity FROM $planets"))
    assert session.column_names == ["name", "gravity"]
    session.close()


def test_a_reused_session_reports_the_latest_statements_columns():
    session = opteryx.session()

    list(session.execute_to_morsels("SELECT name FROM $planets"))
    assert session.column_names == ["name"]

    list(session.execute_to_morsels("SELECT id, gravity FROM $planets"))
    assert session.column_names == ["id", "gravity"]

    list(session.execute_to_morsels("SELECT name, mass, density FROM $planets"))
    assert session.column_names == ["name", "mass", "density"]

    session.close()


def test_a_reused_session_reports_the_latest_statements_description():
    session = opteryx.session()

    list(session.execute_to_morsels("SELECT name FROM $planets"))
    assert [column[0] for column in session.description] == ["name"]

    list(session.execute_to_morsels("SELECT id, gravity FROM $planets"))
    assert [column[0] for column in session.description] == ["id", "gravity"]

    session.close()


def test_a_result_with_no_rows_still_reports_its_columns():
    """The engine sends a zero-row morsel carrying the schema; it must be read."""
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT name, gravity FROM $planets WHERE id < 0"))
    assert session.column_names == ["name", "gravity"]
    assert session.rowcount == 0
    session.close()


def test_an_empty_result_does_not_inherit_the_previous_columns():
    session = opteryx.session()

    list(session.execute_to_morsels("SELECT name FROM $planets"))
    list(session.execute_to_morsels("SELECT id, gravity FROM $planets WHERE id < 0"))

    assert session.column_names == ["id", "gravity"]
    session.close()


def test_an_unstarted_statement_reports_no_columns():
    """Not the columns of the statement before it."""
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT name FROM $planets"))
    assert session.column_names == ["name"]

    session.execute_to_morsels("SELECT id, gravity FROM $planets")  # never iterated
    assert session.column_names == []
    assert session.description is None
    session.close()


def test_the_schema_carries_the_full_type():
    """The type of a value, not just its category - DECIMAL(3, 1), not DECIMAL."""
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT id, name, gravity FROM $planets"))
    types = [str(column.column_type) for column in session._schema.columns]
    assert types == ["INT8", "VARCHAR", "DECIMAL(3, 1)"]
    session.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
