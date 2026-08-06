# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`Session.rowcount` for tabular results.

It used to delegate to a `DataFrame.rowcount` which does not exist, so asking a
session how many rows a SELECT returned raised AttributeError. It now reports
the rows the engine delivered - and, because the engine streams, refuses to
answer until the result has been read to the end rather than reporting a
part-count that reads like a total.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import InvalidCursorStateError


def test_rowcount_of_a_read_result():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))
    assert session.rowcount == 9
    session.close()


def test_rowcount_of_an_empty_result():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets WHERE id < 0"))
    assert session.rowcount == 0
    session.close()


def test_rowcount_counts_every_morsel():
    """A result large enough to arrive in several morsels, not just the first."""
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT * FROM GENERATE_SERIES(1, 200000) AS g"))
    assert len(morsels) > 1
    assert session.rowcount == sum(morsel.num_rows for morsel in morsels)
    assert session.rowcount == 200000
    session.close()


def test_rowcount_is_the_last_statements():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))
    list(session.execute_to_morsels("SELECT * FROM $planets WHERE id < 4"))
    assert session.rowcount == 3
    session.close()


def test_rowcount_of_an_unread_result_is_an_error():
    session = opteryx.session()
    morsels = session.execute_to_morsels("SELECT * FROM $planets")
    next(morsels)
    with pytest.raises(InvalidCursorStateError):
        session.rowcount
    session.close()


def test_an_unread_result_does_not_report_the_previous_count():
    """The count of the statement before is a wrong answer, not a stale one."""
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))
    assert session.rowcount == 9

    morsels = session.execute_to_morsels("SELECT * FROM $planets WHERE id < 4")
    next(morsels)
    with pytest.raises(InvalidCursorStateError):
        session.rowcount
    session.close()


def test_submitting_a_statement_supersedes_the_last_one_immediately():
    """Submitting is enough - the stream does not have to be started."""
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))
    assert session.rowcount == 9

    session.execute_to_morsels("SELECT * FROM $planets WHERE id < 4")  # never iterated
    with pytest.raises(InvalidCursorStateError):
        session.rowcount
    session.close()


def test_submitting_a_statement_does_not_execute_it():
    """Execution stays lazy: a statement which cannot even be planned is quiet
    until the stream it returned is read."""
    session = opteryx.session()
    session.execute_to_morsels("SELECT * FROM $no_such_dataset")
    session.close()


def test_rowcount_before_any_statement_is_an_error():
    session = opteryx.session()
    with pytest.raises(InvalidCursorStateError):
        session.rowcount
    session.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
