# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Session - execution surface and lifecycle.

This file tested `opteryx.Session`, `session.execute()`, `fetchone/fetchmany/
fetchall`, `execute_to_arrow*` and `cursor.shape` long after all of them were
removed, so every test in it errored and several had bodies of `pass  # migrated
from query` above dead references. It now tests the session there is:
`opteryx.session()` and `execute_to_morsels()`.

What a session reports about its result has its own files - see
test_session_rowcount.py and test_session_schema.py.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.constants import QueryStatus, ResultType
from opteryx.exceptions import (
    DatasetNotFoundError,
    InvalidCursorStateError,
    MissingSqlStatement,
    ProgrammingError,
    UnsupportedSyntaxError,
)


def test_session_executes_and_streams_its_result():
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT * FROM $planets"))

    assert sum(morsel.num_rows for morsel in morsels) == 9
    assert len(session.column_names) == 20
    assert session.result_type == ResultType.TABULAR
    assert session.query_status == QueryStatus.SQL_SUCCESS
    session.close()


def test_morsels_are_capped_at_max_size():
    """The output boundary is row-bounded, whatever shape the engine produced."""
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SELECT * FROM $planets", max_size=3))

    assert [morsel.num_rows for morsel in morsels] == [3, 3, 3]
    session.close()


def test_a_session_can_run_more_than_one_statement():
    session = opteryx.session()

    first = sum(m.num_rows for m in session.execute_to_morsels("SELECT * FROM $planets"))
    second = sum(
        m.num_rows for m in session.execute_to_morsels("SELECT name FROM $planets LIMIT 2")
    )

    assert (first, second) == (9, 2)
    session.close()


def test_named_parameters_are_bound():
    session = opteryx.session()
    morsels = list(
        session.execute_to_morsels("SELECT * FROM $planets WHERE id = :want", params={"want": 3})
    )

    assert sum(morsel.num_rows for morsel in morsels) == 1
    session.close()


def test_a_batch_returns_only_the_last_result():
    session = opteryx.session()
    morsels = list(
        session.execute_to_morsels("SELECT * FROM $planets; SELECT name FROM $planets LIMIT 2")
    )

    assert sum(morsel.num_rows for morsel in morsels) == 2
    assert session.column_names == ["name"]
    session.close()


def test_a_batch_cannot_take_a_parameter_list():
    """Which statement a positional parameter belongs to is not knowable."""
    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError):
        list(
            session.execute_to_morsels(
                "SELECT * FROM $planets; SELECT * FROM $planets", params=[1]
            )
        )
    session.close()


def test_an_empty_statement_is_an_error():
    session = opteryx.session()
    with pytest.raises(MissingSqlStatement):
        list(session.execute_to_morsels(""))
    session.close()


def test_an_unknown_dataset_is_an_error():
    session = opteryx.session()
    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("SELECT * FROM $no_such_dataset"))
    session.close()


def test_a_session_is_falsy_until_it_has_executed():
    session = opteryx.session()
    assert not session

    list(session.execute_to_morsels("SELECT * FROM $planets"))
    assert session

    session.close()
    assert not session


def test_a_closed_session_refuses_further_statements():
    """And refuses at the call, not at the first morsel - nothing is planned."""
    session = opteryx.session()
    session.close()

    with pytest.raises(InvalidCursorStateError):
        session.execute_to_morsels("SELECT * FROM $planets")


def test_closing_twice_is_harmless():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))
    session.close()
    session.close()


def test_a_session_validates_the_identity_it_is_given():
    """Caller identity decides what a query may read, so it is checked up front."""
    with pytest.raises(ProgrammingError):
        opteryx.session(user=7)
    with pytest.raises(ProgrammingError):
        opteryx.session(memberships=[1])
    with pytest.raises(ProgrammingError):
        opteryx.session(entitlements=[object()])
    with pytest.raises(ProgrammingError):
        opteryx.session(access_policies=["not a policy"])
    with pytest.raises(ProgrammingError):
        opteryx.session(billing_account=object())


def test_the_removed_io_trace_file_argument_is_rejected():
    from opteryx.query_session import Session

    with pytest.raises(TypeError):
        Session(io_trace_file="/tmp/trace.jsonl")


def test_a_session_has_a_query_id_and_will_take_one():
    assert len(opteryx.session().query_id) == 32
    assert opteryx.session(query_id="a-known-id").query_id == "a-known-id"


def test_a_non_tabular_statement_reports_an_outcome_not_a_relation():
    session = opteryx.session()
    morsels = list(session.execute_to_morsels("SET @answer = 42"))

    assert morsels == []
    assert session.result_type == ResultType.NON_TABULAR
    assert session.query_status == QueryStatus.SQL_SUCCESS
    assert session.rowcount == 1
    assert session.column_names == []
    session.close()


def test_a_variable_set_by_one_statement_is_seen_by_the_next():
    """What a session is for - state which outlives the statement that set it."""
    session = opteryx.session()
    list(session.execute_to_morsels("SET @wanted = 3"))

    morsels = list(session.execute_to_morsels("SELECT * FROM $planets WHERE id = @wanted"))

    assert sum(morsel.num_rows for morsel in morsels) == 1
    session.close()


def test_tracing_is_not_armed_unless_it_is_asked_for():
    session = opteryx.session()
    list(session.execute_to_morsels("SELECT * FROM $planets"))

    assert not session.trace_armed
    with pytest.raises(RuntimeError):
        session.trace()
    session.close()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
