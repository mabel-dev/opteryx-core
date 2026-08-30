"""Tests for `CALL <procedure>(<literals>)`.

CALL is the ONLY way to reach a procedure the host process registered through
`opteryx.register_procedure`. It is a statement rather than a function on purpose: a
registered function is an expression, and the optimizer is free to constant-fold it at
plan time, duplicate it and evaluate it once per row per morsel, so a side-effecting
callable in that position fires an unpredictable number of times - including before
execution, on a plan that is then discarded. These tests hold that line, and hold the
refusals that keep a procedure from acquiring expression syntax that would imply it.

Execution goes through the non-tabular statement path (RelationManagementNode), which
runs OFF the native per-morsel engine - the reason a Python callable is admissible here
and nowhere else on the execution path.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.procedures import ProcedureContext
from opteryx.procedures import get_procedure
from opteryx.procedures import register_procedure

# Registered once, at import: the registry is process-global, and re-registering a name
# raises. Each test reads and clears the record rather than installing its own.
_CALLS = []


def _record(context, subject, body):
    _CALLS.append((context, subject, body))


def _explode(context):
    raise RuntimeError("delivery failed")


register_procedure("tests.record_call", _record, parameters=("subject", "body"))
register_procedure("tests.explode", _explode, parameters=())


@pytest.fixture(autouse=True)
def _clear():
    _CALLS.clear()
    yield
    _CALLS.clear()


def _run(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass


# --- it runs, exactly once, with the values as written -------------------


def test_call_invokes_the_handler_once():
    _run("CALL tests.record_call('subject', 'body')")
    assert len(_CALLS) == 1, _CALLS
    assert _CALLS[0][1:] == ("subject", "body"), _CALLS


def test_the_name_is_not_case_sensitive():
    """Identifiers are not case sensitive in this engine, and a procedure name is
    identifiers. Registering `tests.record_call` and calling `TESTS.RECORD_CALL` is one
    procedure, not two."""
    _run("CALL TESTS.Record_Call('a', 'b')")
    assert _CALLS[0][1:] == ("a", "b"), _CALLS


# --- every handler is told who called -------------------------------------


def test_the_handler_is_given_the_caller():
    """The registry is process-global, so a handler that addresses the caller
    ("notify SELF") can learn who that is only from the statement being executed."""
    session = opteryx.session(user="someone@example.com")
    for _ in session.execute_to_morsels("CALL tests.record_call('a', 'b')"):
        pass
    context = _CALLS[0][0]
    assert isinstance(context, ProcedureContext), context
    assert context.user == "someone@example.com", context
    assert context.query_id, context


def test_an_unauthenticated_session_carries_no_user():
    """Passed through as None rather than substituted - a handler that requires an
    identity must refuse, not invent one."""
    _run("CALL tests.record_call('a', 'b')")
    assert _CALLS[0][0].user is None, _CALLS


def test_explain_does_not_run_the_procedure():
    """EXPLAIN describes a plan. A procedure that ran while being explained would make
    reading a plan a side effect."""
    _run("EXPLAIN CALL tests.explode()")
    assert _CALLS == []


# --- failure is failure --------------------------------------------------


def test_a_raising_handler_fails_the_statement():
    """There is no success return value to inspect: the handler raises or it worked.
    A swallowed exception here would report SQL_SUCCESS for a notification that was
    never sent."""
    with pytest.raises(RuntimeError):
        _run("CALL tests.explode()")


def test_unknown_procedure_is_refused_at_plan_time():
    with pytest.raises(UnsupportedSyntaxError):
        _run("CALL tests.no_such_procedure('a')")


def test_argument_count_is_checked_at_plan_time():
    with pytest.raises(UnsupportedSyntaxError):
        _run("CALL tests.record_call('only one')")
    assert _CALLS == []


def test_arguments_must_be_literals():
    """A procedure is planned with no relation beneath it, so a column reference has
    nothing to resolve against."""
    with pytest.raises(UnsupportedSyntaxError):
        _run("CALL tests.record_call(some_column, 'b')")


# --- a procedure does not acquire expression syntax ----------------------


@pytest.mark.parametrize(
    "sql",
    [
        "CALL tests.record_call('a', 'b') OVER ()",
        "CALL tests.record_call('a', 'b') FILTER (WHERE TRUE)",
        "CALL tests.record_call(DISTINCT 'a', 'b')",
        "CALL tests.record_call('a', 'b' ORDER BY 1)",
    ],
)
def test_function_call_syntax_is_refused_not_ignored(sql):
    """sqlparser reuses its Function node for CALL, so these all parse. Dropping them
    silently would run something other than what was written."""
    with pytest.raises(UnsupportedSyntaxError):
        _run(sql)
    assert _CALLS == []


# --- registration --------------------------------------------------------


def test_registering_a_name_twice_raises():
    with pytest.raises(ValueError):
        register_procedure("tests.record_call", _record, parameters=("subject", "body"))


def test_a_name_call_cannot_address_is_refused():
    """`CALL` builds a name by joining dotted identifiers, so a registration that is
    not of that shape could never be reached."""
    with pytest.raises(ValueError):
        register_procedure("not a name", _record, parameters=())


def test_a_non_callable_handler_is_refused():
    with pytest.raises(ValueError):
        register_procedure("tests.not_callable", "nope", parameters=())


def test_lookup_is_on_the_folded_name():
    assert get_procedure("TESTS.RECORD_CALL") is get_procedure("tests.record_call")
    assert get_procedure("tests.never_registered") is None


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
