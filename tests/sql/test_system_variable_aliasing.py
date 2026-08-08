"""`SELECT @@name AS alias` — reading a system variable under an alias.

Every system variable was unreadable the moment it was aliased:

    SELECT @@version         ->  '0.9.58'
    SELECT @@version AS v    ->  ColumnNotFoundError: Unknown column 'v'

`locate_identifier` recognises a variable by testing the leading `@` of the
column's name, and it tested `current_name` — which is `alias or source_column`.
Unaliased those are the same string, so the bug was invisible; aliased,
`current_name` is the ALIAS, the `@` test failed, and the branch that would have
resolved the variable was skipped in favour of `raise ColumnNotFoundError`. The
same substitution reached the lookup itself: `variables.as_column(node.value)`
asked the container for a variable literally named `v`.

The fix is to test and look up `source_column` — the `@@name` as written — and to
carry the alias onto the resulting literal so it names the output column. These
tests pin BOTH halves: that the query runs at all, and that the alias is the name
that comes back.

`VERSION()` is gone (the architect's ruling: `@@version` is the way to read the
version), so the last test here guards the removal — it is also why the aliasing
had to be fixed first, since `VERSION() AS v` was the only spelling that worked.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import VariableNotFoundError


def one_row(sql):
    """(column names, first row) for a single-morsel query."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        names = [
            n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names
        ]
        return names, morsel[0]
    return [], ()


# ---------------------------------------------------------------------------
# The bug: an alias made the variable unreadable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variable", ["@@version", "@@array_agg_memory_budget_bytes"])
def test_aliased_system_variable_reads_the_same_value_as_unaliased(variable):
    """It broke for EVERY variable, not one of them — the `@` test is shared."""
    bare_names, bare_row = one_row(f"SELECT {variable}")
    aliased_names, aliased_row = one_row(f"SELECT {variable} AS v")

    assert bare_names == [variable]
    assert aliased_names == ["v"], "the alias must survive onto the output column"
    assert aliased_row == bare_row


def test_the_version_variable_is_the_running_version():
    """Not just "a string" — the value must still be the build's version."""
    _, row = one_row("SELECT @@version AS v")
    assert row == (opteryx.__version__,)


def test_aliasing_does_not_change_the_value_type():
    """An INT64 variable stays an integer under an alias — the alias renames the
    output, it does not re-resolve the variable through a different path."""
    _, row = one_row("SELECT @@array_agg_memory_budget_bytes AS budget")
    assert isinstance(row[0], int) and row[0] > 0


# ---------------------------------------------------------------------------
# The alias behaves like any other alias
# ---------------------------------------------------------------------------


def test_several_aliased_variables_in_one_projection():
    names, row = one_row("SELECT @@version AS v, @@array_agg_memory_budget_bytes AS b")
    assert names == ["v", "b"]
    assert row[0] == opteryx.__version__


def test_the_same_variable_aliased_and_unaliased():
    """The second reference resolves against the copy the first one published to
    `$derived` — a different branch of `locate_identifier`, with the same `@` test
    and therefore the same bug."""
    names, row = one_row("SELECT @@version, @@version AS v")
    assert names == ["@@version", "v"]
    assert row == (opteryx.__version__, opteryx.__version__)


def test_the_alias_resolves_elsewhere_in_the_query():
    names, _ = one_row("SELECT @@version AS v ORDER BY v")
    assert names == ["v"]


def test_an_aliased_variable_inside_an_expression():
    names, row = one_row("SELECT LENGTH(@@version) AS l")
    assert names == ["l"]
    assert row == (len(opteryx.__version__),)


def test_an_aliased_variable_over_a_relation():
    """A variable is a constant, so it must survive a projection that also has a
    real relation under it."""
    session = opteryx.session()
    rows = 0
    for morsel in session.execute_to_morsels("SELECT @@version AS v FROM $planets"):
        names = [
            n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names
        ]
        assert names == ["v"]
        rows += morsel.num_rows
    assert rows == 9


def test_an_aliased_ad_hoc_user_variable():
    """`@x` variables take a different registration path to `@@name` system
    variables, but the same `@` test in the binder."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels("SET @x = 7; SELECT @x AS seven"):
        names = [
            n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names
        ]
        assert names == ["seven"]
        assert morsel[0] == (7,)


# ---------------------------------------------------------------------------
# An unknown variable fails as a variable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT @@no_such_variable",
        "SELECT @@no_such_variable AS v",
        "SELECT @nope",
    ],
)
def test_an_unknown_variable_raises_variable_not_found(sql):
    """`as_column` subscripted `_variables` on the `@@` branch, so an unknown system
    variable escaped as a bare `KeyError('no_such_variable')` — untyped, and naming
    the key with the `@@` stripped off rather than what the user wrote. The `@x`
    branch beside it already raised properly."""
    with pytest.raises(VariableNotFoundError):
        list(opteryx.session().execute_to_morsels(sql))


def test_an_unknown_variable_does_not_name_a_restricted_one():
    """No "did you mean" on this path. A suggestion is an existence oracle, and
    RESTRICTED variables exist precisely so a non-admin cannot learn whether they
    exist — which is why reading one is a PermissionsError and not a not-found."""
    with pytest.raises(VariableNotFoundError) as caught:
        list(opteryx.session(user="mallory").execute_to_morsels("SELECT @@local_store_roo"))
    assert "local_store_root" not in str(caught.value)


def test_a_restricted_variable_still_reports_a_permissions_error():
    """The not-found fix must not have collapsed the two cases together."""
    with pytest.raises(PermissionsError):
        list(opteryx.session(user="mallory").execute_to_morsels("SELECT @@local_store_root"))


# ---------------------------------------------------------------------------
# VERSION() is gone
# ---------------------------------------------------------------------------


def test_the_version_function_no_longer_exists():
    """`@@version` is the way to read the version. If `VERSION()` comes back, this
    test and the aliasing tests above are the record of why it went."""
    for sql in ("SELECT VERSION()", "SELECT VERSION() AS v"):
        with pytest.raises(FunctionNotFoundError):
            list(opteryx.session().execute_to_morsels(sql))


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
