"""
`@@build` identifies a build; `@@version` cannot.

Two deployments can both report `0.9.63` and be different code — the version moves on
release, the build counter moves on every build. Without something that changes when the
code changes, "is this deployment current?" has no answer you can query, and a change
reported as missing from a deployment whose `@@version` matched the tree it was missing
from is not a hypothetical: it is what prompted this variable.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.__version__ import __build__


def one_row(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        for row in morsel:
            return tuple(row)
    raise AssertionError(f"no rows from {sql}")


def test_build_reports_the_running_build():
    assert one_row("SELECT @@build") == (__build__,)


def test_build_is_an_integer_so_it_can_be_compared():
    """The point of a monotonic counter is `>=`. A VARCHAR would order "999" after
    "3037" and quietly answer the wrong way round on any threshold check."""
    (value,) = one_row("SELECT @@build")
    assert isinstance(value, int) and not isinstance(value, bool)
    assert one_row(f"SELECT @@build >= {__build__}") == (True,)
    assert one_row(f"SELECT @@build > {__build__}") == (False,)


def test_build_and_version_read_together():
    """The pair is the useful answer: the version says which release, the build says
    which of that release's builds."""
    assert one_row("SELECT @@version, @@build") == (opteryx.__version__, __build__)


def test_build_is_readable_by_an_ordinary_caller():
    """UNRESTRICTED, like `@@version`: it identifies the engine, which every caller can
    already see the version of, and says nothing about the host it runs on."""
    session = opteryx.session(user="nobody", entitlements=[])
    for morsel in session.execute_to_morsels("SELECT @@build"):
        for row in morsel:
            assert tuple(row) == (__build__,)


def test_build_is_listed_by_show_variables():
    """`SHOW VARIABLES` is the discovery surface — a variable only readable if you
    already know its name is not discoverable at all."""
    session = opteryx.session(user="nobody")
    listed = {
        row[0]: row
        for morsel in session.execute_to_morsels("SHOW VARIABLES")
        for row in morsel
    }
    assert "build" in listed, "build must be discoverable, like version"
    assert listed["build"].visibility == listed["version"].visibility


def test_build_cannot_be_set():
    """INTERNAL-owned, like `version`. A build number a caller could change would be
    worse than none — the one thing it is for is being trusted about which code ran."""
    from opteryx.exceptions import PermissionsError

    session = opteryx.session()
    with pytest.raises(PermissionsError):
        for _ in session.execute_to_morsels("SET build = 1"):
            pass


@pytest.mark.parametrize("variable", ["build", "version"])
def test_the_sigil_spelling_cannot_shadow_the_real_value(variable):
    """`SET @@build = 1` is accepted — it registers an ad-hoc `@@`-named user variable
    rather than writing the system one, which is how every system variable already
    behaves (`SET @@version` too, hence the parametrize). What matters is that it
    cannot make the engine LIE about itself afterwards."""
    session = opteryx.session()
    for _ in session.execute_to_morsels(f"SET @@{variable} = 1"):
        pass
    expected = __build__ if variable == "build" else opteryx.__version__
    for morsel in session.execute_to_morsels(f"SELECT @@{variable}"):
        for row in morsel:
            assert tuple(row) == (expected,)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
