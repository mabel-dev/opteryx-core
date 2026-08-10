"""`FROM UNNEST(...)` — building a relation out of a literal array.

UNNEST reached from the FROM clause is a SOURCE (a FunctionDataset), not the
CROSS JOIN UNNEST operator: there is no input stream underneath it, so the
argument has to carry its own values. Anything else is refused in the binder
(opteryx/planner/binder/dataset.py::_validate_unnest_argument).

The refusal is load-bearing, not tidiness. `_unnest` reads `args[0].value` and
iterates it as an array, so a shape that is merely *iterable* was silently
accepted and quietly wrong — and a subquery argument, whose `.value` is a
LogicalPlan, was neither: `Graph.__getitem__` returns None instead of raising
IndexError, so Python's legacy __getitem__ iteration protocol walked it forever
and the query hung allocating without bound. That is why the subquery case here
runs in a timed-out child process (below) rather than in-process — a regression
must fail this test, not hang the suite running it.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import subprocess
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.exceptions import InvalidFunctionParameterError

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# The pre-fix hang allocated ~63MB/s, so this bounds a regression to ~1.3GB and
# ~20s rather than the machine's memory. RLIMIT_AS would be the tighter guard but
# is not settable on the dev platform (macOS refuses it outright), so the wall
# clock is the guard that actually exists.
HANG_GUARD_SECONDS = 20


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _refusal(sql):
    """Run `sql`, require InvalidFunctionParameterError, return the message."""
    try:
        _rows(sql)
    except InvalidFunctionParameterError as err:
        return str(err)
    raise AssertionError(f"UNNEST accepted {sql!r}")


def test_literal_array_builds_a_relation():
    assert [r[0] for r in _rows("SELECT * FROM UNNEST((1, 2, 3)) AS block")] == [1, 2, 3]
    assert [r[0] for r in _rows("SELECT * FROM UNNEST(['a', 'b']) AS block")] == ["a", "b"]


def test_parenthesised_single_value_is_a_one_row_relation():
    """`UNNEST((1))` is not an array — it is one value in parentheses, one row.

    Asserted because it is the one non-array shape the source deliberately
    accepts, so a refusal written slightly too wide would take it out.
    """
    assert [r[0] for r in _rows("SELECT * FROM UNNEST((1)) AS block")] == [1]


def test_subquery_source_is_refused_and_does_not_hang():
    """The regression: a subquery argument used to hang, allocating forever.

    Run in a child process with a hard timeout so a regression reports as a
    failure here instead of taking the suite and the machine with it.
    """
    sql = (
        "SELECT * FROM UNNEST((SELECT CIDR_AGG(CAST(id + 167772160 AS IPV4)) "
        "FROM $planets)) AS block"
    )
    child = (
        "import sys\n"
        f"sys.path.insert(1, {ROOT!r})\n"
        "import opteryx\n"
        "from opteryx.exceptions import InvalidFunctionParameterError\n"
        "try:\n"
        f"    list(opteryx.session().execute_to_morsels({sql!r}))\n"
        "except InvalidFunctionParameterError as err:\n"
        "    print('REFUSED')\n"
        "    sys.exit(0)\n"
        "print('ACCEPTED')\n"
        "sys.exit(1)\n"
    )

    try:
        done = subprocess.run(
            [sys.executable, "-c", child],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=HANG_GUARD_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AssertionError(
            f"UNNEST(<subquery>) did not fail within {HANG_GUARD_SECONDS}s — it is "
            "hanging again (the child was killed; check Graph.__getitem__ iteration)"
        )

    assert done.returncode == 0, f"stdout={done.stdout!r} stderr={done.stderr[-2000:]!r}"
    assert "REFUSED" in done.stdout, done.stdout


def test_refusal_names_the_shape_that_was_written():
    """The message has to say WHICH shape was given — that is what picks the remedy.

    A caller who wrote a subquery needs the CROSS JOIN rewrite; a caller who wrote
    a scalar needs to write out the values. One generic "invalid argument" sends
    both of them to read the source.
    """
    assert "a subquery" in _refusal(
        "SELECT * FROM UNNEST((SELECT ARRAY_AGG(name) FROM $planets)) AS block"
    )
    assert "a column reference" in _refusal("SELECT * FROM UNNEST(missions) AS block")
    assert "a function call" in _refusal("SELECT * FROM UNNEST(SPLIT('a,b', ',')) AS block")
    assert "a single value" in _refusal("SELECT * FROM UNNEST('abc') AS block")


def test_refusal_carries_the_supported_rewrite():
    """Both remedies are in the message, so nobody has to guess the working form."""
    message = _refusal("SELECT * FROM UNNEST((SELECT ARRAY_AGG(name) FROM $planets)) AS block")
    assert "UNNEST((1, 2, 3)) AS x" in message
    assert "CROSS JOIN UNNEST(s.a) AS x" in message


def test_scalar_source_is_refused_rather_than_split_into_characters():
    """`UNNEST('abc')` used to return three rows — a, b, c — from _as_list(str)."""
    _refusal("SELECT * FROM UNNEST('abc') AS block")
    _refusal("SELECT * FROM UNNEST(1) AS block")


def test_arity_is_checked():
    assert "takes exactly one array" in _refusal("SELECT * FROM UNNEST(1, 2) AS block")


def test_the_supported_rewrite_actually_works():
    """The form the error recommends — the refusal is only honest if this runs."""
    got = _rows(
        "SELECT block FROM (SELECT CIDR_AGG(CAST(id + 167772160 AS IPV4)) AS blocks "
        "FROM $planets) AS agg CROSS JOIN UNNEST(agg.blocks) AS block"
    )
    assert got, got
    assert all(isinstance(row[0], str) and "/" in row[0] for row in got), got


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
