"""`reference/window_catalog.py`'s window-spec literals, asserted against the engine.

WHY THIS TEST EXISTS

The catalog is otherwise derived: WHICH window functions exist comes from the
engine registry, and which aggregates are legal in which aggregate-window form
comes from the aggregate catalog's own support sets. The window SPEC — whether
each form requires, allows or refuses an ORDER BY and a FRAME — is not
derivable. The planner enforces it as inline `raise UnsupportedSyntaxError(...)`
statements inside `_hoist_windows`
(opteryx/planner/logical_planner/logical_planner.py), not as a table a catalog
could import, so `_ORDERED_WINDOW_SPEC` and `_AGGREGATE_WINDOW_SPEC` are typed
by hand.

Hand-typed literals in a derived file are exactly what went stale before: the
catalog claimed for several releases that a frame was "rejected at plan time for
both window forms" and that aggregate windows REJECT an ORDER BY, long after the
framed-aggregate path landed and made both statements false. Nothing caught it
because nothing executed the claims. This does: every literal below is turned
back into SQL and run.

WHAT "REJECTED" HAS TO MEAN

A rejection must be a clean plan-time refusal — UnsupportedSyntaxError — not an
accepted query that quietly ignores the clause. A silently-dropped window spec
is a wrong answer, not a restriction, so a query that RUNS is a failure of the
"rejected" claim even though it did not raise.

BOTH SPELLINGS

A specification can be written inline in the OVER clause or NAMED in the
statement's WINDOW clause, and the catalog claims the two are the same window.
So every spec literal below is run in both spellings: a rule that held for
`OVER (ORDER BY id)` and not for `OVER w` would make the catalog's claim false.
The named spelling used to be the case that got this wrong — the WINDOW clause
was parsed, dropped, and the window planned as a plain aggregate. What it
ANSWERS is pinned in tests/sql/test_named_windows.py; what the catalog CLAIMS
about it is pinned here.
"""

import os
import sys

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.operators.aggregate.helpers import AGGREGATORS
from opteryx.operators.window.helpers import FRAMED_AGGREGATE_FUNCTIONS
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS
from reference.window_catalog import _AGGREGATE_WINDOW_SPEC
from reference.window_catalog import _ORDERED_WINDOW_SPEC
from reference.window_catalog import export_window_catalog
from tests.helpers import execute_and_get_arrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


# A call of each registry function that is otherwise valid, so the only thing
# under test is the window spec appended to it.
_CALL = {
    "ROW_NUMBER": "ROW_NUMBER()",
    "RANK": "RANK()",
    "DENSE_RANK": "DENSE_RANK()",
    "NTILE": "NTILE(2)",
    "PERCENT_RANK": "PERCENT_RANK()",
    "CUME_DIST": "CUME_DIST()",
    "LAG": "LAG(mass)",
    "LEAD": "LEAD(mass)",
    "FIRST_VALUE": "FIRST_VALUE(mass)",
    "LAST_VALUE": "LAST_VALUE(mass)",
    "NTH_VALUE": "NTH_VALUE(mass, 1)",
}

_FRAME = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"


def _spellings(call: str, spec: str) -> list:
    """(label, SQL) for the inline and the named spelling of one specification.

    The empty specification has no named spelling — `WINDOW w AS ()` does not parse —
    so it yields the inline form alone rather than a form that tests the parser.
    """
    forms = [("inline", f"SELECT {call} OVER {spec} AS r FROM $planets")]
    if spec != "()":
        forms.append(("named", f"SELECT {call} OVER w AS r FROM $planets WINDOW w AS {spec}"))
    return forms


def _runs(sql: str):
    """(ran?, error) — a clean plan-time refusal is a result, not a failure."""
    try:
        execute_and_get_arrow(sql)
        return True, None
    except UnsupportedSyntaxError as error:
        return False, error


def test_call_table_covers_the_registry():
    """A function added to the registry must be exercised here, not skipped."""
    assert set(_CALL) == set(WINDOW_FUNCTIONS)


@pytest.mark.parametrize("function", sorted(_CALL))
def test_ordered_window_spec_order_by(function):
    """`_ORDERED_WINDOW_SPEC["order_by"] == "required"` — with and without."""
    assert _ORDERED_WINDOW_SPEC["order_by"] == "required"
    call = _CALL[function]

    for spelling, sql in _spellings(call, "(ORDER BY id)"):
        ran, error = _runs(sql)
        assert ran, f"{function}: an ORDER BY window is required to run ({spelling}: {error})"

    for spec in ("()", "(PARTITION BY id)"):
        for spelling, sql in _spellings(call, spec):
            ran, error = _runs(sql)
            assert not ran, (
                f"{function} OVER {spec} ran ({spelling}); catalog says ORDER BY is required"
            )
            assert "ORDER BY" in str(error)


@pytest.mark.parametrize("function", sorted(_CALL))
def test_ordered_window_spec_frame_is_rejected(function):
    """`_ORDERED_WINDOW_SPEC["frame"] == "rejected"` — for ROWS and RANGE alike."""
    assert _ORDERED_WINDOW_SPEC["frame"] == "rejected"
    call = _CALL[function]

    for frame in (_FRAME, "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"):
        for spelling, sql in _spellings(call, f"(ORDER BY id {frame})"):
            ran, error = _runs(sql)
            assert not ran, (
                f"{function} accepted `{frame}` ({spelling}); catalog says frame is rejected"
            )
            assert "not supported" in str(error)


@pytest.mark.parametrize("function", sorted(_CALL))
def test_ordered_window_spec_partition_by_is_optional(function):
    """`_ORDERED_WINDOW_SPEC["partition_by"] == "optional"` — legal with and without."""
    assert _ORDERED_WINDOW_SPEC["partition_by"] == "optional"
    call = _CALL[function]

    for spec in ("(ORDER BY id)", "(PARTITION BY gravity ORDER BY id)"):
        for spelling, sql in _spellings(call, spec):
            ran, error = _runs(sql)
            assert ran, f"{function} OVER {spec} refused ({spelling}): {error}"


@pytest.mark.parametrize("aggregate", sorted(AGGREGATORS))
def test_aggregate_window_order_by_and_frame_match_the_support_map(aggregate):
    """`support[agg]["over_order_by"]` / `["over_frame"]` against the engine.

    Both are derived from FRAMED_AGGREGATE_FUNCTIONS, so this is the assertion
    that the derivation is the RIGHT one — that the planner's gate really is
    that set, and really governs the ORDER BY and the frame together.
    """
    assert _AGGREGATE_WINDOW_SPEC["order_by"] == "conditional"
    assert _AGGREGATE_WINDOW_SPEC["frame"] == "conditional"

    support = export_window_catalog()["aggregate_windows"]["support"][aggregate]
    argument = "*" if aggregate == "COUNT" else "mass"
    call = f"{aggregate}({argument})"

    for spelling, sql in _spellings(call, "(ORDER BY id)"):
        ran_order, error_order = _runs(sql)
        assert ran_order == support["over_order_by"], (
            f"{aggregate} OVER (ORDER BY id) [{spelling}]: engine ran={ran_order}, "
            f"catalog over_order_by={support['over_order_by']} ({error_order})"
        )

    for spelling, sql in _spellings(call, f"(ORDER BY id {_FRAME})"):
        ran_frame, error_frame = _runs(sql)
        assert ran_frame == support["over_frame"], (
            f"{aggregate} OVER (ORDER BY id {_FRAME}) [{spelling}]: engine ran={ran_frame}, "
            f"catalog over_frame={support['over_frame']} ({error_frame})"
        )


def test_framed_aggregate_set_is_the_planners_gate():
    """The five names the catalog's prose spells out, against the engine's set."""
    assert set(FRAMED_AGGREGATE_FUNCTIONS) == {"AVG", "COUNT", "MAX", "MIN", "SUM"}


def test_aggregate_window_partition_by_is_optional():
    """`OVER ()` and `OVER (PARTITION BY ...)` both run for a framed aggregate."""
    assert _AGGREGATE_WINDOW_SPEC["partition_by"] == "optional"
    for spec in ("()", "(PARTITION BY gravity)"):
        for spelling, sql in _spellings("SUM(mass)", spec):
            ran, error = _runs(sql)
            assert ran, f"SUM(mass) OVER {spec} refused ({spelling}): {error}"


def test_window_frames_restriction_is_claimed_supported():
    """The catalog no longer claims frames are rejected for both forms."""
    frames = export_window_catalog()["restrictions"]["window_frames"]
    assert frames["supported"] is True


def test_frame_requires_an_order_by():
    """Case (3) of the `window_frames` detail: a frame with no ordering is refused."""
    ran, error = _runs(f"SELECT SUM(mass) OVER ({_FRAME}) AS r FROM $planets")
    assert not ran
    assert "ORDER BY" in str(error)


def test_range_frame_rejects_a_numeric_offset():
    """Case (4): RANGE takes only the unbounded/current-row bounds."""
    ran, error = _runs(
        "SELECT SUM(mass) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM $planets"
    )
    assert not ran
    assert "RANGE" in str(error)


def test_groups_frame_units_are_rejected():
    """Case (4): the units must be ROWS or RANGE."""
    ran, error = _runs(
        "SELECT SUM(mass) OVER (ORDER BY id GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM $planets"
    )
    assert not ran
    assert "Groups" in str(error) or "GROUPS" in str(error)


def test_inverted_frame_bounds_are_rejected():
    """Case (4): the start bound may not come after the end bound."""
    ran, error = _runs(
        "SELECT SUM(mass) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) AS r FROM $planets"
    )
    assert not ran


def test_running_sum_is_actually_running():
    """A frame that is accepted must also be APPLIED.

    The catalog's claim is that these windows compute a running total, not that
    the parser tolerates the syntax. A frame accepted and then ignored would
    satisfy every acceptance assertion above while answering the wrong thing.
    """
    table = execute_and_get_arrow(
        "SELECT id, SUM(mass) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running "
        "FROM $planets"
    )
    rows = sorted(zip(table.column("id").to_pylist(), table.column("running").to_pylist()))
    running = [value for _, value in rows]
    assert running == sorted(running), "a running total must be non-decreasing over positive masses"
    assert running[0] != running[-1], "every row carrying one value means the frame was ignored"


def test_moving_frame_differs_from_the_running_one():
    """A bounded ROWS frame must not answer what UNBOUNDED PRECEDING answers."""
    moving = (
        execute_and_get_arrow(
            "SELECT SUM(mass) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM $planets"
        )
        .column("r")
        .to_pylist()
    )
    runningv = (
        execute_and_get_arrow(
            "SELECT SUM(mass) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM $planets"
        )
        .column("r")
        .to_pylist()
    )
    assert moving != runningv, (
        "a 1 PRECEDING frame answering the unbounded one means the bound was ignored"
    )


def test_named_windows_restriction_is_claimed_supported():
    """The catalog claims the named spelling exists; every test above runs it."""
    named = export_window_catalog()["restrictions"]["named_windows"]
    assert named["supported"] is True


def test_named_and_inline_spellings_of_one_spec_are_one_column():
    """The catalog claims the two spellings dedup onto one computed column."""
    table = execute_and_get_arrow(
        "SELECT SUM(mass) OVER w AS a, SUM(mass) OVER (PARTITION BY gravity) AS b "
        "FROM $planets WINDOW w AS (PARTITION BY gravity)"
    )
    assert table.num_rows == 9
    assert table.column("a").to_pylist() == table.column("b").to_pylist()


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-v"]))
