"""Regression tests for the buffering aggregates' memory guards.

MEDIAN and ARRAY_AGG are holistic — they retain every input value until finalize
— so each is bounded by its OWN global byte budget across all group buffers. The
VALUES live in src/cpp/engine/agg_budgets.hpp (`kMedianFloorBytes`/`kMedianBytes`,
`kArrayAggBytes`); the atomic counters that charge against them stay with the
states that own them (`median_budget_used` in _agg_kernels.hpp,
`array_agg_budget_used` in native_group_sinks.hpp). Both budgets replaced
per-group value caps, which bounded nothing (the group count is unbounded) while
refusing ordinary group sizes.

ONE GUARD, AT EXECUTION TIME. There is deliberately no plan-time estimate in
front of these budgets. What a buffering aggregate actually retains turns on
properties no planner statistic carries, so an estimate can only be a guess, and
a guess that refuses queries turns working ones into plan-time errors. A query
that cannot fit reads its input and then fails loud on a MEASUREMENT.

The failure must never be a truncated result: a short list or a median over a
subset is a wrong answer wearing the shape of a right one, and no caller can
tell. These tests assert on the raising and on the wording.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

PARQUET = "testdata.flat.formats.parquet"   # 100,000 rows


def _rows(sql, session=None):
    session = session or opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def test_array_agg_group_beyond_old_cap():
    """A single group far past the retired 1000-element per-group cap."""
    sizes = {
        row[0]: len(row[1])
        for row in _rows(
            f"SELECT followers > 1000 AS f, ARRAY_AGG(user_name) AS a "
            f"FROM {PARQUET} GROUP BY f"
        )
    }
    assert sum(sizes.values()) == 100000, sizes
    # The point of the change: the biggest group is tens of thousands of
    # elements, which the old cap refused outright.
    assert max(sizes.values()) > 50000, sizes


def test_array_agg_runtime_budget_fails_loud():
    """Exhausting the budget raises; it never returns a truncated list.

    This is the only guard there is, so it is the one that has to be right.
    """
    sql = (
        f"SELECT followers > 1000 AS f, ARRAY_AGG(text) AS a FROM {PARQUET} "
        f"CROSS JOIN $planets AS p1 CROSS JOIN $planets AS p2 GROUP BY f"
    )
    try:
        _rows(sql)
    except Exception as err:  # the native error channel surfaces as RuntimeError
        message = str(err)
        assert "ARRAY_AGG" in message, message
        assert "memory budget" in message, message
        # It measured. Nothing here is an estimate, and the message must not
        # imply one — there is no plan-time gate to confuse it with.
        assert "estimate" not in message.lower(), message
        return
    raise AssertionError("ARRAY_AGG did not fail when the budget was exhausted")


def test_aggregate_within_budget_runs():
    """A query that fits is untouched by the guard.

    Worth pinning separately from the failure case: a budget that refused
    ordinary work would be worse than no budget, and this is the size of input
    the old per-group cap used to reject.
    """
    rows = _rows(
        f"SELECT followers > 1000 AS f, MEDIAN(followers) FROM {PARQUET} "
        f"CROSS JOIN $planets p1 GROUP BY f"
    )
    assert len(rows) == 2, rows
    assert all(row[1] is not None for row in rows), rows


def test_ordinary_aggregates_are_untouched():
    """Everyday queries never come near the guard."""
    assert _rows(f"SELECT MEDIAN(followers) FROM {PARQUET}")[0][0] is not None
    assert len(_rows("SELECT name, ARRAY_AGG(id) FROM $planets GROUP BY name")) == 9


def test_budgets_come_from_the_native_constants():
    """Python reads the budget from C++ rather than mirroring it, so the figure
    reported and the figure enforced cannot drift apart.

    MEDIAN's figure is its hard CEILING (`kMedianBytes`), which a query reaches
    only after escalation: it starts entitled to `kMedianFloorBytes` (256MB) and
    the ceiling doubles on MEASURED demand up to this value before the query is
    refused. That is why the two aggregates' numbers differ — ARRAY_AGG has a
    single flat budget.
    """
    from opteryx.compiled.agg_budgets import array_agg_budget_bytes
    from opteryx.compiled.agg_budgets import median_budget_bytes

    assert median_budget_bytes() == 2048 * 1024 * 1024
    assert array_agg_budget_bytes() == 512 * 1024 * 1024


def test_budgets_are_discoverable_from_sql():
    """The budgets are reported, not just enforced.

    `SHOW VARIABLES` is the single discoverable surface for engine limits (see
    opteryx/variables.py) — a limit an author can only learn by tripping it is a
    limit they cannot plan around. These are SERVER-owned, so they COMMUNICATE
    rather than let anyone change behaviour; the value is a compile-time constant
    and a session that appeared to change it would be lying.

    That surface matters more now than it did: with the plan-time gate gone,
    `SHOW VARIABLES` is the ONLY way to see the line before hitting it.
    """
    from opteryx.compiled.agg_budgets import array_agg_budget_bytes
    from opteryx.compiled.agg_budgets import median_budget_bytes

    shown = {row[0]: row[1] for row in _rows("SHOW VARIABLES")}
    for name, native in (
        ("median_memory_budget_bytes", median_budget_bytes()),
        ("array_agg_memory_budget_bytes", array_agg_budget_bytes()),
    ):
        assert name in shown, sorted(shown)
        # Reported value must be the ENFORCED value — that is the whole reason
        # variables.py reads the native constant instead of mirroring a literal.
        assert int(shown[name]) == native, (name, shown[name], native)

    # And readable directly, so a query can branch on it.
    assert _rows("SELECT @@array_agg_memory_budget_bytes")[0][0] == array_agg_budget_bytes()


def test_budget_variables_are_not_settable():
    """SERVER-owned: communication, not a knob."""
    from opteryx.exceptions import PermissionsError

    try:
        _rows("SET array_agg_memory_budget_bytes = 1")
    except PermissionsError as err:
        assert "array_agg_memory_budget_bytes" in str(err), str(err)
        return
    raise AssertionError("a SERVER-owned budget was settable")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
