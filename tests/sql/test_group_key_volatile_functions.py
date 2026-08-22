"""Regression tests: a GROUP BY key containing a per-row volatile function must
not be reduced away.

`GroupKeyReductionStrategy` drops a GROUP BY expression when the partition is
already determined by a bare key also in the GROUP BY — `GROUP BY x, x + 1` is
partitioned entirely by `x`, so `x + 1` is recomputed in a Project above the
aggregate instead of being grouped on. That is only sound when the expression is
a DETERMINISTIC function of `x`.

The strategy carried its own private list of non-deterministic function names.
It listed RANDOM and RAND but not NORMAL or RANDOM_STRING, so `GROUP BY g,
g + NORMAL()` was treated as a deterministic function of `g` and reduced away:
over 12 rows with 3 distinct `g`, the query returned 3 groups instead of 12. No
error, no warning — a silent wrong answer that differed from the otherwise
identical RANDOM() spelling only by which name happened to be on the list.

The list now comes from `opteryx.planner.expression_traits.VOLATILE_FUNCTIONS`,
shared with join-key hoisting, which asks the same question: may this expression
be RELOCATED? (`constant_folding` keeps a narrower list on purpose — folding
NOW() to one plan-time timestamp is desired there.)

These tests pin the GROUP COUNT, not the row count. The row count is 12 either
way; only the number of groups distinguishes the defect from the fix.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.planner.expression_traits import VOLATILE_FUNCTIONS

# 12 rows, 3 distinct values of `g`.
SRC = (
    "(SELECT g FROM (VALUES (1),(1),(1),(1),(2),(2),(2),(2),(3),(3),(3),(3)) AS v(g)) AS t"
)


def _group_count(sql):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


# --------------------------------------------------------------------------
# The defect: a volatile function in a derived group key
# --------------------------------------------------------------------------


def test_normal_in_group_key_is_not_reduced():
    """The motivating case. NORMAL() is evaluated per row, so every row is its
    own group: 12, not 3."""
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, g + NORMAL()")
    assert got == 12, got


def test_random_string_in_group_key_is_not_reduced():
    """RANDOM_STRING takes an argument, so it reaches the strategy as `f(g)` —
    a shape the arithmetic case above does not cover — and was equally broken."""
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, RANDOM_STRING(g)")
    assert got == 12, got


def test_random_in_group_key_is_not_reduced():
    """RANDOM() was already on the old list. Pinned so the two spellings cannot
    drift apart again."""
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, g + RANDOM()")
    assert got == 12, got


def test_bare_volatile_group_key_is_not_reduced():
    """Without an enclosing arithmetic expression: NORMAL() alone as a second
    group key."""
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, NORMAL()")
    assert got == 12, got


# --------------------------------------------------------------------------
# The reduction itself must still happen — this is not a blanket disable
# --------------------------------------------------------------------------


def test_deterministic_derived_key_is_still_reduced():
    """`g * 2` IS a function of `g`; reducing it is the strategy working."""
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, g * 2")
    assert got == 3, got


def test_deterministic_function_key_is_still_reduced():
    got = _group_count(f"SELECT COUNT(*) FROM {SRC} GROUP BY g, ABS(g)")
    assert got == 3, got


def test_counts_are_correct_when_reduction_applies():
    """The reduced form must return the right COUNT per group, not just the
    right number of groups."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
        f"SELECT g, COUNT(*) AS n FROM {SRC} GROUP BY g, g * 2"
    ):
        for i in range(morsel.num_rows):
            rows.append(tuple(morsel[i]))
    assert sorted(rows) == [(1, 4), (2, 4), (3, 4)], rows


# --------------------------------------------------------------------------
# The shared list is what the strategy actually consults
# --------------------------------------------------------------------------


def test_shared_volatile_set_covers_the_random_family():
    """The two names whose absence caused the defect, plus the ones that were
    already there. A name dropped from VOLATILE_FUNCTIONS now breaks a test
    instead of silently collapsing groups."""
    for name in ("NORMAL", "RANDOM_STRING", "RANDOM", "RAND"):
        assert name in VOLATILE_FUNCTIONS, name


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
