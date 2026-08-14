"""`BETWEEN` is INCLUSIVE at both ends, and `NOT BETWEEN` is its strict complement.

BETWEEN has no node of its own — `between()` in logical_planner_builders.py LOWERS it
in the planner:

    x BETWEEN low AND high      ->  x >= low AND x <= high
    x NOT BETWEEN low AND high  ->  x <  low OR  x >  high

Which means the bound semantics live entirely in a choice of four comparison
operators, and swapping any one of `GtEq`/`LtEq` for its strict form is a silent
wrong answer at exactly one row per bound — the kind of edge that a row-count shape
test over an interior range never touches. The comments in `between()` documented the
STRICT spelling for years while the code was inclusive; this pins the code so the
next reader can trust which of the two is authoritative.

The boundary rows are the whole point: `n BETWEEN 2 AND 4` must be TRUE at 2 and at 4.

NULL is three-valued and is asserted separately. `NOT BETWEEN` complements `BETWEEN`
only where the operand is NOT NULL — over a NULL operand BOTH are NULL, so a test
that phrased the complement as "exactly one of them is TRUE" would be asserting the
wrong law.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# 1, 2, 3 and a SQL NULL, so both bounds and the unknown case are in one relation.
NUMS = "(SELECT * FROM (VALUES (1), (2), (3), (NULL)) AS v(n))"


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


def test_both_bounds_are_included():
    # The rows that separate `>=`/`<=` from `>`/`<`: the low bound and the high one.
    rows = results("SELECT id, id BETWEEN 2 AND 4 AS b FROM $planets ORDER BY id LIMIT 5")
    assert rows == {"id": [1, 2, 3, 4, 5], "b": [False, True, True, True, False]}, rows


def test_not_between_excludes_both_bounds():
    rows = results("SELECT id, id NOT BETWEEN 2 AND 4 AS b FROM $planets ORDER BY id LIMIT 5")
    assert rows == {"id": [1, 2, 3, 4, 5], "b": [True, False, False, False, True]}, rows


def test_not_between_is_the_exact_complement_where_the_operand_is_not_null():
    rows = results(
        "SELECT id BETWEEN 2 AND 4 AS b, id NOT BETWEEN 2 AND 4 AS nb FROM $planets ORDER BY id"
    )
    assert all(b is not nb for b, nb in zip(rows["b"], rows["nb"])), rows


def test_a_degenerate_range_matches_exactly_one_value():
    # low == high collapses to equality, which only holds if BOTH bounds are
    # inclusive — make either one strict and this range matches nothing at all.
    rows = results(f"SELECT n, n BETWEEN 2 AND 2 AS b FROM {NUMS}")
    assert rows == {"n": [1, 2, 3, None], "b": [False, True, False, None]}, rows


def test_an_inverted_range_matches_nothing():
    # `low > high` is not an error and not silently reordered: the lowering is
    # `n >= 3 AND n <= 1`, which no value satisfies. NULL stays NULL.
    rows = results(f"SELECT n, n BETWEEN 3 AND 1 AS b FROM {NUMS}")
    assert rows == {"n": [1, 2, 3, None], "b": [False, False, False, None]}, rows


def test_a_null_operand_is_null_in_both_directions():
    # Three-valued: the answer is UNKNOWN, not FALSE. A NULL folded to FALSE here
    # would make `NOT BETWEEN` claim the row is outside the range.
    rows = results(f"SELECT n BETWEEN 1 AND 3 AS b, n NOT BETWEEN 1 AND 3 AS nb FROM {NUMS}")
    assert rows["b"] == [True, True, True, None], rows
    assert rows["nb"] == [False, False, False, None], rows


@pytest.mark.parametrize(
    "predicate,expected",
    [
        # In WHERE the same bounds must hold, and an UNKNOWN row is kept by
        # NEITHER direction — that is what makes the two counts sum to 3, not 4.
        ("n BETWEEN 1 AND 3", 3),
        ("n NOT BETWEEN 1 AND 3", 0),
        ("n BETWEEN 2 AND 3", 2),
        ("n NOT BETWEEN 2 AND 3", 1),
        ("n BETWEEN 2 AND 2", 1),
        ("n BETWEEN 3 AND 1", 0),
    ],
)
def test_bounds_hold_in_a_predicate_position(predicate, expected):
    rows = results(f"SELECT COUNT(*) AS c FROM {NUMS} WHERE {predicate}")
    assert rows == {"c": [expected]}, rows


def test_bounds_are_inclusive_for_a_non_numeric_type():
    # Nothing about inclusivity is integer-specific; the bounds here are exact
    # stored values, so both are boundary rows.
    rows = results(
        "SELECT name, name BETWEEN 'Earth' AND 'Mars' AS b FROM $planets ORDER BY id LIMIT 4"
    )
    assert rows == {
        "name": ["Mercury", "Venus", "Earth", "Mars"],
        "b": [False, False, True, True],
    }, rows


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
