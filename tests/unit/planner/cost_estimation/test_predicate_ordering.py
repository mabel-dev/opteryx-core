"""Unit tests for opteryx.planner.cost_estimation.predicate_ordering."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.planner.cost_estimation import PredicateStats
from opteryx.planner.cost_estimation import order_predicates


def _p(sel, cost):
    return PredicateStats(selectivity=sel, cost=cost)


# ---------------------------------------------------------------------------
# Trivial inputs
# ---------------------------------------------------------------------------


def test_empty_list():
    assert order_predicates([]) == []


def test_single_predicate_unchanged():
    assert order_predicates([("a", _p(0.5, 1.0))]) == ["a"]


# ---------------------------------------------------------------------------
# Two-predicate corner cases
# ---------------------------------------------------------------------------


def test_two_predicates_more_selective_first_when_costs_equal():
    # Equal cost; "a" filters more rows (sel 0.1) so should run first.
    result = order_predicates(
        [("a", _p(0.1, 1.0)), ("b", _p(0.9, 1.0))]
    )
    assert result == ["a", "b"]


def test_two_predicates_cheaper_first_when_selectivities_equal():
    # Equal selectivity; cheaper predicate should run first.
    result = order_predicates(
        [("expensive", _p(0.5, 10.0)), ("cheap", _p(0.5, 1.0))]
    )
    assert result == ["cheap", "expensive"]


# ---------------------------------------------------------------------------
# Hand-computed three-predicate ordering
# ---------------------------------------------------------------------------


def test_three_predicates_hand_computed():
    # Cost minimisation:
    #   a: sel=0.5, cost=1
    #   b: sel=0.1, cost=2
    #   c: sel=0.9, cost=3
    # Optimal order is a, b, c:
    #   1 + 2*0.5 + 3*0.5*0.1 = 1 + 1.0 + 0.15 = 2.15
    # (Other notable: b,a,c = 2 + 0.1 + 0.15 = 2.25; a is cheap enough that
    # paying its cost up-front beats running b first.)
    result = order_predicates(
        [("a", _p(0.5, 1.0)), ("b", _p(0.1, 2.0)), ("c", _p(0.9, 3.0))]
    )
    assert result == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Brute-force vs rank agreement at threshold boundary
# ---------------------------------------------------------------------------


def test_brute_force_and_rank_agree_at_boundary():
    preds = [
        ("a", _p(0.5, 1.0)),
        ("b", _p(0.1, 2.0)),
        ("c", _p(0.9, 3.0)),
        ("d", _p(0.3, 1.5)),
    ]
    brute = order_predicates(preds, brute_force_threshold=4)
    rank = order_predicates(preds, brute_force_threshold=1)
    assert brute == rank


# ---------------------------------------------------------------------------
# Above threshold: rank beats greedy-by-cost
# ---------------------------------------------------------------------------


def test_rank_ordering_beats_greedy_by_cost_above_threshold():
    # Hand-crafted case where the cheapest predicate is not the right one
    # to run first.
    #   cheap_unselective: sel=0.99, cost=1  (rank ≈ -0.01)
    #   pricier_selective: sel=0.05, cost=2  (rank ≈ -0.475)
    #   plus 5 noise predicates so n > brute_force_threshold (default 6).
    preds = [
        ("cheap_unselective", _p(0.99, 1.0)),
        ("pricier_selective", _p(0.05, 2.0)),
        ("n1", _p(0.5, 5.0)),
        ("n2", _p(0.5, 5.0)),
        ("n3", _p(0.5, 5.0)),
        ("n4", _p(0.5, 5.0)),
        ("n5", _p(0.5, 5.0)),
    ]
    result = order_predicates(preds)
    # rank ordering must put pricier_selective before cheap_unselective
    assert result.index("pricier_selective") < result.index("cheap_unselective")


# ---------------------------------------------------------------------------
# Stability on equal ranks
# ---------------------------------------------------------------------------


def test_stable_for_equal_ranks_above_threshold():
    # All equal rank — input order should be preserved.
    preds = [(name, _p(0.5, 1.0)) for name in ("a", "b", "c", "d", "e", "f", "g")]
    assert order_predicates(preds) == list("abcdefg")


def test_stable_for_equal_ranks_brute_force():
    # All equal rank; brute force ties on cost → first arrangement wins,
    # which is the input order.
    preds = [(name, _p(0.5, 1.0)) for name in ("a", "b", "c")]
    assert order_predicates(preds) == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_rejects_selectivity_above_one():
    with pytest.raises(ValueError, match="selectivity"):
        order_predicates([("a", _p(1.5, 1.0))])


def test_rejects_negative_selectivity():
    with pytest.raises(ValueError, match="selectivity"):
        order_predicates([("a", _p(-0.1, 1.0))])


def test_rejects_zero_cost():
    with pytest.raises(ValueError, match="cost"):
        order_predicates([("a", _p(0.5, 0.0))])


def test_rejects_negative_cost():
    with pytest.raises(ValueError, match="cost"):
        order_predicates([("a", _p(0.5, -1.0))])


def test_rejects_zero_threshold():
    with pytest.raises(ValueError, match="brute_force_threshold"):
        order_predicates([("a", _p(0.5, 1.0))], brute_force_threshold=0)


def test_rejects_negative_threshold():
    with pytest.raises(ValueError, match="brute_force_threshold"):
        order_predicates([("a", _p(0.5, 1.0))], brute_force_threshold=-1)


if __name__ == "__main__":
    import traceback

    failures = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception:
                failures += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    if failures:
        sys.exit(1)
    print("ok")
