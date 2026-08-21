# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit-level coverage for the suffix LIKE ('%foo') selectivity estimators
(opteryx/planner/cost_estimation/selectivity.py): `_selectivity_ends_with`
(case-sensitive) and `_selectivity_ci_ends_with` (case-insensitive) -- both
char-class, single-anchor-position estimators. See
tests/unit/planner/cost_estimation/test_starts_with_selectivity.py for the
prefix-side counterpart; unlike STARTS_WITH, there is no ordinal-range tier
here at all -- a suffix has no relationship to a column's ordinal-key
min/max/histogram (see the module comment above _selectivity_ends_with in
selectivity.py), so char-class is the ONLY real tier for both case
variants.

predicate_rewriter.py rewrites "x LIKE '%foo'" / "x ILIKE '%foo'" into a
`_ENDS_WITH`/`_CI_ENDS_WITH` FUNCTION node before selectivity estimation ever
runs, so these estimators are reached via `estimate_selectivity`'s
NodeType.FUNCTION dispatch branch, not `_selectivity_comparison`.

Exercises the estimators' own math directly against hand-built
ColumnStatistics (no manifest/ANALYZE plumbing).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Importing opteryx.planner.optimizer (the package) resolves the optimizer <->
# cost_estimation.selectivity import cycle first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation.selectivity import (
    _LIKE_PREFIX_SELECTIVITY,
    _selectivity_ci_ends_with,
    _selectivity_ends_with,
    estimate_selectivity,
    predicate_estimator_tag,
)
from opteryx.planner.optimizer.statistics import ColumnStatistics, RelationStatistics
from opteryx.types.logical_type import NVARCHAR, VARCHAR

_IDENTITY = b"tes_col_00000001"

_UNIFORM_PROPORTIONS = {
    "upper": 0.05,
    "lower": 0.70,
    "digit": 0.10,
    "whitespace": 0.10,
    "punct_text": 0.03,
    "semantic": 0.02,
    "extended": 0.0,
    "control": 0.0,
}


def _column_node(identity=_IDENTITY, column_type=VARCHAR):
    identifier = Node(NodeType.IDENTIFIER, source_column="col")
    identifier.schema_column = Node(NodeType.IDENTIFIER, identity=identity, column_type=column_type)
    return identifier


def _func_node(suffix, op="_ENDS_WITH", identity=_IDENTITY, column_type=VARCHAR):
    literal = Node(NodeType.LITERAL, value=suffix)
    return Node(NodeType.FUNCTION, value=op, parameters=[_column_node(identity, column_type), literal])


def _stats_with_char_class(
    class_proportions=_UNIFORM_PROPORTIONS, avg_length=50.0, identity=_IDENTITY, length_bounds=None
):
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        class_proportions=class_proportions,
        avg_length=avg_length,
        length_bounds=length_bounds,
    )
    return RelationStatistics(row_count_estimate=1000, columns={identity: col})


# ── _selectivity_ends_with (case-sensitive, char-class) ─────────────────────


def test_basic_match_is_selective():
    stats = _stats_with_char_class()
    s = _selectivity_ends_with(_func_node(b"foo"), stats)
    assert 0.0 < s < 1.0


def test_upper_and_lower_needle_give_different_selectivity():
    # Case-SENSITIVE variant -- 'FOO' (all upper) and 'foo' (all lower) sit
    # against different class proportions (upper=0.05 vs lower=0.70 in the
    # fixture), so they must NOT collapse to the same estimate the way the
    # case-insensitive variant does.
    stats = _stats_with_char_class()
    s_lower = _selectivity_ends_with(_func_node(b"foo"), stats)
    s_upper = _selectivity_ends_with(_func_node(b"FOO"), stats)
    assert s_lower != pytest.approx(s_upper)


def test_longer_needle_is_never_more_selective_than_its_own_suffix():
    # Monotonicity: appending a character in FRONT of an already-anchored
    # suffix (extending the needle) must never raise the estimate, mirroring
    # _decayed_char_class_selectivity's own monotonicity guarantee and
    # _selectivity_ci_starts_with's prefix-side analogue.
    stats = _stats_with_char_class(avg_length=200.0)
    prev = 1.0
    needle = ""
    for c in "abcdefgh":
        needle = c + needle
        s = _selectivity_ends_with(_func_node(needle.encode()), stats)
        assert s <= prev + 1e-12, (needle, s, prev)
        prev = s


def test_length_discount_when_needle_longer_than_avg_length():
    # avg_length=3, needle="abcdefgh" (8 chars) -- soft discount
    # min(1, 3/8), not a hard floor to 0.
    stats = _stats_with_char_class(avg_length=3.0)
    s = _selectivity_ends_with(_func_node(b"abcdefgh"), stats)
    assert s > 0.0


def test_empty_needle_matches_everything():
    stats = _stats_with_char_class()
    s = _selectivity_ends_with(_func_node(b""), stats)
    assert s == 1.0


def test_falls_back_without_class_proportions():
    stats = _stats_with_char_class(class_proportions=None)
    s = _selectivity_ends_with(_func_node(b"foo"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_falls_back_when_avg_length_is_zero():
    stats = _stats_with_char_class(avg_length=0.0)
    s = _selectivity_ends_with(_func_node(b"foo"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_unknown_column_falls_back():
    stats = _stats_with_char_class(identity=_IDENTITY)
    node = _func_node(b"foo", identity=b"tes_other_0000")
    assert _selectivity_ends_with(node, stats) == _LIKE_PREFIX_SELECTIVITY


def test_result_always_in_unit_interval():
    import random

    rng = random.Random(11)
    stats = _stats_with_char_class()
    for _ in range(100):
        needle = "".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(0, 12)))
        s = _selectivity_ends_with(_func_node(needle.encode()), stats)
        assert 0.0 <= s <= 1.0


# ── _selectivity_ci_ends_with (case-insensitive, char-class) ────────────────


def test_ci_upper_and_lower_needle_give_same_selectivity():
    # Case-insensitivity must be blind to the needle's own casing -- 'FOO' and
    # 'foo' should estimate identically against the same column stats.
    stats = _stats_with_char_class()
    s_lower = _selectivity_ci_ends_with(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
    s_upper = _selectivity_ci_ends_with(_func_node(b"FOO", op="_CI_ENDS_WITH"), stats)
    assert s_lower == pytest.approx(s_upper)


def test_ci_longer_needle_is_never_more_selective_than_its_own_suffix():
    stats = _stats_with_char_class(avg_length=200.0)
    prev = 1.0
    needle = ""
    for c in "abcdefgh":
        needle = c + needle
        s = _selectivity_ci_ends_with(_func_node(needle.encode(), op="_CI_ENDS_WITH"), stats)
        assert s <= prev + 1e-12, (needle, s, prev)
        prev = s


def test_ci_length_discount_when_needle_longer_than_avg_length():
    stats = _stats_with_char_class(avg_length=3.0)
    s = _selectivity_ci_ends_with(_func_node(b"abcdefgh", op="_CI_ENDS_WITH"), stats)
    assert s > 0.0


def test_ci_empty_needle_matches_everything():
    stats = _stats_with_char_class()
    s = _selectivity_ci_ends_with(_func_node(b"", op="_CI_ENDS_WITH"), stats)
    assert s == 1.0


def test_ci_falls_back_without_class_proportions():
    stats = _stats_with_char_class(class_proportions=None)
    s = _selectivity_ci_ends_with(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_ci_falls_back_when_avg_length_is_zero():
    stats = _stats_with_char_class(avg_length=0.0)
    s = _selectivity_ci_ends_with(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_ci_result_always_in_unit_interval():
    import random

    rng = random.Random(17)
    stats = _stats_with_char_class()
    for _ in range(100):
        needle = "".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(0, 12)))
        s = _selectivity_ci_ends_with(_func_node(needle.encode(), op="_CI_ENDS_WITH"), stats)
        assert 0.0 <= s <= 1.0


# ── ends_with vs starts_with should agree on a symmetric needle/column ──────


def test_ci_ends_with_and_ci_starts_with_agree_on_uniform_stats():
    # Both estimators are the same anchored char-class product/discount
    # shape; against IDENTICAL uniform stats and needle, they must produce
    # the same number even though they model opposite ends of the string --
    # a real difference here would mean the two implementations drifted.
    from opteryx.planner.cost_estimation.selectivity import _selectivity_ci_starts_with

    stats = _stats_with_char_class()
    s_ends = _selectivity_ci_ends_with(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
    s_starts = _selectivity_ci_starts_with(_func_node(b"foo", op="_CI_STARTS_WITH"), stats)
    assert s_ends == pytest.approx(s_starts)


# ── hard length guard: possible-by-MIN/MAX, not just probable-by-AVG ────────
#
# The existing avg_length discount (test_length_discount_when_needle_longer_
# than_avg_length above) is soft: min(1.0, avg_length/needle_len) still
# returns a substantial nonzero for a needle moderately past avg_length,
# even when the column's real MAX length makes a match impossible (e.g.
# avg_length=20, needle_len=25, real max=22 -> discount=0.8, still large).
# The hard guard is a separate, certain, MAX-length-based short-circuit.


def test_ends_with_hard_zero_when_needle_exceeds_max_length():
    stats = _stats_with_char_class(avg_length=20.0, length_bounds=(3, 22))
    node = _func_node(b"x" * 25)  # 25 bytes > max_length 22
    assert _selectivity_ends_with(node, stats) == 0.0


def test_ci_ends_with_hard_zero_when_needle_exceeds_max_length():
    stats = _stats_with_char_class(avg_length=20.0, length_bounds=(3, 22))
    node = _func_node(b"x" * 25, op="_CI_ENDS_WITH")
    assert _selectivity_ci_ends_with(node, stats) == 0.0


def test_ends_with_not_hard_zeroed_within_max_length():
    stats = _stats_with_char_class(length_bounds=(1, 50))
    node = _func_node(b"foo")
    assert _selectivity_ends_with(node, stats) != 0.0


def test_ends_with_hard_guard_skipped_for_nvarchar():
    # Same byte-vs-char risk as STARTS_WITH/INSTR's guard -- NVARCHAR length
    # stats from the external catalog producer are character-based.
    stats = _stats_with_char_class(avg_length=20.0, length_bounds=(1, 3))
    node = _func_node(b"x" * 25, column_type=NVARCHAR)
    assert _selectivity_ends_with(node, stats) != 0.0


def test_not_ends_with_hard_zero_complements_to_one():
    stats = _stats_with_char_class(avg_length=20.0, length_bounds=(3, 22))
    inner = _func_node(b"x" * 25)
    not_node = Node(NodeType.NOT, centre=inner)
    assert estimate_selectivity(inner, stats) == 0.0
    assert estimate_selectivity(not_node, stats) == 1.0


# ── dispatcher wiring ─────────────────────────────────────────────────────


def test_estimate_selectivity_dispatches_ends_with():
    stats = _stats_with_char_class(avg_length=3.0)
    node = _func_node(b"abcdefgh")  # needle >> avg_length
    assert estimate_selectivity(node, stats) < 1.0


def test_estimate_selectivity_dispatches_ci_ends_with():
    stats = _stats_with_char_class(avg_length=3.0)
    node = _func_node(b"abcdefgh", op="_CI_ENDS_WITH")
    assert estimate_selectivity(node, stats) < 1.0


def test_not_ends_with_is_the_complement():
    stats = _stats_with_char_class()
    inner = _func_node(b"foo")
    not_node = Node(NodeType.NOT, centre=inner)
    s = estimate_selectivity(inner, stats)
    not_s = estimate_selectivity(not_node, stats)
    assert s == pytest.approx(1.0 - not_s)


def test_estimate_selectivity_real_like_predicate_is_not_one():
    # End-to-end: a `LIKE '%foo'` predicate rewritten by predicate_rewriter
    # into a `_ENDS_WITH` FUNCTION node must not silently fall through to the
    # non-filtering 1.0 default when char-class stats are present -- the gap
    # this work closes.
    stats = _stats_with_char_class(avg_length=3.0)
    node = _func_node(b"abcdefgh")
    assert estimate_selectivity(node, stats) != 1.0


# ── predicate_estimator_tag ──────────────────────────────────────────────


def test_predicate_estimator_tag_char_class_suffix_when_stats_present():
    stats = _stats_with_char_class()
    assert predicate_estimator_tag(_func_node(b"foo"), stats) == "char_class_suffix"
    assert (
        predicate_estimator_tag(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
        == "char_class_suffix"
    )


def test_predicate_estimator_tag_flat_fallback_without_char_class_stats():
    stats = _stats_with_char_class(class_proportions=None)
    assert predicate_estimator_tag(_func_node(b"foo"), stats) == "flat_fallback"
    assert (
        predicate_estimator_tag(_func_node(b"foo", op="_CI_ENDS_WITH"), stats)
        == "flat_fallback"
    )


def test_predicate_estimator_tag_none_for_unrelated_function():
    stats = _stats_with_char_class()
    node = Node(NodeType.FUNCTION, value="SOMETHING_ELSE", parameters=[])
    assert predicate_estimator_tag(node, stats) is None


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
