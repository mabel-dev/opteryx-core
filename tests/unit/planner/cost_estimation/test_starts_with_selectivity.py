# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit-level coverage for the prefix LIKE ('foo%') selectivity estimators
(opteryx/planner/cost_estimation/selectivity.py): `_selectivity_starts_with`
(case-sensitive, ordinal-range) and `_selectivity_ci_starts_with`
(case-insensitive, char-class).

predicate_rewriter.py rewrites "x LIKE 'foo%'" / "x ILIKE 'foo%'" into a
`_STARTS_WITH`/`_CI_STARTS_WITH` FUNCTION node before selectivity estimation
ever runs, so these estimators are reached via `estimate_selectivity`'s
NodeType.FUNCTION dispatch branch, not `_selectivity_comparison`.

Exercises the estimators' own math directly against hand-built
ColumnStatistics/Distogram (no manifest/ANALYZE plumbing — see
tests/unit/models/test_manifest_selectivity.py for the Manifest-level,
ordinalized-bounds-through-get_distogram coverage).
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
    _ci_char_probability,
    _exceeds_max_length,
    _selectivity_ci_starts_with,
    _selectivity_starts_with,
    estimate_selectivity,
    predicate_estimator_tag,
)
from opteryx.planner.optimizer.statistics import ColumnStatistics, RelationStatistics
from opteryx.third_party.maki_nage.distogram import load_counts_i64
from opteryx.types.logical_type import NVARCHAR, VARCHAR, DrakenType

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


def _func_node(prefix, op="_STARTS_WITH", identity=_IDENTITY, column_type=VARCHAR):
    literal = Node(NodeType.LITERAL, value=prefix)
    return Node(NodeType.FUNCTION, value=op, parameters=[_column_node(identity, column_type), literal])


def _distogram_over_values(*values, bin_count=64):
    """A real Distogram spanning exactly [min(values), max(values)], one bin
    per point (enough resolution for these small fixtures)."""
    lo, hi = min(values), max(values)
    counts = [0] * bin_count
    counts[0] = len(values)  # all mass at the low end is fine -- range/point
    # tests below only care about density INSIDE vs OUTSIDE [lo, hi].
    import array

    return load_counts_i64(array.array("q", counts), float(lo), float(hi))


def _stats_with_histogram(dgram, identity=_IDENTITY):
    col = ColumnStatistics(column_name="col", data_type="VARCHAR", histogram=dgram)
    return RelationStatistics(row_count=1000, columns={identity: col})


def _stats_with_char_class(
    class_proportions=_UNIFORM_PROPORTIONS, avg_length=50.0, identity=_IDENTITY
):
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        class_proportions=class_proportions,
        avg_length=avg_length,
    )
    return RelationStatistics(row_count=1000, columns={identity: col})


# ── _selectivity_starts_with (case-sensitive, ordinal range) ────────────────


def test_prefix_inside_range_is_selective():
    # Data spans "alpha".."omega"; "al" is a narrow slice near the low end.
    lo = VARCHAR.ordinalize("alpha")
    hi = VARCHAR.ordinalize("omega")
    dgram = _distogram_over_values(lo, hi)
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"al")
    s = _selectivity_starts_with(node, stats)
    assert 0.0 <= s < 1.0


def test_prefix_entirely_above_range_is_near_zero():
    # Data spans "a".."m"; a "zzz" prefix's ordinal range sits entirely above
    # the column's max, so density between [lo_key, hi_key] should be ~0.
    lo = VARCHAR.ordinalize("a")
    hi = VARCHAR.ordinalize("m")
    dgram = _distogram_over_values(lo, hi)
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"zzz")
    s = _selectivity_starts_with(node, stats)
    assert s == pytest.approx(0.0, abs=1e-9)


def test_eight_byte_prefix_uses_point_density_path():
    # A prefix >= 8 bytes collides to a single ordinal key (ordinalize's
    # documented 8-byte limit) -- must not divide-by-zero-width and must
    # still return a value in [0, 1], not silently fall back to 1.0.
    lo = VARCHAR.ordinalize("aaaaaaaaaa")
    hi = VARCHAR.ordinalize("zzzzzzzzzz")
    dgram = _distogram_over_values(lo, hi, bin_count=64)
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"aaaaaaaaaaaaaaa")  # 15 bytes, collides beyond byte 8
    s = _selectivity_starts_with(node, stats)
    assert 0.0 <= s <= 1.0


def test_no_histogram_falls_back_to_prefix_constant():
    col = ColumnStatistics(column_name="col", data_type="VARCHAR")  # histogram=None
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})
    node = _func_node(b"foo")
    assert _selectivity_starts_with(node, stats) == _LIKE_PREFIX_SELECTIVITY


def test_unknown_column_falls_back_to_prefix_constant():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("z"))
    stats = _stats_with_histogram(dgram, identity=_IDENTITY)
    node = _func_node(b"foo", identity=b"tes_other_0000")
    assert _selectivity_starts_with(node, stats) == _LIKE_PREFIX_SELECTIVITY


def test_no_resolvable_physical_type_falls_back():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("z"))
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"foo", column_type=None)
    assert _selectivity_starts_with(node, stats) == _LIKE_PREFIX_SELECTIVITY


def test_result_always_in_unit_interval():
    import random

    rng = random.Random(7)
    lo = VARCHAR.ordinalize("alpha")
    hi = VARCHAR.ordinalize("omega")
    dgram = _distogram_over_values(lo, hi)
    stats = _stats_with_histogram(dgram)
    for _ in range(100):
        needle = "".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(1, 12)))
        node = _func_node(needle.encode())
        s = _selectivity_starts_with(node, stats)
        assert 0.0 <= s <= 1.0


# ── _selectivity_starts_with: ordinal_bounds tier (no histogram) ────────────
#
# Backs the case where per-file min/max exist (ordinary write-time bounds)
# but no ANALYZE-produced histogram does -- exactly the shape a catalog-backed
# table has before an explicit ANALYZE. Regression-tests two real bugs found
# against live production data (see git history / session notes): (1) a
# field_id-vs-position mixup in Manifest.get_ordinal_bounds (covered in
# tests/unit/models/test_manifest_ordinal_bounds.py, not here), and (2) large
# ordinal keys silently losing precision through float(int) and comparing as
# spuriously "less than" their own exact-integer bound.


def _stats_with_ordinal_bounds(bounds, identity=_IDENTITY, distinct_count=None):
    col = ColumnStatistics(
        column_name="col", data_type="VARCHAR", ordinal_bounds=bounds, distinct_count=distinct_count
    )
    return RelationStatistics(row_count=1000, columns={identity: col})


def test_ordinal_bounds_disjoint_range_is_zero():
    bounds = (VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m"))
    stats = _stats_with_ordinal_bounds(bounds)
    node = _func_node(b"zzz")
    assert _selectivity_starts_with(node, stats) == 0.0


def test_ordinal_bounds_overlapping_range_uniform_interpolation():
    bounds = (VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))
    stats = _stats_with_ordinal_bounds(bounds)
    node = _func_node(b"al")
    s = _selectivity_starts_with(node, stats)
    assert 0.0 < s < 1.0


def test_ordinal_bounds_range_covering_the_whole_span_is_one():
    bounds = (VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))
    stats = _stats_with_ordinal_bounds(bounds)
    node = _func_node(b"")  # empty prefix -- matches everything
    assert _selectivity_starts_with(node, stats) == pytest.approx(1.0)


def test_ordinal_bounds_degenerate_span_matching_prefix_is_one():
    # Real production shape: a column with exactly one distinct value across
    # every live file (bound_lo == bound_hi).
    only_value = "projects/mabeldev/logs/run.googleapis.com%2Fstderr"
    point = VARCHAR.ordinalize(only_value)
    stats = _stats_with_ordinal_bounds((point, point))
    for prefix in (b"p", b"proj", b"projects", b"projects/mabeldev"):
        node = _func_node(prefix)
        assert _selectivity_starts_with(node, stats) == 1.0, prefix


def test_ordinal_bounds_degenerate_span_non_matching_prefix_is_zero():
    only_value = "projects/mabeldev/logs/run.googleapis.com%2Fstderr"
    point = VARCHAR.ordinalize(only_value)
    stats = _stats_with_ordinal_bounds((point, point))
    for prefix in (b"x", b"z", b"a", b"stderr"):
        node = _func_node(prefix)
        assert _selectivity_starts_with(node, stats) == 0.0, prefix


def test_ordinal_bounds_large_key_exact_match_does_not_lose_float_precision():
    # Regression: float(big_int) can round DOWN past a >52-bit integer (e.g.
    # float(4051330591175588409) == 4051330591175588352.0, a DIFFERENT
    # integer), which previously made an exact prefix match compare as
    # spuriously "less than" its own bound and wrongly return 0.0. Assert the
    # bound itself is actually large enough to exercise float64's 53-bit
    # mantissa limit, then confirm the match still succeeds.
    only_value = "projects/mabeldev/logs/run.googleapis.com%2Fstderr"
    point = VARCHAR.ordinalize(only_value)
    assert point > (1 << 52), "fixture bound must exceed float64 exact-integer range"
    assert float(point) != point, "fixture bound must actually be float-lossy"
    stats = _stats_with_ordinal_bounds((point, point))
    node = _func_node(only_value[:8].encode())  # exactly the 8-byte prefix
    assert _selectivity_starts_with(node, stats) == 1.0


def test_ordinal_bounds_point_case_uses_ndv_when_known():
    bounds = (VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))
    stats = _stats_with_ordinal_bounds(bounds, distinct_count=25)
    node = _func_node(b"alphabetic")  # >= 8 bytes -> point case, inside bounds
    s = _selectivity_starts_with(node, stats)
    assert s == pytest.approx(1.0 / 25)


def test_ordinal_bounds_point_case_falls_back_without_ndv():
    bounds = (VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))
    stats = _stats_with_ordinal_bounds(bounds)  # distinct_count=None
    node = _func_node(b"alphabetic")
    assert _selectivity_starts_with(node, stats) == _LIKE_PREFIX_SELECTIVITY


def test_histogram_takes_precedence_over_ordinal_bounds():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m"))
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        histogram=dgram,
        # Deliberately wrong/wide bounds -- if this fired instead of the
        # histogram, "zzz" would NOT read as disjoint.
        ordinal_bounds=(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("zzz")),
    )
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})
    node = _func_node(b"zzz")
    assert _selectivity_starts_with(node, stats) == pytest.approx(0.0, abs=1e-9)
    assert predicate_estimator_tag(node, stats) == "ordinal_range"


def test_predicate_estimator_tag_ordinal_bounds_without_histogram():
    stats = _stats_with_ordinal_bounds((VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m")))
    node = _func_node(b"foo")
    assert predicate_estimator_tag(node, stats) == "ordinal_bounds"


# ── _selectivity_ci_starts_with (case-insensitive, char-class) ──────────────


def test_ci_upper_and_lower_needle_give_same_selectivity():
    # Case-insensitivity must be blind to the needle's own casing -- 'FOO' and
    # 'foo' should estimate identically against the same column stats.
    stats = _stats_with_char_class()
    s_lower = _selectivity_ci_starts_with(_func_node(b"foo", op="_CI_STARTS_WITH"), stats)
    s_upper = _selectivity_ci_starts_with(_func_node(b"FOO", op="_CI_STARTS_WITH"), stats)
    assert s_lower == pytest.approx(s_upper)


def test_ci_char_probability_sums_upper_and_lower_over_single_case_cardinality():
    # P(byte matches either case) = prop_upper/26 + prop_lower/26, NOT
    # (prop_upper + prop_lower) / 52 -- the latter silently halves it.
    proportions = {"upper": 0.10, "lower": 0.20}
    p = _ci_char_probability("f", proportions)
    assert p == pytest.approx((0.10 + 0.20) / 26)


def test_ci_non_alpha_char_is_unaffected_by_merge():
    proportions = {**_UNIFORM_PROPORTIONS, "digit": 0.10}
    p = _ci_char_probability("5", proportions)
    assert p == pytest.approx(0.10 / 10)  # digit cardinality == 10


def test_ci_longer_needle_is_never_more_selective_than_its_prefix():
    stats = _stats_with_char_class(avg_length=200.0)
    prev = 1.0
    needle = ""
    for c in "abcdefgh":
        needle += c
        s = _selectivity_ci_starts_with(_func_node(needle.encode(), op="_CI_STARTS_WITH"), stats)
        assert s <= prev + 1e-12, (needle, s, prev)
        prev = s


def test_ci_length_discount_when_needle_longer_than_avg_length():
    # avg_length=3, needle="abcdefgh" (8 chars) -- soft discount min(1, 3/8),
    # not a hard floor to 0 the way the infix estimator's n_positions clamp
    # would give.
    stats = _stats_with_char_class(avg_length=3.0)
    s = _selectivity_ci_starts_with(_func_node(b"abcdefgh", op="_CI_STARTS_WITH"), stats)
    assert s > 0.0


def test_ci_empty_needle_matches_everything():
    stats = _stats_with_char_class()
    s = _selectivity_ci_starts_with(_func_node(b"", op="_CI_STARTS_WITH"), stats)
    assert s == 1.0


def test_ci_falls_back_without_class_proportions():
    stats = _stats_with_char_class(class_proportions=None)
    s = _selectivity_ci_starts_with(_func_node(b"foo", op="_CI_STARTS_WITH"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_ci_falls_back_when_avg_length_is_zero():
    stats = _stats_with_char_class(avg_length=0.0)
    s = _selectivity_ci_starts_with(_func_node(b"foo", op="_CI_STARTS_WITH"), stats)
    assert s == _LIKE_PREFIX_SELECTIVITY


def test_ci_result_always_in_unit_interval():
    import random

    rng = random.Random(13)
    stats = _stats_with_char_class()
    for _ in range(100):
        needle = "".join(chr(rng.randint(97, 122)) for _ in range(rng.randint(0, 12)))
        s = _selectivity_ci_starts_with(_func_node(needle.encode(), op="_CI_STARTS_WITH"), stats)
        assert 0.0 <= s <= 1.0


# ── dispatcher wiring ─────────────────────────────────────────────────────


def test_estimate_selectivity_dispatches_starts_with():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m"))
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"zzz")  # entirely above range -> ~0, i.e. != 1.0
    assert estimate_selectivity(node, stats) != 1.0


def test_estimate_selectivity_dispatches_ci_starts_with():
    stats = _stats_with_char_class(avg_length=3.0)
    node = _func_node(b"abcdefgh", op="_CI_STARTS_WITH")  # needle >> avg_length
    assert estimate_selectivity(node, stats) < 1.0


def test_not_starts_with_is_the_complement():
    dgram = _distogram_over_values(VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))
    stats = _stats_with_histogram(dgram)
    inner = _func_node(b"al")
    not_node = Node(NodeType.NOT, centre=inner)
    s = estimate_selectivity(inner, stats)
    not_s = estimate_selectivity(not_node, stats)
    assert s == pytest.approx(1.0 - not_s)


def test_unrecognized_function_still_falls_through_to_one():
    stats = _stats_with_histogram(_distogram_over_values(1, 2))
    node = Node(NodeType.FUNCTION, value="SOMETHING_ELSE", parameters=[])
    assert estimate_selectivity(node, stats) == 1.0


# ── predicate_estimator_tag ──────────────────────────────────────────────


def test_predicate_estimator_tag_ordinal_range_when_stats_present():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m"))
    stats = _stats_with_histogram(dgram)
    node = _func_node(b"foo")
    assert predicate_estimator_tag(node, stats) == "ordinal_range"


def test_predicate_estimator_tag_flat_fallback_without_histogram():
    col = ColumnStatistics(column_name="col", data_type="VARCHAR")
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})
    node = _func_node(b"foo")
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_predicate_estimator_tag_char_class_prefix_when_stats_present():
    stats = _stats_with_char_class()
    node = _func_node(b"foo", op="_CI_STARTS_WITH")
    assert predicate_estimator_tag(node, stats) == "char_class_prefix"


def test_predicate_estimator_tag_flat_fallback_without_char_class_stats():
    stats = _stats_with_char_class(class_proportions=None)
    node = _func_node(b"foo", op="_CI_STARTS_WITH")
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_predicate_estimator_tag_none_for_unrelated_function():
    stats = _stats_with_char_class()
    node = Node(NodeType.FUNCTION, value="SOMETHING_ELSE", parameters=[])
    assert predicate_estimator_tag(node, stats) is None


# ── hard length guard: possible-by-MIN/MAX, not just probable-by-AVG ────────
#
# Regression for the reported bug: STARTS_WITH(col, <66-byte literal>) against
# a column whose only real value is 11 bytes returned selectivity 1.0 -- the
# ordinal-key point-density tier only ever compares the first ~8 bytes, so a
# prefix sharing that 8-byte bucket "matched" regardless of whether the real
# value was even long enough to contain the rest of the prefix. The hard
# guard (_exceeds_max_length, via col.length_bounds) short-circuits to 0.0
# before any of that byte-level math runs whenever it's certain no row can
# match, independent of ordinal-key coincidence.


def test_reported_bug_long_prefix_against_short_only_value_is_zero():
    real_value = "lorem ipsom"
    point = VARCHAR.ordinalize(real_value)
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        ordinal_bounds=(point, point),
        distinct_count=1,
        length_bounds=(len(real_value), len(real_value)),
    )
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})

    # Shares the first 8 bytes with "lorem ipsom" ("lorem ip") but is far
    # longer than any value in the column -- provably impossible.
    long_prefix = "lorem ipshjkjhbgjklkjhb,nklmkj,hvmgbj,nklmjb,hn.kjb,hnkh,vjbkhjvhj"
    node = _func_node(long_prefix.encode())

    assert _selectivity_starts_with(node, stats) == 0.0


def test_hard_guard_does_not_reject_a_genuinely_shorter_prefix():
    # Same column stats as above -- a prefix within the real value's length
    # must NOT be caught by the guard, only the impossible one.
    real_value = "lorem ipsom"
    point = VARCHAR.ordinalize(real_value)
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        ordinal_bounds=(point, point),
        distinct_count=1,
        length_bounds=(len(real_value), len(real_value)),
    )
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})

    node = _func_node(b"lorem")
    assert _selectivity_starts_with(node, stats) > 0.0


def test_exceeds_max_length_true_when_needle_longer_than_max():
    # _physical_type() resolves the raw DrakenType (ColumnType.physical), not
    # the ColumnType wrapper -- pass what production code actually passes.
    col = ColumnStatistics(column_name="col", data_type="VARCHAR", length_bounds=(3, 10))
    assert _exceeds_max_length(11, col, DrakenType.VARCHAR) is True
    assert _exceeds_max_length(10, col, DrakenType.VARCHAR) is False
    assert _exceeds_max_length(1, col, DrakenType.VARCHAR) is False


def test_exceeds_max_length_false_without_length_bounds():
    col = ColumnStatistics(column_name="col", data_type="VARCHAR")
    assert _exceeds_max_length(999, col, DrakenType.VARCHAR) is False


def test_exceeds_max_length_skipped_for_nvarchar():
    # NVARCHAR's length stats are character-based from the external catalog
    # producer, not byte-based -- comparing a byte-length needle against them
    # risks a false "impossible" verdict for non-ASCII content, so the guard
    # is skipped entirely for this type regardless of what length_bounds says.
    col = ColumnStatistics(column_name="col", data_type="NVARCHAR", length_bounds=(3, 5))
    assert _exceeds_max_length(999, col, DrakenType.NVARCHAR) is False


def test_nvarchar_starts_with_not_hard_zeroed_by_length_guard():
    dgram = _distogram_over_values(VARCHAR.ordinalize("a"), VARCHAR.ordinalize("m"))
    col = ColumnStatistics(
        column_name="col",
        data_type="NVARCHAR",
        histogram=dgram,
        length_bounds=(1, 3),  # would otherwise hard-zero a long needle
    )
    stats = RelationStatistics(row_count=1000, columns={_IDENTITY: col})
    node = _func_node(b"averylongneedlefarbeyondthelengthbounds", column_type=NVARCHAR)
    # Not hard-zeroed by the guard -- falls through to whatever the
    # histogram/bounds/constant tiers actually compute (still likely small,
    # but not a certain 0.0 the way the byte-safe VARCHAR/VARBINARY path is).
    s = _selectivity_starts_with(node, stats)
    assert 0.0 <= s <= 1.0


# ── point-density-only AVG discount ──────────────────────────────────────
#
# The <8-byte RANGE case is an exact computation against the real observed
# distribution and must NOT get the extra avg_length discount (it would
# double-count uncertainty already reflected in the real ordinal_bounds/
# histogram data). Only the >=8-byte POINT case, where ordinalize's
# precision genuinely runs out, gets it.


def test_avg_discount_applies_to_point_case_not_range_case():
    bounds = (VARCHAR.ordinalize("alpha"), VARCHAR.ordinalize("omega"))

    # Range case (< 8 bytes): identical result regardless of avg_length.
    col_no_avg = ColumnStatistics(column_name="col", data_type="VARCHAR", ordinal_bounds=bounds)
    col_short_avg = ColumnStatistics(
        column_name="col", data_type="VARCHAR", ordinal_bounds=bounds, avg_length=1.0
    )
    stats_no_avg = RelationStatistics(row_count=1000, columns={_IDENTITY: col_no_avg})
    stats_short_avg = RelationStatistics(row_count=1000, columns={_IDENTITY: col_short_avg})
    node_range = _func_node(b"al")  # 2 bytes, range case
    assert _selectivity_starts_with(node_range, stats_no_avg) == pytest.approx(
        _selectivity_starts_with(node_range, stats_short_avg)
    )

    # Point case (>= 8 bytes): a short avg_length must discount the result
    # relative to no avg_length signal at all.
    node_point = _func_node(b"alphabet")  # 8 bytes, point case, NDV known
    col_no_avg_ndv = ColumnStatistics(
        column_name="col", data_type="VARCHAR", ordinal_bounds=bounds, distinct_count=4
    )
    col_short_avg_ndv = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        ordinal_bounds=bounds,
        distinct_count=4,
        avg_length=1.0,
    )
    s_no_avg = _selectivity_starts_with(
        node_point, RelationStatistics(row_count=1000, columns={_IDENTITY: col_no_avg_ndv})
    )
    s_short_avg = _selectivity_starts_with(
        node_point, RelationStatistics(row_count=1000, columns={_IDENTITY: col_short_avg_ndv})
    )
    assert s_short_avg < s_no_avg


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
