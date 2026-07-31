# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit-level coverage for the infix LIKE '%needle%' char-class selectivity
estimator (opteryx/planner/cost_estimation/selectivity.py), ported verbatim
from scratch/like_selectivity/estimators.py's decayed_char_class_selectivity.

Exercises the estimator's own math directly (no manifest/ANALYZE plumbing —
see tests/storage/test_analyze_statistics.py and tests/compiled/ for the
native-kernel/end-to-end coverage) plus the _selectivity_instr /
predicate_estimator_tag tier-selection logic against hand-built
ColumnStatistics.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) resolves the optimizer <->
# cost_estimation.selectivity import cycle first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.cost_estimation.selectivity import (
    _CHAR_CLASSES,
    _CLASS_CARDINALITY,
    _classify_char,
    _decayed_char_class_selectivity,
    _LIKE_INFIX_SELECTIVITY,
    _like_needle_str,
    _selectivity_instr,
    estimate_selectivity,
    predicate_estimator_tag,
)
from opteryx.planner.optimizer.statistics import ColumnStatistics, RelationStatistics
from opteryx.types.logical_type import NVARCHAR, VARCHAR

_IDENTITY = b"tes_col_00000001"

# A uniform-ish column: every class present with a plausible proportion,
# roughly matching the offline experiment's typical VARCHAR shape.
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


def _stats(
    class_proportions=_UNIFORM_PROPORTIONS, avg_length=50.0, distinct_count=None, length_bounds=None
):
    col = ColumnStatistics(
        column_name="col",
        data_type="VARCHAR",
        class_proportions=class_proportions,
        avg_length=avg_length,
        distinct_count=distinct_count,
        length_bounds=length_bounds,
    )
    return RelationStatistics(row_count=1000, columns={_IDENTITY: col})


def _instr_node(needle, decay=0.7, op="InStr", column_type=VARCHAR):
    identifier = Node(NodeType.IDENTIFIER, source_column="col")
    identifier.schema_column = Node(NodeType.IDENTIFIER, identity=_IDENTITY, column_type=column_type)
    literal = Node(NodeType.LITERAL, value=needle)
    node = Node(NodeType.COMPARISON_OPERATOR, value=op, left=identifier, right=literal)
    node.like_selectivity_decay = decay
    return node


# ── _decayed_char_class_selectivity direct math ─────────────────────────────


def test_empty_needle_matches_everything():
    assert _decayed_char_class_selectivity("", _UNIFORM_PROPORTIONS, 50.0, 0.7) == 1.0


def test_longer_needle_is_never_more_selective_than_its_own_prefix():
    # Monotonicity is a required property of this model (see estimators.py's
    # own docstring on why an earlier hand-derived variant broke it).
    prev = 1.0
    needle = ""
    for c in "abcdefgh":
        needle += c
        s = _decayed_char_class_selectivity(needle, _UNIFORM_PROPORTIONS, 200.0, 0.7)
        assert s <= prev + 1e-12, (needle, s, prev)
        prev = s


def test_zero_avg_length_or_negative_n_positions_yields_zero():
    assert _decayed_char_class_selectivity("hello", _UNIFORM_PROPORTIONS, 0.0, 0.7) == 0.0
    # needle longer than avg_length -> n_positions clamps to 0.
    assert _decayed_char_class_selectivity("verylongneedle", _UNIFORM_PROPORTIONS, 3.0, 0.7) == 0.0


def test_missing_class_proportion_uses_the_log_probability_floor_not_a_hard_zero():
    # A class absent from the stored proportions gets the floor probability
    # (_LOG_PCHAR_FLOOR = log(1e-6)), not p_char=0 -- a hard zero would let one
    # unseen-class character zero out the WHOLE needle's probability
    # regardless of its other characters, a much harsher cliff than the
    # design intends. Compare against a present class to confirm the
    # absent-class estimate is still much smaller.
    sparse = {"lower": 1.0}
    absent = _decayed_char_class_selectivity("A", sparse, 50.0, 0.7)  # 'A' is 'upper', absent
    present = _decayed_char_class_selectivity("a", sparse, 50.0, 0.7)  # 'a' is 'lower', present
    assert 0.0 < absent < present


def test_decay_one_is_the_undamped_product_model():
    # decay**i == 1 for every i when decay == 1.0 -- every position gets full
    # weight, matching the undamped char_class_selectivity model exactly.
    import math

    needle = "abc"
    s = _decayed_char_class_selectivity(needle, _UNIFORM_PROPORTIONS, 200.0, 1.0)
    p_pos = 1.0
    for c in needle:
        cls = _classify_char(c)
        p_pos *= _UNIFORM_PROPORTIONS[cls] / _CLASS_CARDINALITY[cls]
    expected = 1.0 - math.exp(-max(200.0 - len(needle) + 1, 0.0) * p_pos)
    assert s == pytest_approx(expected)


def pytest_approx(x, rel=1e-9):
    import pytest

    return pytest.approx(x, rel=rel)


def test_result_always_in_unit_interval():
    import random

    rng = random.Random(99)
    for _ in range(200):
        needle = "".join(chr(rng.randint(32, 126)) for _ in range(rng.randint(0, 12)))
        avg_len = rng.uniform(0, 500)
        decay = rng.uniform(0.01, 1.0)
        s = _decayed_char_class_selectivity(needle, _UNIFORM_PROPORTIONS, avg_len, decay)
        assert 0.0 <= s <= 1.0


# ── _classify_char / _CHAR_CLASSES / _CLASS_CARDINALITY sanity ──────────────


def test_classify_char_known_examples():
    assert _classify_char("A") == "upper"
    assert _classify_char("z") == "lower"
    assert _classify_char("5") == "digit"
    assert _classify_char(" ") == "whitespace"


def test_class_cardinality_keys_match_char_classes():
    assert set(_CLASS_CARDINALITY.keys()) == set(_CHAR_CLASSES)
    assert all(v > 0 for v in _CLASS_CARDINALITY.values())


# ── _like_needle_str coercion ────────────────────────────────────────────────


def test_like_needle_str_decodes_bytes():
    assert _like_needle_str(b"hello") == "hello"
    assert _like_needle_str("hello") == "hello"
    assert _like_needle_str(123) is None
    assert _like_needle_str(None) is None


# ── _selectivity_instr / predicate_estimator_tag tier selection ─────────────


def test_selectivity_instr_uses_char_class_when_stats_and_decay_present():
    stats = _stats()
    node = _instr_node("hello", decay=0.7)
    s = _selectivity_instr(_IDENTITY, "hello", node, stats)
    assert s != _LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "char_class_decay"


def test_selectivity_instr_falls_back_without_decay():
    stats = _stats()
    node = _instr_node("hello", decay=None)
    s = _selectivity_instr(_IDENTITY, "hello", node, stats)
    assert s == _LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_selectivity_instr_falls_back_without_class_proportions():
    stats = _stats(class_proportions=None)
    node = _instr_node("hello", decay=0.7)
    s = _selectivity_instr(_IDENTITY, "hello", node, stats)
    assert s == _LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_selectivity_instr_falls_back_when_avg_length_is_zero():
    stats = _stats(avg_length=0.0)
    node = _instr_node("hello", decay=0.7)
    s = _selectivity_instr(_IDENTITY, "hello", node, stats)
    assert s == _LIKE_INFIX_SELECTIVITY


def test_selectivity_instr_falls_back_for_unknown_column():
    stats = _stats()
    unknown_identifier = Node(NodeType.IDENTIFIER, source_column="other")
    unknown_identifier.schema_column = Node(NodeType.IDENTIFIER, identity=b"tes_other_0000")
    literal = Node(NodeType.LITERAL, value="hello")
    node = Node(NodeType.COMPARISON_OPERATOR, value="InStr", left=unknown_identifier, right=literal)
    node.like_selectivity_decay = 0.7
    s = _selectivity_instr(b"tes_other_0000", "hello", node, stats)
    assert s == _LIKE_INFIX_SELECTIVITY


def test_predicate_estimator_tag_none_for_non_instr_predicate():
    stats = _stats()
    identifier = Node(NodeType.IDENTIFIER, source_column="col")
    identifier.schema_column = Node(NodeType.IDENTIFIER, identity=_IDENTITY)
    literal = Node(NodeType.LITERAL, value="hello")
    node = Node(NodeType.COMPARISON_OPERATOR, value="Eq", left=identifier, right=literal)
    assert predicate_estimator_tag(node, stats) is None


def test_not_instr_is_the_complement_of_instr():
    stats = _stats()
    node = _instr_node("hello", decay=0.7, op="InStr")
    not_node = _instr_node("hello", decay=0.7, op="NotInStr")
    s = estimate_selectivity(node, stats)
    not_s = estimate_selectivity(not_node, stats)
    assert s == pytest_approx(1.0 - not_s)


# ── hard length guard: needle longer than the column's real max ────────────
#
# _containment_selectivity's n_positions already tends toward 0 as needle_len
# approaches avg_length, but that's a SOFT, probabilistic mechanism keyed on
# the AVERAGE -- it can (a) coincidentally still be nonzero for a needle just
# past avg_length but under max_length (which is correct, still possible),
# and it conflates "improbable relative to average" with "impossible". The
# new hard guard is a separate, certain, MAX-length-based short-circuit that
# fires before any of that probabilistic math, independent of avg_length.


def test_selectivity_instr_hard_zero_when_needle_exceeds_max_length():
    stats = _stats(length_bounds=(3, 10))
    node = _instr_node("this needle is far longer than ten bytes", decay=0.7)
    s = _selectivity_instr(_IDENTITY, "this needle is far longer than ten bytes", node, stats)
    assert s == 0.0


def test_selectivity_instr_not_hard_zeroed_within_max_length():
    stats = _stats(length_bounds=(3, 50))
    node = _instr_node("hello", decay=0.7)
    s = _selectivity_instr(_IDENTITY, "hello", node, stats)
    assert s != 0.0


def test_selectivity_instr_hard_guard_skipped_for_nvarchar():
    # Same byte-vs-char risk as the STARTS_WITH guard -- NVARCHAR length
    # stats from the external catalog producer are character-based, so the
    # guard must not fire even when needle_len appears to exceed max_length.
    stats = _stats(length_bounds=(1, 3))
    node = _instr_node("this needle is far longer than three bytes", decay=0.7, column_type=NVARCHAR)
    s = _selectivity_instr(
        _IDENTITY, "this needle is far longer than three bytes", node, stats
    )
    assert s != 0.0  # falls through to the normal char-class/decay math instead


def test_not_instr_hard_zero_complements_to_one():
    stats = _stats(length_bounds=(3, 10))
    needle = "this needle is far longer than ten bytes"
    node = _instr_node(needle, decay=0.7, op="InStr")
    not_node = _instr_node(needle, decay=0.7, op="NotInStr")
    assert estimate_selectivity(node, stats) == 0.0
    assert estimate_selectivity(not_node, stats) == 1.0


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
