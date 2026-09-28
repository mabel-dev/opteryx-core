# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit-level coverage for the infix LIKE '%needle%' char-class selectivity
estimator (src/cpp/planner/selectivity.hpp, reached through
opteryx.compiled.planner.statistics), ported from
scratch/like_selectivity/estimators.py's decayed_char_class_selectivity.

Exercises the estimator's own math through `estimate_selectivity` on an InStr
predicate (no manifest/ANALYZE plumbing — see
tests/storage/test_analyze_statistics.py and tests/compiled/ for the
native-kernel/end-to-end coverage) plus the instr / predicate_estimator_tag
tier-selection logic against hand-built StatisticsInput.
"""

import math
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.statistics import BYTE_CLASS
from opteryx.compiled.planner.statistics import CHAR_CLASSES
from opteryx.compiled.planner.statistics import CLASS_CARDINALITY
from opteryx.compiled.planner.statistics import LIKE_INFIX_SELECTIVITY
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.planner.statistics import estimate_selectivity
from opteryx.compiled.planner.statistics import predicate_estimator_tag
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.expression import NodeType
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64, NVARCHAR, VARCHAR

# One query context for the columns AND the expressions this module builds:
# the native estimator resolves a predicate's columns through its arena's
# bound ColumnTable.
_PLAN_CONTEXT = PlanContext()
_ARENA = _PLAN_CONTEXT.expressions

# Statistics are keyed by the identity of a column minted in a query's ColumnTable.
_IDENTITY = _PLAN_CONTEXT.columns.relation_column("t", "col").identity

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
    return StatisticsInput(
        _PLAN_CONTEXT.columns,
        row_count_estimate=1000,
        column_stats={
            _IDENTITY: {
                "class_proportions": class_proportions,
                "avg_length": avg_length,
                "distinct_count": distinct_count,
                "length_bounds": length_bounds,
            }
        },
    )


def _instr_node(needle, decay=0.7, op="InStr", column_type=VARCHAR, literal_type=VARCHAR):
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="col", arena=_ARENA)
    identifier.schema_column = _PLAN_CONTEXT.columns.reference(_IDENTITY, "col", column_type)
    literal = Literal(value=needle, type=literal_type, arena=_ARENA)
    node = Comparison(value=op, left=identifier, right=literal, arena=_ARENA)
    node.like_selectivity_decay = decay
    return node


def _decayed_char_class_selectivity(needle, class_proportions, avg_length, decay):
    """The decayed char-class infix model, observed through the estimator: an
    InStr predicate carrying `decay` against a column with these byte-class
    statistics (no length bounds, so no hard length guard)."""
    stats = _stats(class_proportions=class_proportions, avg_length=avg_length)
    return estimate_selectivity(_instr_node(needle.encode(), decay=decay), stats)


def _classify_char(c):
    return CHAR_CLASSES[BYTE_CLASS[ord(c)]]


# ── decayed char-class model math ─────────────────────────────


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
    # needle longer than avg_length -> n_positions clamps to 0.
    assert _decayed_char_class_selectivity("verylongneedle", _UNIFORM_PROPORTIONS, 3.0, 0.7) == 0.0
    # (avg_length == 0 never reaches the containment math: the estimator's
    # avg_length guard answers LIKE_INFIX_SELECTIVITY first - see
    # test_selectivity_instr_falls_back_when_avg_length_is_zero.)


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
    needle = "abc"
    s = _decayed_char_class_selectivity(needle, _UNIFORM_PROPORTIONS, 200.0, 1.0)
    p_pos = 1.0
    for c in needle:
        cls = _classify_char(c)
        p_pos *= _UNIFORM_PROPORTIONS[cls] / CLASS_CARDINALITY[cls]
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


# ── BYTE_CLASS / CHAR_CLASSES / CLASS_CARDINALITY sanity ──────────────


def test_classify_char_known_examples():
    assert _classify_char("A") == "upper"
    assert _classify_char("z") == "lower"
    assert _classify_char("5") == "digit"
    assert _classify_char(" ") == "whitespace"


def test_class_cardinality_keys_match_char_classes():
    assert set(CLASS_CARDINALITY.keys()) == set(CHAR_CLASSES)
    assert all(v > 0 for v in CLASS_CARDINALITY.values())


# ── needle coercion ─────────────────────────────────────────────────────────
#
# A bytes or str needle is the same needle; a non-string or NULL literal has
# no needle, so the flat infix constant prices it.


def test_like_needle_str_decodes_bytes():
    stats = _stats()
    from_bytes = estimate_selectivity(_instr_node(b"hello"), stats)
    assert from_bytes != LIKE_INFIX_SELECTIVITY
    assert estimate_selectivity(_instr_node(123, literal_type=INT64), stats) == LIKE_INFIX_SELECTIVITY
    assert estimate_selectivity(_instr_node(None), stats) == LIKE_INFIX_SELECTIVITY
    # (a VARCHAR literal's native value is bytes; a str is not a literal form)


# ── instr / predicate_estimator_tag tier selection ─────────────


def test_selectivity_instr_uses_char_class_when_stats_and_decay_present():
    stats = _stats()
    node = _instr_node(b"hello", decay=0.7)
    s = estimate_selectivity(node, stats)
    assert s != LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "char_class_decay"


def test_selectivity_instr_falls_back_without_decay():
    stats = _stats()
    node = _instr_node(b"hello", decay=None)
    s = estimate_selectivity(node, stats)
    assert s == LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_selectivity_instr_falls_back_without_class_proportions():
    # class_proportions and avg_length are recorded together (StatisticsInput
    # refuses one without the other): no byte-class statistics at all.
    stats = _stats(class_proportions=None, avg_length=None)
    node = _instr_node(b"hello", decay=0.7)
    s = estimate_selectivity(node, stats)
    assert s == LIKE_INFIX_SELECTIVITY
    assert predicate_estimator_tag(node, stats) == "flat_fallback"


def test_selectivity_instr_falls_back_when_avg_length_is_zero():
    stats = _stats(avg_length=0.0)
    node = _instr_node(b"hello", decay=0.7)
    s = estimate_selectivity(node, stats)
    assert s == LIKE_INFIX_SELECTIVITY


def test_selectivity_instr_falls_back_for_unknown_column():
    stats = _stats()
    unknown_identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="other", arena=_ARENA)
    unknown_identifier.schema_column = _PLAN_CONTEXT.columns.relation_column("t", "other")
    literal = Literal(value=b"hello", type=VARCHAR, arena=_ARENA)
    node = Comparison(value="InStr", left=unknown_identifier, right=literal, arena=_ARENA)
    node.like_selectivity_decay = 0.7
    s = estimate_selectivity(node, stats)
    assert s == LIKE_INFIX_SELECTIVITY


def test_predicate_estimator_tag_none_for_non_instr_predicate():
    stats = _stats()
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="col", arena=_ARENA)
    identifier.schema_column = _PLAN_CONTEXT.columns.reference(_IDENTITY, "col", None)
    literal = Literal(value=b"hello", type=VARCHAR, arena=_ARENA)
    node = Comparison(value="Eq", left=identifier, right=literal, arena=_ARENA)
    assert predicate_estimator_tag(node, stats) is None


def test_not_instr_is_the_complement_of_instr():
    stats = _stats()
    node = _instr_node(b"hello", decay=0.7, op="InStr")
    not_node = _instr_node(b"hello", decay=0.7, op="NotInStr")
    s = estimate_selectivity(node, stats)
    not_s = estimate_selectivity(not_node, stats)
    assert s == pytest_approx(1.0 - not_s)


# ── hard length guard: needle longer than the column's real max ────────────
#
# the containment model's n_positions already tends toward 0 as needle_len
# approaches avg_length, but that's a SOFT, probabilistic mechanism keyed on
# the AVERAGE -- it can (a) coincidentally still be nonzero for a needle just
# past avg_length but under max_length (which is correct, still possible),
# and it conflates "improbable relative to average" with "impossible". The
# new hard guard is a separate, certain, MAX-length-based short-circuit that
# fires before any of that probabilistic math, independent of avg_length.


def test_selectivity_instr_hard_zero_when_needle_exceeds_max_length():
    stats = _stats(length_bounds=(3, 10))
    node = _instr_node(b"this needle is far longer than ten bytes", decay=0.7)
    s = estimate_selectivity(node, stats)
    assert s == 0.0


def test_selectivity_instr_not_hard_zeroed_within_max_length():
    stats = _stats(length_bounds=(3, 50))
    node = _instr_node(b"hello", decay=0.7)
    s = estimate_selectivity(node, stats)
    assert s != 0.0


def test_selectivity_instr_hard_guard_skipped_for_nvarchar():
    # Same byte-vs-char risk as the STARTS_WITH guard -- NVARCHAR length
    # stats from the external catalog producer are character-based, so the
    # guard must not fire even when needle_len appears to exceed max_length.
    stats = _stats(length_bounds=(1, 3))
    node = _instr_node(b"this needle is far longer than three bytes", decay=0.7, column_type=NVARCHAR)
    s = estimate_selectivity(node, stats)
    assert s != 0.0  # falls through to the normal char-class/decay math instead


def test_not_instr_hard_zero_complements_to_one():
    stats = _stats(length_bounds=(3, 10))
    needle = b"this needle is far longer than ten bytes"
    node = _instr_node(needle, decay=0.7, op="InStr")
    not_node = _instr_node(needle, decay=0.7, op="NotInStr")
    assert estimate_selectivity(node, stats) == 0.0
    assert estimate_selectivity(not_node, stats) == 1.0


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
