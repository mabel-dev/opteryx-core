# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-5: PredicateOrdering consumes statistics-driven selectivity.

Previously the strategy ordered filters with a hard-coded ``DEFAULT_SELECTIVITY``
table keyed only on the operator. It now calls ``estimate_selectivity`` against
the input relation's ``RelationStatistics`` (histograms / NDV / null fractions)
when available, falling back to the constants only when no statistics exist.

Selectivity affects filter *order*, never results, so these tests assert the
ordering inputs and the resulting order — not query output.
"""

import array
import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryTelemetry
from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.strategies.predicate_ordering import _order_complex_predicates
from opteryx.planner.optimizer.strategies.predicate_ordering import _order_simple_predicates
from opteryx.planner.optimizer.strategies.predicate_ordering import _resolve_predicate_stats
from opteryx.third_party.maki_nage.distogram import load_counts_i64
from opteryx.types.logical_type import VARCHAR


# RelationStatistics is keyed by column identity, never by name — a name is not
# unique across a plan. The identifier nodes below therefore carry an identity
# on their schema_column, exactly as bound identifiers do.
_LOW = b"tes_low_00000001"
_HIGH = b"tes_high_0000002"
_MISSING = b"tes_msg_00000003"


def _cmp(op, col_identity, literal, col_name="col"):
    """A minimal COMPARISON_OPERATOR node: <col> <op> <literal>."""
    node = SimpleNamespace(node_type=NodeType.COMPARISON_OPERATOR, value=op)
    node.left = SimpleNamespace(
        node_type=NodeType.IDENTIFIER,
        source_column=col_name,
        value=col_name,
        schema_column=SimpleNamespace(category=None, identity=col_identity),
    )
    node.right = SimpleNamespace(node_type=NodeType.LITERAL, value=literal)
    node.centre = None
    return node


def _pred(condition):
    return SimpleNamespace(condition=condition)


_STATS = RelationStatistics(
    row_count_estimate=1000,
    columns={
        # low-cardinality column: Eq matches ~1/2 of rows
        _LOW: ColumnStatistics(column_name="low", data_type="INTEGER", distinct_count=2),
        # high-cardinality column: Eq matches ~1/1000 of rows
        _HIGH: ColumnStatistics(column_name="high", data_type="INTEGER", distinct_count=1000),
    },
)


# --- _resolve_predicate_stats ------------------------------------------------


def test_resolve_uses_statistics_when_present():
    low = _resolve_predicate_stats(_cmp("Eq", _LOW, 1, "low"), _STATS)
    high = _resolve_predicate_stats(_cmp("Eq", _HIGH, 1, "high"), _STATS)
    # NDV-driven: 1/2 vs 1/1000 — nothing like the flat 0.1 constant.
    assert low.selectivity == pytest.approx(0.5, rel=0.1), low.selectivity
    assert high.selectivity < 0.01, high.selectivity


def test_resolve_falls_back_to_default_without_statistics():
    eq = _resolve_predicate_stats(_cmp("Eq", _LOW, 1, "low"), None)
    not_eq = _resolve_predicate_stats(_cmp("NotEq", _LOW, 1, "low"), None)
    assert eq.selectivity == pytest.approx(0.1)  # DEFAULT_SELECTIVITY["Eq"]
    assert not_eq.selectivity == pytest.approx(0.9)  # DEFAULT_SELECTIVITY["NotEq"]


def test_resolve_unknown_column_degrades_to_constant():
    # For a column the stats don't cover, estimate_selectivity degrades to the
    # textbook Eq constant (0.1) rather than failing — same as the no-stats path.
    unknown = _resolve_predicate_stats(_cmp("Eq", _MISSING, 1, "missing"), _STATS)
    assert unknown.selectivity == pytest.approx(0.1)


# --- ordering flips with statistics -----------------------------------------


def test_order_prefers_more_selective_predicate_with_statistics():
    # Same operator and column type -> same cost. Constants alone would tie both
    # at 0.1 (input order preserved). With statistics the high-cardinality Eq
    # (far more selective) must be ordered first.
    low = _pred(_cmp("Eq", _LOW, 1, "low"))
    high = _pred(_cmp("Eq", _HIGH, 1, "high"))
    telemetry = QueryTelemetry.detached()

    # input order: [low, high] -> statistics should reorder to [high, low]
    ordered = _order_simple_predicates([low, high], telemetry, _STATS)
    assert ordered[0] is high, "more selective predicate should run first"
    assert ordered[1] is low


def test_order_without_statistics_keeps_constant_tie_order():
    # Without statistics both Eq predicates score the same constant -> the
    # ordering must not spuriously reorder a genuine tie.
    a = _pred(_cmp("Eq", _LOW, 1, "low"))
    b = _pred(_cmp("Eq", _HIGH, 1, "high"))
    telemetry = QueryTelemetry.detached()
    ordered = _order_simple_predicates([a, b], telemetry, None)
    assert ordered == [a, b]


# --- _order_complex_predicates: selectivity now informs FUNCTION ordering ---
#
# _STARTS_WITH(col, 'foo') (predicate_rewriter's FUNCTION-node form of "x LIKE
# 'foo%'") is FUNCTION-rooted, so it always lands in the "complex" group
# (order_predicates' node_type check), never the "simple" group above. Before
# this change, _order_complex_predicates ranked purely by catalog function
# cost -- a highly selective STARTS_WITH could never be pulled ahead of a
# cheaper, non-filtering function call. These tests pin the new
# (selectivity - 1) / cost ranking (cost as a tie-break) as a strict
# generalization: with no relation_stats, or for predicates with no
# selectivity model, behavior is unchanged from pure cost ordering.

_SW_IDENTITY = b"tes_sw_000000001"


def _varchar_identifier(col_identity, col_name="col"):
    n = Node(NodeType.IDENTIFIER, source_column=col_name)
    n.schema_column = Node(NodeType.IDENTIFIER, identity=col_identity, column_type=VARCHAR)
    return n


def _starts_with_pred(prefix: bytes, col_identity=_SW_IDENTITY):
    literal = Node(NodeType.LITERAL, value=prefix)
    condition = Node(
        NodeType.FUNCTION, value="_STARTS_WITH", parameters=[_varchar_identifier(col_identity), literal]
    )
    return _pred(condition)


def _cheap_no_model_func_pred(col_identity=_LOW):
    # LENGTH has no selectivity estimator (not _STARTS_WITH/_CI_STARTS_WITH),
    # so estimate_selectivity falls through to 1.0 -- and its real catalog
    # cost is far below _STARTS_WITH's, so pre-change cost-only ordering
    # would always place it first.
    identifier = Node(NodeType.IDENTIFIER, source_column="col2")
    identifier.schema_column = Node(NodeType.IDENTIFIER, identity=col_identity)
    condition = Node(NodeType.FUNCTION, value="LENGTH", parameters=[identifier])
    return _pred(condition)


def _sw_stats(col_min="a", col_max="m", identity=_SW_IDENTITY):
    """RelationStatistics whose VARCHAR column spans [col_min, col_max] in
    ordinal-key space -- a prefix well outside that range estimates near 0."""
    lo, hi = VARCHAR.ordinalize(col_min), VARCHAR.ordinalize(col_max)
    dgram = load_counts_i64(array.array("q", [0] * 63 + [1000]), float(lo), float(hi))
    col = ColumnStatistics(column_name="col", data_type="VARCHAR", histogram=dgram)
    return RelationStatistics(row_count_estimate=1000, columns={identity: col})


def test_selective_starts_with_moves_ahead_of_cheaper_function_with_statistics():
    stats = _sw_stats(col_min="a", col_max="m")
    cheap = _cheap_no_model_func_pred()  # LENGTH -- cheap, no model, selectivity 1.0
    sw = _starts_with_pred(b"zzz")  # entirely outside [a, m] -> selectivity ~0
    telemetry = QueryTelemetry.detached()

    ordered = _order_complex_predicates([cheap, sw], telemetry, stats)
    assert ordered[0] is sw, "highly selective STARTS_WITH should run first despite higher cost"
    assert ordered[1] is cheap


def test_complex_ordering_without_statistics_keeps_cost_only_order():
    # No relation_stats -> selectivity defaults to 1.0 for every predicate ->
    # ranking collapses to the old cost-only sort (LENGTH cheaper than
    # _STARTS_WITH), unchanged from before this feature existed.
    cheap = _cheap_no_model_func_pred()
    sw = _starts_with_pred(b"zzz")
    telemetry = QueryTelemetry.detached()

    ordered = _order_complex_predicates([cheap, sw], telemetry, None)
    assert ordered[0] is cheap
    assert ordered[1] is sw


def test_complex_ordering_no_model_predicates_keep_cost_order_even_with_statistics():
    # Two predicates neither of which _selectivity_starts_with/_ci_starts_with
    # can model (both fall through to 1.0) -- statistics being present must
    # not disturb the cost-only tie-break among them.
    stats = _sw_stats()
    a = _cheap_no_model_func_pred(col_identity=_LOW)
    b_condition = Node(
        NodeType.FUNCTION,
        value="UPPER",
        parameters=[Node(NodeType.IDENTIFIER, source_column="col3", schema_column=Node(NodeType.IDENTIFIER, identity=_HIGH))],
    )
    b = _pred(b_condition)
    telemetry = QueryTelemetry.detached()

    ordered = _order_complex_predicates([a, b], telemetry, stats)
    # LENGTH's catalog cost is lower than UPPER's -- cost order preserved.
    assert ordered[0] is a
    assert ordered[1] is b


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
