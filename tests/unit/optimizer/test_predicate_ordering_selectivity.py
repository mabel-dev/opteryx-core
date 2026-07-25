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

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.expression import NodeType
from opteryx.models import QueryTelemetry
from opteryx.planner.optimizer.statistics import ColumnRange
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.strategies.predicate_ordering import _order_simple_predicates
from opteryx.planner.optimizer.strategies.predicate_ordering import _resolve_predicate_stats


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
    row_count=1000,
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
    telemetry = QueryTelemetry()

    # input order: [low, high] -> statistics should reorder to [high, low]
    ordered = _order_simple_predicates([low, high], telemetry, _STATS)
    assert ordered[0] is high, "more selective predicate should run first"
    assert ordered[1] is low


def test_order_without_statistics_keeps_constant_tie_order():
    # Without statistics both Eq predicates score the same constant -> the
    # ordering must not spuriously reorder a genuine tie.
    a = _pred(_cmp("Eq", _LOW, 1, "low"))
    b = _pred(_cmp("Eq", _HIGH, 1, "high"))
    telemetry = QueryTelemetry()
    ordered = _order_simple_predicates([a, b], telemetry, None)
    assert ordered == [a, b]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
