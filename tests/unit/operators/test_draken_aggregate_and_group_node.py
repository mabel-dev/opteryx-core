# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

from opteryx.operators.draken_aggregate_and_group_node import DrakenAggregateAndGroupNode
from opteryx.operators.draken_aggregate_node import DrakenAggregateNode
from opteryx.expression import NodeType


class _Aggregate:
    def __init__(self, value: str, duplicate_treatment=None, parameters=None):
        self.value = value
        self.duplicate_treatment = duplicate_treatment
        self.parameters = parameters or [_Wildcard()]


class _Wildcard:
    node_type = NodeType.WILDCARD


class _Literal:
    node_type = NodeType.LITERAL

    def __init__(self, value):
        self.value = value


def test_draken_groupby_supports_simple_count():
    assert DrakenAggregateAndGroupNode.supports([_Aggregate("COUNT")])


def test_draken_groupby_supports_count_distinct():
    assert DrakenAggregateAndGroupNode.supports(
        [_Aggregate("COUNT", duplicate_treatment="Distinct")]
    )


def test_draken_groupby_supports_count_distinct_value():
    assert DrakenAggregateAndGroupNode.supports([_Aggregate("COUNT_DISTINCT")])


def test_draken_aggregate_supports_count_distinct_value():
    assert DrakenAggregateNode.supports([_Aggregate("COUNT_DISTINCT")])


def test_draken_groupby_supports_approx_percentile():
    assert DrakenAggregateAndGroupNode.supports(
        [_Aggregate("APPROX_PERCENTILE", parameters=[_Wildcard(), _Literal(0.5)])]
    )


def test_draken_groupby_supports_approx_count_distinct():
    assert DrakenAggregateAndGroupNode.supports([_Aggregate("APPROX_COUNT_DISTINCT")])


def test_draken_aggregate_supports_approx_count_distinct():
    assert DrakenAggregateNode.supports([_Aggregate("APPROX_COUNT_DISTINCT")])
