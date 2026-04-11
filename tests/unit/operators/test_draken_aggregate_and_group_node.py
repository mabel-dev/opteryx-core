# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

from opteryx.operators.draken_aggregate_and_group_node import DrakenAggregateAndGroupNode
from opteryx.expression import NodeType
from opteryx.models import QueryProperties
from opteryx.types import OrsoTypes


class _Aggregate:
    def __init__(
        self,
        value: str,
        duplicate_treatment=None,
        parameters=None,
        order=None,
        limit=None,
    ):
        self.value = value
        self.duplicate_treatment = duplicate_treatment
        self.parameters = parameters or [_Wildcard()]
        self.order = order
        self.limit = limit


class _Wildcard:
    node_type = NodeType.WILDCARD


class _Literal:
    node_type = NodeType.LITERAL

    def __init__(self, value):
        self.value = value


class _SchemaColumn:
    def __init__(self, identity):
        self.identity = identity


class _ExprNode:
    def __init__(self, node_type, identity, value=None, parameters=None, value_type=OrsoTypes.VARCHAR):
        self.node_type = node_type
        self.schema_column = _SchemaColumn(identity)
        self.value = value
        self.parameters = parameters or []
        self.type = value_type
        self.duplicate_treatment = None
        self.order = None
        self.limit = None


class _DummyGroupStateEngine:
    def __init__(self):
        self.readings = {}


def test_draken_groupby_expression_uses_carchar_backend_when_opted_in(monkeypatch):
    import opteryx.operators.draken_aggregate_and_group_node as module

    monkeypatch.setenv("FEATURE_GROUPBY_FORCE_CARCHAR_BACKEND", "1")
    monkeypatch.setattr(module, "get_all_nodes_of_type", lambda *args, **kwargs: [])
    monkeypatch.setattr(module, "extract_evaluations", lambda *args, **kwargs: [])
    monkeypatch.setattr(module, "create_groupby_engine", lambda *args, **kwargs: _DummyGroupStateEngine())

    group = _ExprNode(NodeType.LITERAL, "minute_expr")
    aggregate = _ExprNode(NodeType.AGGREGATOR, "count_star", value="COUNT", parameters=[_Wildcard()])

    node = DrakenAggregateAndGroupNode(
        properties=QueryProperties(query_id="test-draken-groupby-expression", variables={}),
        groups=[group],
        aggregates=[aggregate],
        projection=[group],
    )

    assert isinstance(node._groupby_engine, _DummyGroupStateEngine)
