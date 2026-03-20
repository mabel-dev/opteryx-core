import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.draken.vectors.integer_vector import IntegerVector
from opteryx.draken.vectors.string_vector import StringVector
from opteryx.expression import NodeType
from opteryx.operators.draken_aggregate_node import _DrakenAggregateCollector
from opteryx.operators.draken_aggregate_node import _vector_max
from opteryx.operators.draken_aggregate_node import _vector_min
from opteryx.operators.draken_aggregate_node import _vector_sum


def _identifier_parameter(identity="value"):
    return SimpleNamespace(
        node_type=NodeType.IDENTIFIER,
        schema_column=SimpleNamespace(identity=identity),
    )


def _aggregate(name: str):
    return SimpleNamespace(
        value=name,
        duplicate_treatment=None,
        schema_column=SimpleNamespace(identity=name.lower()),
        parameters=[_identifier_parameter()],
    )


def test_vector_sum_uses_typed_constant_encoding():
    vec = IntegerVector.from_constant(7, 4)

    assert _vector_sum(vec) == 28


def test_vector_sum_all_null_typed_constant_returns_none():
    vec = IntegerVector.from_constant(None, 4, is_null=True)

    assert _vector_sum(vec) is None


def test_vector_min_max_use_typed_constant_encoding_for_strings():
    vec = StringVector.from_constant("pear", 3)

    assert _vector_min(vec) == b"pear"
    assert _vector_max(vec) == b"pear"


def test_vector_min_max_all_null_typed_constant_return_none():
    vec = StringVector.from_constant(None, 2, is_null=True)

    assert _vector_min(vec) is None
    assert _vector_max(vec) is None


def test_aggregate_collector_avg_uses_typed_constant_vector():
    collector = _DrakenAggregateCollector(_aggregate("AVG"))

    collector._collect_vector(IntegerVector.from_constant(5, 4))

    assert collector.finalize() == 5


def test_aggregate_collector_min_max_use_typed_string_constant_vector():
    min_collector = _DrakenAggregateCollector(_aggregate("MIN"))
    max_collector = _DrakenAggregateCollector(_aggregate("MAX"))
    vec = StringVector.from_constant("zebra", 3)

    min_collector._collect_vector(vec)
    max_collector._collect_vector(vec)

    assert min_collector.finalize() == b"zebra"
    assert max_collector.finalize() == b"zebra"
