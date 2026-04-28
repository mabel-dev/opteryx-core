import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa

from draken.morsels.morsel import Morsel
from draken.vectors.integer_vector import IntegerVector
from draken.vectors.string_vector import StringVector

from opteryx.operators.aggregate.ungrouped_agg import (
    CountAggregate,
    MaxBytesAggregate,
    MinBytesAggregate,
    SumInt64Aggregate,
    UngroupedAggregateEngine,
)

try:
    from opteryx.operators.draken_aggregate_node import _vector_max, _vector_min, _vector_sum
except ModuleNotFoundError:
    from opteryx.operators.aggregate.aggregate_node import _vector_max, _vector_min, _vector_sum


def test_vector_sum_uses_typed_constant_encoding():
    vec = IntegerVector.from_constant(7, 4)

    assert _vector_sum(vec) == 28


def test_vector_sum_all_null_typed_constant_returns_none():
    vec = IntegerVector.from_constant(None, 4, is_null=True)

    assert _vector_sum(vec) is None


def test_vector_sum_uses_null_bitmap_for_non_constant_integer_vectors():
    vec = IntegerVector.from_arrow(pa.array([1, None, 3], type=pa.int32()))

    assert _vector_sum(vec) == 4


def test_vector_min_max_use_typed_constant_encoding_for_strings():
    vec = StringVector.from_constant("pear", 3)

    assert _vector_min(vec) == b"pear"
    assert _vector_max(vec) == b"pear"


def test_vector_min_max_all_null_typed_constant_return_none():
    vec = StringVector.from_constant(None, 2, is_null=True)

    assert _vector_min(vec) is None
    assert _vector_max(vec) is None


def test_vector_min_max_use_null_bitmap_for_non_constant_integer_vectors():
    vec = IntegerVector.from_arrow(pa.array([5, None, 3], type=pa.int32()))

    assert _vector_min(vec) == 3
    assert _vector_max(vec) == 5


def test_ungrouped_engine_avg_uses_typed_constant_vector():
    engine = UngroupedAggregateEngine()
    engine.add_aggregate(SumInt64Aggregate(b"value", b"__avg_sum_value"))
    engine.add_aggregate(CountAggregate(b"value", b"__avg_count_value"))
    engine.add_avg_finalizer(b"__avg_sum_value", b"__avg_count_value", b"avg_value")

    engine.ingest(Morsel.from_vectors([b"value"], [IntegerVector.from_constant(5, 4)]))
    result = engine.finalize()

    assert result.column(b"avg_value")[0] == 5


def test_ungrouped_engine_min_max_use_typed_string_constant_vector():
    engine = UngroupedAggregateEngine()
    engine.add_aggregate(MinBytesAggregate(b"value", b"min_value"))
    engine.add_aggregate(MaxBytesAggregate(b"value", b"max_value"))

    engine.ingest(Morsel.from_vectors([b"value"], [StringVector.from_constant("zebra", 3)]))
    result = engine.finalize()

    assert result.column(b"min_value")[0] == b"zebra"
    assert result.column(b"max_value")[0] == b"zebra"
