import os
import sys
import datetime

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.interop.vector_sequence import vector_from_sequence
from opteryx.expression.operations import filter_operations, to_temporal_array
from opteryx.types import OrsoTypes


def _as_list(result):
    to_pylist = getattr(result, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    return list(result)


def test_to_temporal_array_converts_date_to_timestamp_natively():
    source = vector_from_sequence([1, None, 2], dtype=OrsoTypes.DATE)
    result = to_temporal_array(source, OrsoTypes.DATE, OrsoTypes.TIMESTAMP)

    assert _as_list(result) == [
        datetime.datetime(1970, 1, 2, 0, 0),
        None,
        datetime.datetime(1970, 1, 3, 0, 0),
    ]


def test_filter_operations_coerces_temporal_vectors_without_arrow():
    left = vector_from_sequence([0, 1, 2], dtype=OrsoTypes.DATE)
    right = vector_from_sequence(
        [0, 86_400_000_000, 3 * 86_400_000_000],
        dtype=OrsoTypes.TIMESTAMP,
    )

    result = filter_operations(left, OrsoTypes.DATE, "Eq", right, OrsoTypes.TIMESTAMP)

    assert _as_list(result) == [True, True, False]


def test_filter_operations_coerces_integer_date_comparisons():
    left = vector_from_sequence([0, 1, 2], dtype=OrsoTypes.INTEGER)
    right = vector_from_sequence([0, 1, 3], dtype=OrsoTypes.DATE)

    result = filter_operations(left, OrsoTypes.INTEGER, "Eq", right, OrsoTypes.DATE)

    assert _as_list(result) == [True, True, False]
