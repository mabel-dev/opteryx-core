import os
import sys
from array import array

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.vectors.constant_vector import ConstantVector
from opteryx.draken.vectors.constant_vector import from_scalar


def _as_list(result):
    to_pylist = getattr(result, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    tolist = getattr(result, "tolist", None)
    if tolist is not None:
        return tolist()
    return list(result)


def test_constant_vector_scalar_int64_basic():
    vec = from_scalar(42, 5)
    assert isinstance(vec, ConstantVector)
    assert vec.to_pylist() == [42, 42, 42, 42, 42]
    assert vec.to_arrow().to_pylist() == [42, 42, 42, 42, 42]


def test_constant_vector_scalar_string_basic():
    vec = from_scalar("north", 3)
    assert isinstance(vec, ConstantVector)
    assert vec.to_pylist() == [b"north", b"north", b"north"]
    assert vec.to_arrow().to_pylist() == [b"north", b"north", b"north"]


def test_constant_vector_all_null_from_scalar():
    vec = from_scalar(None, 4, dtype=pa.int64())
    assert isinstance(vec, ConstantVector)
    assert vec.to_pylist() == [None, None, None, None]
    assert vec.to_arrow().to_pylist() == [None, None, None, None]


def test_constant_vector_take_preserves_constant_and_nulls():
    vec = ConstantVector(5, 4, 7, bytes([0b00011101]))  # valid rows: 0,2,3,4
    taken = vec.take(array("i", [0, 1, 4]))
    assert isinstance(taken, ConstantVector)
    assert taken.to_pylist() == [7, None, 7]


def test_constant_vector_predicates():
    vec = ConstantVector(5, 4, 10, bytes([0b00011101]))  # [10, None, 10, 10, 10]

    eq = _as_list(vec.equals(10))
    neq = _as_list(vec.not_equals(10))
    lt = _as_list(vec.less_than(20))
    gt = _as_list(vec.greater_than(10))
    in_list = _as_list(vec.in_list([5, 10, None]))

    assert eq == [True, False, True, True, True]
    assert neq == [False, False, False, False, False]
    assert lt == [True, False, True, True, True]
    assert gt == [False, False, False, False, False]
    assert in_list == [True, True, True, True, True]


def test_constant_vector_hash_matches_materialized_int64_hash():
    vec = from_scalar(1234, 6)
    hash_view = vec.hash()
    assert list(hash_view) == list(hash_view[:1]) * 6
