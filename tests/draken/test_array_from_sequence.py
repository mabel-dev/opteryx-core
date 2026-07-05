"""Tests for building ARRAY Draken Vectors from nested Python sequences.

vector_from_sequence() (the generic dispatcher in
draken.interop.vector_sequence) is documented to build scalar (INT64-default)
vectors — it has no nested-list auto-detection, by the same explicit-dispatch
design as DECIMAL/FP16 (see its own docstring). Nested lists must go through
the dedicated vector_array_from_sequence() native constructor instead.
"""

import draken.draken_native as dn
from draken.vectors.vector import Vector


def _array_vector(data):
    return Vector(dn.vector_array_from_sequence(data))


def test_simple_nested_lists_of_integers():
    data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
    vec = _array_vector(data)
    assert vec.length == len(data)
    assert vec.to_arrow().to_pylist() == data


def test_nested_lists_with_none():
    data = [[1, 2], None, [3, 4, 5]]
    vec = _array_vector(data)
    assert vec.to_arrow().to_pylist() == data


def test_empty_nested_lists():
    data = [[], [1], [], [2, 3]]
    vec = _array_vector(data)
    assert vec.to_arrow().to_pylist() == data


def test_nested_lists_of_strings():
    data = [["a", "b"], ["c", "d", "e"], ["f"]]
    vec = _array_vector(data)
    result = vec.to_arrow().to_pylist()
    result = [
        [item.decode("utf-8") if isinstance(item, bytes) else item for item in row]
        if row
        else row
        for row in result
    ]
    assert result == data


def test_nested_lists_of_floats():
    data = [[1.1, 2.2], [3.3], [4.4, 5.5, 6.6]]
    vec = _array_vector(data)
    assert vec.to_arrow().to_pylist() == data
