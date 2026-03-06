"""
Tests for the Draken-native list_in_list.

New signature:
    list_in_list(Int64Vector | StringVector | Vector, set) -> BoolVector
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.list_ops import list_in_list
from opteryx.draken.interop.arrow import vector_from_sequence, vector_from_arrow


def _vec(values):
    return vector_from_sequence(values)


def _str_vec(values):
    """Always returns a StringVector regardless of input size."""
    return vector_from_arrow(pa.array(values, type=pa.string()))


def _result(bool_vec):
    return bool_vec.to_pylist()


# ---------------------------------------------------------------------------
# Int64Vector
# ---------------------------------------------------------------------------


class TestListInListInt64:
    def test_basic_match(self):
        vec = _vec([1, 2, 3, 4])
        assert _result(list_in_list(vec, {1, 3})) == [True, False, True, False]

    def test_no_match(self):
        vec = _vec([5, 6, 7])
        assert _result(list_in_list(vec, {1, 2, 3})) == [False, False, False]

    def test_all_match(self):
        vec = _vec([10, 20, 30])
        assert _result(list_in_list(vec, {10, 20, 30})) == [True, True, True]

    def test_empty_vector(self):
        vec = _vec([])
        assert _result(list_in_list(vec, {1})) == []

    def test_empty_values_set(self):
        vec = _vec([1, 2, 3])
        assert _result(list_in_list(vec, set())) == [False, False, False]

    def test_negative_values(self):
        vec = _vec([-1, 0, 1])
        assert _result(list_in_list(vec, {-1, 1})) == [True, False, True]

    def test_large_values(self):
        vec = _vec([10**15, 10**16, 42])
        assert _result(list_in_list(vec, {10**15, 42})) == [True, False, True]

    def test_null_not_in_values(self):
        vec = _vec([1, None, 3])
        result = _result(list_in_list(vec, {1, 3}))
        # null row is not in the set → False; result is nullable so may be None
        assert result[0] == True
        assert result[2] == True
        # null position is either False or None depending on null propagation
        assert result[1] in (False, None)

    def test_null_in_values(self):
        vec = _vec([1, None, 3])
        result = _result(list_in_list(vec, {1, None}))
        assert result[0] == True
        assert result[1] == True
        assert result[2] == False


# ---------------------------------------------------------------------------
# StringVector
# ---------------------------------------------------------------------------


class TestListInListString:
    def test_basic_match(self):
        vec = _str_vec(["apple", "banana", "cherry"])
        assert _result(list_in_list(vec, {"apple", "cherry"})) == [True, False, True]

    def test_no_match(self):
        vec = _str_vec(["foo", "bar"])
        assert _result(list_in_list(vec, {"baz"})) == [False, False]

    def test_all_match(self):
        vec = _str_vec(["a", "b", "c"])
        assert _result(list_in_list(vec, {"a", "b", "c"})) == [True, True, True]

    def test_empty_vector(self):
        vec = _str_vec([])
        assert _result(list_in_list(vec, {"x"})) == []

    def test_empty_values_set(self):
        vec = _str_vec(["a", "b"])
        assert _result(list_in_list(vec, set())) == [False, False]

    def test_null_not_in_values(self):
        vec = _str_vec(["a", None, "b"])
        result = _result(list_in_list(vec, {"a", "b"}))
        assert result[0] == True
        assert result[2] == True
        assert result[1] in (False, None)

    def test_null_in_values(self):
        vec = _str_vec(["a", None, "b"])
        result = _result(list_in_list(vec, {"a", None}))
        assert result[0] == True
        assert result[1] == True
        assert result[2] == False

    def test_single_row_match(self):
        vec = _str_vec(["hello"])
        assert _result(list_in_list(vec, {"hello"})) == [True]

    def test_single_row_no_match(self):
        vec = _str_vec(["hello"])
        assert _result(list_in_list(vec, {"world"})) == [False]


# ---------------------------------------------------------------------------
# TypeError for non-Draken input
# ---------------------------------------------------------------------------


class TestListInListTypeError:
    def test_arrow_array_raises(self):
        import pyarrow as pa
        arr = pa.array([1, 2, 3], type=pa.int64())
        with pytest.raises(TypeError):
            list_in_list(arr, {1})

    def test_numpy_raises(self):
        import numpy as np
        arr = np.array([1, 2, 3])
        with pytest.raises(TypeError):
            list_in_list(arr, {1})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
