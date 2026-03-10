import numpy as np
import pytest

from opteryx.exceptions import IncorrectTypeError
from opteryx.expression.binary_operators import MapAccessOp


def _to_list(result):
    if hasattr(result, "to_pylist"):
        return result.to_pylist()
    return list(result)


def test_map_access_list_positive_and_negative_indices():
    values = np.array([[1, 2, 3], [4], None], dtype=object)

    assert _to_list(MapAccessOp(values, np.array([0], dtype=np.int64))) == [1, 4, None]
    assert _to_list(MapAccessOp(values, np.array([-1], dtype=np.int64))) == [3, 4, None]


def test_map_access_list_out_of_range_returns_nulls():
    values = np.array([[1, 2, 3], [4], None], dtype=object)

    assert _to_list(MapAccessOp(values, np.array([9], dtype=np.int64))) == [None, None, None]


def test_map_access_varchar_by_integer():
    values = np.array(["abc", "d", None], dtype=object)

    assert _to_list(MapAccessOp(values, np.array([1], dtype=np.int64))) == ["b", None, None]


def test_map_access_blob_by_integer():
    values = np.array([b"abc", b"d", None], dtype=object)

    assert _to_list(MapAccessOp(values, np.array([1], dtype=np.int64))) == [b"b", None, None]


def test_map_access_all_null_container_returns_nulls():
    values = np.array([None, None], dtype=object)

    assert _to_list(MapAccessOp(values, np.array([0], dtype=np.int64))) == [None, None]


@pytest.mark.parametrize(
    "key",
    [
        np.array(["1"], dtype=object),
        np.array([None], dtype=object),
        np.array([True], dtype=object),
        np.array([1.2], dtype=float),
    ],
)
def test_map_access_rejects_non_integer_key_types(key):
    values = np.array(["abc"], dtype=object)

    with pytest.raises(IncorrectTypeError):
        MapAccessOp(values, key)
