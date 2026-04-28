import numpy as np
import pytest

import pyarrow as pa
from draken.morsels.morsel import Morsel
from draken.vectors.int64_vector import Int64Vector
from opteryx.exceptions import IncorrectTypeError
from opteryx.expression.binary_operators import MapAccessOp


def _to_list(result):
    if hasattr(result, "to_pylist"):
        return result.to_pylist()
    return list(result)


def _const_key(value: int):
    return Int64Vector.from_constant(value, 1)


def _vector(values):
    return Morsel.from_arrow(pa.table({"v": pa.array(values)})).column(b"v")


def test_map_access_list_positive_and_negative_indices():
    values = _vector([[1, 2, 3], [4], None])

    assert _to_list(MapAccessOp(values, _const_key(0))) == [1, 4, None]
    assert _to_list(MapAccessOp(values, _const_key(-1))) == [3, 4, None]


def test_map_access_list_out_of_range_returns_nulls():
    values = _vector([[1, 2, 3], [4], None])

    assert _to_list(MapAccessOp(values, _const_key(9))) == [None, None, None]


def test_map_access_varchar_by_integer():
    values = _vector(["abc", "d", None])

    assert _to_list(MapAccessOp(values, _const_key(1))) == [b"b", None, None]


def test_map_access_blob_by_integer():
    values = _vector([b"abc", b"d", None])

    assert _to_list(MapAccessOp(values, _const_key(1))) == [b"b", None, None]


def test_map_access_draken_string_vector_zero_index_fast_path():
    morsel = Morsel.from_arrow(pa.table({"v": pa.array(["abc", "d", None])}))
    values = morsel.column(b"v")

    assert _to_list(MapAccessOp(values, _const_key(0))) == [b"a", b"d", None]


def test_map_access_all_null_container_returns_nulls():
    values = Morsel.from_arrow(
        pa.table({"v": pa.array([None, None], type=pa.list_(pa.int64()))})
    ).column(b"v")

    assert _to_list(MapAccessOp(values, _const_key(0))) == [None, None]


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
    values = _vector(["abc"])

    with pytest.raises(IncorrectTypeError):
        MapAccessOp(values, key)


def test_map_access_rejects_non_constant_int64_key():
    values = _vector(["abc"])
    key = Int64Vector.from_arrow(pa.array([1, 2], type=pa.int64()))

    with pytest.raises(IncorrectTypeError):
        MapAccessOp(values, key)
