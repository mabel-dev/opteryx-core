import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pyarrow as pa
import pytest

from opteryx.compiled.vector_ops import vector_round_digits
from opteryx.draken import Morsel, Vector


def _as_list(result):
    to_pylist = getattr(result, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    return result.tolist()


def test_dictionary_vector_round_trip_from_arrow():
    dictionary = pa.array([b"alpha", b"beta"], type=pa.binary())
    indices = pa.array([0, 1, None, 0, 1], type=pa.int16())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = Vector.from_arrow(arr)
    roundtrip = vec.to_arrow()

    assert vec.__class__.__name__ == "DictionaryVector"
    assert vec.to_pylist() == [b"alpha", b"beta", None, b"alpha", b"beta"]
    assert roundtrip.to_pylist() == arr.to_pylist()
    assert pa.types.is_dictionary(roundtrip.type)


def test_dictionary_vector_invalid_indices_rejected():
    dictionary = pa.array([b"a", b"b"], type=pa.binary())
    indices = pa.array([0, 2], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary, safe=False)

    with pytest.raises(ValueError, match="out of bounds"):
        Vector.from_arrow(arr)


def test_morsel_from_arrow_preserves_dictionary_vector():
    dictionary = pa.array([b"north", b"south"], type=pa.binary())
    indices = pa.array([0, 1, 0, None], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    table = pa.table({"region": arr})

    morsel = Morsel.from_arrow(table)
    column = morsel.column(b"region")

    assert column.__class__.__name__ == "DictionaryVector"
    assert column.to_pylist() == [b"north", b"south", b"north", None]


def test_dictionary_vector_predicates():
    dictionary = pa.array([b"alpha", b"beta", b"gamma"], type=pa.binary())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    eq = vec.equals("beta")
    neq = vec.not_equals("beta")
    in_list = vec.in_list(["alpha", None])

    assert _as_list(eq) == [False, True, False, False, True]
    assert _as_list(neq) == [True, False, False, True, False]
    assert _as_list(in_list) == [True, False, True, False, False]


def test_dictionary_vector_pattern_predicates():
    dictionary = pa.array([b"alpha", b"Beta", b"gamma"], type=pa.binary())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    like = vec.like("a%")
    ilike = vec.like("b%", True)
    rlike = vec.rlike("^g")

    assert _as_list(like) == [True, False, False, False, False]
    assert _as_list(ilike) == [False, True, False, False, True]
    assert _as_list(rlike) == [False, False, False, True, False]


def test_dictionary_vector_numeric_round_trip_from_arrow():
    dictionary = pa.array([10, 20, 30], type=pa.int64())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = Vector.from_arrow(arr)
    roundtrip = vec.to_arrow()

    assert vec.__class__.__name__ == "DictionaryVector"
    assert vec.to_pylist() == [10, 20, None, 30, 20]
    assert roundtrip.to_pylist() == arr.to_pylist()
    assert pa.types.is_dictionary(roundtrip.type)
    assert roundtrip.type.value_type == pa.int64()


def test_dictionary_vector_numeric_predicates():
    dictionary = pa.array([1, 2, 3], type=pa.int32())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    eq = vec.equals(2)
    neq = vec.not_equals(2)
    in_list = vec.in_list([1, None])

    assert _as_list(eq) == [False, True, False, False, True]
    assert _as_list(neq) == [True, False, False, True, False]
    assert _as_list(in_list) == [True, False, True, False, False]


def test_dictionary_vector_take_reuses_dictionary_payload_buffers():
    dictionary = pa.array([b"alpha", b"beta", b"gamma"], type=pa.binary())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    taken = vec.take(array("i", [0, 2, 4]))
    out = taken.to_arrow()

    assert pa.types.is_dictionary(out.type)
    assert out.to_pylist() == [b"alpha", None, b"beta"]

    src_dict = vec.to_arrow().dictionary
    out_dict = out.dictionary

    # take() should copy only row codes/nulls; dictionary payload buffers are shared.
    assert out_dict.buffers()[1].address == src_dict.buffers()[1].address
    assert out_dict.buffers()[2].address == src_dict.buffers()[2].address


def test_dictionary_vector_numeric_range_predicates():
    dictionary = pa.array([1, 2, 3], type=pa.int32())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    lt = vec.less_than(3)
    gt = vec.greater_than(1)
    lte = vec.less_than_or_equals(2)
    gte = vec.greater_than_or_equals(2)

    assert _as_list(lt) == [True, True, False, False, True]
    assert _as_list(gt) == [False, True, False, True, True]
    assert _as_list(lte) == [True, True, False, False, True]
    assert _as_list(gte) == [False, True, False, True, True]


def test_dictionary_vector_round_uses_dictionary_accessor_path():
    dictionary = pa.array([1.234, 2.345, 3.456], type=pa.float64())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    rounded = vector_round_digits(vec, 2)

    assert rounded.__class__.__name__ == "Float64Vector"
    assert rounded.to_pylist() == [1.23, 2.35, None, 3.46, 2.35]


def test_dictionary_vector_string_range_predicates_raise():
    dictionary = pa.array([b"alpha", b"beta", b"gamma"], type=pa.binary())
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    vec = Vector.from_arrow(arr)

    with pytest.raises(TypeError, match="numeric dictionary values"):
        vec.less_than("beta")


@pytest.mark.parametrize(
    "value_type,dictionary_values",
    [
        (pa.int64(), [10, 20, 30]),
        (pa.float64(), [1.5, 2.5, 3.5]),
        (pa.int32(), [10, 20, 30]),
        (pa.int16(), [10, 20, 30]),
        (pa.int8(), [10, 20, 30]),
    ],
)
def test_dictionary_vector_numeric_type_coverage_round_trip(value_type, dictionary_values):
    dictionary = pa.array(dictionary_values, type=value_type)
    indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = Vector.from_arrow(arr)
    roundtrip = vec.to_arrow()

    assert vec.__class__.__name__ == "DictionaryVector"
    assert vec.to_pylist() == arr.to_pylist()
    assert pa.types.is_dictionary(roundtrip.type)
    assert roundtrip.type.value_type == value_type


@pytest.mark.parametrize(
    "dict_size,expected_code_width,expected_index_type",
    [
        (256, 1, pa.uint8()),
        (257, 2, pa.uint16()),
        (65536, 2, pa.uint16()),
        (65537, 4, pa.uint32()),
    ],
)
def test_dictionary_vector_code_width_thresholds(dict_size, expected_code_width, expected_index_type):
    dictionary = pa.array(range(dict_size), type=pa.int32())
    indices = pa.array([0, dict_size - 1, None], type=pa.int32())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = Vector.from_arrow(arr)
    roundtrip = vec.to_arrow()

    assert vec.code_width == expected_code_width
    assert roundtrip.indices.type == expected_index_type
    assert roundtrip.to_pylist() == [0, dict_size - 1, None]
