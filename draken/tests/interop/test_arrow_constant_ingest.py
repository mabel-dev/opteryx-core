import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
from draken.interop.arrow import vector_from_arrow

DRAKEN_ENCODING_CONSTANT = 3


def test_vector_from_arrow_single_entry_dictionary_becomes_string_constant():
    dictionary = pa.array(["north"], type=pa.string())
    indices = pa.array([0, 0, 0], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = vector_from_arrow(arr)

    assert vec.__class__.__name__ == "StringVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [b"north", b"north", b"north"]


def test_vector_from_arrow_single_entry_dictionary_becomes_integer_constant():
    dictionary = pa.array([7], type=pa.int32())
    indices = pa.array([0, 0, 0, 0], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = vector_from_arrow(arr)

    assert vec.__class__.__name__ == "IntegerVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [7, 7, 7, 7]


def test_vector_from_arrow_single_entry_dictionary_with_mixed_nulls_stays_non_constant():
    dictionary = pa.array(["north"], type=pa.string())
    indices = pa.array([0, None, 0], type=pa.int8())
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    vec = vector_from_arrow(arr)

    assert vec.encoding != DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [b"north", None, b"north"]


def test_vector_from_arrow_single_run_ree_becomes_constant():
    arr = pa.RunEndEncodedArray.from_arrays(
        pa.array([3], type=pa.int16()),
        pa.array(["x"], type=pa.string()),
    )

    vec = vector_from_arrow(arr)

    assert vec.__class__.__name__ == "StringVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [b"x", b"x", b"x"]


def test_vector_from_arrow_single_run_ree_null_becomes_all_null_constant():
    arr = pa.RunEndEncodedArray.from_arrays(
        pa.array([2], type=pa.int16()),
        pa.array([None], type=pa.string()),
    )

    vec = vector_from_arrow(arr)

    assert vec.__class__.__name__ == "StringVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None]
