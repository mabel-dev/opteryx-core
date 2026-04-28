import sys
from array import array
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from draken.vectors.bool_vector import BoolVector
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.integer_vector import IntegerVector
from draken.vectors.scalar_constructors import from_scalar
from draken.vectors.string_vector import StringVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector
from opteryx.operators.group_state_store import (
    DRAKEN_ENCODING_CONSTANT,
    DRAKEN_ENCODING_DENSE,
    DRAKEN_ENCODING_DICTIONARY,
    DRAKEN_ENCODING_RLE,
)


def test_vector_contract_declares_constant_accessor():
    repo_root = Path(__file__).resolve().parents[3]
    vector_pxd = (repo_root / "opteryx/draken/vectors/vector.pxd").read_text()
    vector_pyx = (repo_root / "third_party/mabel/draken/vectors/vector.pyx").read_text()
    buffers_pxd = (repo_root / "opteryx/draken/core/buffers.pxd").read_text()

    assert "cdef struct ConstAccessor:" in buffers_pxd
    assert "cdef ConstAccessor* const_accessor(self) noexcept" in vector_pxd
    assert "cdef ConstAccessor* const_accessor(self) noexcept:" in vector_pyx
    assert "return NULL" in vector_pyx


def test_vector_contract_tracks_explicit_encoding_state():
    repo_root = Path(__file__).resolve().parents[3]
    vector_pxd = (repo_root / "opteryx/draken/vectors/vector.pxd").read_text()
    vector_pyx = (repo_root / "third_party/mabel/draken/vectors/vector.pyx").read_text()

    assert "cdef DrakenEncoding _encoding" in vector_pxd
    assert "self._encoding = DRAKEN_ENCODING_DENSE" in vector_pyx
    assert "return self._encoding" in vector_pyx


def test_encoding_enum_values_are_stable():
    assert DRAKEN_ENCODING_DENSE == 0
    assert DRAKEN_ENCODING_DICTIONARY == 1
    assert DRAKEN_ENCODING_RLE == 2
    assert DRAKEN_ENCODING_CONSTANT == 3


def test_dense_vector_encoding():
    vec = Int64Vector(3)
    assert vec.encoding == DRAKEN_ENCODING_DENSE


def test_dictionary_vector_encoding():
    vec = Int64Vector.from_dict([0, 1, 0], [10, 20])
    assert vec.encoding == DRAKEN_ENCODING_DICTIONARY


def test_string_dictionary_vector_encoding():
    vec = StringVector.from_dict([0, 1, 0], [b"aa", b"bb"])

    assert vec.encoding == DRAKEN_ENCODING_DICTIONARY
    assert vec.dictionary_size == 2
    assert vec.code_width == 1
    assert vec.to_pylist() == [b"aa", b"bb", b"aa"]


def test_string_dictionary_take_preserves_dictionary_encoding():
    vec = StringVector.from_dict([0, 1, 0, 1], [b"aa", b"bb"])
    taken = vec.take(array("i", [3, 2, 0]))

    assert taken.encoding == DRAKEN_ENCODING_DICTIONARY
    assert taken.dictionary_size == 2
    assert taken.to_pylist() == [b"bb", b"aa", b"aa"]


def test_constant_from_scalar_prefers_typed_integer_vector_for_int8_dtype():
    vec = from_scalar(1, 4, dtype="int8")

    assert vec.__class__.__name__ == "IntegerVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [1, 1, 1, 1]


def test_constant_from_scalar_prefers_typed_string_vector_for_strings():
    vec = from_scalar("north", 3)

    assert vec.__class__.__name__ == "StringVector"
    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [b"north", b"north", b"north"]


def test_int64_constant_vector_encoding_and_access():
    vec = Int64Vector.from_constant(7, 4)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(vec) == 4
    assert vec.to_pylist() == [7, 7, 7, 7]
    assert vec[0] == 7
    assert vec[3] == 7
    assert vec.null_count == 0


def test_int64_constant_all_null_vector():
    vec = Int64Vector.from_constant(0, 3, is_null=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None, None]
    assert vec[1] is None
    assert vec.null_count == 3
    assert list(vec.is_null()) == [1, 1, 1]


def test_int64_constant_take_preserves_constant_encoding():
    vec = Int64Vector.from_constant(11, 5)
    taken = vec.take(array("i", [4, 2, 0]))

    assert taken.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(taken) == 3
    assert taken.to_pylist() == [11, 11, 11]


def test_int64_constant_to_arrow_roundtrips():
    vec = Int64Vector.from_constant(13, 4)
    arr = vec.to_arrow()

    assert arr.type == pa.int64()
    assert arr.to_pylist() == [13, 13, 13, 13]


def test_int64_constant_null_to_arrow_roundtrips():
    vec = Int64Vector.from_constant(0, 2, is_null=True)
    arr = vec.to_arrow()

    assert arr.type == pa.int64()
    assert arr.to_pylist() == [None, None]


def test_float64_constant_vector_encoding_and_access():
    vec = Float64Vector.from_constant(7.5, 4)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(vec) == 4
    assert vec.to_pylist() == [7.5, 7.5, 7.5, 7.5]
    assert vec[0] == 7.5
    assert vec[3] == 7.5
    assert vec.null_count == 0


def test_float64_constant_all_null_vector():
    vec = Float64Vector.from_constant(0.0, 3, is_null=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None, None]
    assert vec[1] is None
    assert vec.null_count == 3
    assert list(vec.is_null()) == [1, 1, 1]


def test_float64_constant_take_preserves_constant_encoding():
    vec = Float64Vector.from_constant(11.25, 5)
    taken = vec.take(array("i", [4, 2, 0]))

    assert taken.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(taken) == 3
    assert taken.to_pylist() == [11.25, 11.25, 11.25]


def test_float64_constant_to_arrow_roundtrips():
    vec = Float64Vector.from_constant(13.5, 4)
    arr = vec.to_arrow()

    assert arr.type == pa.float64()
    assert arr.to_pylist() == [13.5, 13.5, 13.5, 13.5]


def test_float64_constant_null_to_arrow_roundtrips():
    vec = Float64Vector.from_constant(0.0, 2, is_null=True)
    arr = vec.to_arrow()

    assert arr.type == pa.float64()
    assert arr.to_pylist() == [None, None]


def test_string_constant_vector_encoding_and_access():
    vec = StringVector.from_constant("hello", 3)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(vec) == 3
    assert vec.to_pylist() == [b"hello", b"hello", b"hello"]
    assert vec[0] == b"hello"
    assert vec[2] == b"hello"
    assert vec.null_count == 0


def test_string_constant_all_null_vector():
    vec = StringVector.from_constant(b"", 2, is_null=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None]
    assert vec[1] is None
    assert vec.null_count == 2


def test_string_constant_take_preserves_constant_encoding():
    vec = StringVector.from_constant("abc", 5)
    taken = vec.take(array("i", [4, 2, 0]))

    assert taken.encoding == DRAKEN_ENCODING_CONSTANT
    assert len(taken) == 3
    assert taken.to_pylist() == [b"abc", b"abc", b"abc"]


def test_string_constant_to_arrow_roundtrips():
    vec = StringVector.from_constant("xyz", 4)
    arr = vec.to_arrow()

    assert arr.type == pa.binary()
    assert arr.to_pylist() == [b"xyz", b"xyz", b"xyz", b"xyz"]


def test_string_constant_empty_string_is_not_null():
    vec = StringVector.from_constant(b"", 2)

    assert vec.to_pylist() == [b"", b""]
    assert vec.null_count == 0
    assert vec[0] == b""


def test_bool_constant_vector_encoding_and_access():
    vec = BoolVector.from_constant(True, 3)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [True, True, True]
    assert vec[1] is True


def test_bool_constant_all_null_vector():
    vec = BoolVector.from_constant(False, 2, is_null=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None]
    assert vec.null_count == 2


def test_date32_constant_vector_encoding_and_access():
    vec = Date32Vector.from_constant(123, 4)
    taken = vec.take(array("i", [3, 1]))

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [123, 123, 123, 123]
    assert taken.encoding == DRAKEN_ENCODING_CONSTANT
    assert taken.to_pylist() == [123, 123]


def test_time32_constant_vector_encoding_and_access():
    vec = TimeVector.from_constant(45, 3)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [45, 45, 45]
    assert vec.to_arrow().type == pa.time32("s")


def test_time64_constant_vector_encoding_and_access():
    vec = TimeVector.from_constant(123456, 2, is_time64=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [123456, 123456]
    assert vec.to_arrow().type == pa.time64("us")


def test_timestamp_constant_vector_encoding_and_access():
    vec = TimestampVector.from_constant(999, 3, timestamp_unit="us")
    taken = vec.take(array("i", [2, 0]))

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [999, 999, 999]
    assert taken.encoding == DRAKEN_ENCODING_CONSTANT
    assert taken.to_pylist() == [999, 999]


def test_integer_constant_vector_encoding_and_width_inference():
    vec = IntegerVector.from_constant(7, 4)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [7, 7, 7, 7]
    assert vec.to_arrow().type == pa.int8()


def test_integer_constant_all_null_vector():
    vec = IntegerVector.from_constant(0, 2, is_null=True)

    assert vec.encoding == DRAKEN_ENCODING_CONSTANT
    assert vec.to_pylist() == [None, None]
