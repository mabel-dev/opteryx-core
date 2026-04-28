import struct

import pyarrow as pa
import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from draken import Morsel
from draken.vectors.scalar_constructors import from_scalar as constant_from_scalar


mio = pytest.importorskip("draken.storage.morsel_io")
from draken.storage.morsel_io import DrakenMorselStorageError
from draken.storage.morsel_io import read_morsel
from draken.storage.morsel_io import write_morsel
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.string_vector import StringVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector


DRAKEN_ENCODING_CONSTANT = 3
DRAKEN_ENCODING_DICTIONARY = 1


def _as_py_columns(morsel):
    result = {}
    for name in morsel.column_names:
        encoded = name.encode("utf-8") if isinstance(name, str) else name
        key = name.decode("utf-8") if isinstance(name, bytes) else name
        result[key] = morsel.column(encoded).to_pylist()
    return result


def _sample_morsel():
    table = pa.table(
        {
            "i": pa.array([1, None, 3, -4], type=pa.int64()),
            "f": pa.array([1.5, None, -2.0, 9.25], type=pa.float64()),
            "b": pa.array([True, None, False, True], type=pa.bool_()),
            "s": pa.array([b"one", None, b"three", b""], type=pa.binary()),
            "d": pa.array([1, None, 3, 4], type=pa.date32()),
            "t32": pa.array([12, None, 34, 56], type=pa.time32("s")),
            "t64": pa.array([1_000_000, None, 3_000_000, 0], type=pa.time64("us")),
            "ts": pa.array([1_000_000, None, 2_000_000, -1], type=pa.timestamp("us")),
        }
    )
    return Morsel.from_arrow(table)


def test_morsel_io_round_trip_none_codec(tmp_path):
    original = _sample_morsel()
    path = tmp_path / "morsel.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.num_rows == original.num_rows
    assert restored.num_columns == original.num_columns
    assert restored.column_names == original.column_names
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_lz4_codec(tmp_path):
    original = _sample_morsel()
    path = tmp_path / "morsel_lz4.drkm"

    stats = write_morsel(path, original, {"codec_default": "lz4", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert stats["codec_default"] == "lz4"
    assert restored.num_rows == original.num_rows
    assert restored.num_columns == original.num_columns
    assert restored.column_names == original.column_names
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_dictionary_column(tmp_path):
    dictionary = pa.array([b"one", None, b"three"], type=pa.binary())
    indices = pa.array([0, 1, 2, None, 1, 0], type=pa.int8())
    table = pa.table({"k": pa.DictionaryArray.from_arrays(indices, dictionary)})
    original = Morsel.from_arrow(table)
    path = tmp_path / "morsel_dict.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.column(b"k").encoding == DRAKEN_ENCODING_DICTIONARY
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_numeric_dictionary_column(tmp_path):
    dictionary = pa.array([10, 20, 30], type=pa.int32())
    indices = pa.array([0, 1, 2, None, 1, 0], type=pa.int8())
    table = pa.table({"k": pa.DictionaryArray.from_arrays(indices, dictionary)})
    original = Morsel.from_arrow(table)
    path = tmp_path / "morsel_dict_numeric.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.column(b"k").encoding == DRAKEN_ENCODING_DICTIONARY
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_typed_int64_dictionary_storage(tmp_path):
    original = Morsel.from_vectors(
        ["k"],
        [Int64Vector.from_dict([0, 1, 2, 1, 0], [10, 20, 30])],
    )
    path = tmp_path / "morsel_typed_int64_dict.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.column(b"k").__class__.__name__ == "Int64Vector"
    assert getattr(restored.column(b"k"), "dictionary_value_type", None) is not None
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_typed_float64_dictionary_storage(tmp_path):
    original = Morsel.from_vectors(
        ["k"],
        [Float64Vector.from_dict([0, 1, 2, 1, 0], [1.5, 2.5, 3.5])],
    )
    path = tmp_path / "morsel_typed_float64_dict.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.column(b"k").__class__.__name__ == "Float64Vector"
    assert getattr(restored.column(b"k"), "dictionary_value_type", None) is not None
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_constant_columns(tmp_path):
    original = Morsel.from_vectors(
        ["i", "n", "s"],
        [
            constant_from_scalar(7, 6),
            constant_from_scalar(None, 6, dtype=pa.int64()),
            constant_from_scalar("x", 6),
        ],
    )
    path = tmp_path / "morsel_constant.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert getattr(restored.column(b"i"), "encoding", None) == 3
    assert getattr(restored.column(b"n"), "encoding", None) == 3
    assert getattr(restored.column(b"s"), "encoding", None) == 3
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_typed_constant_columns(tmp_path):
    original = Morsel.from_vectors(
        ["i", "s", "d", "t", "ts", "n"],
        [
            Int64Vector.from_constant(7, 4),
            StringVector.from_constant("x", 4),
            Date32Vector.from_constant(12_345, 4),
            TimeVector.from_constant(1_000_000, 4, is_time64=True),
            TimestampVector.from_constant(2_000_000, 4),
            StringVector.from_constant(None, 4, is_null=True),
        ],
    )
    path = tmp_path / "morsel_typed_constant.drkm"

    stats = write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})
    restored = read_morsel(path, {"checksum_enabled": True})

    assert stats["rows"] == original.num_rows
    assert stats["columns"] == original.num_columns
    assert restored.column(b"i").__class__.__name__ == "Int64Vector"
    assert restored.column(b"s").__class__.__name__ == "StringVector"
    assert restored.column(b"d").__class__.__name__ == "Date32Vector"
    assert restored.column(b"t").__class__.__name__ == "TimeVector"
    assert restored.column(b"ts").__class__.__name__ == "TimestampVector"
    assert restored.column(b"n").__class__.__name__ == "StringVector"
    assert restored.column(b"i").encoding == DRAKEN_ENCODING_CONSTANT
    assert restored.column(b"s").encoding == DRAKEN_ENCODING_CONSTANT
    assert restored.column(b"d").encoding == DRAKEN_ENCODING_CONSTANT
    assert restored.column(b"t").encoding == DRAKEN_ENCODING_CONSTANT
    assert restored.column(b"ts").encoding == DRAKEN_ENCODING_CONSTANT
    assert restored.column(b"n").encoding == DRAKEN_ENCODING_CONSTANT
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_detects_payload_corruption(tmp_path):
    original = _sample_morsel()
    path = tmp_path / "morsel_corrupt.drkm"
    write_morsel(path, original, {"codec_default": "none", "checksum_enabled": True})

    raw = bytearray(path.read_bytes())
    offset = mio.HEADER_SIZE
    for name in original.column_names:
        encoded = name.encode("utf-8") if isinstance(name, str) else name
        offset += mio.COLUMN_ENTRY_SIZE + len(encoded)

    block_header = struct.unpack(
        mio.BLOCK_HEADER_FMT, raw[offset : offset + mio.BLOCK_HEADER_SIZE]
    )
    comp_len = block_header[8]
    payload_start = offset + mio.BLOCK_HEADER_SIZE
    assert comp_len > 0

    raw[payload_start] ^= 0x01
    path.write_bytes(raw)

    with pytest.raises(DrakenMorselStorageError):
        read_morsel(path, {"checksum_enabled": True})


def test_morsel_io_round_trip_bytes_payload():
    original = _sample_morsel()

    payload = write_morsel(None, original, {"codec_default": "none", "checksum_enabled": True})
    assert isinstance(payload, (bytes, bytearray))
    assert len(payload) > 0

    restored = read_morsel(payload, {"checksum_enabled": True})
    assert restored.num_rows == original.num_rows
    assert restored.num_columns == original.num_columns
    assert restored.column_names == original.column_names
    assert _as_py_columns(restored) == _as_py_columns(original)


def test_morsel_io_round_trip_bytearray_and_memoryview():
    original = _sample_morsel()

    bytearray_sink = bytearray()
    bytearray_stats = write_morsel(
        bytearray_sink, original, {"codec_default": "none", "checksum_enabled": True}
    )
    assert bytearray_stats["path"] is None
    assert bytearray_stats["bytes_output"] == len(bytearray_sink)
    restored_from_bytearray = read_morsel(memoryview(bytearray_sink), {"checksum_enabled": True})
    assert _as_py_columns(restored_from_bytearray) == _as_py_columns(original)

    payload = write_morsel(None, original, {"codec_default": "none", "checksum_enabled": True})
    target = bytearray(len(payload))
    target_view = memoryview(target)
    memoryview_stats = write_morsel(
        target_view, original, {"codec_default": "none", "checksum_enabled": True}
    )
    assert memoryview_stats["bytes_output"] == len(payload)
    restored_from_memoryview = read_morsel(
        target_view[: memoryview_stats["bytes_output"]], {"checksum_enabled": True}
    )
    assert _as_py_columns(restored_from_memoryview) == _as_py_columns(original)


def test_morsel_io_memoryview_target_too_small():
    original = _sample_morsel()
    payload = write_morsel(None, original, {"codec_default": "none", "checksum_enabled": True})
    target = bytearray(max(1, len(payload) - 1))
    with pytest.raises(ValueError, match="too small"):
        write_morsel(memoryview(target), original, {"codec_default": "none", "checksum_enabled": True})
