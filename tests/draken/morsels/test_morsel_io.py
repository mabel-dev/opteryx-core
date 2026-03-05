import struct

import pyarrow as pa
import pytest

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.draken import Morsel


mio = pytest.importorskip("opteryx.draken.storage.morsel_io")
from opteryx.draken.storage.morsel_io import DrakenMorselStorageError
from opteryx.draken.storage.morsel_io import read_morsel
from opteryx.draken.storage.morsel_io import write_morsel


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
    assert restored.column(b"k").__class__.__name__ == "DictionaryVector"
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
    assert restored.column(b"k").__class__.__name__ == "DictionaryVector"
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
