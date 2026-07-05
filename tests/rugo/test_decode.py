"""
Tests for Parquet data decoding functionality.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import rugo.rugo_native as rp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info


def test_can_decode_uncompressed_plain():
    """Test that can_decode returns True for uncompressed PLAIN-encoded files."""
    # The binary.parquet file has uncompressed, PLAIN-encoded byte_array columns
    assert rp.can_decode("testdata/parquet_tests/binary.parquet") is True


def test_can_decode_compressed():
    """Test that can_decode returns True for SNAPPY compressed files."""
    # The snappy_compressed.parquet file uses SNAPPY compression with PLAIN encoding
    # SNAPPY compression is supported by our decoder
    assert rp.can_decode("testdata/parquet_tests/snappy_compressed.parquet") is True


def test_can_decode_dictionary_encoded():
    """Test that can_decode returns True for files with dictionary encoding."""
    # The dictionary_encoded.parquet file uses SNAPPY compression with RLE_DICTIONARY encoding
    # Both SNAPPY and RLE_DICTIONARY are supported
    assert rp.can_decode("testdata/parquet_tests/dictionary_encoded.parquet") is True


def test_can_decode_unsupported_types():
    """Test that can_decode returns False for files with unsupported types."""
    # The alltypes_plain.parquet has boolean, float, etc. which are not supported
    assert rp.can_decode("testdata/parquet_tests/alltypes_plain.parquet") is False


def test_decode_string_column():
    """Test decoding a string column from binary.parquet."""
    with open("testdata/parquet_tests/binary.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["foo"])

    # binary.parquet has 12 string values in first row group
    assert result is not None
    morsel = result[0]
    assert morsel.column_names == [b"foo"]
    data = morsel.column("foo").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 12
    assert all(isinstance(s, str) for s in data)


def test_decode_nonexistent_column():
    """Test that requesting a non-existent column yields an empty (columnless) morsel."""
    with open("testdata/parquet_tests/binary.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["nonexistent"])
    assert result is not None
    morsel = result[0]
    assert morsel.column_names == []
    assert morsel.num_rows == 0


def test_decode_compressed_column():
    """Test decoding a DELTA_BYTE_ARRAY-encoded column (planets.parquet 'name')."""
    with open("testdata/planets/planets.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["name"])
    assert result is not None
    morsel = result[0]
    assert morsel.column_names == [b"name"]
    data = morsel.column("name").to_pylist()
    assert len(data) == morsel.num_rows
    assert all(isinstance(s, str) for s in data)


def test_decode_int32_column():
    """Test decoding an int32 column."""
    with open("testdata/parquet_tests/test_decode.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["int32_col"])

    assert result is not None
    morsel = result[0]
    data = morsel.column("int32_col").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == [10, 20, 30, 40, 50]


def test_decode_int64_column():
    """Test decoding an int64 column."""
    with open("testdata/parquet_tests/test_decode.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["int64_col"])

    assert result is not None
    morsel = result[0]
    data = morsel.column("int64_col").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == [100, 200, 300, 400, 500]


def test_decode_string_column_types():
    """Test decoding a string column."""
    with open("testdata/parquet_tests/test_decode.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["string_col"])

    assert result is not None
    morsel = result[0]
    data = morsel.column("string_col").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == ["test1", "test2", "test3", "test4", "test5"]


def test_can_decode_test_file():
    """Test that can_decode works for test_decode.parquet."""
    assert rp.can_decode("testdata/parquet_tests/test_decode.parquet") is True


def test_decode_snappy_compressed_column():
    """Test decoding a column from a SNAPPY compressed file."""
    # snappy_compressed.parquet has SNAPPY compression with PLAIN encoding
    with open("testdata/parquet_tests/snappy_compressed.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["id"])

    # File has 2 row groups with 500 rows each
    assert result is not None
    morsel = result[0]
    data = morsel.column("id").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 500
    assert all(isinstance(x, int) for x in data)


def test_decode_dictionary_encoded_column():
    """Test decoding a dictionary-encoded column."""
    # dictionary_encoded.parquet has RLE_DICTIONARY encoding
    with open("testdata/parquet_tests/dictionary_encoded.parquet", "rb") as f:
        file_data = f.read()

    result = rp.read_parquet(file_data, ["category"])

    # File has 2 row groups with 500 rows each
    assert result is not None
    morsel = result[0]
    data = morsel.column("category").to_pylist()
    assert isinstance(data, list)
    assert len(data) == 500


def _merged_column_stats(path, raw, col_name):
    """Merge read_rowgroup_stats + fetch_column_chunk_info into the single
    col_stats dict shape decode_column_from_chunk expects (see
    test_all_metadata_fields.py's _first_column_metadata for why this needs
    two calls under the current API)."""
    rg_stats = next(
        c for c in rp.read_rowgroup_stats(raw)[0]["columns"] if c["name"] == col_name
    )
    chunk_info = fetch_column_chunk_info(path, 0, [col_name])[col_name]
    return {**rg_stats, **chunk_info}


def test_decode_all_null_dictionary_encoded_zstd_column():
    """Decode an all-null dictionary-encoded string column compressed with ZSTD."""
    table = pa.table({"severity": pa.array([None] * 20000, type=pa.string())})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=True, data_page_size=1024)
    raw = sink.getvalue().to_pybytes()

    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.write(raw)
        f.flush()
        col_stats = _merged_column_stats(f.name, raw, "severity")

    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off

    chunk = raw[base_offset : base_offset + col_stats["total_compressed_size"]]
    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.length == 20000
    null_bitmap = decoded.is_null()
    assert len(null_bitmap) == 20000
    assert all(b == 1 for b in null_bitmap)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
