import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx.config as config
import opteryx.rugo.parquet as rp

DRAKEN_ENCODING_CONSTANT = 3


def _column_chunk(raw: bytes, col_stats: dict) -> bytes:
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off
    return raw[base_offset : base_offset + col_stats["total_compressed_size"]]


@pytest.fixture(autouse=True)
def _native_dictionary_defaults():
    prior_ratio = config.PARQUET_DICT_MAX_CARDINALITY_RATIO
    config.PARQUET_DICT_MAX_CARDINALITY_RATIO = 0.5
    try:
        yield
    finally:
        config.PARQUET_DICT_MAX_CARDINALITY_RATIO = prior_ratio


def test_decode_column_from_chunk_dictionary_only_returns_typed_string_vector():
    values = ["alpha", "beta", None, "alpha", "beta"] * 200
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    encodings = set(col_stats.get("encodings") or [])
    if "RLE_DICTIONARY" not in encodings:
        pytest.skip("writer did not emit dictionary-encoded pages on this platform")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.__class__.__name__ == "StringVector"
    assert decoded.to_pylist() == [v.encode("utf8") if v is not None else None for v in values]


def test_decode_column_from_chunk_mixed_pages_stays_typed_string_encoded():
    frequent = [f"c{i % 5}" for i in range(600)]
    rare = [f"rare-{i:04d}-{'x' * 24}" for i in range(600)]
    values = frequent + rare
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="snappy",
        use_dictionary=True,
        data_page_size=256,
        dictionary_pagesize_limit=128,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]

    encodings = set(col_stats.get("encodings") or [])
    if not ({"RLE_DICTIONARY", "PLAIN"} <= encodings):
        pytest.skip("writer did not emit mixed dictionary/plain pages on this platform")

    chunk = _column_chunk(raw, col_stats)
    config.PARQUET_DICT_MAX_CARDINALITY_RATIO = 1.0
    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.__class__.__name__ == "StringVector"
    assert decoded.to_pylist() == [v.encode("utf8") for v in values]
    assert decoded.to_arrow().to_pylist() == [v.encode("utf8") for v in values]


def test_decode_column_from_chunk_numeric_dictionary_returns_typed_vector():
    values = [10, 20, None, 10, 30] * 300
    table = pa.table({"n": pa.array(values, type=pa.int64())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.__class__.__name__ == "Int64Vector"
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_single_entry_string_dictionary_becomes_constant():
    values = ["north"] * 256
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.__class__.__name__ == "StringVector"
    assert decoded.encoding == DRAKEN_ENCODING_CONSTANT
    assert decoded.to_pylist() == [b"north"] * len(values)


def test_decode_column_from_chunk_single_entry_numeric_dictionary_becomes_constant():
    values = [7] * 256
    table = pa.table({"n": pa.array(values, type=pa.int64())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.__class__.__name__ == "Int64Vector"
    assert decoded.encoding == DRAKEN_ENCODING_CONSTANT
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_cardinality_ratio_fallback_to_string_vector():
    values = [f"token-{i % 8}" for i in range(800)]
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    rp.reset_telemetry()
    config.PARQUET_DICT_MAX_CARDINALITY_RATIO = 0.001
    decoded = rp.decode_column_from_chunk(chunk, col_stats)
    tel = rp.get_telemetry()

    assert decoded is not None
    assert decoded.__class__.__name__ == "StringVector"
    assert decoded.to_pylist() == [v.encode("utf8") for v in values]
    assert tel["parquet_dict_materialize_fallbacks"] >= 1


def test_decode_column_from_chunk_null_heavy_dictionary_correctness():
    values = [None if i % 5 else f"cat-{i % 3}" for i in range(900)]
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="snappy",
        use_dictionary=True,
        data_page_size=512,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == [v.encode("utf8") if v is not None else None for v in values]


def test_decode_column_from_chunk_all_null_column_correctness():
    values = [None] * 1024
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        data_page_size=1024,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    col_stats = metadata["row_groups"][0]["columns"][0]
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_multi_rowgroup_independent_dictionaries():
    row_group_rows = 256
    rg1 = [f"north-{i % 4}" for i in range(row_group_rows)]
    rg2 = [f"south-{i % 4}" for i in range(row_group_rows)]
    values = rg1 + rg2
    table = pa.table({"category": pa.array(values, type=pa.string())})

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        row_group_size=row_group_rows,
        data_page_size=512,
    )
    raw = sink.getvalue().to_pybytes()

    metadata = rp.read_metadata_from_bytes(raw)
    assert len(metadata["row_groups"]) >= 2

    col_stats_rg1 = metadata["row_groups"][0]["columns"][0]
    col_stats_rg2 = metadata["row_groups"][1]["columns"][0]
    chunk_rg1 = _column_chunk(raw, col_stats_rg1)
    chunk_rg2 = _column_chunk(raw, col_stats_rg2)

    decoded_rg1 = rp.decode_column_from_chunk(chunk_rg1, col_stats_rg1)
    decoded_rg2 = rp.decode_column_from_chunk(chunk_rg2, col_stats_rg2)

    assert decoded_rg1 is not None
    assert decoded_rg2 is not None
    assert decoded_rg1.to_pylist() == [v.encode("utf8") for v in rg1]
    assert decoded_rg2.to_pylist() == [v.encode("utf8") for v in rg2]
