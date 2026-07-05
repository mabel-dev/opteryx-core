import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import rugo.rugo_native as rp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info


def _merged_col_stats(path, raw, rg_idx, col_name):
    """Build the col_stats dict decode_column_from_chunk expects by merging
    read_rowgroup_stats (min/max/null_count/encodings) with
    fetch_column_chunk_info (offsets/total_compressed_size) — the current API
    splits what used to be one dict-shaped read_metadata result across the two
    calls (see test_all_metadata_fields.py's _first_column_metadata)."""
    rg_stats = next(
        c for c in rp.read_rowgroup_stats(raw)[rg_idx]["columns"] if c["name"] == col_name
    )
    chunk_info = fetch_column_chunk_info(path, rg_idx, [col_name])[col_name]
    return {**rg_stats, **chunk_info}


def _merged_col_stats_from_bytes(raw, rg_idx, col_name):
    """_merged_col_stats for in-memory (pyarrow-generated) parquet bytes —
    fetch_column_chunk_info needs a real file path, so spool to a temp file."""
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.write(raw)
        f.flush()
        return _merged_col_stats(f.name, raw, rg_idx, col_name)


def _column_chunk(raw: bytes, col_stats: dict) -> bytes:
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off
    return raw[base_offset : base_offset + col_stats["total_compressed_size"]]


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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")
    encodings = set(col_stats.get("encodings") or [])
    if "RLE_DICTIONARY" not in encodings:
        pytest.skip("writer did not emit dictionary-encoded pages on this platform")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")

    encodings = set(col_stats.get("encodings") or [])
    if not ({"RLE_DICTIONARY", "PLAIN"} <= encodings):
        pytest.skip("writer did not emit mixed dictionary/plain pages on this platform")

    chunk = _column_chunk(raw, col_stats)
    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values
    assert decoded.to_arrow().to_pylist() == values


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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "n")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_missions_nullable_int64_dictionary():
    path = Path("testdata/missions/space_missions.parquet")
    raw = path.read_bytes()
    col_stats = _merged_col_stats(str(path), raw, 0, "Lauched_at")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)
    assert decoded is not None

    table = pq.read_table(path, columns=["Lauched_at"])
    assert decoded.to_pylist() == table["Lauched_at"].cast(pa.int64()).to_pylist()


def test_decode_column_from_chunk_single_entry_string_dictionary_becomes_constant():
    """decode_column_from_chunk flattens dictionary-encoded columns to plain
    Python-facing values by design (see its docstring) — it does not preserve
    constant/dict vector shape, even for a single-distinct-value column. This
    test now checks correct flattened values only."""
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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_single_entry_numeric_dictionary_becomes_constant():
    """See test_decode_column_from_chunk_single_entry_string_dictionary_becomes_constant:
    decode_column_from_chunk flattens by design, no constant-shape to assert on."""
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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "n")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


def test_decode_column_from_chunk_dictionary_remains_typed_string_vector():
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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")
    chunk = _column_chunk(raw, col_stats)

    rp.reset_telemetry()
    decoded = rp.decode_column_from_chunk(chunk, col_stats)
    tel = rp.get_telemetry()

    assert decoded is not None
    assert decoded.to_pylist() == values
    assert tel["parquet_dict_materialize_fallbacks"] == 0


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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")
    chunk = _column_chunk(raw, col_stats)

    decoded = rp.decode_column_from_chunk(chunk, col_stats)

    assert decoded is not None
    assert decoded.to_pylist() == values


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

    col_stats = _merged_col_stats_from_bytes(raw, 0, "category")
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

    row_groups = rp.read_rowgroup_stats(raw)
    assert len(row_groups) >= 2

    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.write(raw)
        f.flush()
        col_stats_rg1 = _merged_col_stats(f.name, raw, 0, "category")
        col_stats_rg2 = _merged_col_stats(f.name, raw, 1, "category")

    chunk_rg1 = _column_chunk(raw, col_stats_rg1)
    chunk_rg2 = _column_chunk(raw, col_stats_rg2)

    decoded_rg1 = rp.decode_column_from_chunk(chunk_rg1, col_stats_rg1)
    decoded_rg2 = rp.decode_column_from_chunk(chunk_rg2, col_stats_rg2)

    assert decoded_rg1 is not None
    assert decoded_rg2 is not None
    assert decoded_rg1.to_pylist() == rg1
    assert decoded_rg2.to_pylist() == rg2
