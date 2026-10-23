# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The optional footer sections rugo's writer states (docs/PARQUET_WRITE_FOR_READ_SPEED.md).

  * BYTE_ARRAY min/max longer than 16 bytes are cut to bounds of at most 16
    bytes, with is_min/max_value_exact = false; a UTF-8 cut stays valid UTF-8;
  * ColumnMetaData.encoding_stats states the dictionary page and every data
    page with its encoding;
  * ColumnMetaData.size_statistics states unencoded_byte_array_data_bytes;
  * FileMetaData key_value_metadata states each column's file-level NDV as
    `rugo.ndv.<column>`;
  * the column patch path carries the flags and the NDV and never restates a
    truncated bound as exact.

The footer is decoded with thriftpy2 from the vendored parquet.thrift, an
oracle independent of rugo's own footer parser; rugo's row-group statistics
are checked alongside it.
"""

import math
import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.utils import deserialize

import opteryx
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import patch_columns
from rugo.parquet import write_parquet
from rugo.rugo_native import read_rowgroup_stats

_SPEC = os.path.join(os.path.dirname(__file__), "../../rugo/src/parquet_spec/parquet.thrift")
_THRIFT = thriftpy2.load(_SPEC, module_name="parquet_spec_thrift")

PAGE_DICTIONARY = 2
PAGE_DATA = 0
ENC_PLAIN = 0
ENC_RLE_DICTIONARY = 8


def _footer(buf: bytes):
    assert buf[-4:] == b"PAR1"
    length = int.from_bytes(buf[-8:-4], "little")
    return deserialize(_THRIFT.FileMetaData(), buf[-8 - length : -8], TCompactProtocolFactory())


def _chunk(fmd, name, rg=0):
    for col in fmd.row_groups[rg].columns:
        if col.meta_data.path_in_schema == [name]:
            return col
    raise KeyError(name)


def _kv(fmd):
    return {kv.key: kv.value for kv in (fmd.key_value_metadata or [])}


def _count(sql):
    return sum(
        v for morsel in opteryx.session().execute_to_morsels(sql) for v in morsel.column("n").to_pylist()
    )


def _morsel(**columns):
    names = list(columns)
    vectors = [vector_from_sequence(values, dtype=dtype) for dtype, values in columns.values()]
    return Morsel.from_vectors(names, vectors)


# ---- BYTE_ARRAY bound truncation ----------------------------------------------------------------


def test_long_strings_are_cut_to_sixteen_byte_bounds():
    values = ["https://www.example.com/a/long/path?q=1", "https://www.apple.com/iphone/compare", None]
    buf = write_parquet(_morsel(s=("VARCHAR", values)), bloom_filters=False)
    stats = _chunk(_footer(buf), "s").meta_data.statistics
    present = sorted(v.encode() for v in values if v is not None)
    # lower: a 16-byte prefix of the true min; upper: the true max's first 16
    # bytes with the last one incremented ('e' -> 'f').
    assert stats.min_value == present[0][:16] == b"https://www.appl"
    assert stats.max_value == b"https://www.exam"[:15] + b"n"
    assert stats.min_value <= present[0] and stats.max_value > present[-1]
    assert stats.is_min_value_exact is False and stats.is_max_value_exact is False

    rugo_col = read_rowgroup_stats(buf)[0]["columns"][0]
    assert (rugo_col["min"], rugo_col["max"]) == (stats.min_value, stats.max_value)
    assert (rugo_col["min_exact"], rugo_col["max_exact"]) == (False, False)


def test_short_strings_are_exact():
    buf = write_parquet(_morsel(s=("VARCHAR", ["apple", "cherry", "banana"])), bloom_filters=False)
    stats = _chunk(_footer(buf), "s").meta_data.statistics
    assert (stats.min_value, stats.max_value) == (b"apple", b"cherry")
    assert stats.is_min_value_exact is True and stats.is_max_value_exact is True
    rugo_col = read_rowgroup_stats(buf)[0]["columns"][0]
    assert (rugo_col["min_exact"], rugo_col["max_exact"]) == (True, True)


def test_utf8_cut_lands_on_a_character_boundary():
    # Byte 16 of the min is the continuation byte of 'é' (bytes 15-16): the cut
    # backs off to 15. The max's last incrementable byte in its first 16 is the
    # ASCII 'z' at index 3 — the 'é's after it are multi-byte.
    low = "a" * 15 + "é" + "tail"
    high = "zzzz" + "é" * 10
    buf = write_parquet(_morsel(s=("VARCHAR", [low, high])), bloom_filters=False)
    stats = _chunk(_footer(buf), "s").meta_data.statistics
    assert stats.min_value == b"a" * 15
    assert stats.max_value == b"zzz{"
    stats.min_value.decode("utf-8")
    stats.max_value.decode("utf-8")
    assert stats.min_value <= low.encode() and stats.max_value > high.encode()


def test_upper_bound_with_nothing_to_increment_is_written_whole():
    # No ASCII byte in the first 16: there is no valid shorter upper bound, so
    # the max is written whole and stays exact; the min is still cut.
    high = "日本語のテキストがここにあります"
    low = "あいうえおかきくけこさしすせそ"
    buf = write_parquet(_morsel(s=("VARCHAR", [low, high])), bloom_filters=False)
    stats = _chunk(_footer(buf), "s").meta_data.statistics
    assert stats.max_value == high.encode() and stats.is_max_value_exact is True
    assert len(stats.min_value) <= 16 and stats.is_min_value_exact is False
    stats.min_value.decode("utf-8")


def test_binary_bounds_cut_on_bytes():
    low = b"\x00" * 20
    high = b"\x01" + b"\xff" * 20
    buf = write_parquet(_morsel(b=("VARBINARY", [low, high])), bloom_filters=False)
    stats = _chunk(_footer(buf), "b").meta_data.statistics
    assert stats.min_value == b"\x00" * 16
    assert stats.max_value == b"\x02"  # the 0xFF run cannot be incremented
    assert stats.is_min_value_exact is False and stats.is_max_value_exact is False


def test_pruning_on_truncated_bounds_keeps_every_match(tmp_path):
    # Two row groups whose long values share their first 16 bytes with values
    # in the OTHER row group: only exact bounds could tell them apart, so a
    # pruner that treated a cut bound as a value would drop real rows.
    first = [f"https://example.org/first/{i:04d}" for i in range(64)]
    second = [f"https://example.org/second/{i:04d}" for i in range(64)]
    directory = tmp_path / "ds"
    directory.mkdir()
    (directory / "part.parquet").write_bytes(
        write_parquet(_morsel(s=("VARCHAR", first + second)), max_rows_per_row_group=64)
    )
    for literal, expected in [
        (first[0], 1),
        (first[-1], 1),
        (second[0], 1),
        (second[-1], 1),
        ("https://example.org/f", 0),
    ]:
        assert _count(f"SELECT COUNT(*) AS n FROM '{directory}' WHERE s = '{literal}'") == expected, literal
    assert _count(f"SELECT COUNT(*) AS n FROM '{directory}' WHERE s LIKE 'https://example.org/second/%'") == 64


# ---- encoding_stats -------------------------------------------------------------------------------


def _data_pages(buf, fmd, name):
    """Data pages of the chunk, counted from its OffsetIndex (the page index)."""
    chunk = _chunk(fmd, name)
    raw = buf[chunk.offset_index_offset : chunk.offset_index_offset + chunk.offset_index_length]
    offset_index = deserialize(_THRIFT.OffsetIndex(), raw, TCompactProtocolFactory())
    return len(offset_index.page_locations)


def test_encoding_stats_state_every_page():
    rows = 8192
    morsel = _morsel(
        code=("INT64", [i % 7 for i in range(rows)]),
        wide=("INT64", list(range(rows))),
    )
    buf = write_parquet(morsel, max_page_bytes=4096, page_index=True, bloom_filters=False)
    fmd = _footer(buf)

    code = _chunk(fmd, "code").meta_data
    assert code.dictionary_page_offset is not None
    stats = sorted((s.page_type, s.encoding, s.count) for s in code.encoding_stats)
    assert stats == sorted(
        [(PAGE_DICTIONARY, ENC_PLAIN, 1), (PAGE_DATA, ENC_RLE_DICTIONARY, _data_pages(buf, fmd, "code"))]
    )

    wide = _chunk(fmd, "wide").meta_data
    assert wide.dictionary_page_offset is None
    pages = _data_pages(buf, fmd, "wide")
    assert pages > 1
    assert [(s.page_type, s.encoding, s.count) for s in wide.encoding_stats] == [(PAGE_DATA, ENC_PLAIN, pages)]


def test_single_page_chunk_states_one_data_page():
    buf = write_parquet(_morsel(i=("INT64", list(range(100)))), dictionary=False, bloom_filters=False)
    meta = _chunk(_footer(buf), "i").meta_data
    assert [(s.page_type, s.encoding, s.count) for s in meta.encoding_stats] == [(PAGE_DATA, ENC_PLAIN, 1)]


# ---- size_statistics ------------------------------------------------------------------------------


def test_size_statistics_count_string_payload_bytes():
    values = ["", "a", "日本", None, "https://example.org/x"]
    buf = write_parquet(
        _morsel(s=("VARCHAR", values), i=("INT64", [1, 2, 3, 4, 5])), bloom_filters=False
    )
    fmd = _footer(buf)
    size = _chunk(fmd, "s").meta_data.size_statistics
    assert size.unencoded_byte_array_data_bytes == sum(len(v.encode()) for v in values if v is not None)
    assert _chunk(fmd, "i").meta_data.size_statistics is None


# ---- file-level NDV -------------------------------------------------------------------------------


def test_file_ndv_is_exact_below_the_sketch_width():
    rng = random.Random(3)
    values = [rng.randrange(500) for _ in range(32 * 1024)]
    buf = write_parquet(_morsel(k=("INT64", values)), max_rows_per_row_group=1024)
    assert _kv(_footer(buf))["rugo.ndv.k"] == f"exact:{len(set(values))}"


def test_file_ndv_estimates_above_the_sketch_width():
    rng = random.Random(5)
    values = [rng.randrange(50_000) for _ in range(64 * 1024)]
    buf = write_parquet(_morsel(k=("INT64", values)), max_rows_per_row_group=4096)
    tag, count = _kv(_footer(buf))["rugo.ndv.k"].split(":")
    assert tag == "kmv1024"
    true_ndv = len(set(values))
    assert abs(int(count) - true_ndv) <= 0.1 * true_ndv, (count, true_ndv)


def test_ndv_is_written_without_bloom_filters():
    values = [i % 300 for i in range(4096)]
    buf = write_parquet(_morsel(k=("INT64", values)), bloom_filters=False, max_rows_per_row_group=1024)
    fmd = _footer(buf)
    assert _kv(fmd)["rugo.ndv.k"] == "exact:300"
    assert all(_chunk(fmd, "k", rg).meta_data.statistics.distinct_count == 300 for rg in range(4))
    assert all(_chunk(fmd, "k", rg).meta_data.bloom_filter_offset is None for rg in range(4))


def test_all_null_and_bool_columns_state_no_ndv():
    buf = write_parquet(
        _morsel(n=("INT64", [None, None, None]), b=("BOOLEAN", [True, False, True]), k=("INT64", [1, 2, 2]))
    )
    kv = _kv(_footer(buf))
    assert "rugo.ndv.n" not in kv and "rugo.ndv.b" not in kv
    assert kv["rugo.ndv.k"] == "exact:2"


def test_streaming_writer_states_the_same_ndv():
    from rugo.parquet import open_parquet_writer

    rng = random.Random(9)
    batches = [[rng.randrange(700) for _ in range(1024)] for _ in range(8)]
    pieces = []
    with open_parquet_writer(pieces.append) as writer:
        for batch in batches:
            writer.write_row_group(_morsel(k=("INT64", batch)))
    expected = len({v for batch in batches for v in batch})
    assert _kv(_footer(b"".join(pieces)))["rugo.ndv.k"] == f"exact:{expected}"


# ---- the patch path -------------------------------------------------------------------------------


def test_patch_carries_ndv_and_inexact_bounds():
    values = [f"https://example.org/path/{i % 40:03d}" for i in range(2048)]
    source = write_parquet(
        _morsel(s=("VARCHAR", values), k=("INT64", [i % 17 for i in range(2048)])),
        max_rows_per_row_group=512,
    )
    out = patch_columns(source, rename={"s": "url"}, drop=["k"])
    fmd = _footer(out)
    kv = _kv(fmd)
    assert kv["rugo.ndv.url"] == "exact:40"
    assert "rugo.ndv.s" not in kv and "rugo.ndv.k" not in kv
    for rg in range(4):
        stats = _chunk(fmd, "url", rg).meta_data.statistics
        assert len(stats.min_value) <= 16 and len(stats.max_value) <= 16
        assert stats.is_min_value_exact is False and stats.is_max_value_exact is False


def test_patch_does_not_restate_a_cut_bound_as_exact():
    # A 16-byte cut bound is short enough that the writer would call it exact
    # if it did not carry the source's flag.
    values = ["0123456789abcdefXYZ", "0123456789abcdefABC"]
    source = write_parquet(_morsel(s=("VARCHAR", values)), bloom_filters=False)
    out = patch_columns(source, rename={"s": "t"})
    stats = _chunk(_footer(out), "t").meta_data.statistics
    assert stats.min_value == b"0123456789abcdef"
    assert stats.is_min_value_exact is False and stats.is_max_value_exact is False


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
