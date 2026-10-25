"""
`statistics=False` on rugo's Parquet writers (write_parquet, open_parquet_writer,
write_parquet_stream).

A file no query engine will prune does not need the footer statistics, the
chunk sums or the file-level NDV entries, and computing them is a pass over every
column. Switching them off must:

  1. drop the Statistics struct (null count, min/max, distinct count), the
     `rugo.sum` chunk entry and the file's `rugo.ndv.*` entries, and nothing else;
  2. leave the data byte-for-value identical to the default write;
  3. leave bloom filters and dictionaries to their own switches;
  4. refuse a request it cannot honour (a page index is per-page min/max)
     instead of silently writing neither.

The files are read back with rugo itself.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pytest

from draken.draken_native import DrakenType, vector_array_from_sequence, VARCHAR
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from rugo import rugo_native
from rugo.parquet import (open_parquet_writer, read_metadata, read_parquet,
                          write_parquet, write_parquet_stream)


def _make_morsel(lo, hi):
    ids = list(range(lo, hi))
    names = [f"name_{i % 40}" if i % 7 else None for i in range(lo, hi)]
    vals = [float(i) * 1.5 for i in range(lo, hi)]
    flags = [(i % 2 == 0) for i in range(lo, hi)]
    tags = [[f"t{i}", f"t{i}b"] if i % 3 else [] for i in range(lo, hi)]
    return Morsel.from_vectors(
        [b"id", b"name", b"val", b"flag", b"tags"],
        [vector_from_sequence(ids, DrakenType.INT64),
         vector_from_sequence(names, DrakenType.VARCHAR),
         vector_from_sequence(vals, DrakenType.FLOAT64),
         vector_from_sequence(flags, DrakenType.BOOL),
         vector_array_from_sequence(tags, int(VARCHAR.value), 1)])


def _rows(morsels):
    out = []
    for m in morsels:
        cols = {(n.decode() if isinstance(n, bytes) else n): m.column(n).to_pylist()
                for n in m.column_names}
        keys = list(cols)
        for i in range(m.num_rows):
            out.append(tuple(cols[k][i] for k in keys))
    return out


def _column_stats(data, name):
    """The first row group's footer record for column `name`."""
    for col in rugo_native.read_rowgroup_stats(data)[0]["columns"]:
        if col["name"] == name:
            return col
    raise AssertionError(f"no column {name!r}")


def test_default_writes_statistics():
    data = write_parquet(_make_morsel(0, 1000))
    ident = _column_stats(data, "id")
    assert ident["null_count"] == 0
    assert ident["min"] is not None and ident["max"] is not None
    assert ident["distinct_count"] == 1000
    assert _column_stats(data, "name")["null_count"] == 143
    assert b"rugo.sum" in data
    assert b"rugo.ndv." in data


@pytest.mark.parametrize("column", ["id", "name", "val", "flag"])
def test_statistics_false_drops_the_statistics(column):
    data = write_parquet(_make_morsel(0, 1000), statistics=False)
    stats = _column_stats(data, column)
    assert stats["min"] is None and stats["max"] is None
    assert stats["null_count"] == -1          # "the file did not record one"
    assert stats["distinct_count"] is None


def test_statistics_false_drops_chunk_sum_and_file_ndv():
    data = write_parquet(_make_morsel(0, 1000), statistics=False)
    assert b"rugo.sum" not in data
    assert b"rugo.ndv." not in data


def test_statistics_false_keeps_the_data():
    morsel = _make_morsel(0, 3000)
    with_stats = write_parquet(morsel, max_rows_per_row_group=1000)
    without = write_parquet(morsel, max_rows_per_row_group=1000, statistics=False)
    assert read_metadata(without).num_rows == 3000
    assert _rows(read_parquet(without)) == _rows(read_parquet(with_stats))
    assert len(without) < len(with_stats)


def test_statistics_false_all_null_and_empty_chunks():
    nulls = Morsel.from_vectors(
        [b"a", b"b"],
        [vector_from_sequence([None] * 100, DrakenType.INT64),
         vector_from_sequence([None] * 100, DrakenType.VARCHAR)])
    data = write_parquet(nulls, statistics=False)
    assert _rows(read_parquet(data)) == [(None, None)] * 100


def test_statistics_false_leaves_bloom_and_dictionary_alone():
    morsel = _make_morsel(0, 2000)
    default = write_parquet(morsel)
    off = write_parquet(morsel, statistics=False)
    # The bloom filter is its own switch: still written, byte-for-byte.
    assert _column_stats(off, "id")["bloom_offset"] >= 0
    assert _column_stats(off, "id")["bloom_length"] == _column_stats(default, "id")["bloom_length"]
    # Both switches off as well: the smallest file this writer makes.
    bare = write_parquet(morsel, statistics=False, bloom_filters=False, dictionary=False)
    assert _column_stats(bare, "id")["bloom_offset"] < 0
    assert len(bare) < len(off)
    assert _rows(read_parquet(bare)) == _rows(read_parquet(default))


def test_statistics_false_with_a_page_index_is_refused():
    morsel = _make_morsel(0, 1000)
    with pytest.raises(ValueError, match="statistics=False.*page index"):
        write_parquet(morsel, statistics=False, max_page_bytes=1000)
    # page_index=False (or no page splitting) leaves nothing to refuse.
    write_parquet(morsel, statistics=False, max_page_bytes=1000, page_index=False)
    write_parquet(morsel, statistics=False, max_page_bytes=0)


def test_statistics_false_streaming_writer():
    chunks = []
    with open_parquet_writer(chunks.append, statistics=False) as w:
        for lo in range(0, 3000, 1000):
            w.write_row_group(_make_morsel(lo, lo + 1000))
    streamed = b"".join(chunks)
    assert read_metadata(streamed).num_rows == 3000
    for rg in rugo_native.read_rowgroup_stats(streamed):
        for col in rg["columns"]:
            assert col["min"] is None and col["null_count"] == -1
    assert b"rugo.sum" not in streamed and b"rugo.ndv." not in streamed
    expected = [r for lo in range(0, 3000, 1000) for r in _rows([_make_morsel(lo, lo + 1000)])]
    assert _rows(read_parquet(streamed)) == expected


def test_statistics_false_streaming_page_index_is_refused():
    with pytest.raises(ValueError, match="statistics=False.*page index"):
        open_parquet_writer(lambda _b: None, statistics=False, max_page_bytes=1000)


def test_statistics_false_write_parquet_stream():
    chunks = []
    n = write_parquet_stream(iter([_make_morsel(0, 500), _make_morsel(500, 1000)]),
                             chunks.append, statistics=False)
    assert n == 2
    data = b"".join(chunks)
    assert _column_stats(data, "id")["min"] is None
    assert read_metadata(data).num_rows == 1000


# ---- write_parquet_fast: the preset that switches every reader aid off ----

def test_write_parquet_fast_is_the_bare_file():
    from rugo.parquet import write_parquet_fast

    morsel = _make_morsel(0, 3000)
    fast = write_parquet_fast(morsel, max_rows_per_row_group=1000)
    default = write_parquet(morsel, max_rows_per_row_group=1000)

    assert read_metadata(fast).num_rows == 3000
    assert _rows(read_parquet(fast)) == _rows(read_parquet(default))
    # No statistics, no bloom filter, no chunk sums / NDV entries.
    for rg in rugo_native.read_rowgroup_stats(fast):
        for col in rg["columns"]:
            assert col["min"] is None and col["null_count"] == -1
            assert col["distinct_count"] is None
            assert col["bloom_offset"] < 0
    assert b"rugo.sum" not in fast and b"rugo.ndv." not in fast
    # It is exactly write_parquet with the four switches off.
    assert fast == write_parquet(morsel, max_rows_per_row_group=1000,
                                 bloom_filters=False, dictionary=False,
                                 statistics=False, page_index=False)
    assert len(fast) < len(default)


def test_write_parquet_fast_takes_only_options_that_cannot_contradict_it():
    from rugo.parquet import write_parquet_fast

    morsel = _make_morsel(0, 2000)
    zstd = write_parquet_fast(morsel)
    raw = write_parquet_fast(morsel, compression="none")
    assert len(raw) > len(zstd)
    assert len(rugo_native.read_rowgroup_stats(
        write_parquet_fast(morsel, max_rows_per_row_group=500))) == 4
    for forbidden in ("bloom_filters", "dictionary", "statistics", "page_index",
                      "max_page_bytes"):
        with pytest.raises(TypeError):
            write_parquet_fast(morsel, **{forbidden: True})
