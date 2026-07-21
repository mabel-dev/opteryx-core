"""
Tests for the streaming, constant-memory rugo parquet writer
(`open_parquet_writer` / `write_parquet_stream` in rugo/parquet.py, backed by
StreamingParquetWriter in rugo/src/parquet/_parquet_writer.hpp).

Covers:
  1. Streaming N batches produces byte-for-data-identical output to the one-shot
     write_parquet over the concatenated morsel (all types incl. ARRAY).
  2. The output is a spec-compliant multi-row-group parquet (pyarrow oracle).
  3. The sink is called incrementally (one flush per row group + footer), i.e.
     the writer never buffers the whole file.
  4. Context-manager finalisation: clean exit writes the footer; an exception
     leaves the partial file uncapped.
  5. write_parquet_stream wrapper: one row group per morsel, skips empties.

pyarrow is used purely as the test oracle.
"""

import io
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow.parquet as pq  # test oracle only

from draken.draken_native import DrakenType, vector_array_from_sequence, VARCHAR
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from rugo.parquet import (write_parquet, read_parquet, read_metadata,
                          open_parquet_writer, write_parquet_stream)


def _make_morsel(lo, hi):
    ids = list(range(lo, hi))
    names = [f"name_{i}" for i in range(lo, hi)]
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


def test_streaming_matches_one_shot():
    chunks = []
    with open_parquet_writer(chunks.append, compression="zstd") as w:
        for lo in range(0, 4000, 1000):
            w.write_row_group(_make_morsel(lo, lo + 1000))
    streamed = b"".join(chunks)

    oneshot = write_parquet(_make_morsel(0, 4000), compression="zstd",
                            max_rows_per_row_group=1000)

    assert read_metadata(streamed).num_rows == 4000
    assert _rows(read_parquet(streamed)) == _rows(read_parquet(oneshot))


def test_streaming_is_spec_compliant_multi_row_group():
    chunks = []
    with open_parquet_writer(chunks.append) as w:
        for lo in range(0, 3000, 1000):
            w.write_row_group(_make_morsel(lo, lo + 1000))
    data = b"".join(chunks)

    t = pq.read_table(io.BytesIO(data))
    pf = pq.ParquetFile(io.BytesIO(data))
    assert t.num_rows == 3000
    assert pf.num_row_groups == 3
    assert t.column("id").to_pylist() == list(range(3000))
    assert t.column("tags").to_pylist()[1] == ["t1", "t1b"]


def test_sink_is_flushed_incrementally():
    # One flush per row group plus one for the footer — never a single whole-file
    # buffer. This is the constant-memory guarantee.
    calls = []
    with open_parquet_writer(calls.append) as w:
        for lo in range(0, 3000, 1000):
            w.write_row_group(_make_morsel(lo, lo + 1000))
    assert len(calls) == 4  # 3 row groups + footer
    assert all(isinstance(c, (bytes, bytearray)) for c in calls)


def test_exception_leaves_file_uncapped():
    calls = []

    class Boom(Exception):
        pass

    try:
        with open_parquet_writer(calls.append) as w:
            w.write_row_group(_make_morsel(0, 100))
            raise Boom()
    except Boom:
        pass
    # The row group was flushed, but no footer was written (no trailing PAR1
    # capping a partial file).
    data = b"".join(calls)
    assert not data.endswith(b"PAR1"), "footer should not be written on exception"


def test_write_parquet_stream_wrapper_skips_empty():
    chunks = []

    def batches():
        yield _make_morsel(0, 500)
        yield _make_morsel(500, 500)   # empty (no rows) -> skipped
        yield _make_morsel(500, 1000)

    n = write_parquet_stream(batches(), chunks.append)
    assert n == 2  # empty morsel skipped
    data = b"".join(chunks)
    got = _rows(read_parquet(data))
    assert [r[0] for r in got] == list(range(1000))


def test_streaming_predicate_read_roundtrip():
    # The array column is first-in-schema after nothing; exercises the reader
    # predicate/filter path over streamed output.
    chunks = []
    with open_parquet_writer(chunks.append) as w:
        for lo in range(0, 2000, 500):
            w.write_row_group(_make_morsel(lo, lo + 500))
    data = b"".join(chunks)
    got = _rows(read_parquet(data, predicates=[("id", ">=", 1000)]))
    assert [r[0] for r in got] == list(range(1000, 2000))


if __name__ == "__main__":
    test_streaming_matches_one_shot()
    test_streaming_is_spec_compliant_multi_row_group()
    test_sink_is_flushed_incrementally()
    test_exception_leaves_file_uncapped()
    test_write_parquet_stream_wrapper_skips_empty()
    test_streaming_predicate_read_roundtrip()
    print("✅ all streaming writer tests passed")
