# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the rugo parquet writer's per-column compression POLICY.

Two rules, both decided by the writer rather than the caller:

  1. The zstd level comes from the column's physical type and the write
     profile. Callers pick "fast" (default, CTAS/upload) or "storage"
     (defragmenter); they never pass a level. Only BYTE_ARRAY columns respond
     to the profile — numerics are byte-identical across both.

  2. Keep whichever is smaller. A column chunk whose compressed form is not
     smaller than its raw form is stored raw and records CODEC_UNCOMPRESSED in
     its own ColumnMetaData. The codec is therefore PER CHUNK, not per file.

PyArrow is the read-side oracle only (tests may use pyarrow).
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo.parquet import write_parquet, write_parquet_stream


def _morsel(sql: str):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) >= 1
    return morsels[0]


def _codecs(buf: bytes):
    """{column name: {codec names across row groups}} via pyarrow."""
    import pyarrow.parquet as pq

    md = pq.ParquetFile(io.BytesIO(buf)).metadata
    out = {}
    for r in range(md.num_row_groups):
        rg = md.row_group(r)
        for c in range(md.num_columns):
            col = rg.column(c)
            out.setdefault(col.path_in_schema, set()).add(col.compression)
    return out


def _roundtrip(buf: bytes):
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(buf))
    return {n: table.column(n).to_pylist() for n in table.column_names}


# Long, repetitive strings: the one type that responds to the profile.
STRINGY = "SELECT name || ' ' || name || ' ' || name AS s, id FROM $planets"


def test_profiles_round_trip_identically():
    morsel = _morsel(STRINGY)
    expected = _roundtrip(write_parquet(morsel, compression="none"))
    for profile in ("fast", "storage"):
        assert _roundtrip(write_parquet(morsel, profile=profile)) == expected


def test_fast_is_the_default():
    morsel = _morsel(STRINGY)
    assert write_parquet(morsel) == write_parquet(morsel, profile="fast")


def test_numeric_columns_are_identical_across_profiles():
    """Only BYTE_ARRAY takes the storage level, so an all-numeric file must not
    change at all between profiles — the policy's whole point."""
    morsel = _morsel("SELECT id, gravity, orbitalPeriod FROM $planets")
    assert write_parquet(morsel, profile="fast") == write_parquet(
        morsel, profile="storage"
    )


def test_incompressible_chunk_is_stored_raw():
    """zstd emits a frame header even when it finds nothing to compress, so an
    incompressible chunk comes back larger than it went in. The writer must
    store the raw pages and mark that chunk UNCOMPRESSED — never inflate.

    The value must fill all 64 bits. A bare RANDOM()*2**53 does NOT work: it
    leaves the high bytes near-constant, which zstd happily exploits. Nor does
    RANDOM_STRING — a BYTE_ARRAY page repeats a 4-byte length prefix per value,
    which is real redundancy even when the payload is noise."""
    morsel = _morsel(
        "SELECT CAST(RANDOM() * 9007199254740992 AS INTEGER)"
        " ^ (CAST(RANDOM() * 9007199254740992 AS INTEGER) * 2048) AS r"
        " FROM GENERATE_SERIES(20000) AS g"
    )
    compressed = write_parquet(morsel, compression="zstd")
    raw = write_parquet(morsel, compression="none")
    assert len(compressed) == len(raw), "incompressible chunk was not stored raw"
    assert _codecs(compressed)["r"] == {"UNCOMPRESSED"}


def test_tiny_chunk_is_stored_raw():
    """The frame header alone exceeds a one-row body, so the smallest possible
    chunk is the clearest case of compression making things worse."""
    morsel = _morsel("SELECT id FROM $planets LIMIT 1")
    compressed = write_parquet(morsel, compression="zstd")
    assert len(compressed) == len(write_parquet(morsel, compression="none"))
    assert _codecs(compressed)["id"] == {"UNCOMPRESSED"}


def test_compressible_chunk_still_compresses():
    """The keep-smaller rule must not disable compression generally."""
    morsel = _morsel(STRINGY)
    compressed = write_parquet(morsel, compression="zstd")
    assert _codecs(compressed)["s"] == {"ZSTD"}
    assert len(compressed) < len(write_parquet(morsel, compression="none"))


def test_mixed_codecs_in_one_file_are_readable():
    """A file may carry both codecs at once — legal parquet, and every reader
    dispatches per chunk. PyArrow is the independent oracle here."""
    morsel = _morsel(
        "SELECT CAST(g AS VARCHAR) || 'aaaaaaaaaaaaaaaaaaaaaaaa' AS s,"
        " CAST(RANDOM() * 9007199254740992 AS INTEGER)"
        " ^ (CAST(RANDOM() * 9007199254740992 AS INTEGER) * 2048) AS r"
        " FROM GENERATE_SERIES(20000) AS g"
    )
    buf = write_parquet(morsel, compression="zstd")
    codecs = _codecs(buf)
    assert codecs["s"] == {"ZSTD"}
    assert codecs["r"] == {"UNCOMPRESSED"}
    assert _roundtrip(buf) == _roundtrip(write_parquet(morsel, compression="none"))


def test_storage_profile_is_not_larger_on_strings():
    morsel = _morsel(STRINGY)
    fast = write_parquet(morsel, profile="fast")
    storage = write_parquet(morsel, profile="storage")
    assert len(storage) <= len(fast)


def test_streaming_writer_carries_the_profile():
    morsel = _morsel(STRINGY)
    sink = io.BytesIO()
    assert write_parquet_stream([morsel], sink.write, profile="storage") == 1
    assert _roundtrip(sink.getvalue()) == _roundtrip(
        write_parquet(morsel, compression="none")
    )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"compression": "none", "profile": "storage"}, "requires compression='zstd'"),
        ({"compression": "zstd", "profile": "turbo"}, "must be 'fast' or 'storage'"),
    ],
)
def test_contradictions_fail_loud(kwargs, message):
    morsel = _morsel(STRINGY)
    with pytest.raises(ValueError, match=message):
        write_parquet(morsel, **kwargs)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
