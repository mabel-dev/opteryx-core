"""
Regression test: GZIP and LZ4 parquet decompression.

rugo vendors LZ4 (block codec) and miniz (raw DEFLATE), but for a long time the
`DecompressInto` switch only handled UNCOMPRESSED/SNAPPY/ZSTD and the codec
whitelist (decode.cpp / decode_column.cpp) admitted only 0/1/6 — so any parquet
file written with GZIP or LZ4 was silently rejected by the native scan.

This exercises the full native scan path end-to-end for both codecs:

  * GZIP  (parquet codec 2)     -> miniz raw-DEFLATE inflate after gzip-header skip
  * LZ4    (parquet 'lz4')      -> Arrow emits LZ4_RAW (codec 7)
  * LZ4_RAW (parquet 'lz4_raw') -> LZ4_RAW (codec 7)

Fixtures are written with PyArrow (allowed in tests/ only). We compare against
the known-good SNAPPY/ZSTD paths so a divergence is unambiguous.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector

N_ROWS = 4000


def _table():
    # A numeric column and a string column so both the fixed-width and the
    # byte-array decode paths run over decompressed page bytes.
    return pa.table(
        {
            "a": pa.array(list(range(N_ROWS)), type=pa.int64()),
            "s": pa.array([f"row-{i:05d}-payload" for i in range(N_ROWS)]),
        }
    )


def _scan_all(tmp, workspace, column):
    """Read `column` from <tmp>/<workspace>/t via the native parquet scan."""
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        opteryx.register_workspace(workspace, DiskConnector)
        rows = []
        for morsel in opteryx.session().execute_to_morsels(
            f"SELECT {column} FROM {workspace}.t"
        ):
            rows.extend(morsel.column(column.encode()).to_pylist())
        return rows
    finally:
        os.chdir(cwd)


def _roundtrip_for_codec(codec):
    table = _table()
    expected_a = list(range(N_ROWS))
    expected_s = [f"row-{i:05d}-payload" for i in range(N_ROWS)]
    with tempfile.TemporaryDirectory() as tmp:
        # Unique workspace name per codec keeps the process-global footer cache
        # from serving another codec's parsed footer.
        workspace = f"ws_{codec}_{os.getpid()}"
        data_dir = os.path.join(tmp, workspace, "t")
        os.makedirs(data_dir)
        # Multiple row groups + small pages so several compressed pages (and a
        # dictionary page for the string column) are decoded per column chunk.
        pq.write_table(
            table,
            os.path.join(data_dir, "data.parquet"),
            compression=codec,
            row_group_size=1000,
        )
        got_a = _scan_all(tmp, workspace, "a")
        got_s = _scan_all(tmp, workspace, "s")
    assert sorted(got_a) == expected_a, (
        f"{codec}: int column mismatch (got {len(got_a)} rows)"
    )
    assert sorted(got_s) == sorted(expected_s), (
        f"{codec}: string column mismatch (got {len(got_s)} rows)"
    )


def test_gzip_roundtrip():
    """GZIP (parquet codec 2) decodes via miniz raw-DEFLATE inflate."""
    _roundtrip_for_codec("gzip")


def test_lz4_roundtrip():
    """PyArrow 'lz4' -> LZ4_RAW (codec 7) decodes via the LZ4 block API."""
    _roundtrip_for_codec("lz4")


def test_lz4_raw_roundtrip():
    """PyArrow 'lz4_raw' -> LZ4_RAW (codec 7) decodes via the LZ4 block API."""
    _roundtrip_for_codec("lz4_raw")


def test_gzip_matches_snappy():
    """A GZIP file and a SNAPPY file of identical data yield identical scans."""
    table = _table()
    with tempfile.TemporaryDirectory() as tmp:
        results = {}
        for codec in ("snappy", "gzip", "lz4_raw"):
            workspace = f"ws_cmp_{codec}_{os.getpid()}"
            data_dir = os.path.join(tmp, workspace, "t")
            os.makedirs(data_dir)
            pq.write_table(
                table,
                os.path.join(data_dir, "data.parquet"),
                compression=codec,
                row_group_size=1000,
            )
            results[codec] = sorted(_scan_all(tmp, workspace, "a"))
    assert results["gzip"] == results["snappy"]
    assert results["lz4_raw"] == results["snappy"]


if __name__ == "__main__":
    test_gzip_roundtrip()
    test_lz4_roundtrip()
    test_lz4_raw_roundtrip()
    test_gzip_matches_snappy()
    print("✅ okay")
