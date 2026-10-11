"""
Regression test: the parallel page walk applies the same page checks as the
sequential walk.

The Tier-3 parallel page walk (non-nullable, non-nested, no dictionary,
fixed-width, more than two pages) kept its own copy of the decode rules: it
decoded DELTA for integers and copied every other encoding as PLAIN. A
non-nullable BYTE_STREAM_SPLIT column therefore came back as plausible garbage
(SUM/MAX = NaN) with no error, while the sequential walk refuses the encoding.

Fixtures are written with PyArrow (allowed in tests/ only).
"""

import io
import os
import random
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
import rugo.parquet as rp
from opteryx.connectors import DiskConnector

N_ROWS = 50000
VALUES = [i * 0.5 for i in range(N_ROWS)]


def _bss_table():
    # Non-nullable so the column is eligible for the parallel page walk.
    return pa.table(
        {"f": pa.array(VALUES, type=pa.float64())},
        schema=pa.schema([pa.field("f", pa.float64(), nullable=False)]),
    )


def _write_bss(sink, page_version):
    # Small pages so the chunk has well over two pages.
    pq.write_table(
        _bss_table(),
        sink,
        use_dictionary=False,
        use_byte_stream_split=["f"],
        data_page_size=4096,
        compression="zstd",
        data_page_version=page_version,
    )


@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
def test_byte_stream_split_refused_by_rugo(page_version):
    sink = io.BytesIO()
    _write_bss(sink, page_version)
    with pytest.raises(RuntimeError, match=r"unsupported page encoding 9 \(BYTE_STREAM_SPLIT\)"):
        for _ in rp.read_parquet(sink.getvalue()):
            pass


@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
def test_byte_stream_split_refused_by_engine(page_version):
    with tempfile.TemporaryDirectory() as tmp:
        workspace = f"ws_bss_{page_version.replace('.', '')}_{os.getpid()}"
        data_dir = os.path.join(tmp, workspace, "t")
        os.makedirs(data_dir)
        _write_bss(os.path.join(data_dir, "data.parquet"), page_version)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(workspace, DiskConnector)
            with pytest.raises(Exception, match=r"unsupported page encoding 9 \(BYTE_STREAM_SPLIT\)"):
                for _ in opteryx.session().execute_to_morsels(
                    f"SELECT SUM(f), MAX(f) FROM {workspace}.t"
                ):
                    pass
        finally:
            os.chdir(cwd)


def test_plain_non_nullable_still_decodes():
    """The same shape written PLAIN takes the parallel walk and decodes exactly."""
    sink = io.BytesIO()
    pq.write_table(
        _bss_table(), sink, use_dictionary=False, data_page_size=4096, compression="zstd"
    )
    got = [v for m in rp.read_parquet(sink.getvalue()) for v in m.column(b"f").to_pylist()]
    assert got == VALUES


def _uncompressed_v2_table():
    # Random 63-bit integers do not compress, so pyarrow stores each DATA_PAGE_V2
    # values region raw and marks it is_compressed=false, inside a zstd column.
    rng = random.Random(7)
    values = [rng.getrandbits(63) for _ in range(N_ROWS)]
    table = pa.table(
        {"i": pa.array(values, type=pa.int64())},
        schema=pa.schema([pa.field("i", pa.int64(), nullable=False)]),
    )
    return table, values


def _write_uncompressed_v2(sink, table):
    pq.write_table(
        table,
        sink,
        use_dictionary=False,
        data_page_size=4096,
        compression="zstd",
        data_page_version="2.0",
        column_encoding={"i": "PLAIN"},
    )


def test_uncompressed_v2_pages_in_compressed_column_rugo():
    """V2 pages with is_compressed=false in a zstd column take the parallel walk raw."""
    table, values = _uncompressed_v2_table()
    sink = io.BytesIO()
    _write_uncompressed_v2(sink, table)
    got = [v for m in rp.read_parquet(sink.getvalue()) for v in m.column(b"i").to_pylist()]
    assert got == values


def test_uncompressed_v2_pages_in_compressed_column_engine():
    table, values = _uncompressed_v2_table()
    with tempfile.TemporaryDirectory() as tmp:
        workspace = f"ws_v2raw_{os.getpid()}"
        data_dir = os.path.join(tmp, workspace, "t")
        os.makedirs(data_dir)
        _write_uncompressed_v2(os.path.join(data_dir, "data.parquet"), table)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(workspace, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                f"SELECT SUM(i % 1000), MAX(i), MIN(i), COUNT(*) FROM {workspace}.t"
            ):
                rows.append([morsel.column(c).to_pylist()[0] for c in morsel.column_names])
        finally:
            os.chdir(cwd)
    assert rows == [[sum(v % 1000 for v in values), max(values), min(values), N_ROWS]]


if __name__ == "__main__":
    for v in ("1.0", "2.0"):
        test_byte_stream_split_refused_by_rugo(v)
        test_byte_stream_split_refused_by_engine(v)
    test_plain_non_nullable_still_decodes()
    test_uncompressed_v2_pages_in_compressed_column_rugo()
    test_uncompressed_v2_pages_in_compressed_column_engine()
    print("✅ okay")
