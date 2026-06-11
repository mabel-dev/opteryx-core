"""
Regression test: BOOLEAN parquet columns.

Parquet stores booleans bit-packed (1 bit/value, LSB-first), and DRAKEN_BOOL
carries the same layout. The rugo decoder unpacks parquet bits into one
byte-per-value (DecodedColumn::boolean_values), and the IPC serializer used to
ship those bytes verbatim under TAG_BOOL — but the deserializer wraps the buffer
as a bit-packed DRAKEN_BOOL vector. The mismatch meant a byte-expanded buffer was
read bit-wise: e.g. [True]*8 + [False]*8 came back True only at rows 0 and 8.

serialize_bool now packs to bits at the producer. For nullable columns the value
stream is compact (parquet omits null rows), so the deserializer bit-scatters the
present bits to their row positions. These tests exercise both shapes plus the
partial-tail-byte, all-true/all-false, and multi-page (>64K rows) corners.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector


_WS_COUNTER = [0]


def _unique_ws():
    """A fresh workspace name per call. opteryx caches dataset metadata by
    workspace.table; reusing a name across TemporaryDirectory teardowns would
    serve a stale path to an already-deleted file (and is unrelated to bool)."""
    _WS_COUNTER[0] += 1
    return f"ws_bool_{_WS_COUNTER[0]}"


def _roundtrip(values, *, page_size=None):
    """Write a single bool column `b` to parquet and read it back via SQL."""
    ws = _unique_ws()
    table = pa.table({"b": pa.array(values, type=pa.bool_())})
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "bool_table")
        os.makedirs(data_dir)
        parquet_path = os.path.join(data_dir, "data.parquet")
        kwargs = {}
        if page_size is not None:
            kwargs["data_page_size"] = page_size
        # use_dictionary off: keep the plain bit-packed bool encoding path.
        pq.write_table(table, parquet_path, use_dictionary=False, **kwargs)

        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                f"SELECT b FROM {ws}.bool_table"
            ):
                rows.extend(morsel.column(b"b").to_pylist())
            return rows
        finally:
            os.chdir(cwd)


def test_bool_nonnull_block():
    values = [True] * 8 + [False] * 8
    assert _roundtrip(values) == values


def test_bool_alternating():
    values = [i % 2 == 0 for i in range(37)]  # 37 % 8 != 0 -> partial tail byte
    assert _roundtrip(values) == values


def test_bool_all_true():
    values = [True] * 20
    assert _roundtrip(values) == values


def test_bool_all_false():
    values = [False] * 20
    assert _roundtrip(values) == values


def test_bool_nullable():
    values = [True, None, False, None, True, True, None, False, False]
    assert _roundtrip(values) == values


def test_bool_nullable_partial_tail():
    # Nulls scattered so present-count != num_rows and tail byte is partial.
    values = [None if i % 3 == 0 else (i % 2 == 0) for i in range(29)]
    assert _roundtrip(values) == values


def test_bool_all_null():
    values = [None] * 11
    assert _roundtrip(values) == values


def test_bool_multipage_nonnull():
    # >64K rows + tiny page size forces many pages; parquet resets the bit index
    # per page, and present-value packing must stay aligned across page joins.
    n = 70_000
    values = [i % 3 == 0 for i in range(n)]
    assert _roundtrip(values, page_size=4096) == values


def test_bool_multipage_nullable():
    n = 70_000
    values = [None if i % 5 == 0 else (i % 3 == 0) for i in range(n)]
    assert _roundtrip(values, page_size=4096) == values


def test_bool_order_by_descending():
    ws = _unique_ws()
    values = [True] * 8 + [False] * 8
    table = pa.table(
        {"id": pa.array(list(range(16))), "b": pa.array(values, type=pa.bool_())}
    )
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "bool_sort")
        os.makedirs(data_dir)
        pq.write_table(table, os.path.join(data_dir, "data.parquet"), use_dictionary=False)

        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                f"SELECT b FROM {ws}.bool_sort ORDER BY b DESC"
            ):
                rows.extend(morsel.column(b"b").to_pylist())
            assert rows == [True] * 8 + [False] * 8, rows
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    test_bool_nonnull_block()
    test_bool_alternating()
    test_bool_all_true()
    test_bool_all_false()
    test_bool_nullable()
    test_bool_nullable_partial_tail()
    test_bool_all_null()
    test_bool_multipage_nonnull()
    test_bool_multipage_nullable()
    test_bool_order_by_descending()
    print("✅ BOOL parquet round-trip regression tests passed")
