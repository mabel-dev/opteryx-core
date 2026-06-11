"""
Regression tests for WP-6a: serialize decoded columns directly into MemoryPool
reserved memory (no intermediate heap buffer, no consumer-side commit() copy).

The risk in WP-6a is the two-pass serializer: serialized_size() (count pass) must
agree exactly with serialize_decoded_column_into() (write pass), because the
worker reserves `size` bytes then writes into them. A divergence would either
over-run the reserved region or hand the deserializer the wrong byte count — in
both cases the read-back values would be wrong (or it would crash). These tests
drive a mixed-type, with-nulls dataset through the real iter_row_groups_ipc path
and assert the values survive the round trip, exercising the count/write pair for
every serializer tag (fixed-width, dict-string, plain-string, bool, with nulls).

They also cover the new decode-time failure mode: when the pool cannot fit a
column, the worker must surface a clean error (not a crash or hang).
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))


def _write(table, tmp, **kwargs):
    path = os.path.join(tmp, "data.parquet")
    pq.write_table(table, path, **kwargs)
    return path


def _read_all(path, columns, **pipeline_kwargs):
    """Read every row group via the C++ pipeline; return {col: [values...]}."""
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    out = {c: [] for c in columns}
    for _scan_rg, rg in iter_row_groups_ipc(None, [path], columns, **pipeline_kwargs):
        for c in columns:
            out[c].extend(rg[c.encode("utf-8")].to_pylist())
    return out


def test_mixed_types_with_nulls_roundtrip():
    """Every serializer tag through the direct-into-pool path, values intact."""
    n = 5000
    table = pa.table(
        {
            "i64": pa.array([i if i % 7 else None for i in range(n)], type=pa.int64()),
            "i32": pa.array([(-i) if i % 5 else None for i in range(n)], type=pa.int32()),
            "f64": pa.array([i * 1.5 if i % 3 else None for i in range(n)], type=pa.float64()),
            "f32": pa.array([i * 0.25 if i % 4 else None for i in range(n)], type=pa.float32()),
            "lowcard": pa.array([None if i % 11 == 0 else f"cat_{i % 8}" for i in range(n)]),
            "highcard": pa.array([f"{i:08x}" for i in range(n)]),
            "flag": pa.array([None if i % 13 == 0 else (i % 2 == 0) for i in range(n)], type=pa.bool_()),
        }
    )
    cols = ["i64", "i32", "f64", "f32", "lowcard", "highcard", "flag"]
    with tempfile.TemporaryDirectory() as tmp:
        # Multiple row groups + dict encoding on the low-cardinality column so the
        # dict-string and rle paths are exercised, not just plain.
        path = _write(
            table, tmp, row_group_size=1000, use_dictionary=["lowcard"], compression="zstd"
        )
        # decode_workers=1 so row groups arrive in file order — lets us assert
        # exact, row-aligned equality (multi-worker yields completion order).
        got = _read_all(path, cols, decode_workers=1)

    expected = {c: table.column(c).to_pylist() for c in cols}
    for c in cols:
        assert got[c] == expected[c], f"column {c} diverged through direct-serialize"


def test_high_cardinality_strings_roundtrip():
    """German-string slots can exceed the source size; confirms the count pass
    accounts for them exactly (the sizing-headroom-sensitive case)."""
    n = 20000
    vals = [f"row-{i}-{'x' * (i % 40)}" for i in range(n)]
    table = pa.table({"s": pa.array(vals)})
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(table, tmp, row_group_size=4000, compression="zstd")
        # decode_workers=4 deliberately exercises concurrent reserve into the
        # pool; row groups arrive in completion order, so compare as a multiset
        # (still catches any value corruption from a size/write divergence).
        got = _read_all(path, ["s"], decode_workers=4)
    assert sorted(got["s"]) == sorted(vals)


def test_pool_exhaustion_is_clean_error():
    """When the pool cannot fit a column, the worker surfaces a clean error
    rather than crashing or hanging. Drives the low-level CppIOPipeline with a
    deliberately tiny pool."""
    import pytest

    from opteryx.connectors.parquet_io.pool_reader import (
        CppIOPipeline,
        fetch_column_chunk_info,
    )

    n = 200000
    table = pa.table({"v": pa.array(list(range(n)), type=pa.int64())})
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(table, tmp, row_group_size=n)  # one big row group
        col_info = fetch_column_chunk_info(path, 0, ["v"])
        stats = [col_info["v"]]

        # Pool far too small to hold the serialized int64 column (~1.6MB).
        pipe = CppIOPipeline(decode_workers=1, queue_capacity=4, pool_size=4096)
        try:
            pipe.submit_work(path, 0, ["v"], stats)
            result = pipe.wait_result()
            assert result is not None, "pipeline drained without a result"
            assert result["success"] is False, "tiny pool should have failed the row group"
            assert "exhaust" in result["error"].lower(), result["error"]
        finally:
            pipe.close()


if __name__ == "__main__":
    test_mixed_types_with_nulls_roundtrip()
    print("mixed types + nulls: OK")
    test_high_cardinality_strings_roundtrip()
    print("high-cardinality strings: OK")
    test_pool_exhaustion_is_clean_error()
    print("pool exhaustion clean error: OK")
    print("all WP-6a tests passed")
