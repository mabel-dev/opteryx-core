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
    """Read every row group via the C++ pipeline; return {col: [values...]} in file order.

    The pipeline delivers row groups in COMPLETION order, at any decode_workers
    setting: a consumer blocked in wait_and_get_result claims a pending item and
    decodes it itself rather than waiting (the Gap #3 Phase 2b deadlock fix in
    io_pipeline.hpp), so the consumer thread always races the pool workers.
    decode_workers=1 does not mean file order and never did after that fix.

    So reassemble by the (path, rg_idx) the pipeline reports. This keeps the
    assertions exact and row-aligned — strictly stronger than a multiset compare,
    and it still catches a genuinely missing or short row group, which a caller
    that discards rg_idx cannot distinguish from reordering.
    """
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    by_rg = {}
    for scan_rg, rg in iter_row_groups_ipc(None, [path], columns, **pipeline_kwargs):
        key = (scan_rg.path, scan_rg.rg_idx)
        assert key not in by_rg, f"row group {key} delivered twice"
        by_rg[key] = {c: rg[c.encode("utf-8")].to_pylist() for c in columns}
    return {c: [v for key in sorted(by_rg) for v in by_rg[key][c]] for c in columns}


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
        # _read_all reassembles by rg_idx, so this is exact, row-aligned equality
        # regardless of the completion order the pipeline delivers in.
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
        # pool; _read_all restores file order from rg_idx, so compare exactly.
        got = _read_all(path, ["s"], decode_workers=4)
    assert got["s"] == vals


def test_pool_exhaustion_is_clean_error():
    """When the pool cannot fit a column, the worker surfaces a clean error
    rather than crashing or hanging. Drives the low-level CppIOPipeline with a
    deliberately tiny pool.

    The column MUST be a shape that still serializes through the pool. WP-6b
    routes fixed-width columns and plain/dict strings — nullable or not — to the
    direct path (a worker-allocated Draken buffer, zero pool bytes), where a tiny
    pool is never touched and there is nothing to exhaust. A LIST column is
    unconditionally DK_POOL (direct_kind_for bails on a non-empty rep_levels),
    so it still exercises the reserve failure.

    """
    from opteryx.connectors.parquet_io.pool_reader import (
        CppIOPipeline,
        fetch_column_chunk_info,
    )

    n = 100000
    table = pa.table(
        {
            "v": pa.array(
                [[f"cat_{i % 50}", f"cat_{(i + 1) % 50}"] for i in range(n)],
                type=pa.list_(pa.string()),
            )
        }
    )
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(table, tmp, row_group_size=n, use_dictionary=True)  # one big row group
        col_info = fetch_column_chunk_info(path, 0, ["v"])
        stats = [col_info["v"]]

        # Pool far too small to hold the serialized list column (megabytes).
        pipe = CppIOPipeline(decode_workers=1, queue_capacity=4, pool_size=4096)
        try:
            pipe.submit_work(path, 0, ["v"], stats)
            result = pipe.wait_result()
            assert result is not None, "pipeline drained without a result"
            assert result["success"] is False, "tiny pool should have failed the row group"
            assert "exhaust" in result["error"].lower(), result["error"]
        finally:
            pipe.close()


def test_column_chunk_info_feeds_submit_work_without_dictionary_page():
    """fetch_column_chunk_info's output must be directly consumable by
    submit_work — they are two halves of one public API.

    They disagreed on how to spell "absent": the reader emits None, rugo's
    ColumnStats uses a -1 sentinel. submit_work's `.get(key, -1)` default never
    fires on a present-but-None value, so every column WITHOUT a dictionary page
    (any plain-encoded column) raised `TypeError: an integer is required` at
    submission. Covers plain and dict, compressed and not, nullable and not.
    """
    from opteryx.connectors.parquet_io.pool_reader import (
        CppIOPipeline,
        fetch_column_chunk_info,
    )

    n = 20000
    cases = {
        "plain_i64": (pa.array(list(range(n)), type=pa.int64()), {"use_dictionary": False}),
        "plain_str": (pa.array([f"{i:020x}" for i in range(n)]), {"use_dictionary": False}),
        "plain_f64_nulls": (
            pa.array([i * 1.5 if i % 3 else None for i in range(n)], type=pa.float64()),
            {"use_dictionary": False},
        ),
        "zstd_plain_i64": (
            pa.array(list(range(n)), type=pa.int64()),
            {"use_dictionary": False, "compression": "zstd"},
        ),
        "dict_str": (pa.array([f"c{i % 50}" for i in range(n)]), {"use_dictionary": True}),
    }
    for label, (arr, write_kwargs) in cases.items():
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(pa.table({"v": arr}), tmp, row_group_size=n, **write_kwargs)
            col_info = fetch_column_chunk_info(path, 0, ["v"])
            pipe = CppIOPipeline(decode_workers=1, queue_capacity=4, pool_size=64 * 1024 * 1024)
            try:
                pipe.submit_work(path, 0, ["v"], [col_info["v"]])
                result = pipe.wait_result()
                assert result is not None, f"{label}: pipeline drained without a result"
                assert result["success"] is True, f"{label}: {result.get('error')}"
                got = {**result["ref_ids"], **result["direct"]}[b"v"]
                assert got.to_pylist() == arr.to_pylist(), f"{label}: values diverged"
            finally:
                pipe.close()


def test_submit_work_refuses_undecodable_metadata():
    """The absent-field translation must not invent a value where none is sound:
    an absent codec would silently decode as UNCOMPRESSED, and an absent
    definition/repetition level drives the decoder's nullability and list
    branches. Both refuse at submission."""
    import pytest

    from opteryx.connectors.parquet_io.pool_reader import CppIOPipeline

    base = {
        "name": "v",
        "physical_type": "int64",
        "compression_codec": "SNAPPY",
        "encodings": ["PLAIN"],
        "max_definition_level": 0,
        "max_repetition_level": 0,
        "data_page_offset": 4,
        "dictionary_page_offset": None,  # the legitimately-absent field
        "total_compressed_size": 10,
        "num_values": 1,
    }
    pipe = CppIOPipeline(decode_workers=1, queue_capacity=4, pool_size=4096)
    try:
        for field in ("compression_codec", "max_definition_level", "max_repetition_level"):
            with pytest.raises(ValueError, match="absent"):
                pipe.submit_work("/nonexistent.parquet", 0, ["v"], [{**base, field: None}])

        # A NAME neither table recognises (typo, corruption, a future spec value
        # this build predates) must refuse too — codec_map.get(name, 0) used to
        # silently coerce it to UNCOMPRESSED; encoding_map's membership filter
        # used to silently drop it from the list.
        with pytest.raises(ValueError, match="unrecognised"):
            pipe.submit_work("/nonexistent.parquet", 0, ["v"], [{**base, "compression_codec": "BOGUS_CODEC"}])
        with pytest.raises(ValueError, match="unrecognised"):
            pipe.submit_work(
                "/nonexistent.parquet", 0, ["v"], [{**base, "encodings": ["PLAIN", "BOGUS_ENC"]}]
            )
    finally:
        pipe.close()


def test_submit_work_accepts_every_spec_codec_and_encoding_name():
    """codec_map/encoding_map must cover every name
    CompressionCodecToString/EncodingToString (rugo/src/parquet/metadata.cpp)
    can produce, not just the ones common test fixtures happen to hit —
    otherwise a real column using LZ4_RAW compression or a DELTA_BINARY_PACKED-
    only encoding (typical for a non-dictionary int column) either silently
    mis-decodes (codec coerced to UNCOMPRESSED) or is falsely rejected as
    unsupported (a recognised-but-unmapped encoding filtered to nothing).

    Recognised-by-spec is not the same as decodable-by-rugo: BROTLI is a valid
    codec name this layer must accept, but the C++ decoder doesn't support it —
    that must surface as a clean per-row-group result['success'] is False, not
    a Python-level exception and not silent corruption.
    """
    from opteryx.connectors.parquet_io.pool_reader import CppIOPipeline, fetch_column_chunk_info

    n = 20000

    def submit(arr, **write_kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(pa.table({"v": arr}), tmp, row_group_size=n, **write_kwargs)
            col_info = fetch_column_chunk_info(path, 0, ["v"])
            pipe = CppIOPipeline(decode_workers=1, queue_capacity=4, pool_size=64 * 1024 * 1024)
            try:
                pipe.submit_work(path, 0, ["v"], [col_info["v"]])
                return pipe.wait_result()
            finally:
                pipe.close()

    # LZ4_RAW: previously missing from codec_map -> coerced to UNCOMPRESSED.
    r = submit(pa.array(list(range(n)), type=pa.int64()), use_dictionary=False, compression="lz4_raw")
    assert r["success"] is True, r.get("error")
    got = {**r["ref_ids"], **r["direct"]}[b"v"]
    assert got.to_pylist() == list(range(n))

    # DELTA_BINARY_PACKED-only encoding: previously filtered to an empty list ->
    # falsely reported unsupported by the has_supported_encoding gate downstream.
    r = submit(
        pa.array(list(range(n)), type=pa.int64()),
        use_dictionary=False,
        column_encoding={"v": "DELTA_BINARY_PACKED"},
    )
    assert r["success"] is True, r.get("error")

    # BROTLI: a spec-valid codec name (must be accepted here) that rugo's decoder
    # does not support (must fail cleanly downstream, not silently or in Python).
    r = submit(pa.array(list(range(n)), type=pa.int64()), use_dictionary=False, compression="brotli")
    assert r["success"] is False
    assert r["error"], "unsupported codec must surface a reason, not a silent empty result"


def _serialized_bytes(path, columns, **kw):
    """Read once with the IO diag JSON enabled; return ipc_bytes_serialized."""
    import json
    import tempfile

    diag = os.path.join(tempfile.gettempdir(), f"wp6b_diag_{os.getpid()}.jsonl")
    os.environ["OPTERYX_IO_DIAG_JSON"] = diag
    try:
        for _s, rg in __import__(
            "opteryx.connectors.parquet_io.pool_reader", fromlist=["iter_row_groups_ipc"]
        ).iter_row_groups_ipc(None, [path], columns, **kw):
            pass
    finally:
        del os.environ["OPTERYX_IO_DIAG_JSON"]
    with open(diag) as f:
        rec = json.loads(f.read().strip().splitlines()[-1])
    os.remove(diag)
    return rec["ipc_bytes_serialized"]


def test_direct_path_nonnull_numeric_roundtrip():
    """WP-6b: non-nullable plain int64/int32/float32/float64 take the direct
    (worker-built Draken buffer) path. Verify exact values AND that the path
    actually engaged (zero IPC bytes serialized)."""
    n = 8000
    table = pa.table(
        {
            "i64": pa.array(list(range(n)), type=pa.int64()),
            "i32": pa.array([(-i) for i in range(n)], type=pa.int32()),
            "f64": pa.array([i * 0.5 for i in range(n)], type=pa.float64()),
            "f32": pa.array([i * 0.25 for i in range(n)], type=pa.float32()),
        }
    )
    cols = ["i64", "i32", "f64", "f32"]
    with tempfile.TemporaryDirectory() as tmp:
        # use_dictionary=False, no nulls → every column is plain positional.
        path = _write(table, tmp, row_group_size=2000, use_dictionary=False)
        got = _read_all(path, cols, decode_workers=1)
        serialized = _serialized_bytes(path, cols, decode_workers=1)

    expected = {c: table.column(c).to_pylist() for c in cols}
    for c in cols:
        assert got[c] == expected[c], f"direct column {c} diverged"
    assert serialized == 0, f"expected all columns direct (0 IPC bytes), got {serialized}"


def test_direct_path_nullable_bool_decimal_roundtrip():
    """WP-6b-2: nullable fixed-width (compact→positional scatter), nullable bool
    (bit-scatter), and int128 DECIMAL128 (scatter + descriptor) all take the
    direct path. Values must match pyarrow exactly through the C++ scatter, and
    the path must engage (zero IPC bytes)."""
    from decimal import Decimal

    n = 6000
    table = pa.table(
        {
            # interleaved nulls force the compact→positional scatter
            "i64n": pa.array([i if i % 4 else None for i in range(n)], type=pa.int64()),
            "f64n": pa.array([i * 1.5 if i % 3 else None for i in range(n)], type=pa.float64()),
            "f32n": pa.array([i * 0.5 if i % 5 else None for i in range(n)], type=pa.float32()),
            "booln": pa.array([None if i % 7 == 0 else (i % 2 == 0) for i in range(n)], type=pa.bool_()),
            # precision > 18 → int128-backed DECIMAL128 direct path
            "dec": pa.array(
                [Decimal(f"{i}.{i % 100:02d}") if i % 6 else None for i in range(n)],
                type=pa.decimal128(24, 2),
            ),
            # all-null and no-null edges of a fixed-width column
            "allnull": pa.array([None] * n, type=pa.int64()),
            "nonull": pa.array(list(range(n)), type=pa.float64()),
        }
    )
    cols = ["i64n", "f64n", "f32n", "booln", "dec", "allnull", "nonull"]
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(table, tmp, row_group_size=1500, use_dictionary=False)
        got = _read_all(path, cols, decode_workers=1)
        serialized = _serialized_bytes(path, cols, decode_workers=1)

    expected = {c: table.column(c).to_pylist() for c in cols}
    for c in cols:
        assert got[c] == expected[c], f"direct column {c} diverged from pyarrow"
    assert serialized == 0, f"expected all columns direct (0 IPC bytes), got {serialized}"


def test_direct_path_concurrent_abandon_no_crash():
    """Abandon row groups mid-stream (LIMIT-style early exit) with multiple
    workers, so the result queue holds undrained MorselRefs carrying direct
    Draken buffers. Closing the pipeline must free them without crash or
    double-free (run under ASan to prove no leak/UAF)."""
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    n = 200000
    # Mix non-null direct, nullable direct (carries a validity buffer), and a
    # string (pool) column so abandoned MorselRefs hold data + validity + ref_ids.
    table = pa.table(
        {
            "v": pa.array(list(range(n)), type=pa.int64()),
            "vn": pa.array([i if i % 3 else None for i in range(n)], type=pa.int64()),
            "s": pa.array([f"x{i % 100}" for i in range(n)]),
        }
    )
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(table, tmp, row_group_size=10000, use_dictionary=False)  # 20 row groups
        it = iter_row_groups_ipc(None, [path], ["v", "vn", "s"], decode_workers=4)
        first = next(it)  # consume one; let workers race ahead on the rest
        assert len(first[1][b"v"]) > 0
        it.close()  # abandon the rest → MorselRef dtors free direct data + validity
    # Reaching here without a crash is the assertion; ASan catches leaks/UAF.


if __name__ == "__main__":
    test_mixed_types_with_nulls_roundtrip()
    print("mixed types + nulls: OK")
    test_high_cardinality_strings_roundtrip()
    print("high-cardinality strings: OK")
    test_pool_exhaustion_is_clean_error()
    print("pool exhaustion clean error: OK")
    test_column_chunk_info_feeds_submit_work_without_dictionary_page()
    print("chunk info feeds submit_work: OK")
    test_submit_work_refuses_undecodable_metadata()
    print("submit_work refuses undecodable metadata: OK")
    test_submit_work_accepts_every_spec_codec_and_encoding_name()
    print("submit_work accepts every spec codec/encoding name: OK")
    test_direct_path_nonnull_numeric_roundtrip()
    print("direct path non-null numeric: OK")
    test_direct_path_nullable_bool_decimal_roundtrip()
    print("direct path nullable/bool/decimal: OK")
    test_direct_path_concurrent_abandon_no_crash()
    print("direct path concurrent abandon: OK")
    print("all WP-6a/6b tests passed")
