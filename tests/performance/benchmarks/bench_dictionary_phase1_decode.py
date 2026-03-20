"""
Phase 1 dictionary decode benchmark harness.

Measures decode throughput, peak RSS, and output-memory footprint for parquet string columns.
The dictionary path is always enabled now, so this benchmark compares datasets with different
cardinality rather than toggling a decode policy.

Run with:
    python tests/performance/benchmarks/bench_dictionary_phase1_decode.py
"""

import argparse
import json
import os
import resource
import statistics
import subprocess
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx.rugo.parquet as rp


def _storage_size_bytes(arr: pa.Array) -> int:
    total = 0
    for buf in arr.buffers():
        if buf is not None:
            total += buf.size
    if pa.types.is_dictionary(arr.type):
        for buf in arr.dictionary.buffers():
            if buf is not None:
                total += buf.size
    return total


def _build_parquet_bytes(rows: int, cardinality: int) -> bytes:
    values = [f"key-{i % cardinality:06d}" for i in range(rows)]
    table = pa.table({"k": pa.array(values, type=pa.string())})
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=True,
        row_group_size=max(rows // 4, 1),
        data_page_size=1024,
    )
    return sink.getvalue().to_pybytes()


def _decode_once(raw: bytes):
    morsels = rp.read_parquet(raw, ["k"])
    vector_types = set()
    storage_bytes = 0
    rows = 0
    for morsel in morsels:
        vec = morsel.column(b"k")
        vector_types.add(vec.__class__.__name__)
        arr = vec.to_arrow()
        storage_bytes += _storage_size_bytes(arr)
        rows += morsel.num_rows
    return rows, storage_bytes, ",".join(sorted(vector_types))


def _measure(raw: bytes, repeat: int = 6):
    _decode_once(raw)  # warm-up
    samples = []
    rows = 0
    storage_bytes = 0
    vector_types = ""
    for _ in range(repeat):
        t0 = time.perf_counter()
        rows, storage_bytes, vector_types = _decode_once(raw)
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.mean(samples), rows, storage_bytes, vector_types


def _run_case(raw: bytes):
    return _measure(raw)


def _peak_rss_bytes() -> int:
    # macOS returns bytes, Linux returns kilobytes.
    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def _run_case_subprocess(raw_path: str) -> dict:
    cmd = [
        sys.executable,
        __file__,
        "--run-case",
        "--raw-path",
        raw_path,
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("subprocess case run produced no output")
    return json.loads(lines[-1])


def _run_case_cli(raw_path: str):
    with open(raw_path, "rb") as f:
        raw = f.read()
    decode_ms, out_rows, storage_bytes, vector_types = _run_case(raw)
    print(
        json.dumps(
            {
                "rows": out_rows,
                "decode_ms": decode_ms,
                "storage_bytes": storage_bytes,
                "vector_types": vector_types,
                "peak_rss_bytes": _peak_rss_bytes(),
            }
        )
    )


def benchmark_phase1_decode(rows: int = 400_000):
    print("=" * 122)
    print("Phase 1 Dictionary Decode Benchmark (throughput + peak RSS + output storage)")
    print("=" * 122)
    print(
        f"{'case':<24}  {'rows':>10}  {'decode(ms)':>12}  {'peak-rss(MB)':>13}  {'storage(MB)':>12}  {'vector-types':<20}"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        datasets = [
            ("low-card", 64),
            ("high-card", max(rows * 3 // 4, 1)),
        ]
        dataset_paths = {}
        for label, cardinality in datasets:
            raw_path = os.path.join(temp_dir, f"{label}.parquet.bin")
            with open(raw_path, "wb") as f:
                f.write(_build_parquet_bytes(rows, cardinality))
            dataset_paths[label] = raw_path

        for label, _ in datasets:
            print(f"\n[{label}]")
            payload = _run_case_subprocess(dataset_paths[label])
            print(
                f"{'native-dictionary':<24}  {payload['rows']:10d}  {payload['decode_ms']:12.2f}"
                f"  {payload['peak_rss_bytes'] / (1024 * 1024):13.2f}  {payload['storage_bytes'] / (1024 * 1024):12.2f}"
                f"  {payload['vector_types']:<20}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-case", action="store_true")
    parser.add_argument("--rows", type=int, default=400_000)
    parser.add_argument("--raw-path", type=str, default="")
    args = parser.parse_args()

    if args.run_case:
        if not args.raw_path:
            raise ValueError("--raw-path is required with --run-case")
        _run_case_cli(args.raw_path)
    else:
        benchmark_phase1_decode(rows=args.rows)
