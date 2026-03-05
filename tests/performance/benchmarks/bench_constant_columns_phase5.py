"""
Phase 5 constant-column benchmark harness.

Measures:
1. Group-by runtime with constant key vs materialized repeated key.
2. Predicate runtime (`k = 'g'`) with constant key vs materialized repeated key.
3. DRKM spill payload size/time with constant key vs materialized repeated key.

Run with:
    python tests/performance/benchmarks/bench_constant_columns_phase5.py
"""

import argparse
import json
import os
import resource
import statistics
import subprocess
import sys
import time

import pyarrow as pa
from orso.types import OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.interop.arrow import vector_from_arrow
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.storage.morsel_io import write_morsel
from opteryx.draken.vectors.constant_vector import from_scalar as constant_from_scalar
from opteryx.managers.expression.ops import filter_operations
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
from opteryx.operators.shuffle import AggregationSpec


def _peak_rss_bytes() -> int:
    # macOS returns bytes, Linux returns kilobytes.
    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return rss
    return rss * 1024


def _build_case(case: str, rows: int):
    value_arr = pa.array(range(rows), type=pa.int64())

    if case == "constant":
        key_vec = constant_from_scalar("g", rows, dtype=pa.string())
        morsel = Morsel.from_vectors(["k", "v"], [key_vec, vector_from_arrow(value_arr)])
        return morsel, key_vec

    if case == "materialized":
        key_arr = pa.array(["g"] * rows, type=pa.string())
        morsel = Morsel.from_arrow(pa.table({"k": key_arr, "v": value_arr}))
        return morsel, key_arr

    raise ValueError(f"unknown case `{case}`")


def _bench_groupby(morsel: Morsel, repeat: int) -> float:
    timings = []
    for _ in range(repeat):
        op = ShuffleGroupByOperationV2(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        )
        t0 = time.perf_counter()
        op.ingest(morsel)
        result = op.finalize()
        timings.append((time.perf_counter() - t0) * 1000.0)
        if result.num_rows != 1:
            raise RuntimeError("unexpected group count in benchmark")
    return statistics.mean(timings)


def _bench_predicate(key_vector, rows: int, repeat: int) -> float:
    timings = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        mask = filter_operations(
            key_vector,
            OrsoTypes.VARCHAR,
            "Eq",
            ["g"],
            OrsoTypes.VARCHAR,
        )
        timings.append((time.perf_counter() - t0) * 1000.0)
        if len(mask) != rows:
            raise RuntimeError("unexpected predicate mask length in benchmark")
    return statistics.mean(timings)


def _bench_spill(morsel: Morsel, repeat: int):
    timings = []
    payload_size = 0
    for _ in range(repeat):
        t0 = time.perf_counter()
        payload = write_morsel(None, morsel, {"codec_default": "none", "checksum_enabled": False})
        timings.append((time.perf_counter() - t0) * 1000.0)
        payload_size = len(payload)
    return statistics.mean(timings), payload_size


def _run_case(case: str, rows: int, repeat: int):
    morsel, key_vector = _build_case(case, rows)
    groupby_ms = _bench_groupby(morsel, repeat)
    predicate_ms = _bench_predicate(key_vector, rows, repeat)
    spill_ms, spill_bytes = _bench_spill(morsel, repeat)
    return {
        "case": case,
        "rows": rows,
        "repeat": repeat,
        "key_vector_type": key_vector.__class__.__name__,
        "groupby_ms": groupby_ms,
        "predicate_ms": predicate_ms,
        "spill_ms": spill_ms,
        "spill_bytes": spill_bytes,
        "peak_rss_bytes": _peak_rss_bytes(),
    }


def _run_case_subprocess(case: str, rows: int, repeat: int) -> dict:
    cmd = [
        sys.executable,
        __file__,
        "--run-case",
        "--case",
        case,
        "--rows",
        str(rows),
        "--repeat",
        str(repeat),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("case subprocess produced no output")
    return json.loads(lines[-1])


def benchmark(rows: int = 250_000, repeat: int = 5):
    print("=" * 130)
    print("Phase 5 Constant-Column Benchmark (group-by + predicate + spill, constant vs materialized)")
    print("=" * 130)
    print(
        f"{'case':<14}  {'rows':>10}  {'groupby(ms)':>12}  {'predicate(ms)':>14}"
        f"  {'spill(ms)':>10}  {'spill(MB)':>10}  {'peak-rss(MB)':>13}  {'key-vector':<16}"
    )

    constant = _run_case_subprocess("constant", rows, repeat)
    materialized = _run_case_subprocess("materialized", rows, repeat)

    for payload in (constant, materialized):
        print(
            f"{payload['case']:<14}  {payload['rows']:10d}  {payload['groupby_ms']:12.2f}"
            f"  {payload['predicate_ms']:14.2f}  {payload['spill_ms']:10.2f}"
            f"  {payload['spill_bytes'] / (1024 * 1024):10.2f}  {payload['peak_rss_bytes'] / (1024 * 1024):13.2f}"
            f"  {payload['key_vector_type']:<16}"
        )

    print("\nRelative (materialized / constant):")
    print(f"  groupby speedup:   {materialized['groupby_ms'] / constant['groupby_ms']:.2f}x")
    print(f"  predicate speedup: {materialized['predicate_ms'] / constant['predicate_ms']:.2f}x")
    print(f"  spill bytes ratio: {materialized['spill_bytes'] / constant['spill_bytes']:.2f}x")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-case", action="store_true")
    parser.add_argument("--case", choices=("constant", "materialized"), default="constant")
    parser.add_argument("--rows", type=int, default=250_000)
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args()

    if args.run_case:
        print(json.dumps(_run_case(args.case, args.rows, args.repeat)))
    else:
        benchmark(rows=args.rows, repeat=args.repeat)
