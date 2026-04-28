"""
Micro-benchmark for InStr (substring search) performance.

Measures StringVector.contains() across:
- Dense, dictionary-encoded, and constant-encoded vectors
- Case-sensitive and case-insensitive searches
- Various needle lengths and string lengths
- Null and non-null data

Run with:
    python tests/performance/benchmarks/bench_instr.py
    python tests/performance/benchmarks/bench_instr.py --save-baseline
    python tests/performance/benchmarks/bench_instr.py --rows 500000
"""

from __future__ import annotations

import argparse
import json
import os
import random
import string
import statistics
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa
from draken.interop.arrow import vector_from_arrow
from draken.vectors.string_vector import StringVector

BASELINE_PATH = os.path.join(os.path.dirname(__file__), ".bench_instr_baseline.json")

DEFAULT_ROWS = 1_000_000
DEFAULT_REPEAT = 5
WARMUP = 1


def _random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits + " ", k=length))


def _build_dense_vec(n: int, avg_len: int, null_rate: float = 0.0, seed: int = 42):
    """Build a dense StringVector with random strings."""
    random.seed(seed)
    values = []
    for _ in range(n):
        if null_rate > 0 and random.random() < null_rate:
            values.append(None)
        else:
            slen = max(1, int(random.gauss(avg_len, avg_len * 0.3)))
            values.append(_random_string(slen))
    arr = pa.array(values, type=pa.string())
    return vector_from_arrow(arr)


def _build_dict_vec(n: int, dict_size: int, avg_len: int, seed: int = 42):
    """Build a dictionary-encoded StringVector."""
    random.seed(seed)
    dictionary = [_random_string(avg_len).encode() for _ in range(dict_size)]
    codes = [random.randint(0, dict_size - 1) for _ in range(n)]
    return StringVector.from_dict(codes, dictionary)


def _build_constant_vec(n: int, avg_len: int, seed: int = 42):
    """Build a constant-encoded StringVector."""
    random.seed(seed)
    value = _random_string(avg_len).encode()
    return StringVector.from_constant(value, n)


def _measure(fn, repeat: int) -> tuple[float, float]:
    """Return (min_ms, mean_ms) over repeat runs, after warmup."""
    for _ in range(WARMUP):
        fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    return min(samples), statistics.mean(samples)


def _run_case(label: str, vec, needle: bytes, ignore_case: bool, repeat: int, results: dict):
    """Run a single benchmark case."""
    n = len(vec)

    def run():
        return vec.contains(needle, ignore_case)

    best_ms, avg_ms = _measure(run, repeat)
    rows_per_sec = n / (best_ms / 1000.0)
    key = label

    print(
        f"  {label:<55s} best={best_ms:>8.2f} ms  avg={avg_ms:>8.2f} ms  "
        f"{rows_per_sec:>12,.0f} rows/sec"
    )
    results[key] = {"best_ms": best_ms, "avg_ms": avg_ms, "rows_per_sec": rows_per_sec}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--repeat", type=int, default=DEFAULT_REPEAT)
    parser.add_argument("--save-baseline", action="store_true")
    args = parser.parse_args()

    n = args.rows
    repeat = args.repeat
    results = {}

    print(f"\nInStr Benchmark: {n:,d} rows, {repeat} repeats\n")

    # --- Dense vectors ---
    print("=== Dense Encoding ===")
    for avg_len, label_len in [(16, "short"), (64, "medium"), (256, "long")]:
        vec = _build_dense_vec(n, avg_len)
        for ndl, ndl_label in [(b"x", "1-char"), (b"abcd", "4-char"), (b"abcdefghijklmnop", "16-char")]:
            _run_case(
                f"dense/{label_len}/{ndl_label}/cs",
                vec, ndl, False, repeat, results,
            )
            _run_case(
                f"dense/{label_len}/{ndl_label}/ci",
                vec, ndl, True, repeat, results,
            )

    # Dense with nulls
    print("\n=== Dense + 10% Nulls ===")
    vec_null = _build_dense_vec(n, 64, null_rate=0.1)
    _run_case("dense/medium/4-char/cs/nulls", vec_null, b"abcd", False, repeat, results)
    _run_case("dense/medium/4-char/ci/nulls", vec_null, b"abcd", True, repeat, results)

    # --- Dictionary-encoded ---
    print("\n=== Dictionary Encoding ===")
    for dict_size in [10, 100, 1000]:
        dvec = _build_dict_vec(n, dict_size, 64)
        _run_case(
            f"dict/{dict_size}-entries/4-char/cs",
            dvec, b"abcd", False, repeat, results,
        )
        _run_case(
            f"dict/{dict_size}-entries/4-char/ci",
            dvec, b"abcd", True, repeat, results,
        )

    # --- Constant-encoded ---
    print("\n=== Constant Encoding ===")
    cvec = _build_constant_vec(n, 64)
    _run_case("constant/64B/4-char/cs", cvec, b"abcd", False, repeat, results)
    _run_case("constant/64B/4-char/ci", cvec, b"abcd", True, repeat, results)

    # --- Compare with baseline ---
    if os.path.exists(BASELINE_PATH) and not args.save_baseline:
        print("\n=== Comparison vs Baseline ===")
        with open(BASELINE_PATH) as f:
            baseline = json.load(f)
        for key in sorted(results):
            if key in baseline:
                old = baseline[key]["best_ms"]
                new = results[key]["best_ms"]
                if old > 0:
                    ratio = old / new
                    marker = "FASTER" if ratio > 1.05 else ("SLOWER" if ratio < 0.95 else "~same")
                    sym = "+" if ratio > 1.05 else ("-" if ratio < 0.95 else " ")
                    print(f"  {key:<55s} {ratio:>5.2f}x {marker} {sym}")

    # --- Save baseline ---
    if args.save_baseline:
        with open(BASELINE_PATH, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nBaseline saved to {BASELINE_PATH}")

    print()


if __name__ == "__main__":
    main()
