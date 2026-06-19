#!/usr/bin/env python3
"""
Benchmark rugo.csv vs PyArrow CSV reader.

Two datasets:
  narrow  — 3-col, 1M rows  (synthetic_abs_bench.csv — worst case for rugo)
  wide    — 50-col, 200k rows (generated inline — shows projection/predicate wins)

Query shapes:
  SELECT *
  SELECT 2 cols
  WHERE ~10% selectivity
  WHERE ~1% selectivity
  SELECT 2 cols WHERE predicate (predicate col not in projection)

Run from repo root:
  python dev/bench_rugo_csv.py
"""

import gc
import io
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

import draken  # noqa: F401 — must precede rugo.csv to resolve draken symbols
import rugo.csv as rc

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NARROW_CSV = os.path.join(REPO_ROOT, "testdata", "synthetic_abs_bench.csv")

REPEATS = 7
WARMUP = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def median(times):
    s = sorted(times)
    return s[len(s) // 2]


def bench_fn(fn, repeats=REPEATS, warmup=WARMUP):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(repeats):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return median(times)


def print_header(title, size_mb, num_rows, num_cols):
    print(f"\n{'=' * 68}")
    print(f"  {title}")
    print(f"  {size_mb:.1f} MB  |  {num_rows:,} rows  |  {num_cols} cols")
    print(f"{'=' * 68}")
    print(f"  {'Query':<44} {'rugo':>8} {'PyArrow':>8}  {'ratio':>6}")
    print(f"  {'-' * 44} {'-' * 8} {'-' * 8}  {'-' * 6}")


def print_row(label, rugo_ms, pa_ms):
    ratio = pa_ms / rugo_ms if rugo_ms > 0 else float("inf")
    winner = "<" if rugo_ms < pa_ms else ">"
    print(f"  {label:<44} {rugo_ms:>7.1f}ms {pa_ms:>7.1f}ms  {ratio:>5.1f}x {winner}")


# ---------------------------------------------------------------------------
# Generate wide CSV
# ---------------------------------------------------------------------------


def generate_wide_csv(num_rows=200_000, num_extra_cols=48):
    """Generate a wide CSV: id (int), score (int), then N filler string columns."""
    import random

    rng = random.Random(42)
    cols = ["id", "score"] + [f"col_{i:02d}" for i in range(num_extra_cols)]
    header = ",".join(cols)
    rows = [header]
    words = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"]
    for i in range(num_rows):
        score = rng.randint(0, 9999)
        fillers = ",".join(rng.choice(words) for _ in range(num_extra_cols))
        rows.append(f"{i},{score},{fillers}")
    return "\n".join(rows).encode()


# ---------------------------------------------------------------------------
# Run one dataset
# ---------------------------------------------------------------------------


def run(label, data, pred_col, two_cols):
    size_mb = len(data) / 1024 / 1024

    # Discover shape
    header = data[: data.index(b"\n")].decode()
    all_cols = [c.strip() for c in header.split(",")]
    num_cols = len(all_cols)

    # Predicate thresholds from rugo read of pred_col
    probe = rc.read_csv(data, columns=[pred_col])
    vals = sorted(v for v in probe["columns"][0].to_pylist() if v is not None)
    num_rows = len(vals)
    thresh_10 = vals[int(num_rows * 0.90)]
    thresh_1 = vals[int(num_rows * 0.99)]

    print_header(label, size_mb, num_rows, num_cols)

    # SELECT *
    def rugo_star():
        rc.read_csv(data)

    def pa_star():
        pa_csv.read_csv(io.BytesIO(data))

    print_row("SELECT *", bench_fn(rugo_star), bench_fn(pa_star))

    # SELECT 2 cols
    col_label = f"SELECT {two_cols[0]}, {two_cols[1]}"

    def rugo_2col():
        rc.read_csv(data, columns=two_cols)

    def pa_2col():
        pa_csv.read_csv(
            io.BytesIO(data),
            convert_options=pa_csv.ConvertOptions(include_columns=two_cols),
        )

    print_row(col_label, bench_fn(rugo_2col), bench_fn(pa_2col))

    # WHERE 10% selectivity
    def rugo_10():
        rc.read_csv(data, predicates=[(pred_col, ">", thresh_10)])

    def pa_10():
        t = pa_csv.read_csv(io.BytesIO(data))
        pc.filter(t, pc.greater(t.column(pred_col), thresh_10))

    print_row(f"SELECT * WHERE {pred_col} > P90 (~10% pass)", bench_fn(rugo_10), bench_fn(pa_10))

    # WHERE 1% selectivity
    def rugo_1():
        rc.read_csv(data, predicates=[(pred_col, ">", thresh_1)])

    def pa_1():
        t = pa_csv.read_csv(io.BytesIO(data))
        pc.filter(t, pc.greater(t.column(pred_col), thresh_1))

    print_row(f"SELECT * WHERE {pred_col} > P99 (~1% pass)", bench_fn(rugo_1), bench_fn(pa_1))

    # SELECT 2 cols WHERE pred not in projection
    def rugo_pp():
        rc.read_csv(data, columns=two_cols, predicates=[(pred_col, ">", thresh_10)])

    def pa_pp():
        t = pa_csv.read_csv(io.BytesIO(data))
        t = pc.filter(t, pc.greater(t.column(pred_col), thresh_10))
        t.select(two_cols)

    print_row(f"SELECT 2 cols WHERE {pred_col} > P90", bench_fn(rugo_pp), bench_fn(pa_pp))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print(f"CSV benchmark: rugo vs PyArrow")
    print(f"Python {sys.version.split()[0]}, PyArrow {pa.__version__}")
    print(f"Median of {REPEATS} runs (after {WARMUP} warmup)")
    print(f"< = rugo faster   > = PyArrow faster")

    # Narrow: 3-col file (worst case — minimal projection benefit)
    if os.path.exists(NARROW_CSV):
        with open(NARROW_CSV, "rb") as f:
            narrow = f.read()
        run("Narrow (3 cols, 1M rows) — synthetic_abs_bench.csv", narrow, "id", ["id", "value"])
    else:
        print(f"\nSKIP narrow: {NARROW_CSV} not found")

    # Wide: 50-col generated (shows projection/predicate advantage)
    print("\n  Generating wide CSV (50 cols, 200k rows)...", end=" ", flush=True)
    wide = generate_wide_csv(num_rows=200_000, num_extra_cols=48)
    print(f"{len(wide) // 1024 // 1024} MB")
    run("Wide (50 cols, 200k rows) — generated", wide, "score", ["id", "score"])

    print()


if __name__ == "__main__":
    main()
