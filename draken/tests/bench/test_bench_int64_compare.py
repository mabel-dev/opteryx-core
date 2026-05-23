"""
Microbench for int64 compare_scalar / compare_vector.

Run standalone:
    python draken/tests/bench/test_bench_int64_compare.py

Or with --json for machine-readable output:
    python draken/tests/bench/test_bench_int64_compare.py --json

Measures throughput (elements/second) for:
  - compare_scalar (nonnull, nullable) at N_LARGE
  - compare_vector (nonnull, nullable) at N_LARGE
  - compare_scalar constant shape at N_LARGE
  - compare_scalar dict shape at N_LARGE
"""

import json
import os
import sys
import timeit
from dataclasses import dataclass, field
from typing import Callable

if __name__ == "__main__":
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if _root not in sys.path:
        sys.path.insert(0, _root)

import draken.draken_native as dn

WARMUP_REPS  = 3
TIMING_REPS  = 20
N_LARGE      = 100_000
EQ           = 0


@dataclass
class BenchCase:
    name: str
    setup: Callable
    fn: Callable
    n_elements: int
    waiver: str = ""  # non-empty = explicit waiver text


def run_case(case: BenchCase) -> dict:
    state = case.setup()
    for _ in range(WARMUP_REPS):
        case.fn(state)
    t = timeit.timeit(lambda: case.fn(state), number=TIMING_REPS)
    throughput = (case.n_elements * TIMING_REPS) / t

    return {
        "name": case.name,
        "n": case.n_elements,
        "Mrows_per_s": throughput / 1e6,
        "waiver": case.waiver,
    }


# ---------------------------------------------------------------------------
# Bench setup helpers
# ---------------------------------------------------------------------------

def _data_nonnull(n):
    return list(range(n))

def _data_nullable(n):
    return [i if i % 10 != 0 else None for i in range(n)]


# ---------------------------------------------------------------------------
# Bench cases
# ---------------------------------------------------------------------------

BENCH_CASES = [
    BenchCase(
        name="compare_scalar_nonnull_dense",
        setup=lambda: dn.vector_from_sequence(_data_nonnull(N_LARGE)),
        fn=lambda v: v.compare_scalar(N_LARGE // 2, EQ),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="compare_scalar_nullable_dense",
        setup=lambda: dn.vector_from_sequence(_data_nullable(N_LARGE)),
        fn=lambda v: v.compare_scalar(N_LARGE // 2, EQ),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="compare_vector_nonnull_dense",
        setup=lambda: (
            dn.vector_from_sequence(_data_nonnull(N_LARGE)),
            dn.vector_from_sequence(list(range(N_LARGE - 1, -1, -1))),
        ),
        fn=lambda pair: pair[0].compare_vector(pair[1], EQ),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="compare_scalar_constant_shape",
        setup=lambda: dn.vector_from_constant(42, N_LARGE),
        fn=lambda v: v.compare_scalar(42, EQ),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="compare_scalar_dict_shape",
        setup=lambda: dn.vector_from_dict(
            list(range(100)),
            [i % 100 for i in range(N_LARGE)],
        ),
        fn=lambda v: v.compare_scalar(50, EQ),
        n_elements=N_LARGE,
    ),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main():
    emit_json = "--json" in sys.argv
    results = []
    for case in BENCH_CASES:
        r = run_case(case)
        results.append(r)
        if not emit_json:
            waiver_str = f"  [WAIVER: {r['waiver']}]" if r["waiver"] else ""
            print(
                f"{r['name']:45s}  "
                f"{r['Mrows_per_s']:7.1f} M/s"
                f"{waiver_str}"
            )

    if emit_json:
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
