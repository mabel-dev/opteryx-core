"""
Microbench for int64 between and in_list.

Run standalone:
    python draken/tests/bench/test_bench_int64_predicates.py

Or with --json for machine-readable output:
    python draken/tests/bench/test_bench_int64_predicates.py --json

Measures throughput (elements/second) for:
  - between (nonnull, nullable) at N_LARGE — all 4 inclusivity combos
  - in_list (nonnull, nullable) at N_LARGE — small, medium, large sets
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


@dataclass
class BenchCase:
    name: str
    setup: Callable
    fn: Callable
    n_elements: int
    waiver: str = ""


def run_case(case: BenchCase) -> dict:
    state = case.setup()
    for _ in range(WARMUP_REPS):
        case.fn(state)
    t = timeit.timeit(lambda: case.fn(state), number=TIMING_REPS)
    throughput = (case.n_elements * TIMING_REPS) / t

    return {
        "name": case.name,
        "n": case.n_elements,
        "throughput": throughput,
        "waiver": case.waiver,
    }


def fmt(r: dict) -> str:
    m = r["throughput"] / 1e6
    w = f" [WAIVER: {r['waiver']}]" if r["waiver"] else ""
    return f"  {r['name']:<44}  {m:8.1f}M/s{w}"


# ---------------------------------------------------------------------------
# Data setup
# ---------------------------------------------------------------------------

data_nonnull = list(range(N_LARGE))
data_nullable = [i if i % 7 != 0 else None for i in range(N_LARGE)]
set_small  = list(range(0, N_LARGE, 1000))   # 100 elements
set_medium = list(range(0, N_LARGE, 100))    # 1 000 elements
set_large  = list(range(0, N_LARGE, 10))     # 10 000 elements

LO, HI = N_LARGE // 4, N_LARGE * 3 // 4

CASES = [
    # ------------------------------------------------------------------
    # between — nonnull, all 4 combos
    # ------------------------------------------------------------------
    BenchCase(
        name="between_closed_closed_nonnull",
        setup=lambda: dn.vector_from_sequence(data_nonnull),
        fn=lambda v: v.between(LO, HI, True, True),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="between_open_open_nonnull",
        setup=lambda: dn.vector_from_sequence(data_nonnull),
        fn=lambda v: v.between(LO, HI, False, False),
        n_elements=N_LARGE,
    ),
    # ------------------------------------------------------------------
    # between — nullable
    # ------------------------------------------------------------------
    BenchCase(
        name="between_closed_closed_nullable",
        setup=lambda: dn.vector_from_sequence(data_nullable),
        fn=lambda v: v.between(LO, HI, True, True),
        n_elements=N_LARGE,
    ),
    # ------------------------------------------------------------------
    # in_list — nonnull, set sizes
    # ------------------------------------------------------------------
    BenchCase(
        name="in_list_small_set_nonnull",
        setup=lambda: dn.vector_from_sequence(data_nonnull),
        fn=lambda v: v.in_list(set_small),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="in_list_medium_set_nonnull",
        setup=lambda: dn.vector_from_sequence(data_nonnull),
        fn=lambda v: v.in_list(set_medium),
        n_elements=N_LARGE,
    ),
    BenchCase(
        name="in_list_large_set_nonnull",
        setup=lambda: dn.vector_from_sequence(data_nonnull),
        fn=lambda v: v.in_list(set_large),
        n_elements=N_LARGE,
    ),
    # ------------------------------------------------------------------
    # in_list — nullable
    # ------------------------------------------------------------------
    BenchCase(
        name="in_list_medium_set_nullable",
        setup=lambda: dn.vector_from_sequence(data_nullable),
        fn=lambda v: v.in_list(set_medium),
        n_elements=N_LARGE,
    ),
]


def run_all(as_json: bool = False):
    results = [run_case(c) for c in CASES]

    if as_json:
        print(json.dumps(results, indent=2))
    else:
        print(f"\nPredicate microbench  (N={N_LARGE:,})")
        print("=" * 80)
        for r in results:
            print(fmt(r))
        print()


if __name__ == "__main__":
    as_json = "--json" in sys.argv
    run_all(as_json)
