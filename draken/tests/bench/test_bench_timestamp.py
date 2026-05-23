"""
Per-op microbench rig for draken TIMESTAMP64 vectors.

Run standalone:
    python draken/tests/bench/test_bench_timestamp.py

Or with JSON output:
    python draken/tests/bench/test_bench_timestamp.py --json

Platform targets:
    Dev (ARM/NEON):   Apple Silicon M-series
    Prod (x86/AVX2):  GCP Cloud Run (x86_64)

Shape coverage:
    dense × nullable / non-nullable — ingestion + readback
    Operations: hash, min, max, compare_scalar, between, in_list,
                take, materialize, compress
"""

import json
import os
import sys
import timeit
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

if __name__ == "__main__":
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if _root not in sys.path:
        sys.path.insert(0, _root)

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Bench infrastructure
# ---------------------------------------------------------------------------

WARMUP_REPS = 3
TIMING_REPS = 20
N_SMALL = 1_000
N_LARGE = 100_000

EQ = 0


@dataclass
class BenchCase:
    name: str
    setup: Callable[[], None]
    fn: Callable[[], None]
    n_elements: int
    reps: int = TIMING_REPS


@dataclass
class BenchResult:
    name: str
    n_elements: int
    reps: int
    total_s: float
    per_call_us: float
    throughput_mrows_s: float


def _run_case(case: BenchCase) -> BenchResult:
    case.setup()
    for _ in range(WARMUP_REPS):
        case.fn()
    t = timeit.timeit(case.fn, number=case.reps)
    per_call = t / case.reps
    throughput = (case.n_elements / per_call) / 1e6
    return BenchResult(
        name=case.name,
        n_elements=case.n_elements,
        reps=case.reps,
        total_s=t,
        per_call_us=per_call * 1e6,
        throughput_mrows_s=throughput,
    )


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------

_BASE = datetime(2000, 1, 1, tzinfo=timezone.utc)
_PIVOT = _BASE + timedelta(hours=N_LARGE // 4)

_fixtures: dict = {}


def _make_fixtures():
    _fixtures["dense_nonnull_small"] = [_BASE + timedelta(hours=i) for i in range(N_SMALL)]
    _fixtures["dense_nullable_small"] = [
        None if i % 7 == 0 else _BASE + timedelta(hours=i) for i in range(N_SMALL)
    ]
    _fixtures["dense_nonnull_large"] = [_BASE + timedelta(hours=i) for i in range(N_LARGE)]
    _fixtures["dense_nullable_large"] = [
        None if i % 7 == 0 else _BASE + timedelta(hours=i) for i in range(N_LARGE)
    ]

    _fixtures["vec_nonnull_small"]  = dn.vector_timestamp_from_sequence(_fixtures["dense_nonnull_small"])
    _fixtures["vec_nullable_small"] = dn.vector_timestamp_from_sequence(_fixtures["dense_nullable_small"])
    _fixtures["vec_nonnull_large"]  = dn.vector_timestamp_from_sequence(_fixtures["dense_nonnull_large"])
    _fixtures["vec_nullable_large"] = dn.vector_timestamp_from_sequence(_fixtures["dense_nullable_large"])


# ---------------------------------------------------------------------------
# Case builders
# ---------------------------------------------------------------------------

def _ingest_readback_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["seq"] = _fixtures[key]

    def fn():
        v = dn.vector_timestamp_from_sequence(state["seq"])
        _ = v.to_pylist()

    return BenchCase(name=f"ts_ingest_readback/{key}", setup=setup, fn=fn, n_elements=n)


def _ingest_only_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["seq"] = _fixtures[key]

    def fn():
        _ = dn.vector_timestamp_from_sequence(state["seq"])

    return BenchCase(name=f"ts_ingest/{key}", setup=setup, fn=fn, n_elements=n)


def _readback_only_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = dn.vector_timestamp_from_sequence(_fixtures[key])

    def fn():
        _ = state["vec"].to_pylist()

    return BenchCase(name=f"ts_readback/{key}", setup=setup, fn=fn, n_elements=n)


def _hash_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].hash()

    return BenchCase(name=f"ts_hash/{key}", setup=setup, fn=fn, n_elements=n)


def _min_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].min()

    return BenchCase(name=f"ts_min/{key}", setup=setup, fn=fn, n_elements=n)


def _max_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].max()

    return BenchCase(name=f"ts_max/{key}", setup=setup, fn=fn, n_elements=n)


def _compare_scalar_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].compare_scalar(_PIVOT, EQ)

    return BenchCase(name=f"ts_compare_scalar/{key}", setup=setup, fn=fn, n_elements=n)


def _between_case(key: str, n: int) -> BenchCase:
    lo = _BASE
    hi = _BASE + timedelta(hours=N_LARGE // 2)
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].between(lo, hi)

    return BenchCase(name=f"ts_between/{key}", setup=setup, fn=fn, n_elements=n)


def _in_list_case(key: str, n: int) -> BenchCase:
    search_set = [_BASE + timedelta(hours=i * 100) for i in range(10)]
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].in_list(search_set)

    return BenchCase(name=f"ts_in_list/{key}", setup=setup, fn=fn, n_elements=n)


def _take_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]
        state["indices"] = list(range(n))

    def fn():
        _ = state["vec"].take(state["indices"])

    return BenchCase(name=f"ts_take/{key}", setup=setup, fn=fn, n_elements=n)


def _materialize_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].materialize()

    return BenchCase(name=f"ts_materialize/{key}", setup=setup, fn=fn, n_elements=n)


def _compress_case(key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[key]

    def fn():
        _ = state["vec"].compress()

    return BenchCase(name=f"ts_compress/{key}", setup=setup, fn=fn, n_elements=n)


BENCH_CASES: list[BenchCase] = [
    # ingest + readback (round-trip)
    _ingest_readback_case("dense_nonnull_small",   N_SMALL),
    _ingest_readback_case("dense_nonnull_large",   N_LARGE),
    _ingest_readback_case("dense_nullable_large",  N_LARGE),
    # ingest only
    _ingest_only_case("dense_nonnull_large",   N_LARGE),
    _ingest_only_case("dense_nullable_large",  N_LARGE),
    # readback only
    _readback_only_case("dense_nonnull_large",   N_LARGE),
    _readback_only_case("dense_nullable_large",  N_LARGE),
    # hash
    _hash_case("vec_nonnull_small",   N_SMALL),
    _hash_case("vec_nonnull_large",   N_LARGE),
    _hash_case("vec_nullable_large",  N_LARGE),
    # min / max
    _min_case("vec_nonnull_large",  N_LARGE),
    _max_case("vec_nonnull_large",  N_LARGE),
    # compare_scalar
    _compare_scalar_case("vec_nonnull_large",  N_LARGE),
    _compare_scalar_case("vec_nullable_large", N_LARGE),
    # between
    _between_case("vec_nonnull_large",  N_LARGE),
    _between_case("vec_nullable_large", N_LARGE),
    # in_list
    _in_list_case("vec_nonnull_large",  N_LARGE),
    # gather ops
    _take_case("vec_nonnull_small",  N_SMALL),
    _take_case("vec_nonnull_large",  N_LARGE),
    _materialize_case("vec_nonnull_large",  N_LARGE),
    _materialize_case("vec_nullable_large", N_LARGE),
    _compress_case("vec_nonnull_large",  N_LARGE),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(emit_json: bool = False) -> list[BenchResult]:
    _make_fixtures()
    results = []
    for case in BENCH_CASES:
        r = _run_case(case)
        results.append(r)
    if emit_json:
        payload = [
            {
                "name": r.name,
                "n_elements": r.n_elements,
                "reps": r.reps,
                "per_call_us": round(r.per_call_us, 2),
                "throughput_mrows_s": round(r.throughput_mrows_s, 3),
            }
            for r in results
        ]
        print(json.dumps(payload, indent=2))
    else:
        col_w = max(len(r.name) for r in results) + 2
        print(f"\n{'Benchmark':<{col_w}}  {'rows':>8}  {'µs/call':>10}  {'Mrows/s':>10}")
        print("-" * (col_w + 34))
        for r in results:
            print(
                f"{r.name:<{col_w}}  {r.n_elements:>8,}  "
                f"{r.per_call_us:>10.1f}  {r.throughput_mrows_s:>10.3f}"
            )
        print()
    return results


# ---------------------------------------------------------------------------
# Pytest smoke
# ---------------------------------------------------------------------------

def test_bench_runs_without_error():
    """Smoke: harness executes, produces one result per case, timings are positive."""
    _make_fixtures()
    results = []
    for case in BENCH_CASES:
        fast_case = BenchCase(
            name=case.name,
            setup=case.setup,
            fn=case.fn,
            n_elements=case.n_elements,
            reps=1,
        )
        r = _run_case(fast_case)
        assert r.per_call_us > 0, f"non-positive timing for {case.name}"
        assert r.throughput_mrows_s > 0
        results.append(r)
    assert len(results) == len(BENCH_CASES)


if __name__ == "__main__":
    emit_json = "--json" in sys.argv
    run(emit_json=emit_json)
