"""
Per-op microbench rig for draken int64 vectors.

Run standalone:
    python draken/tests/bench/test_bench_int64.py

Or as part of a benchmark sweep (hook for make clickbench):
    python draken/tests/bench/test_bench_int64.py --json  # emits JSON to stdout

Platform targets:
    Dev (ARM/NEON):   Apple Silicon M-series
    Prod (x86/AVX2):  GCP Cloud Run (x86_64)

    NEON and AVX2 paths are selected at compile time.  This harness is
    platform-agnostic — it reports wall-clock throughput, which the SIMD
    specialist can then compare across platforms.

Shape coverage (current — Milestone A.5):
    dense × nullable / non-nullable (ingestion + to_pylist readback)

    dict and constant shapes are placeholders: add cases when those shapes
    are wired into vector_from_sequence (Milestone C/D).

Adding a new op:
    Append one BenchCase to BENCH_CASES.  The harness discovers it automatically.
"""

import json
import os
import sys
import timeit
from dataclasses import dataclass, field
from typing import Callable

# When run as a standalone script the script directory ends up on sys.path rather
# than the project root.  Walk up three levels (bench→tests→draken→root) so that
# `import draken.draken_native` resolves correctly either way.
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


@dataclass
class BenchCase:
    name: str
    setup: Callable[[], None]  # called once before timing; returns setup state
    fn: Callable[[], None]     # the callable being timed
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
    # Warmup — fills caches, JIT-warms any Python bytecode.
    for _ in range(WARMUP_REPS):
        case.fn()
    t = timeit.timeit(case.fn, number=case.reps)
    per_call = t / case.reps
    throughput = (case.n_elements / per_call) / 1e6  # million rows / sec
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

_fixtures: dict[str, list] = {}


def _make_fixtures():
    _fixtures["dense_nonnull_small"] = list(range(N_SMALL))
    _fixtures["dense_nullable_small"] = [
        None if i % 7 == 0 else i for i in range(N_SMALL)
    ]
    _fixtures["dense_nonnull_large"] = list(range(N_LARGE))
    _fixtures["dense_nullable_large"] = [
        None if i % 7 == 0 else i for i in range(N_LARGE)
    ]

    # Pre-built vectors (amortise ingestion cost for op benchmarks).
    _fixtures["vec_nonnull_small"]  = dn.vector_from_sequence(_fixtures["dense_nonnull_small"])
    _fixtures["vec_nullable_small"] = dn.vector_from_sequence(_fixtures["dense_nullable_small"])
    _fixtures["vec_nonnull_large"]  = dn.vector_from_sequence(_fixtures["dense_nonnull_large"])
    _fixtures["vec_nullable_large"] = dn.vector_from_sequence(_fixtures["dense_nullable_large"])

    # Constant and dict shapes (Milestone C.2).
    _fixtures["vec_constant_small"] = dn.vector_from_constant(42, N_SMALL)
    _fixtures["vec_constant_large"] = dn.vector_from_constant(42, N_LARGE)

    # Dict: 16 unique values repeated across N rows — high-cardinality dict.
    _dict_vals = list(range(16)) * (N_SMALL // 16)
    _fixtures["vec_dict_small"] = dn.vector_from_sequence(_dict_vals)
    _dict_vals_large = list(range(16)) * (N_LARGE // 16)
    _fixtures["vec_dict_large"] = dn.vector_from_sequence(_dict_vals_large)


# ---------------------------------------------------------------------------
# Bench cases
# ---------------------------------------------------------------------------
# Structure: one BenchCase per (shape × nullable × size × op).
# Each case is self-contained — setup() prepares the input; fn() is timed.
#
# To add an op at Milestone C:
#   1. Build the vector in setup() and store it in a closure variable.
#   2. Call the new op in fn().
#   3. Append the BenchCase to BENCH_CASES below.


def _ingest_readback_case(fixture_key: str, n: int) -> BenchCase:
    """Ingestion (vector_from_sequence) + full readback (to_pylist)."""
    state: dict = {}

    def setup():
        state["seq"] = _fixtures[fixture_key]

    def fn():
        v = dn.vector_from_sequence(state["seq"])
        _ = v.to_pylist()

    return BenchCase(name=f"ingest_readback/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _ingest_only_case(fixture_key: str, n: int) -> BenchCase:
    """Ingestion only (vector_from_sequence) — isolates the write path."""
    state: dict = {}

    def setup():
        state["seq"] = _fixtures[fixture_key]

    def fn():
        _ = dn.vector_from_sequence(state["seq"])

    return BenchCase(name=f"ingest_only/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _readback_only_case(fixture_key: str, n: int) -> BenchCase:
    """Readback only (to_pylist) — isolates the read path."""
    state: dict = {}

    def setup():
        state["vec"] = dn.vector_from_sequence(_fixtures[fixture_key])

    def fn():
        _ = state["vec"].to_pylist()

    return BenchCase(name=f"readback_only/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _hash_case(fixture_key: str, n: int) -> BenchCase:
    """hash() on a pre-built vector — isolates the hash kernel."""
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].hash()

    return BenchCase(name=f"hash/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _sum_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].sum()

    return BenchCase(name=f"sum/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _min_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].min()

    return BenchCase(name=f"min/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _max_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].max()

    return BenchCase(name=f"max/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _binary_arith_case(op: str, fixture_key: str, n: int) -> BenchCase:
    """Binary vector × vector arithmetic."""
    state: dict = {}

    def setup():
        state["a"] = _fixtures[fixture_key]
        state["b"] = _fixtures[fixture_key]

    def fn():
        _ = getattr(state["a"], op)(state["b"])

    return BenchCase(name=f"{op}/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _neg_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].neg()

    return BenchCase(name=f"neg/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _take_case(fixture_key: str, n: int) -> BenchCase:
    """take() with a sequential index list (best-case memory access pattern)."""
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]
        state["indices"] = list(range(n))

    def fn():
        _ = state["vec"].take(state["indices"])

    return BenchCase(name=f"take/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _materialize_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].materialize()

    return BenchCase(name=f"materialize/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _compress_case(fixture_key: str, n: int) -> BenchCase:
    state: dict = {}

    def setup():
        state["vec"] = _fixtures[fixture_key]

    def fn():
        _ = state["vec"].compress()

    return BenchCase(name=f"compress/{fixture_key}", setup=setup, fn=fn, n_elements=n)


def _constant_factory_case(value: int, n: int) -> BenchCase:
    """vector_from_constant() — allocates a constant-shape vector."""
    def setup():
        pass

    def fn():
        _ = dn.vector_from_constant(value, n)

    return BenchCase(name=f"constant_factory/n={n}", setup=setup, fn=fn, n_elements=n)

BENCH_CASES: list[BenchCase] = [
    # dense × non-nullable
    _ingest_readback_case("dense_nonnull_small", N_SMALL),
    _ingest_only_case("dense_nonnull_small", N_SMALL),
    _readback_only_case("dense_nonnull_small", N_SMALL),
    _ingest_readback_case("dense_nonnull_large", N_LARGE),
    _ingest_only_case("dense_nonnull_large", N_LARGE),
    _readback_only_case("dense_nonnull_large", N_LARGE),
    # dense × nullable
    _ingest_readback_case("dense_nullable_small", N_SMALL),
    _ingest_only_case("dense_nullable_small", N_SMALL),
    _readback_only_case("dense_nullable_small", N_SMALL),
    _ingest_readback_case("dense_nullable_large", N_LARGE),
    _ingest_only_case("dense_nullable_large", N_LARGE),
    _readback_only_case("dense_nullable_large", N_LARGE),
    # hash — Milestone C.1
    _hash_case("vec_nonnull_small",  N_SMALL),
    _hash_case("vec_nullable_small", N_SMALL),
    _hash_case("vec_nonnull_large",  N_LARGE),
    _hash_case("vec_nullable_large", N_LARGE),
    # Milestone C.2 — reductions
    _sum_case("vec_nonnull_small",  N_SMALL),
    _sum_case("vec_nonnull_large",  N_LARGE),
    _sum_case("vec_nullable_large", N_LARGE),
    _min_case("vec_nonnull_large",  N_LARGE),
    _max_case("vec_nonnull_large",  N_LARGE),
    # Milestone C.2 — arithmetic
    _binary_arith_case("add", "vec_nonnull_small",  N_SMALL),
    _binary_arith_case("add", "vec_nonnull_large",  N_LARGE),
    _binary_arith_case("mul", "vec_nonnull_large",  N_LARGE),
    _neg_case("vec_nonnull_large",  N_LARGE),
    _neg_case("vec_nullable_large", N_LARGE),
    # Milestone C.2 — gather
    _take_case("vec_nonnull_small",  N_SMALL),
    _take_case("vec_nonnull_large",  N_LARGE),
    _materialize_case("vec_nonnull_large",   N_LARGE),
    _materialize_case("vec_nullable_large",  N_LARGE),
    _materialize_case("vec_constant_large",  N_LARGE),
    _compress_case("vec_nonnull_small",  N_SMALL),
    _compress_case("vec_nonnull_large",  N_LARGE),
    # Milestone C.2 — constant/dict shape factories
    _constant_factory_case(0,  N_SMALL),
    _constant_factory_case(0,  N_LARGE),
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
# Pytest smoke (confirm the harness runs without error)
# ---------------------------------------------------------------------------


def test_bench_runs_without_error():
    """Smoke test: the bench harness executes and produces one result per case."""
    _make_fixtures()
    results = []
    for case in BENCH_CASES:
        # Use a single repetition so CI doesn't spend time on perf numbers.
        fast_case = BenchCase(
            name=case.name,
            setup=case.setup,
            fn=case.fn,
            n_elements=case.n_elements,
            reps=1,
        )
        r = _run_case(fast_case)
        assert r.per_call_us > 0, f"Expected positive timing for {case.name}"
        assert r.throughput_mrows_s > 0
        results.append(r)
    assert len(results) == len(BENCH_CASES)


if __name__ == "__main__":
    emit_json = "--json" in sys.argv
    run(emit_json=emit_json)
