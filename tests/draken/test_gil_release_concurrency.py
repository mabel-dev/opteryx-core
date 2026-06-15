"""Concurrency stress test for the GIL-released draken nanobind ops.

WHY THIS EXISTS
---------------
A sweep added `nb::gil_scoped_release` to ~56 hot draken Vector ops (take / mask
/ slice / materialize / compress / compare_* / sum / min / max / arithmetic /
in_list / between …) so the engine can run them on multiple Python threads in
parallel (M4). Releasing the GIL exposes any *shared mutable state* the GIL was
previously serialising.

`make q` / `make tpch` are single-threaded and can NEVER catch a threading bug.
This test drives the released ops from many threads at once and asserts:
  1. the process survives (a data race on shared global state typically segfaults
     or corrupts), and
  2. every threaded result is byte-identical to a single-threaded reference
     computed before any thread starts.

It specifically targets `logical_type_intern` (draken/logical_type.h): a
process-global `std::deque` that `vecresult_to_owner` touches for TIMESTAMP64
results (take/mask/slice on a timestamp column) — previously GIL-guarded, now
mutex-guarded. The barrier-synchronised timestamp phase below forces many
threads to intern distinct timestamp units (s/ms/us/ns) simultaneously, which is
the exact iterate+push_back race the mutex fixes.

It also exercises the lock-protected growth of the shared identity/zero
selection buffers (draken/core/vector_alloc.cpp) by running ops at varying
lengths concurrently.
"""

import os
import sys
import threading

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from draken import draken_native as dn

THREADS = 16
ITERS = 400
N = 4096  # vector length


def _to_list(v):
    """Materialise a result Vector to a Python list for comparison."""
    return v.to_pylist()


def _build_vectors():
    """Diverse dense vectors, including 4 timestamp units (distinct logical types)."""
    ints = dn.vector_from_sequence([(i * 2654435761) % 100003 for i in range(N)])
    floats = dn.vector_float64_from_sequence([float((i % 997) - 500) for i in range(N)])
    strs = dn.vector_from_string_sequence(
        [(b"row-%d-payload" % (i % 311)) for i in range(N)]
    )
    # Four timestamp vectors with DIFFERENT units → four distinct interned
    # LogicalTypes; take/mask on each routes through vecresult_to_owner →
    # logical_type_intern. Base magnitude (~1.6e9) yields valid datetimes under
    # every unit (s→2020, ns→1970+1.6s) so to_pylist verification works.
    ts = {
        unit: dn.vector_reinterpret_as_timestamp64(
            dn.vector_from_sequence([1_600_000_000 + i for i in range(N)]),
            unit,
        )
        for unit in ("s", "ms", "us", "ns")
    }
    return {"int": ints, "float": floats, "str": strs, "ts": ts}


def _make_jobs(vecs):
    """A fixed list of (label, callable) jobs with deterministic outputs."""
    ints = vecs["int"]
    floats = vecs["float"]
    strs = vecs["str"]

    idx_full = list(range(N))
    idx_half = list(range(0, N, 2))
    idx_small = list(range(7))
    mask_int = ints.compare_scalar(50000, 2)  # int > 50000 → bool mask
    mask_str = strs.compare_scalar(b"row-100-payload", 4)  # str < … → bool mask

    jobs = []
    # take at varying lengths (exercises sel-buffer growth)
    jobs.append(("take_full_int", lambda: _to_list(ints.take(idx_full))))
    jobs.append(("take_half_float", lambda: _to_list(floats.take(idx_half))))
    jobs.append(("take_small_str", lambda: _to_list(strs.take(idx_small))))
    # mask (the filter gather)
    jobs.append(("mask_int", lambda: _to_list(ints.mask(mask_int))))
    jobs.append(("mask_str", lambda: _to_list(strs.mask(mask_str))))
    # slice at varying lengths
    jobs.append(("slice_int", lambda: _to_list(ints.slice(10, 1000))))
    jobs.append(("slice_str", lambda: _to_list(strs.slice(0, 333))))
    # compare / between / reductions
    jobs.append(("cmp_int", lambda: _to_list(ints.compare_scalar(12345, 3))))
    jobs.append(("cmp_str", lambda: _to_list(strs.compare_scalar(b"row-200-payload", 0))))
    jobs.append(("between_int", lambda: _to_list(ints.between(100, 50000, True, True))))
    jobs.append(("sum_int", lambda: ints.sum()))
    jobs.append(("min_float", lambda: floats.min()))
    jobs.append(("max_int", lambda: ints.max()))
    # timestamp take/mask → exercises logical_type_intern (per unit)
    for unit, tsv in vecs["ts"].items():
        jobs.append((f"ts_take_{unit}", lambda tsv=tsv: _to_list(tsv.take(idx_half))))
        jobs.append((f"ts_mask_{unit}", lambda tsv=tsv: _to_list(tsv.mask(mask_int))))
        jobs.append((f"ts_slice_{unit}", lambda tsv=tsv: _to_list(tsv.slice(5, 500))))
    return jobs


def test_released_ops_are_thread_safe():
    vecs = _build_vectors()
    jobs = _make_jobs(vecs)

    # Single-threaded reference (computed before any thread runs).
    reference = {label: fn() for label, fn in jobs}

    errors = []
    mismatches = []
    barrier = threading.Barrier(THREADS)

    def worker(seed):
        try:
            # Phase 1: barrier-synchronised burst of timestamp ops so many
            # threads hit logical_type_intern at the same instant (the race).
            barrier.wait()
            for label, fn in jobs:
                if label.startswith("ts_"):
                    if fn() != reference[label]:
                        mismatches.append((seed, label, "phase1"))

            # Phase 2: sustained mixed hammer across all ops.
            n = len(jobs)
            for it in range(ITERS):
                label, fn = jobs[(seed * 31 + it * 7) % n]
                if fn() != reference[label]:
                    mismatches.append((seed, label, it))
        except Exception as exc:  # noqa: BLE001 — surface any thread failure
            errors.append((seed, repr(exc)))

    threads = [threading.Thread(target=worker, args=(s,)) for s in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"thread exceptions: {errors[:5]}"
    assert not mismatches, f"result mismatches under concurrency: {mismatches[:5]}"


if __name__ == "__main__":
    test_released_ops_are_thread_safe()
    print("OK: released ops thread-safe across", THREADS, "threads")
