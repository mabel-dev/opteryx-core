"""M4-prerequisite: operator-layer concurrency stress test.

The thread-safety contract (docs/EXECUTION_THREAD_SAFETY_CONTRACT.md) flagged a
missing validation hook: `tests/draken/test_gil_release_concurrency.py` proves
the draken *kernels* are thread-safe, but nothing exercised the full *operator*
ingestion path under real threads. A parallel engine (M4) runs one
GroupHashEngine per worker, each ingesting a disjoint partition CONCURRENTLY,
then merges the partials. This test models exactly that:

  * N OS threads, each with its OWN engine (the clone-per-worker model — no engine
    state is shared between threads);
  * barrier-synchronised so the threads ingest at the same instant, maximising
    concurrent re-entry into the shared draken reduction/hash kernels and any
    module-level state in the aggregate path;
  * after the threads join, the partials are merged and finalised on the main
    thread and asserted byte-identical to a single-threaded reference computed
    before any thread started.

A data race in shared state (a module-level scratch buffer, an unguarded global,
a kernel that isn't actually nogil-safe) would either crash or make a threaded
partial diverge from the reference. Many rounds are run to shake out timing.
"""

import os
import random
import sys
import threading

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence

from opteryx.operators._operators import (
    GroupHashEngine,
    create_collectors,
    AggregationSpec,
)

GROUP = "g"
VALUE = "v"

SPECS = [
    AggregationSpec(alias="cstar", function="count", column="*"),
    AggregationSpec(alias="cnt", function="count", column=VALUE),
    AggregationSpec(alias="sum", function="sum", column=VALUE),
    AggregationSpec(alias="min", function="min", column=VALUE),
    AggregationSpec(alias="max", function="max", column=VALUE),
    AggregationSpec(alias="avg", function="avg", column=VALUE),
]
ALIASES = ["cstar", "cnt", "sum", "min", "max", "avg"]

THREADS = 8
MORSELS_PER_THREAD = 6
ROWS_PER_MORSEL = 4000
N_GROUPS = 256   # > parvi capacity → real carchar hash table per engine


def _make_engine():
    collectors, _ = create_collectors(SPECS, [GROUP])
    return GroupHashEngine([GROUP], collectors, False, False)


def _morsel(groups, values):
    return Morsel.from_vectors(
        [GROUP, VALUE], [vector_from_sequence(groups), vector_from_sequence(values)]
    )


def _finalize(engine):
    out = {}
    for chunk in engine.finalize_morsels():
        gcol = chunk.column(GROUP).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in ALIASES}
        for i, gv in enumerate(gcol):
            out[gv] = {a: cols[a][i] for a in ALIASES}
    return out


def _gen_partition(rng):
    """A worker's share: a list of (groups, values) morsels."""
    morsels = []
    for _ in range(MORSELS_PER_THREAD):
        groups = [rng.randrange(N_GROUPS) for _ in range(ROWS_PER_MORSEL)]
        values = [rng.randint(-(10**9), 10**9) for _ in range(ROWS_PER_MORSEL)]
        morsels.append((groups, values))
    return morsels


def _reference(all_morsels):
    """Single-threaded ground truth over every row, computed before threads run."""
    eng = _make_engine()
    for groups, values in all_morsels:
        eng.ingest(_morsel(groups, values))
    return _finalize(eng)


@pytest.mark.parametrize("round_seed", range(6))
def test_concurrent_ingest_then_merge_equals_serial(round_seed):
    rng = random.Random(round_seed)
    partitions = [_gen_partition(random.Random(round_seed * 100 + t)) for t in range(THREADS)]
    all_morsels = [m for part in partitions for m in part]
    reference = _reference(all_morsels)

    engines = [None] * THREADS
    errors = []
    barrier = threading.Barrier(THREADS)

    def worker(t):
        try:
            eng = _make_engine()
            # Pre-build the morsels so the barrier-released section is pure
            # concurrent ingestion (the contended path), not Python data-gen.
            built = [_morsel(g, v) for g, v in partitions[t]]
            barrier.wait()
            for m in built:
                eng.ingest(m)
            engines[t] = eng
        except Exception as exc:  # noqa: BLE001 — surface any thread failure
            errors.append((t, repr(exc)))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(THREADS)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors, f"worker thread(s) failed under concurrency: {errors[:3]}"

    # Merge all per-worker partials on the main thread, finalize, compare.
    base = engines[0]
    for other in engines[1:]:
        base.merge(other)
    merged = _finalize(base)

    assert merged.keys() == reference.keys()
    for g in reference:
        m, r = merged[g], reference[g]
        assert m["cstar"] == r["cstar"]
        assert m["cnt"] == r["cnt"]
        assert m["sum"] == r["sum"]          # exact big-int
        assert m["min"] == r["min"]
        assert m["max"] == r["max"]
        assert m["avg"] == pytest.approx(r["avg"], rel=1e-9)


def _cxx_morsel(groups, values):
    """Cxx-backed morsel → drives the GIL-released `_ingest_cxx_span` path."""
    return Morsel.from_cxx_vectors(
        [GROUP, VALUE], [vector_from_sequence(groups), vector_from_sequence(values)]
    )


@pytest.mark.parametrize("round_seed", range(6))
def test_concurrent_nogil_ingest_equals_serial(round_seed):
    """S-B.3c: the same harness but every worker ingests Cxx morsels, so the
    contended barrier-released section is the GIL-RELEASED grouped-agg span
    (`_ingest_cxx_span`) re-entered concurrently by THREADS real OS threads. A
    data race in the nogil keying/store/grow/accumulate or a kernel that isn't
    actually nogil-safe would crash or diverge from the single-threaded
    reference. Also asserts each engine genuinely took the GIL-free path."""
    rng = random.Random(round_seed)
    partitions = [_gen_partition(random.Random(round_seed * 100 + t)) for t in range(THREADS)]
    all_morsels = [m for part in partitions for m in part]
    reference = _reference(all_morsels)

    engines = [None] * THREADS
    errors = []
    barrier = threading.Barrier(THREADS)

    def worker(t):
        try:
            eng = _make_engine()  # use_parvi=False → carchar → nogil eligible
            built = [_cxx_morsel(g, v) for g, v in partitions[t]]
            barrier.wait()
            for m in built:
                eng.ingest(m)
            engines[t] = eng
        except Exception as exc:  # noqa: BLE001 — surface any thread failure
            errors.append((t, repr(exc)))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(THREADS)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors, f"worker thread(s) failed under concurrency: {errors[:3]}"

    # Every worker must have actually run the GIL-released span on every morsel.
    for t, eng in enumerate(engines):
        fired = eng.telemetry()["nogil_ingest_morsels"]
        assert fired == MORSELS_PER_THREAD, (t, fired)

    base = engines[0]
    for other in engines[1:]:
        base.merge(other)
    merged = _finalize(base)

    assert merged.keys() == reference.keys()
    for g in reference:
        m, r = merged[g], reference[g]
        assert m["cstar"] == r["cstar"]
        assert m["cnt"] == r["cnt"]
        assert m["sum"] == r["sum"]
        assert m["min"] == r["min"]
        assert m["max"] == r["max"]
        assert m["avg"] == pytest.approx(r["avg"], rel=1e-9)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
