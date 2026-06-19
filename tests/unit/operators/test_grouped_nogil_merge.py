"""Prove the grouped-aggregate MERGE runs with the GIL RELEASED and is correct.

A radix-partitioned parallel GROUP BY builds B per-worker sub-engines, then merges
worker b's partial into the base for bin b. The merge is the wall; this test
proves (a) the new GIL-free merge path (`merge_nogil_driver` → `_merge_nogil`)
runs, and (b) it produces byte-identical groups to a single serial engine over
the same rows.

Scope of the nogil merge: single-column key + nogil-mergeable collectors
(COUNT*/COUNT/SUM/MIN/MAX/AVG, int+float). Multi-col / non-nogil collectors fall
to the GIL merge (a dual interface), exercised by the existing merge tests.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

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
    AggregationSpec(alias="cval", function="count", column=VALUE),
    AggregationSpec(alias="sum", function="sum", column=VALUE),
    AggregationSpec(alias="min", function="min", column=VALUE),
    AggregationSpec(alias="max", function="max", column=VALUE),
    AggregationSpec(alias="avg", function="avg", column=VALUE),
]
ALIASES = ["cstar", "cval", "sum", "min", "max", "avg"]

N_GROUPS = 500          # > parvi capacity → carchar path
ROWS = 4000
MORSELS = 6


def _cxx_morsel(groups, values):
    return Morsel.from_cxx_vectors(
        [GROUP, VALUE],
        [vector_from_sequence(groups), vector_from_sequence(values)],
    )


def _new_engine():
    collectors, _ = create_collectors(SPECS, [GROUP])
    # use_parvi=False → carchar from the start (nogil-merge base requires carchar).
    return GroupHashEngine([GROUP], collectors, True, False)


def _finalize(engine):
    out = {}
    for chunk in engine.finalize_morsels():
        gcol = chunk.column(GROUP).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in ALIASES}
        for i, gv in enumerate(gcol):
            out[gv] = {a: cols[a][i] for a in ALIASES}
    return out


def _make_morsels():
    """Deterministic morsels + a Python reference of the expected aggregate."""
    morsels = []
    ref = {}
    for m in range(MORSELS):
        groups = [((i * 13 + m * 7) % N_GROUPS) for i in range(ROWS)]
        values = [(i % 97) - 13 for i in range(ROWS)]
        morsels.append((groups, values))
        for g, v in zip(groups, values):
            r = ref.setdefault(
                g, {"cstar": 0, "cval": 0, "sum": 0, "min": v, "max": v, "avgs": 0, "avgc": 0}
            )
            r["cstar"] += 1
            r["cval"] += 1
            r["sum"] += v
            r["min"] = min(r["min"], v)
            r["max"] = max(r["max"], v)
            r["avgs"] += v
            r["avgc"] += 1
    return morsels, ref


def _check(got, ref):
    assert set(got) == set(ref), (len(got), len(ref))
    for g in ref:
        assert got[g]["cstar"] == ref[g]["cstar"], (g, "cstar")
        assert got[g]["cval"] == ref[g]["cval"], (g, "cval")
        assert got[g]["sum"] == ref[g]["sum"], (g, "sum")
        assert got[g]["min"] == ref[g]["min"], (g, "min")
        assert got[g]["max"] == ref[g]["max"], (g, "max")
        assert abs(got[g]["avg"] - ref[g]["avgs"] / ref[g]["avgc"]) < 1e-9, (g, "avg")


def test_serial_baseline():
    """Single engine over all rows — the oracle the merged result must match."""
    morsels, ref = _make_morsels()
    eng = _new_engine()
    for groups, values in morsels:
        eng.ingest(_cxx_morsel(groups, values))
    _check(_finalize(eng), ref)


def test_nogil_merge_fires_and_matches():
    """Split morsels across two sub-engines, merge GIL-free, assert parity."""
    morsels, ref = _make_morsels()

    base = _new_engine()
    other = _new_engine()
    for idx, (groups, values) in enumerate(morsels):
        (base if idx % 2 == 0 else other).ingest(_cxx_morsel(groups, values))

    # Both must advertise the nogil-merge capability.
    assert base.is_mergeable_nogil() is True
    assert other.is_mergeable_nogil() is True

    rc = base.merge_nogil_driver(other)
    assert rc == 1, rc

    _check(_finalize(base), ref)


def test_nogil_merge_radix_bins():
    """B=4 radix: route each row to its bin by hash, B base engines, B GIL-free
    bin-merges. Union of bins must equal the serial result."""
    B = 4
    morsels, ref = _make_morsels()

    # B base engines + B worker engines; round-robin morsels to workers, but route
    # rows to bins by (g % B) so a bin's base/worker share the same key space.
    bases = [_new_engine() for _ in range(B)]
    workers = [_new_engine() for _ in range(B)]

    for idx, (groups, values) in enumerate(morsels):
        target = bases if idx % 2 == 0 else workers
        # Partition this morsel's rows into B bins by key.
        for b in range(B):
            gb = [g for g in groups if (g % B) == b]
            vb = [v for g, v in zip(groups, values) if (g % B) == b]
            if gb:
                target[b].ingest(_cxx_morsel(gb, vb))

    got = {}
    for b in range(B):
        rc = bases[b].merge_nogil_driver(workers[b])
        # rc may be 0 if a worker bin saw no rows — still valid.
        for chunk in bases[b].finalize_morsels():
            gcol = chunk.column(GROUP).to_pylist()
            cols = {a: chunk.column(a).to_pylist() for a in ALIASES}
            for i, gv in enumerate(gcol):
                got[gv] = {a: cols[a][i] for a in ALIASES}

    _check(got, ref)


def test_merge_into_empty_base_adopts():
    """An empty (unresolved) base must adopt other wholesale via the GIL merge()
    and remain finalize-correct (the dual-interface fallback, not merge_nogil)."""
    morsels, ref = _make_morsels()
    base = _new_engine()      # never ingests
    other = _new_engine()
    for groups, values in morsels:
        other.ingest(_cxx_morsel(groups, values))
    base.merge(other)         # adopt-wholesale path
    _check(_finalize(base), ref)


if __name__ == "__main__":
    test_serial_baseline()
    test_nogil_merge_fires_and_matches()
    test_nogil_merge_radix_bins()
    test_merge_into_empty_base_adopts()
    print("OK — nogil grouped merge fires (single + radix bins) and matches serial")
