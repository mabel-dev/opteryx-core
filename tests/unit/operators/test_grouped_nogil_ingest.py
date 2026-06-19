"""S-B.3c: prove the grouped-aggregate ingest runs with the GIL RELEASED.

The point of the move is not just "right answer" — the fallback `_do_ingest`
already gives that under the GIL. This test proves the *new* path is the one
doing the work: a single-column key over a Cxx-backed morsel with only
nogil-capable collectors must go through `_ingest_cxx_span` (GIL released),
counted by `nogil_ingest_morsels`. It then asserts byte-parity with a plain
Python reference so the GIL-free path is also correct.
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
    AggregationSpec(alias="sum", function="sum", column=VALUE),
    AggregationSpec(alias="min", function="min", column=VALUE),
    AggregationSpec(alias="max", function="max", column=VALUE),
]
ALIASES = ["cstar", "sum", "min", "max"]

N_GROUPS = 256          # > parvi capacity (16) → carchar → nogil path eligible
ROWS = 4000
MORSELS = 5


def _cxx_morsel(groups, values):
    return Morsel.from_cxx_vectors(
        [GROUP, VALUE],
        [vector_from_sequence(groups), vector_from_sequence(values)],
    )


def _finalize(engine):
    out = {}
    for chunk in engine.finalize_morsels():
        gcol = chunk.column(GROUP).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in ALIASES}
        for i, gv in enumerate(gcol):
            out[gv] = {a: cols[a][i] for a in ALIASES}
    return out


def test_grouped_nogil_ingest_fires_and_matches():
    collectors, _ = create_collectors(SPECS, [GROUP])
    # use_parvi=False → carchar from the start, so the nogil span is eligible
    # on every morsel.
    engine = GroupHashEngine([GROUP], collectors, True, False)

    ref = {}
    for m in range(MORSELS):
        groups = [((i + m) % N_GROUPS) for i in range(ROWS)]
        values = [(i % 97) - 13 for i in range(ROWS)]
        engine.ingest(_cxx_morsel(groups, values))
        for g, v in zip(groups, values):
            r = ref.setdefault(g, {"cstar": 0, "sum": 0, "min": v, "max": v})
            r["cstar"] += 1
            r["sum"] += v
            r["min"] = min(r["min"], v)
            r["max"] = max(r["max"], v)

    got = _finalize(engine)

    # 1) The GIL-released path actually ran — every morsel went through it.
    tel = engine.telemetry()
    assert tel["nogil_ingest_morsels"] == MORSELS, tel["nogil_ingest_morsels"]

    # 2) The GIL-free path is correct.
    assert set(got) == set(ref), (len(got), len(ref))
    for g in ref:
        assert got[g]["cstar"] == ref[g]["cstar"], g
        assert abs(got[g]["sum"] - ref[g]["sum"]) < 1e-6, g
        assert got[g]["min"] == ref[g]["min"], g
        assert got[g]["max"] == ref[g]["max"], g


G2 = "g2"
SPECS2 = [
    AggregationSpec(alias="cstar", function="count", column="*"),
    AggregationSpec(alias="sum", function="sum", column=VALUE),
]
ALIASES2 = ["cstar", "sum"]


def _cxx_morsel2(g1, g2, values):
    return Morsel.from_cxx_vectors(
        [GROUP, G2, VALUE],
        [
            vector_from_sequence(g1),
            vector_from_sequence(g2),
            vector_from_sequence(values),
        ],
    )


def test_grouped_nogil_ingest_multicol_fires_and_matches():
    """Two-column key → the multi-column nogil store path (store_new_rows_multi_view)."""
    collectors, _ = create_collectors(SPECS2, [GROUP, G2])
    engine = GroupHashEngine([GROUP, G2], collectors, True, False)

    ref = {}
    for m in range(MORSELS):
        g1 = [((i + m) % 64) for i in range(ROWS)]
        g2 = [((i * 7 + m) % 50) for i in range(ROWS)]
        values = [(i % 97) - 13 for i in range(ROWS)]
        engine.ingest(_cxx_morsel2(g1, g2, values))
        for a, b, v in zip(g1, g2, values):
            r = ref.setdefault((a, b), {"cstar": 0, "sum": 0})
            r["cstar"] += 1
            r["sum"] += v

    got = {}
    for chunk in engine.finalize_morsels():
        c1 = chunk.column(GROUP).to_pylist()
        c2 = chunk.column(G2).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in ALIASES2}
        for i in range(len(c1)):
            got[(c1[i], c2[i])] = {a: cols[a][i] for a in ALIASES2}

    tel = engine.telemetry()
    assert tel["nogil_ingest_morsels"] == MORSELS, tel["nogil_ingest_morsels"]
    assert set(got) == set(ref), (len(got), len(ref))
    for key in ref:
        assert got[key]["cstar"] == ref[key]["cstar"], key
        assert got[key]["sum"] == ref[key]["sum"], key


def _drive_parvi(n_groups, morsels=4, rows=2000):
    collectors, _ = create_collectors(SPECS2, [GROUP])
    eng = GroupHashEngine([GROUP], collectors, True, True)  # telemetry + use_parvi=True
    ref = {}
    for m in range(morsels):
        g = [((i + m) % n_groups) for i in range(rows)]
        v = [(i % 50) - 7 for i in range(rows)]
        eng.ingest(Morsel.from_cxx_vectors(
            [GROUP, VALUE], [vector_from_sequence(g), vector_from_sequence(v)]))
        for a, b in zip(g, v):
            r = ref.setdefault(a, {"cstar": 0, "sum": 0})
            r["cstar"] += 1
            r["sum"] += b
    got = {}
    for chunk in eng.finalize_morsels():
        gc = chunk.column(GROUP).to_pylist()
        cs = chunk.column("cstar").to_pylist()
        sm = chunk.column("sum").to_pylist()
        for i, gv in enumerate(gc):
            got[gv] = {"cstar": cs[i], "sum": sm[i]}
    return eng.telemetry(), got, ref


def test_parvi_low_card_is_nogil():
    """use_parvi engine, <=16 groups → stays in parvi, GIL-free (gil==0)."""
    tel, got, ref = _drive_parvi(8, morsels=4)
    assert tel["nogil_ingest_morsels"] == 4, tel
    assert tel["gil_ingest_morsels"] == 0, tel
    assert tel["promoted_from_parvi"] is False, tel
    assert got == ref


def test_parvi_overflow_promotes_nogil():
    """use_parvi engine, >16 groups → parvi promotes to carchar MID-MORSEL via
    the nogil drain; still GIL-free (gil==0) and correct."""
    tel, got, ref = _drive_parvi(200, morsels=4)
    assert tel["nogil_ingest_morsels"] == 4, tel
    assert tel["gil_ingest_morsels"] == 0, tel
    assert tel["promoted_from_parvi"] is True, tel
    assert set(got) == set(ref) and len(ref) == 200
    for k in ref:
        assert got[k] == ref[k], k


if __name__ == "__main__":
    test_grouped_nogil_ingest_fires_and_matches()
    test_grouped_nogil_ingest_multicol_fires_and_matches()
    test_parvi_low_card_is_nogil()
    test_parvi_overflow_promotes_nogil()
    print("OK — single/multi-col + parvi(low-card & overflow) nogil ingest matched reference")
