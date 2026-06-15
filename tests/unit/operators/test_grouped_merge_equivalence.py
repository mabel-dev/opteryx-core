"""WP-7 — grouped (hashed) aggregate partition-parallel merge equivalence.

A parallel engine runs one GroupHashEngine per worker over a disjoint partition
of the input, then combines the partial per-group accumulators with
`engine.merge(other)` before a single `finalize_morsels()`. This is the
correctness contract that makes that safe: for COUNT / SUM / MIN / MAX / AVG,
merging k partial engines and finalizing must equal a single engine over all
the rows — group-for-group.

Exact for COUNT, integer SUM and MIN/MAX (integer & float). Float SUM and AVG
are compared with a relative tolerance because cross-partition addition order
can differ from the serial order in the last ULP.

Covered: k ∈ {2, 3, 7}; multiple seeds; NULL group keys; high-cardinality with
the parvi→carchar promotion forced; empty partitions; the structural-move path
(an empty/unresolved base merging a populated partition); and the
non-mergeable-refusal path (a COUNT DISTINCT collector makes is_mergeable False
and merge raise).

The engine + collectors are built exactly as GroupedAggregateHashedNode builds
them (create_collectors → GroupHashEngine), exported from
opteryx.operators._operators.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence, vector_float64_from_sequence

from opteryx.operators._operators import (
    GroupHashEngine,
    create_collectors,
    AggregationSpec,
)

GROUP = "g"
VALUE = "v"

INT_SPECS = [
    AggregationSpec(alias="cstar", function="count", column="*"),
    AggregationSpec(alias="cnt", function="count", column=VALUE),
    AggregationSpec(alias="sum", function="sum", column=VALUE),
    AggregationSpec(alias="min", function="min", column=VALUE),
    AggregationSpec(alias="max", function="max", column=VALUE),
    AggregationSpec(alias="avg", function="avg", column=VALUE),
]
INT_ALIASES = ["cstar", "cnt", "sum", "min", "max", "avg"]

FLOAT_SPECS = [
    AggregationSpec(alias="cstar", function="count", column="*"),
    AggregationSpec(alias="sum", function="sum", column=VALUE),
    AggregationSpec(alias="min", function="min", column=VALUE),
    AggregationSpec(alias="max", function="max", column=VALUE),
    AggregationSpec(alias="avg", function="avg", column=VALUE),
]
FLOAT_ALIASES = ["cstar", "sum", "min", "max", "avg"]


def _make_engine(specs, use_parvi=False):
    collectors, _key_kinds = create_collectors(specs, [GROUP])
    return GroupHashEngine([GROUP], collectors, False, use_parvi)


def _morsel(groups, values, build):
    return Morsel.from_vectors([GROUP, VALUE], [vector_from_sequence(groups), build(values)])


def _results(engine, aliases):
    """{group_value: {alias: value}} from the engine's finalized output."""
    out = {}
    for chunk in engine.finalize_morsels():
        gcol = chunk.column(GROUP).to_pylist()
        cols = {a: chunk.column(a).to_pylist() for a in aliases}
        for i, gv in enumerate(gcol):
            out[gv] = {a: cols[a][i] for a in aliases}
    return out


def _run(specs, aliases, partitions, build, use_parvi=False):
    """Each partition → its own engine; merge all into the first; finalize."""
    engines = []
    for groups, values in partitions:
        eng = _make_engine(specs, use_parvi)
        if groups:  # allow empty partitions
            eng.ingest(_morsel(groups, values, build))
        engines.append(eng)
    base = engines[0]
    for other in engines[1:]:
        base.merge(other)
    return _results(base, aliases)


def _serial(specs, aliases, all_groups, all_values, build, use_parvi=False):
    eng = _make_engine(specs, use_parvi)
    if all_groups:
        eng.ingest(_morsel(all_groups, all_values, build))
    return _results(eng, aliases)


def _partition(rng, groups, values, k):
    parts = [([], []) for _ in range(k)]
    for g, v in zip(groups, values):
        idx = rng.randrange(k)
        parts[idx][0].append(g)
        parts[idx][1].append(v)
    return parts


def _assert_int_equal(merged, serial):
    assert merged.keys() == serial.keys()
    for g in serial:
        m, s = merged[g], serial[g]
        assert m["cstar"] == s["cstar"]
        assert m["cnt"] == s["cnt"]
        assert m["sum"] == s["sum"]      # exact int
        assert m["min"] == s["min"]
        assert m["max"] == s["max"]
        assert m["avg"] == pytest.approx(s["avg"], rel=1e-9, nan_ok=True)


def _assert_float_equal(merged, serial):
    assert merged.keys() == serial.keys()
    for g in serial:
        m, s = merged[g], serial[g]
        assert m["cstar"] == s["cstar"]
        assert m["min"] == s["min"]      # exact (no arithmetic)
        assert m["max"] == s["max"]
        assert m["sum"] == pytest.approx(s["sum"], rel=1e-9)
        assert m["avg"] == pytest.approx(s["avg"], rel=1e-9)


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("seed", range(8))
def test_int_grouped_merge_equals_serial(k, seed):
    rng = random.Random(seed)
    n = rng.randint(1, 400)
    n_groups = rng.randint(1, 25)
    groups = [rng.randrange(n_groups) for _ in range(n)]
    values = [rng.randint(-(10**12), 10**12) for _ in range(n)]
    parts = _partition(rng, groups, values, k)

    merged = _run(INT_SPECS, INT_ALIASES, parts, vector_from_sequence)
    serial = _serial(INT_SPECS, INT_ALIASES, groups, values, vector_from_sequence)
    _assert_int_equal(merged, serial)


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("seed", range(8))
def test_float_grouped_merge_equals_serial(k, seed):
    rng = random.Random(100 + seed)
    n = rng.randint(1, 400)
    n_groups = rng.randint(1, 25)
    groups = [rng.randrange(n_groups) for _ in range(n)]
    values = [rng.uniform(-1e6, 1e6) for _ in range(n)]
    parts = _partition(rng, groups, values, k)

    merged = _run(FLOAT_SPECS, FLOAT_ALIASES, parts, vector_float64_from_sequence)
    serial = _serial(FLOAT_SPECS, FLOAT_ALIASES, groups, values, vector_float64_from_sequence)
    _assert_float_equal(merged, serial)


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("seed", range(4))
def test_null_group_keys_merge_equals_serial(k, seed):
    """NULL group keys form their own group; merge must combine them correctly."""
    rng = random.Random(200 + seed)
    n = rng.randint(1, 300)
    groups = [rng.choice([None, None, 0, 1, 2, 3, 4]) for _ in range(n)]
    values = [rng.randint(-(10**6), 10**6) for _ in range(n)]
    parts = _partition(rng, groups, values, k)

    merged = _run(INT_SPECS, INT_ALIASES, parts, vector_from_sequence)
    serial = _serial(INT_SPECS, INT_ALIASES, groups, values, vector_from_sequence)
    _assert_int_equal(merged, serial)
    assert None in merged


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("use_parvi", [False, True])
def test_high_cardinality_forces_promotion(k, use_parvi):
    """Many groups: with use_parvi the small-map overflows mid-ingest/mid-merge
    and promotes to carchar — exercise that promotion path on both sides."""
    rng = random.Random(999)
    n = 4000
    n_groups = 800  # >> parvi capacity (16), forces promotion
    groups = [rng.randrange(n_groups) for _ in range(n)]
    values = [rng.randint(-(10**9), 10**9) for _ in range(n)]
    parts = _partition(rng, groups, values, k)

    merged = _run(INT_SPECS, INT_ALIASES, parts, vector_from_sequence, use_parvi)
    serial = _serial(INT_SPECS, INT_ALIASES, groups, values, vector_from_sequence, use_parvi)
    _assert_int_equal(merged, serial)
    # Confirm the run actually exceeded the parvi small-map capacity (16) so the
    # promotion path was exercised on both the ingest and the merge sides.
    assert len(merged) > 16


def test_empty_partitions_handled():
    parts = [([], []), ([1, 2, 1, 3], [10, 20, 30, 40]), ([], []), ([2, 2], [5, 15])]
    merged = _run(INT_SPECS, INT_ALIASES, parts, vector_from_sequence)
    serial = _serial(INT_SPECS, INT_ALIASES, [1, 2, 1, 3, 2, 2], [10, 20, 30, 40, 5, 15],
                     vector_from_sequence)
    _assert_int_equal(merged, serial)


def test_empty_base_structural_move():
    """The merge target itself never ingested (empty partition 0): merge must
    adopt the populated partition's resolved state via the structural-move path."""
    base = _make_engine(INT_SPECS)          # never ingested → unresolved
    other = _make_engine(INT_SPECS)
    other.ingest(_morsel([1, 2, 1], [10, 20, 30], vector_from_sequence))
    base.merge(other)
    merged = _results(base, INT_ALIASES)
    serial = _serial(INT_SPECS, INT_ALIASES, [1, 2, 1], [10, 20, 30], vector_from_sequence)
    _assert_int_equal(merged, serial)


def test_all_empty_partitions():
    merged = _run(INT_SPECS, INT_ALIASES, [([], []), ([], [])], vector_from_sequence)
    assert merged == {}


def test_mergeable_engine_reports_true():
    # is_mergeable is meaningful once collectors are resolved (SUM/MIN/MAX/AVG are
    # deferred until the first morsel fixes their type). merge() runs after
    # ingestion, so this mirrors the real check point.
    a = _make_engine(INT_SPECS)
    a.ingest(_morsel([1, 2], [10, 20], vector_from_sequence))
    assert a.is_mergeable() is True
    b = _make_engine(FLOAT_SPECS)
    b.ingest(_morsel([1, 2], [1.0, 2.0], vector_float64_from_sequence))
    assert b.is_mergeable() is True


def test_non_mergeable_aggregate_refuses_merge():
    # COUNT DISTINCT is not yet mergeable — the engine must refuse rather than
    # silently producing a wrong distinct count.
    specs = [AggregationSpec(alias="cd", function="count_distinct", column=VALUE)]
    a = _make_engine(specs)
    b = _make_engine(specs)
    a.ingest(_morsel([1, 2, 2], [1, 1, 2], vector_from_sequence))
    b.ingest(_morsel([2, 3], [1, 1], vector_from_sequence))
    assert a.is_mergeable() is False
    with pytest.raises(NotImplementedError):
        a.merge(b)


def test_median_is_not_mergeable():
    specs = [AggregationSpec(alias="md", function="median", column=VALUE)]
    a = _make_engine(specs)
    b = _make_engine(specs)
    a.ingest(_morsel([1, 1, 2], [1.0, 3.0, 5.0], vector_float64_from_sequence))
    b.ingest(_morsel([2, 3], [2.0, 4.0], vector_float64_from_sequence))
    assert a.is_mergeable() is False
    with pytest.raises(NotImplementedError):
        a.merge(b)


def test_mismatched_collector_count_raises():
    a = _make_engine([AggregationSpec(alias="cstar", function="count", column="*")])
    b = _make_engine(INT_SPECS)
    with pytest.raises(ValueError):
        a.merge(b)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
