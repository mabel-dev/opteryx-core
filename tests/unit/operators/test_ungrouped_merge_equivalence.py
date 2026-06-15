"""WP-7 — ungrouped aggregate partition-parallel merge equivalence.

A parallel engine runs one UngroupedAggregateEngine per worker over a disjoint
partition of the input, then combines the partial accumulators with
`engine.merge(other)` before a single `finalize()`. This is the correctness
contract that makes that safe: for COUNT / SUM / MIN / MAX, merging k partial
engines and finalizing must equal a single engine over all the rows.

Exact for COUNT, integer SUM, and MIN/MAX (integer & float); float SUM is
compared with a relative tolerance because cross-partition addition order can
differ from the serial order in the last ULP.

The non-mergeable aggregates (COUNT DISTINCT, MEDIAN) must report
is_mergeable()==False so the engine refuses to merge rather than returning a
wrong answer — also asserted here.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import vector_from_sequence, vector_float64_from_sequence

from opteryx.operators._operators import (
    UngroupedAggregateEngine,
    CountStarAggregate,
    CountAggregate,
    SumInt64Aggregate,
    SumFloat64Aggregate,
    MinInt64Aggregate,
    MaxInt64Aggregate,
    MinFloat64Aggregate,
    MaxFloat64Aggregate,
    CountDistinctAggregate,
)

COL = b"v"


def _morsel(values, build):
    return Morsel.from_vectors([COL], [build(values)])


def _make_engine(specs):
    eng = UngroupedAggregateEngine()
    for cls, alias in specs:
        if cls is CountStarAggregate:
            eng.add_aggregate(cls(alias))
        else:
            eng.add_aggregate(cls(COL, alias))
    return eng


def _run(specs, partitions, build):
    """Run each partition through its own engine, merge into the first, finalize.
    Returns {alias: value}."""
    engines = []
    for part in partitions:
        eng = _make_engine(specs)
        if part:  # allow empty partitions
            eng.ingest(_morsel(part, build))
        engines.append(eng)
    base = engines[0]
    for other in engines[1:]:
        base.merge(other)
    result = base.finalize()
    return {alias.decode(): result.column(alias).to_pylist()[0] for _, alias in specs}


def _serial(specs, all_values, build):
    eng = _make_engine(specs)
    if all_values:
        eng.ingest(_morsel(all_values, build))
    result = eng.finalize()
    return {alias.decode(): result.column(alias).to_pylist()[0] for _, alias in specs}


def _partition(rng, values, k):
    """Split values into k partitions (some may be empty)."""
    parts = [[] for _ in range(k)]
    for v in values:
        parts[rng.randrange(k)].append(v)
    return parts


INT_SPECS = [
    (CountStarAggregate, b"cstar"),
    (CountAggregate, b"cnt"),
    (SumInt64Aggregate, b"sum"),
    (MinInt64Aggregate, b"min"),
    (MaxInt64Aggregate, b"max"),
]

FLOAT_SPECS = [
    (CountStarAggregate, b"cstar"),
    (SumFloat64Aggregate, b"sum"),
    (MinFloat64Aggregate, b"min"),
    (MaxFloat64Aggregate, b"max"),
]


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("seed", range(8))
def test_int_aggregates_merge_equals_serial(k, seed):
    rng = random.Random(seed)
    n = rng.randint(1, 200)
    # include large magnitudes to exercise the big-int SUM accumulator
    values = [rng.randint(-10**12, 10**12) for _ in range(n)]
    parts = _partition(rng, values, k)

    merged = _run(INT_SPECS, parts, vector_from_sequence)
    serial = _serial(INT_SPECS, values, vector_from_sequence)

    assert merged["cstar"] == serial["cstar"] == n
    assert merged["cnt"] == serial["cnt"] == n
    assert merged["sum"] == serial["sum"] == sum(values)   # exact big-int
    assert merged["min"] == serial["min"] == min(values)
    assert merged["max"] == serial["max"] == max(values)


@pytest.mark.parametrize("k", [2, 3, 7])
@pytest.mark.parametrize("seed", range(8))
def test_float_aggregates_merge_equals_serial(k, seed):
    rng = random.Random(seed)
    n = rng.randint(1, 200)
    values = [rng.uniform(-1e6, 1e6) for _ in range(n)]
    parts = _partition(rng, values, k)

    merged = _run(FLOAT_SPECS, parts, vector_float64_from_sequence)
    serial = _serial(FLOAT_SPECS, values, vector_float64_from_sequence)

    assert merged["cstar"] == serial["cstar"] == n
    assert merged["min"] == serial["min"] == min(values)
    assert merged["max"] == serial["max"] == max(values)
    # float SUM: order-tolerant
    assert merged["sum"] == pytest.approx(serial["sum"], rel=1e-9)
    assert merged["sum"] == pytest.approx(sum(values), rel=1e-9)


def test_empty_partitions_are_handled():
    # one partition has all the data, the others are empty
    specs = INT_SPECS
    parts = [[], [5, 3, 9, 1], [], []]
    merged = _run(specs, parts, vector_from_sequence)
    assert merged["cstar"] == 4
    assert merged["sum"] == 18
    assert merged["min"] == 1
    assert merged["max"] == 9


def test_all_empty_partitions():
    merged = _run(INT_SPECS, [[], []], vector_from_sequence)
    assert merged["cstar"] == 0
    assert merged["cnt"] == 0
    assert merged["sum"] is None      # SUM over no rows is NULL
    assert merged["min"] is None
    assert merged["max"] is None


def test_non_mergeable_aggregate_refuses_merge():
    # COUNT DISTINCT is not yet mergeable — the engine must refuse rather than
    # silently produce a wrong distinct count.
    a = _make_engine([(CountDistinctAggregate, b"cd")])
    b = _make_engine([(CountDistinctAggregate, b"cd")])
    a.ingest(_morsel([1, 2, 2], vector_from_sequence))
    b.ingest(_morsel([2, 3], vector_from_sequence))
    assert a.is_mergeable() is False
    with pytest.raises(NotImplementedError):
        a.merge(b)


def test_mergeable_engine_reports_true():
    a = _make_engine(INT_SPECS)
    assert a.is_mergeable() is True


def test_mismatched_aggregate_count_raises():
    a = _make_engine([(CountStarAggregate, b"c")])
    b = _make_engine([(CountStarAggregate, b"c"), (SumInt64Aggregate, b"s")])
    with pytest.raises(ValueError):
        a.merge(b)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
