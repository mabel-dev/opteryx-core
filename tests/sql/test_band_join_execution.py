"""A recognised BAND JOIN must answer exactly as the plan it replaces.

    ON f.client = l.client
    WHERE l.event_time <= f.flow_start
      AND l.event_time  > f.flow_start - INTERVAL '20' SECOND

Executed as the equality alone, the join emits every pair inside each equi group
and the band throws >99% of them away one node up (measured on live data: 2.55
BILLION rows out for a 4.8M-row answer). Executed as a band join, each equi
group's build rows are kept sorted by the banded column and a probe row emits the
contiguous run between two bisects, so those pairs are never formed.

WHAT THIS FILE PINS

The band is a rewrite from "emit everything, then filter" to "emit only the
range", so the only thing that makes it safe is that it answers IDENTICALLY.
Every test compares against the same query with PredicatePushdownStrategy off --
the unoptimised plan, where the band really is a Filter above the join.

The rows that matter are the WINDOW EDGES. `<=` vs `<` on the upper bound and `>`
vs `>=` on the lower each move the answer by exactly the rows sitting ON that
boundary, which no interior-range row count would ever notice. All four
combinations are run, and their row counts must DIFFER from each other -- two
combinations agreeing would mean the fixture has no row on that edge and the test
is pinning nothing.

`test_one_probe_row_spanning_many_batches` covers the only genuinely new control
flow in the operator: every other probe in native_join2.hpp finishes a probe row
before checking the output batch limit, but one band probe row can select more
rows than a batch holds, so it has to suspend MID-RUN and resume inside the same
run on the next call.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config

# Bands `flow_start`, which lives on `flows` (5 rows) -- the SMALLER side, which is
# the one JoinAlgorithmStrategy puts on the build leg, and the band column has to be
# on the build leg to be the one that gets sorted.
BAND = """
SELECT f.client, l.tag
  FROM testdata.band_join.flows AS f
  INNER JOIN testdata.band_join.lookups AS l ON f.client = l.client
 WHERE f.flow_start {lower} l.event_time
   AND f.flow_start {upper} l.event_time + INTERVAL '20' SECOND
 ORDER BY f.client, l.tag
"""

BOUND_COMBINATIONS = [(">=", "<="), (">=", "<"), (">", "<="), (">", "<")]

# NULLs, an EMPTY band, and an INVERTED one (upper below lower), all in a single
# query so the oracle comparison covers their interaction rather than each alone.
NULL_BUILD = """(SELECT * FROM (VALUES
    ('a', 10), ('a', 20), ('a', NULL), ('b', 5), (NULL, 7)
) AS b(client, t))"""
NULL_PROBE = """(SELECT * FROM (VALUES
    ('a', 0, 100),
    ('a', 50, 60),
    ('a', 30, 5),
    ('a', NULL, 100),
    ('a', 0, NULL),
    (NULL, 0, 100),
    ('b', 0, 100)
) AS p(client, lo, hi))"""

NULL_BAND = """
SELECT p.lo, p.hi, b.client, b.t
  FROM {build} AS b INNER JOIN {probe} AS p ON b.client = p.client
 WHERE b.t >= p.lo AND b.t <= p.hi
 ORDER BY p.lo, p.hi, b.t
""".format(build=NULL_BUILD, probe=NULL_PROBE)

# 40,000 build rows in ONE equi group, every one of them inside ONE probe row's
# band -- ~5 output batches from a single probe row.
BATCH_SPILL = """
SELECT COUNT(*) AS n, MIN(b.t) AS lo_t, MAX(b.t) AS hi_t, SUM(b.t) AS sum_t
  FROM (SELECT 'k' AS client, CAST(value AS INTEGER) AS t
          FROM GENERATE_SERIES(1, 40000) AS value) AS b
  INNER JOIN (SELECT * FROM (VALUES ('k', 0, 100000)) AS v(client, lo, hi)) AS p
    ON b.client = p.client
 WHERE b.t >= p.lo AND b.t <= p.hi
"""

# One bound only: a prefix of the sorted run. Still a band (architect ruling
# 2026-11-23): it removes the pair-then-filter, and costs no more emissions than the
# plan it replaces.
ONE_SIDED = """
SELECT f.client, l.tag
  FROM testdata.band_join.flows AS f
  INNER JOIN testdata.band_join.lookups AS l ON f.client = l.client
 WHERE f.flow_start >= l.event_time
 ORDER BY f.client, l.tag
"""

# UNKEYED bands: no equality in the ON, so the whole build side is one sorted run.
# The expected rows are computed in Python from these literals (three-valued: a NULL
# never matches), NOT from another plan -- an ON-clause theta join has no
# "unoptimised" plan to compare against.
UNKEYED_BUILD = [(1, 10), (2, 20), (3, 20), (4, None), (5, 30)]
UNKEYED_PROBE = [(1, 20, 0), (2, 10, 0), (3, None, 0), (4, 25, 0), (5, 40, 0)]
UNKEYED = """
SELECT b.id, p.pid
  FROM (SELECT * FROM (VALUES {build}) AS v(id, t)) AS b
  INNER JOIN (SELECT * FROM (VALUES {probe}) AS v(pid, v, k)) AS p ON {on}
 ORDER BY b.id, p.pid
"""


def _values(table):
    return ", ".join(
        "(" + ", ".join("NULL" if c is None else str(c) for c in row) + ")" for row in table
    )


def _unkeyed_sql(on, build=UNKEYED_BUILD, probe=UNKEYED_PROBE):
    return UNKEYED.format(build=_values(build), probe=_values(probe), on=on)


# The band column here is `event_time`, on `lookups` (23 rows) -- the LARGER side,
# so JoinAlgorithmStrategy leaves it on the PROBE leg.
PROBE_SIDE_BAND = """
SELECT f.client, l.tag
  FROM testdata.band_join.flows AS f
  INNER JOIN testdata.band_join.lookups AS l ON f.client = l.client
 WHERE l.event_time <= f.flow_start
   AND l.event_time > f.flow_start - INTERVAL '20' SECOND
 ORDER BY f.client, l.tag
"""


def rows(sql):
    """Every row, in order, as tuples -- row-for-row, never a count."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        out.extend(zip(*(table[name] for name in table)))
    return out


def plan(sql):
    session = opteryx.session()
    return "\n".join(str(morsel) for morsel in session.execute_to_morsels("EXPLAIN " + sql))


@pytest.fixture
def unoptimized():
    """Run ONE query with PredicatePushdownStrategy switched off -- the plan where the
    band genuinely is a Filter above the join. This is the oracle.

    ⛔ The flag is scoped to the CALL, not to the test. Held for the whole test body
    (which is what a `yield rows` fixture does) it disables pushdown for the
    optimised side too, and every `rows(sql) == unoptimized(sql)` in this file
    becomes `rows(sql) == rows(sql)` -- a tautology that passes no matter what the
    optimisation does. The comparison only means something while exactly one of the
    two sides is unoptimised.
    """

    def run(sql):
        original = config.features.disable_predicate_pushdown
        config.features.disable_predicate_pushdown = True
        try:
            return rows(sql)
        finally:
            config.features.disable_predicate_pushdown = original

    return run


@pytest.mark.parametrize("lower,upper", BOUND_COMBINATIONS)
def test_band_matches_the_unoptimised_plan(unoptimized, lower, upper):
    sql = BAND.format(lower=lower, upper=upper)
    assert rows(sql) == unoptimized(sql)


@pytest.mark.parametrize("lower,upper", BOUND_COMBINATIONS)
def test_band_join_is_the_plan_taken(lower, upper):
    # Without this every assertion above would still pass with the band doing
    # nothing at all.
    sql = BAND.format(lower=lower, upper=upper)
    explained = plan(sql)
    assert "Band Join" in explained, explained
    # The band conjuncts are CONSUMED by the range, never also left above the join.
    tree = explained.split("OPTIMIZATIONS")[0]
    assert "Filter" not in tree, explained


def test_the_four_bound_combinations_give_four_different_answers():
    # The edge rows are the whole point. If any two combinations agreed, the
    # fixture would have no row sitting on that boundary and the parametrised
    # oracle tests above would be pinning nothing.
    counts = {
        (lower, upper): len(rows(BAND.format(lower=lower, upper=upper)))
        for lower, upper in BOUND_COMBINATIONS
    }
    assert len(set(counts.values())) == len(counts), counts


def test_nulls_empty_and_inverted_bands(unoptimized):
    # A NULL band value, a NULL key, and a NULL in either BOUND all match nothing --
    # the WHERE clause's own behaviour, since a comparison with NULL is UNKNOWN and
    # UNKNOWN is not TRUE. An empty band and an inverted one (upper below lower)
    # emit nothing rather than erroring.
    assert "Band Join" in plan(NULL_BAND), plan(NULL_BAND)
    result = rows(NULL_BAND)
    assert result == unoptimized(NULL_BAND)
    assert result == [(0, 100, "b", 5), (0, 100, "a", 10), (0, 100, "a", 20)], result


def test_one_probe_row_spanning_many_batches(unoptimized):
    # THE new control-flow case: one probe row selects 40,000 build rows, which is
    # ~5 output batches, so the probe must suspend mid-run and resume inside the
    # same run. Checked on SUM as well as COUNT -- a resume that restarted the run,
    # or skipped its first row, keeps a plausible count and moves the sum.
    assert "Band Join" in plan(BATCH_SPILL), plan(BATCH_SPILL)
    assert rows(BATCH_SPILL) == unoptimized(BATCH_SPILL)
    assert rows(BATCH_SPILL) == [(40000, 1, 40000, 800020000)], rows(BATCH_SPILL)


def test_a_one_sided_bound_is_a_band(unoptimized):
    assert "Band Join" in plan(ONE_SIDED), plan(ONE_SIDED)
    assert rows(ONE_SIDED) == unoptimized(ONE_SIDED)


ONE_SIDED_OPS = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
}


@pytest.mark.parametrize("op", sorted(ONE_SIDED_OPS))
def test_unkeyed_one_sided_band(op):
    # `b.t <op> p.v` with no equality: one sorted run, one bound. Equal values sit
    # on the edge (b.t=20 vs p.v=20, b.t=10 vs p.v=10), so each operator's answer is
    # distinct and a flipped closedness moves it.
    sql = _unkeyed_sql(f"b.t {op} p.v")
    explained = plan(sql)
    assert "Band Join" in explained, explained
    expected = sorted(
        (bid, pid)
        for bid, t in UNKEYED_BUILD
        for pid, v, *_ in UNKEYED_PROBE
        if t is not None and v is not None and ONE_SIDED_OPS[op](t, v)
    )
    assert rows(sql) == expected


def test_unkeyed_one_sided_edges_are_on_the_fixture():
    # Open vs closed of the SAME direction must differ, or the fixture has no row on
    # that edge and the parametrised test above pins nothing for it.
    size = {op: len(rows(_unkeyed_sql(f"b.t {op} p.v"))) for op in ONE_SIDED_OPS}
    assert size[">"] != size[">="], size
    assert size["<"] != size["<="], size


def test_unkeyed_band_reads_the_build_column_when_written_second():
    # `p.v < b.t` has two valid readings; the band column must be the build-side
    # one whichever operand is written first.
    swapped = _unkeyed_sql("p.v < b.t")
    assert "Band Join" in plan(swapped), plan(swapped)
    assert rows(swapped) == rows(_unkeyed_sql("b.t > p.v"))


def test_unkeyed_two_sided_band():
    probe = [(1, 15, 0), (2, 10, 0), (3, 40, 0)]
    sql = _unkeyed_sql("b.t >= p.v AND b.t <= p.v + 10", probe=probe)
    assert "Band Join" in plan(sql), plan(sql)
    expected = sorted(
        (bid, pid)
        for bid, t in UNKEYED_BUILD
        for pid, v, _ in probe
        if t is not None and v <= t <= v + 10
    )
    assert rows(sql) == expected


def test_keyed_one_sided_band():
    build = [(1, 10, 1), (2, 20, 1), (3, 20, 2), (4, 5, 2)]
    probe = [(1, 15, 1), (2, 15, 2), (3, 0, 3)]
    sql = """
SELECT b.id, p.pid
  FROM (SELECT * FROM (VALUES {build}) AS v(id, t, k)) AS b
  INNER JOIN (SELECT * FROM (VALUES {probe}) AS v(pid, v, k)) AS p
    ON b.k = p.k AND b.t > p.v
 ORDER BY b.id, p.pid
""".format(build=_values(build), probe=_values(probe))
    assert "Band Join" in plan(sql), plan(sql)
    expected = sorted(
        (bid, pid)
        for bid, t, k in build
        for pid, v, pk in probe
        if k == pk and t > v
    )
    assert rows(sql) == expected


def test_unkeyed_one_sided_spanning_many_batches():
    # One probe row selects all 40,000 build rows: the probe must suspend mid-run
    # and resume, exactly as in the keyed two-sided case.
    sql = """
SELECT COUNT(*) AS n, SUM(b.t) AS sum_t
  FROM (SELECT CAST(value AS INTEGER) AS t FROM GENERATE_SERIES(1, 40000) AS value) AS b
  INNER JOIN (SELECT * FROM (VALUES (0)) AS v(lo)) AS p ON b.t > p.lo
"""
    assert "Band Join" in plan(sql), plan(sql)
    assert rows(sql) == [(40000, 800020000)]


def test_a_probe_side_band_declines_and_still_answers(unoptimized):
    # KNOWN GAP, pinned so that closing it is a deliberate change to this test
    # rather than a silent one: the band column is on the probe leg here, and bound
    # INVERSION (`l.t <= f.t AND l.t > f.t - 20s` IS `f.t >= l.t AND f.t < l.t + 20s`)
    # is not implemented, so this declines to the plan it has today. What must NOT
    # change is the answer.
    assert "Band Join" not in plan(PROBE_SIDE_BAND), plan(PROBE_SIDE_BAND)
    assert rows(PROBE_SIDE_BAND) == unoptimized(PROBE_SIDE_BAND)
