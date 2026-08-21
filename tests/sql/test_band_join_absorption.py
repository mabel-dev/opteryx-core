"""A cross-relation BAND predicate in the WHERE clause belongs in the INNER JOIN's
ON condition.

The temporal band join — attribute each flow to the DNS lookups that preceded it by
less than 20 seconds — is written naturally as:

    FROM flows f INNER JOIN lookups l ON f.client = l.client
    WHERE l.event_time <= f.flow_start
      AND l.event_time > f.flow_start - INTERVAL '20' SECOND

Left as a Filter above the join, the join first materialises the FULL equi-join
product for the Filter to throw >99% of away (measured on live data: 74,501 rows in,
948M rows / 42.4GB out, 96.4s of a 109s query). Folded into `node.on`, the join stays
KEYED on the equality — `extract_join_fields` reads Eq conjuncts only — and the band
becomes a per-pair residual inside the join, so the product is never built.

WHAT THIS FILE PINS

The absorption is a rewrite of one expression into a different evaluation site, so
the only thing that makes it safe is that it answers IDENTICALLY. Every test here
compares the absorbed form against the same query with PredicatePushdownStrategy
switched off — the unoptimised plan, where the band really is a Filter above the
join — row for row, not by row count.

The rows that matter are the WINDOW EDGES. `<=` vs `<` at `flow_start`, and `>` vs
`>=` at `flow_start - 20s`, each differ by exactly ONE row, which no interior-range
count test would ever notice. The fixture puts a lookup on each of the four
positions around the window (-21s, -20s, -19s, +0s, +1s) so a flipped comparator
shows up as a named tag appearing or disappearing.

`test_theta_is_not_absorbed_into_an_outer_join` guards the hard constraint: only
INNER has a residual channel. Every other join type silently DROPS a theta conjunct
from its ON and returns the equi-only answer (see the table in compiler.py's
`_compile_join`), so absorbing one there would be a silent wrong answer.

NOTE ON THE FIXTURE: the two legs deliberately name their raw VALUES columns
differently (`fts` / `ets`). Two identically-spelled computed columns
(`CAST(v.ts AS TIMESTAMP)` on both legs) collide on expression identity and the join
emits one leg's values for both — a separate, pre-existing defect that has nothing to
do with predicate placement. Distinct names keep it out of this file.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config

# One flow per client. `c` has lookups but no flow; `b`'s window is disjoint from
# `a`'s, so a band that ignored the join key would show up as extra rows.
FLOWS = """(SELECT client, CAST(fts AS TIMESTAMP) AS flow_start FROM (VALUES
    ('a', '2024-01-01 00:01:00'),
    ('b', '2024-01-01 00:05:00'),
    ('d', '2024-01-01 00:09:00')
) AS vf(client, fts)) AS f"""

# Around `a`'s 00:01:00 window: one lookup on each side of both bounds, and one ON
# each bound. `minus20` is the OPEN end (strict `>`, excluded); `exact` is the CLOSED
# end (`<=`, included).
LOOKUPS = """(SELECT client, CAST(ets AS TIMESTAMP) AS event_time, tag FROM (VALUES
    ('a', '2024-01-01 00:00:39', 'a_minus21'),
    ('a', '2024-01-01 00:00:40', 'a_minus20'),
    ('a', '2024-01-01 00:00:41', 'a_minus19'),
    ('a', '2024-01-01 00:01:00', 'a_exact'),
    ('a', '2024-01-01 00:01:01', 'a_plus1'),
    ('b', '2024-01-01 00:04:40', 'b_minus20'),
    ('b', '2024-01-01 00:04:41', 'b_minus19'),
    ('b', '2024-01-01 00:05:00', 'b_exact'),
    ('c', '2024-01-01 00:01:00', 'c_no_flow')
) AS vl(client, ets, tag)) AS l"""

BAND = """
SELECT f.client, l.tag
  FROM {flows} INNER JOIN {lookups} ON f.client = l.client
 WHERE l.event_time <= f.flow_start
   AND l.event_time > f.flow_start - INTERVAL '20' SECOND
 ORDER BY l.tag
""".format(flows=FLOWS, lookups=LOOKUPS)

ONE_SIDED = """
SELECT f.client, l.tag
  FROM {flows} INNER JOIN {lookups} ON f.client = l.client
 WHERE l.event_time <= f.flow_start
 ORDER BY l.tag
""".format(flows=FLOWS, lookups=LOOKUPS)

# LEFT OUTER, where a theta conjunct in the ON has no residual channel and must NOT
# be absorbed. WHERE semantics here are inner-like (an unmatched preserved row fails
# `NULL <= x`), which is exactly what the unoptimised plan produces and what the
# absorbed-into-ON form would NOT.
OUTER_BAND = """
SELECT f.client, l.tag
  FROM {flows} LEFT OUTER JOIN {lookups} ON f.client = l.client
 WHERE l.event_time <= f.flow_start
   AND l.event_time > f.flow_start - INTERVAL '20' SECOND
 ORDER BY l.tag
""".format(flows=FLOWS, lookups=LOOKUPS)

# The same band written as a CROSS JOIN. `CROSS JOIN b WHERE a.x < b.y` IS
# `INNER JOIN b ON a.x < b.y`, so the equality and the band both belong in the ON --
# and once the equality is there the join is keyed and the cartesian product the
# CROSS spelling implies is never built.
CROSS_BAND = """
SELECT f.client, l.tag
  FROM {flows} CROSS JOIN {lookups}
 WHERE f.client = l.client
   AND l.event_time <= f.flow_start
   AND l.event_time > f.flow_start - INTERVAL '20' SECOND
 ORDER BY l.tag
""".format(flows=FLOWS, lookups=LOOKUPS)

# No equality at all: the absorbed theta leaves the join with NO key, which is the
# compiler's `zero_key` path -- the same build/probe shape CROSS itself compiles to,
# with the band applied inside the join instead of above it. The answer is the full
# cross product narrowed by the band, so lookups for clients with no flow of their
# own still match, which is what separates this from the keyed form above.
CROSS_UNKEYED = """
SELECT f.client, l.tag
  FROM {flows} CROSS JOIN {lookups}
 WHERE l.event_time > f.flow_start - INTERVAL '20' SECOND
   AND l.event_time <= f.flow_start
 ORDER BY f.client, l.tag
""".format(flows=FLOWS, lookups=LOOKUPS)


def rows(sql):
    """Every row, in order, as tuples — row-for-row comparison, not a count."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        names = list(table.keys())
        out.extend(zip(*(table[name] for name in names)))
    return out


def explain(sql):
    return [
        (r[0].decode() if isinstance(r[0], bytes) else r[0], r[1]) for r in rows("EXPLAIN " + sql)
    ]


@pytest.fixture
def unoptimized():
    """Run a query with PredicatePushdownStrategy switched off — the plan where the
    band genuinely is a Filter above the join. This is the oracle."""
    original = config.features.disable_predicate_pushdown
    config.features.disable_predicate_pushdown = True
    try:
        yield rows
    finally:
        config.features.disable_predicate_pushdown = original


def test_band_window_edges(unoptimized):
    # The four edge rows are the whole point: `a_exact` is IN (the `<=` bound is
    # closed), `a_minus20` is OUT (the `>` bound is open), and the two rows one
    # second either side of them pin the direction of both comparators.
    expected = [
        ("a", "a_exact"),
        ("a", "a_minus19"),
        ("b", "b_exact"),
        ("b", "b_minus19"),
    ]
    assert unoptimized(BAND) == expected, unoptimized(BAND)
    assert rows(BAND) == expected, rows(BAND)


def test_band_matches_the_unoptimised_plan(unoptimized):
    assert rows(BAND) == unoptimized(BAND)


def test_one_sided_band_matches_the_unoptimised_plan(unoptimized):
    # No lower bound at all — a single absorbed conjunct, which is the shape that
    # would expose an absorption that only works when it can pair two conjuncts.
    assert rows(ONE_SIDED) == unoptimized(ONE_SIDED)


def test_one_sided_band_window_edge(unoptimized):
    expected = [
        ("a", "a_exact"),
        ("a", "a_minus19"),
        ("a", "a_minus20"),
        ("a", "a_minus21"),
        ("b", "b_exact"),
        ("b", "b_minus19"),
        ("b", "b_minus20"),
    ]
    assert rows(ONE_SIDED) == expected, rows(ONE_SIDED)


def test_the_band_really_is_absorbed_into_the_join():
    # Without this, every assertion above would still pass with the optimization
    # doing nothing at all.
    plan = explain(BAND)
    assert not any("Filter" in tree for tree, _ in plan), plan
    assert any("theta to inner join" in tree for tree, _ in plan), plan


def test_cross_join_band_matches_the_unoptimised_plan(unoptimized):
    # Same answer as the INNER spelling -- CROSS + WHERE and INNER + ON are the same
    # query -- reached by the cross-join branch instead of the inner one.
    assert rows(CROSS_BAND) == unoptimized(CROSS_BAND)
    assert rows(CROSS_BAND) == rows(BAND)


def test_cross_join_band_becomes_a_keyed_join_with_no_filter_above_it():
    plan = explain(CROSS_BAND)
    assert not any("Filter" in tree for tree, _ in plan), plan
    assert not any("Cross Join" in tree for tree, _ in plan), plan
    assert any("theta to inner join" in tree for tree, _ in plan), plan


def test_cross_join_theta_with_no_equality_matches_the_unoptimised_plan(unoptimized):
    # The zero-key path: nothing to key on, so every build row shares one empty key
    # and the residual does all the narrowing. `c_no_flow` is in range of `a`'s flow
    # and has no flow of its own, so it appears here and not in the keyed form.
    result = rows(CROSS_UNKEYED)
    assert result == unoptimized(CROSS_UNKEYED)
    assert ("a", "c_no_flow") in result, result


def test_cross_join_theta_with_no_equality_has_no_filter_above_it():
    plan = explain(CROSS_UNKEYED)
    assert not any("Filter" in tree for tree, _ in plan), plan
    assert not any("Cross Join" in tree for tree, _ in plan), plan


def test_theta_is_not_absorbed_into_an_outer_join(unoptimized):
    # Only INNER has a residual channel. A theta conjunct written into a LEFT OUTER
    # join's ON is silently dropped by the compiler, which would return the
    # equi-only answer — here, every unmatched flow row as well.
    assert rows(OUTER_BAND) == unoptimized(OUTER_BAND)
    assert not any("theta to inner join" in tree for tree, _ in explain(OUTER_BAND)), explain(
        OUTER_BAND
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
