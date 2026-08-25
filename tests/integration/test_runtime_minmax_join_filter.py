# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Correctness gate for RUNTIME MIN/MAX JOIN FILTERS.

See docs/RUNTIME_MINMAX_FILTER_DESIGN.md. A join's build side runs to completion
before its probe side starts, so the OBSERVED ordinal range of the build keys can
be handed to the probe scan's row-group zone map and row groups that provably hold
no matching row are never read.

The filter is PURE PRUNING: turning it off can only make a query read more, never
change what it returns. This file is what holds that claim up, and it does so in
three independent ways, because each catches a different class of failure:

1. ``test_oracle_identical_results`` — every eligible shape run with the filter on
   and with it off, asserting the FULL RESULT is identical value-for-value. Row
   counts alone are not enough: a bound that dropped one row group would still
   produce a plausible count on an aggregate.

2. ``test_refused_for_unsound_modes`` — the join modes whose probe rows are NOT
   conditional on matching must never arm a bound. This is the sharp edge of the
   whole feature: LEFT/FULL OUTER preserve the probe side, ANTI emits exactly the
   probe rows a bound would prune, INTERSECT/EXCEPT make NULL a matchable value
   (and skene's statistics are over non-null values only), and the existence-flag
   modes keep every probe row and append a verdict. All of them are wrong answers
   rather than slow ones, and several are shape-identical to the eligible modes.
   Asserting "the results match" would NOT catch a regression here — on these
   small fixtures a wrong bound may prune nothing — so this asserts the filter is
   not ARMED, which is the actual invariant.

3. ``test_positive_control_fires_and_prunes`` — the filter really does arm and
   really does skip row groups on the shape it is for. Without this, tests 1 and 2
   would still pass if the feature were silently doing nothing at all.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

DS = "testdata.tpcds_1_skene."

# Shapes that MUST be answered identically with the filter on and off. A mix of
# eligible and refused modes on purpose: the refused ones are here to prove the
# refusal does not itself change an answer.
# fmt:off
ORACLE_STATEMENTS = [
    # --- eligible: INNER on a clustered key with a genuinely narrow build range.
    # This is the shape the feature exists for (TPC-DS Q21's join).
    f"SELECT SUM(inv_quantity_on_hand) q, COUNT(*) c "
    f"FROM {DS}inventory, {DS}date_dim "
    f"WHERE inv_date_sk = d_date_sk AND d_year = 2001 AND d_moy = 1",

    # --- eligible: the same join emitting real rows, not just an aggregate, so a
    # dropped row group cannot hide inside a sum.
    f"SELECT inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand, d_date "
    f"FROM {DS}inventory, {DS}date_dim "
    f"WHERE inv_date_sk = d_date_sk AND d_date = CAST('2001-03-14' AS DATE) "
    f"ORDER BY inv_item_sk, inv_warehouse_sk LIMIT 200",

    # --- eligible: SEMI (IN a subquery).
    f"SELECT COUNT(*) c FROM {DS}inventory WHERE inv_date_sk IN "
    f"(SELECT d_date_sk FROM {DS}date_dim WHERE d_year = 2000 AND d_moy = 1)",

    # --- eligible: SEMI (EXISTS).
    f"SELECT COUNT(*) c FROM {DS}catalog_sales cs WHERE EXISTS "
    f"(SELECT 1 FROM {DS}date_dim WHERE d_date_sk = cs.cs_sold_date_sk AND d_year = 1999)",

    # --- eligible, but the bound is USELESS: the build side spans the whole key
    # range. Must still be exactly right, and is the case that proves a wide
    # bound degrades to "prune nothing" rather than to "prune wrongly".
    f"SELECT COUNT(*) c FROM {DS}inventory, {DS}date_dim "
    f"WHERE inv_date_sk = d_date_sk",

    # --- eligible: the build side is EMPTY. An unfilled bound must prune nothing
    # and the join must still produce the empty answer by itself.
    f"SELECT COUNT(*) c FROM {DS}inventory, {DS}date_dim "
    f"WHERE inv_date_sk = d_date_sk AND d_year = 9999",

    # --- eligible: a VARCHAR key, whose ordinal is a LOSSY 8-byte prefix. Sound
    # because the mapping is monotone and equal values share an ordinal, but it is
    # the type most likely to expose a broken bound.
    f"SELECT COUNT(*) c FROM {DS}item, {DS}item i2 "
    f"WHERE item.i_category = i2.i_category AND i2.i_category = 'Music'",

    # --- eligible: NULL keys on the PROBE side. cs_sold_date_sk carries nulls;
    # skene's statistics are over non-null values, and a null key can never
    # equi-match, so pruning must be unaffected by them.
    f"SELECT COUNT(*) c FROM {DS}catalog_sales, {DS}date_dim "
    f"WHERE cs_sold_date_sk = d_date_sk AND d_year = 2000",

    # --- refused modes. Present so a refusal cannot silently change an answer.
    f"SELECT COUNT(*) c FROM {DS}inventory LEFT OUTER JOIN {DS}date_dim "
    f"ON inv_date_sk = d_date_sk AND d_year = 2001",
    f"SELECT COUNT(*) c FROM {DS}inventory WHERE inv_date_sk NOT IN "
    f"(SELECT d_date_sk FROM {DS}date_dim WHERE d_year = 2001)",
    f"SELECT COUNT(*) c FROM {DS}inventory i WHERE NOT EXISTS "
    f"(SELECT 1 FROM {DS}date_dim WHERE d_date_sk = i.inv_date_sk AND d_year = 2001)",
]

# (statement, expected_bounds_wired). The mode allow-list, asserted directly.
# A shape that refuses must arm ZERO bounds — not "arm one that prunes nothing".
MODE_EXPECTATIONS = [
    # INNER and SEMI arm. One bound each: a single-column equi key.
    (f"SELECT COUNT(*) c FROM {DS}inventory, {DS}date_dim "
     f"WHERE inv_date_sk = d_date_sk AND d_year = 2001", 1),
    (f"SELECT COUNT(*) c FROM {DS}inventory WHERE inv_date_sk IN "
     f"(SELECT d_date_sk FROM {DS}date_dim WHERE d_year = 2001)", 1),
    # LEFT OUTER: the probe leg is the PRESERVED side.
    (f"SELECT COUNT(*) c FROM {DS}inventory LEFT OUTER JOIN {DS}date_dim "
     f"ON inv_date_sk = d_date_sk AND d_year = 2001", 0),
    # FULL OUTER: both sides preserved.
    (f"SELECT COUNT(*) c FROM {DS}inventory FULL OUTER JOIN {DS}date_dim "
     f"ON inv_date_sk = d_date_sk", 0),
    # NOT IN — null-aware ANTI. Emits exactly the probe rows a bound would prune.
    (f"SELECT COUNT(*) c FROM {DS}inventory WHERE inv_date_sk NOT IN "
     f"(SELECT d_date_sk FROM {DS}date_dim WHERE d_year = 2001)", 0),
    # NOT EXISTS — plain ANTI. Same reason.
    (f"SELECT COUNT(*) c FROM {DS}inventory i WHERE NOT EXISTS "
     f"(SELECT 1 FROM {DS}date_dim WHERE d_date_sk = i.inv_date_sk AND d_year = 2001)", 0),
    # EXCEPT / INTERSECT — the not-distinct key rule: NULL is a matchable VALUE,
    # and skene's min/max are over NON-NULL values, so a row group outside the
    # bound can still hold NULL rows that match.
    (f"SELECT d_date_sk FROM {DS}date_dim EXCEPT SELECT inv_date_sk FROM {DS}inventory", 0),
    (f"SELECT d_date_sk FROM {DS}date_dim INTERSECT SELECT inv_date_sk FROM {DS}inventory", 0),
    # Projected IN — the existence-FLAG mode. It reuses SEMI's key rule but emits
    # every probe row with a verdict appended, so a bound would delete rows that
    # should have come back FALSE.
    (f"SELECT inv_date_sk IN (SELECT d_date_sk FROM {DS}date_dim WHERE d_year = 2001) f "
     f"FROM {DS}inventory LIMIT 10", 0),
]
# fmt:on


def _run_with_filter(sql, enabled):
    """(rows, telemetry) with the filter armed or disarmed for this session.

    Flipped through the session variable rather than by patching `config`,
    because that is the surface a caller actually has: the compiler resolves
    through the variable chain once per compile, so a patched module attribute
    would be ignored — and a test that could not tell the difference would be
    asserting nothing."""
    session = opteryx.session()
    if not enabled:
        for _ in session.execute_to_morsels(
            "SET disable_runtime_minmax_join_filter = true"
        ):
            pass
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return rows, session.telemetry


@pytest.mark.parametrize("statement", ORACLE_STATEMENTS)
def test_oracle_identical_results(statement):
    """The filter is pruning only: on and off must agree value-for-value."""
    off_rows, _ = _run_with_filter(statement, False)
    on_rows, _ = _run_with_filter(statement, True)
    # Sorted, because the engine's row order is completion order at dop > 1 and
    # is not part of the contract for an unordered query. Every ordered statement
    # above carries its own ORDER BY, which sorting cannot undo.
    assert sorted(on_rows, key=repr) == sorted(off_rows, key=repr), (
        f"runtime min/max filter changed the ANSWER for: {statement}"
    )


@pytest.mark.parametrize("statement,expected", MODE_EXPECTATIONS)
def test_refused_for_unsound_modes(statement, expected):
    """Only modes that drop unmatched probe rows may arm a bound.

    Asserted on ARMING, not on the answer: on a fixture this small an unsound
    bound might happen to prune nothing, so a result comparison would pass while
    the invariant was broken."""
    _, telemetry = _run_with_filter(statement, True)
    wired = telemetry.get("runtime_minmax_bounds_wired", 0)
    if expected == 0:
        assert wired == 0, (
            f"a bound was armed for a join mode that emits probe rows "
            f"independently of matching: {statement}"
        )
    else:
        assert wired >= expected, (
            f"expected the runtime min/max filter to arm for: {statement}"
        )


def test_positive_control_fires_and_prunes():
    """The feature must actually skip row groups on the shape it exists for.

    Without this, both tests above would pass with the filter doing nothing.
    `inv_date_sk` is clustered (mean row-group range ~2% of the column's span)
    and one month of `date_dim` is a narrow contiguous window of it."""
    statement = (
        f"SELECT COUNT(*) c FROM {DS}inventory, {DS}date_dim "
        f"WHERE inv_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 1"
    )
    _, off = _run_with_filter(statement, False)
    _, on = _run_with_filter(statement, True)

    def inventory_scan(telemetry):
        for reading in telemetry.get("operations", {}).values():
            if reading.get("alias", "").endswith(".inventory"):
                return reading
        raise AssertionError("no inventory scan in the telemetry")

    off_scan = inventory_scan(off)
    on_scan = inventory_scan(on)

    # With the filter off, plan-time pruning gets nothing here: the estimator's
    # per-column range propagation cannot follow `d_year` to `d_date_sk`, which
    # is precisely the gap this feature closes (design note §6.2).
    assert off_scan["row_groups_pruned"] == 0
    assert on_scan["row_groups_pruned"] > 0, "the filter armed but pruned nothing"
    assert on_scan["row_groups_read"] < off_scan["row_groups_read"]
    # And the pruning is attributed to the RUNTIME filter, not silently credited
    # to the plan-time pushed predicates.
    assert on_scan["row_groups_pruned_runtime"] == on_scan["row_groups_pruned"]
    # `row_groups_pruned_runtime` must be ABSENT when nothing was wired — "did
    # not fire" is a different statement from "fired and pruned nothing".
    assert "row_groups_pruned_runtime" not in off_scan


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING {len(ORACLE_STATEMENTS)} ORACLE + {len(MODE_EXPECTATIONS)} MODE TESTS")
    for index, sql in enumerate(ORACLE_STATEMENTS):
        test_oracle_identical_results(sql)
        print(f"  oracle {index + 1:02d} OK")
    for index, (sql, expected) in enumerate(MODE_EXPECTATIONS):
        test_refused_for_unsound_modes(sql, expected)
        print(f"  mode   {index + 1:02d} OK")
    test_positive_control_fires_and_prunes()
    print("  positive control OK")
    print("--- done")
