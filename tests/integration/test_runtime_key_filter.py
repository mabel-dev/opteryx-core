# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Correctness gate for the RUNTIME JOIN KEY FILTER.

See docs/VELOX_BORROW_PLAN.md (item 1) and src/cpp/engine/runtime_key_filter.hpp.
Beside the runtime min/max bound (test_runtime_minmax_join_filter.py), an eligible
single-column join hands its build side's KEY HASHES to the probe parquet scan, whose
decode workers test every row's key and decode the other columns only for rows that
may match. Unlike the bound, this does not depend on the probe file being clustered
on the key: a filtered dimension's scattered keys still reject rows.

The filter admits a SUPERSET of the join's matches (the join itself treats the 64-bit
hash as key identity), so turning it off can only make a scan decode more, never
change an answer. Held up three ways, as for the bound:

1. ``test_oracle_identical_results`` — every shape with the filter on and off, the
   FULL result compared value-for-value. Covers both worker paths (with and without a
   pushed predicate), both filter shapes (exact set, bloom), NULL probe keys, an empty
   build side, a string key, a star chain, and range settlement (where it must not arm).
2. ``test_refused_for_unsound_modes`` — LEFT/FULL OUTER, ANTI, NOT IN, EXCEPT /
   INTERSECT and the existence flag must never TEST a row: asserted on the scan's
   counters, not on the answer, because on a small fixture an unsound filter may
   happen to drop nothing that mattered.
3. ``test_positive_control_*`` — the filter really fires and really rejects rows on
   the shapes it exists for, through both filter shapes. Without these, 1 and 2 would
   pass with the feature doing nothing.

The switch is the runtime min/max filter's: ``disable_runtime_minmax_join_filter``
turns both off (one feature family, one barrier, one eligibility).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

DS = "testdata.tpcds_1."

pytestmark = pytest.mark.skipif(
    not os.path.isdir("testdata/tpcds_1"),
    reason="testdata/tpcds_1 (TPC-DS SF1 parquet) not populated",
)

# fmt:off
ORACLE_STATEMENTS = [
    # --- exact set, NO pushed predicate on the probe: the worker decodes the key
    # column first and the rest under its mask. item keys are scattered across
    # store_sales, so the min/max bound prunes nothing here — this is the key
    # filter's own case.
    f"SELECT COUNT(*) c, SUM(ss_net_paid) s FROM {DS}store_sales, {DS}item "
    f"WHERE ss_item_sk = i_item_sk AND i_category = 'Music'",

    # --- the same join emitting rows (a dropped row cannot hide inside a sum).
    f"SELECT ss_ticket_number, ss_item_sk, ss_quantity, i_brand FROM {DS}store_sales, {DS}item "
    f"WHERE ss_item_sk = i_item_sk AND i_manufact_id = 128 "
    f"ORDER BY ss_ticket_number, ss_item_sk LIMIT 500",

    # --- exact set WITH a pushed predicate the join reads (not filter-only): the key
    # column decodes with the predicate columns and its bits AND into the survivor mask.
    f"SELECT SUM(ss_quantity) q, COUNT(*) c FROM {DS}store_sales, {DS}item "
    f"WHERE ss_item_sk = i_item_sk AND i_category = 'Books' AND ss_quantity > 50",

    # --- range-settlement shape: a filter-only int range on a NOT NULL probe column,
    # which statistics prove ALL-PASS for some row groups and not others — so the scan
    # settles AND evaluates. The key filter must not arm on it (asserted below).
    f"SELECT COUNT(*) c FROM {DS}store_sales, {DS}item "
    f"WHERE ss_item_sk = i_item_sk AND i_category = 'Music' "
    f"AND ss_ticket_number BETWEEN 1 AND 100000",

    # --- BLOOM: a build side with more than 65,536 keyed rows.
    f"SELECT COUNT(*) c, SUM(ss_sales_price) s FROM {DS}store_sales, {DS}customer_demographics "
    f"WHERE ss_cdemo_sk = cd_demo_sk AND cd_gender = 'F' AND cd_marital_status = 'M'",

    # --- SEMI (IN) and SEMI (EXISTS).
    f"SELECT COUNT(*) c FROM {DS}store_sales WHERE ss_item_sk IN "
    f"(SELECT i_item_sk FROM {DS}item WHERE i_color = 'red')",
    f"SELECT COUNT(*) c FROM {DS}catalog_sales cs WHERE EXISTS "
    f"(SELECT 1 FROM {DS}item WHERE i_item_sk = cs.cs_item_sk AND i_category = 'Music')",

    # --- NULL keys on the PROBE side (cs_sold_date_sk carries nulls): a null key
    # can never equi-match, and the filter must reject it without changing the answer.
    f"SELECT COUNT(*) c FROM {DS}catalog_sales, {DS}date_dim "
    f"WHERE cs_sold_date_sk = d_date_sk AND d_dow = 3",

    # --- EMPTY build side: no filter is built, and the join still answers by itself.
    f"SELECT COUNT(*) c FROM {DS}store_sales, {DS}item "
    f"WHERE ss_item_sk = i_item_sk AND i_category = 'no such category'",

    # --- a VARCHAR key.
    f"SELECT COUNT(*) c FROM {DS}item i1, {DS}item i2 "
    f"WHERE i1.i_brand = i2.i_brand AND i2.i_class = 'pop'",

    # --- a STAR chain over one fact scan: every join's key filter lands on the same
    # store_sales scan, each narrowing the mask the next one decodes under.
    f"SELECT COUNT(*) c, SUM(ss_ext_sales_price) s "
    f"FROM {DS}store_sales, {DS}item, {DS}date_dim, {DS}store "
    f"WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk "
    f"AND i_category = 'Music' AND d_year = 2001 AND s_state = 'TN'",

    # --- a key column that is ALSO a pushed-predicate column (ss_item_sk), beside a
    # second key filter on a column that is not (ss_sold_date_sk). The predicate
    # column's key bits are over the predicate's full length and must be mapped onto
    # the predicate's survivors before they meet the other key's bits — mixing the
    # two lengths was a real bug (TPC-H Q08's customer scan, 2026-10-29).
    f"SELECT COUNT(*) c, SUM(ss_net_paid) s FROM {DS}store_sales, {DS}item, {DS}date_dim "
    f"WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk "
    f"AND ss_item_sk < 9000 AND i_category IN ('Music', 'Books') AND d_moy = 11",

    # --- refused modes, present so a refusal cannot silently change an answer.
    f"SELECT COUNT(*) c FROM {DS}store_sales LEFT OUTER JOIN {DS}item "
    f"ON ss_item_sk = i_item_sk AND i_category = 'Music'",
    f"SELECT COUNT(*) c FROM {DS}store_sales WHERE ss_item_sk NOT IN "
    f"(SELECT i_item_sk FROM {DS}item WHERE i_category = 'Music')",
]

# Join modes whose probe rows do NOT depend on matching: no row may be tested.
REFUSED_STATEMENTS = [
    f"SELECT COUNT(*) c FROM {DS}store_sales LEFT OUTER JOIN {DS}item "
    f"ON ss_item_sk = i_item_sk AND i_category = 'Music'",
    f"SELECT COUNT(*) c FROM {DS}store_sales FULL OUTER JOIN {DS}item "
    f"ON ss_item_sk = i_item_sk",
    f"SELECT COUNT(*) c FROM {DS}store_sales WHERE ss_item_sk NOT IN "
    f"(SELECT i_item_sk FROM {DS}item WHERE i_category = 'Music')",
    f"SELECT COUNT(*) c FROM {DS}store_sales s WHERE NOT EXISTS "
    f"(SELECT 1 FROM {DS}item WHERE i_item_sk = s.ss_item_sk AND i_category = 'Music')",
    f"SELECT i_item_sk FROM {DS}item EXCEPT SELECT ss_item_sk FROM {DS}store_sales",
    f"SELECT i_item_sk FROM {DS}item INTERSECT SELECT ss_item_sk FROM {DS}store_sales",
    f"SELECT ss_item_sk IN (SELECT i_item_sk FROM {DS}item WHERE i_category = 'Music') f "
    f"FROM {DS}store_sales LIMIT 10",
]
# fmt:on


def _run(sql, enabled):
    """(rows, per-scan IO diagnostics) with the runtime join filters on or off."""
    session = opteryx.session()
    if not enabled:
        for _ in session.execute_to_morsels("SET disable_runtime_minmax_join_filter = true"):
            pass
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return rows, session.telemetry.get("io_scan_diagnostics") or []


def _tested(scans):
    return sum(s.get("key_filter_rows_in", 0) for s in scans)


def _kept(scans):
    return sum(s.get("key_filter_rows_out", 0) for s in scans)


@pytest.mark.parametrize("statement", ORACLE_STATEMENTS)
def test_oracle_identical_results(statement):
    """The key filter only ever skips rows the join would drop: on == off."""
    off_rows, off_scans = _run(statement, False)
    on_rows, _ = _run(statement, True)
    assert _tested(off_scans) == 0, "the key filter tested rows with the switch off"
    assert sorted(on_rows, key=repr) == sorted(off_rows, key=repr), (
        f"the runtime key filter changed the ANSWER for: {statement}"
    )


@pytest.mark.parametrize("statement", REFUSED_STATEMENTS)
def test_refused_for_unsound_modes(statement):
    """Only modes that drop unmatched probe rows may test a row."""
    _, scans = _run(statement, True)
    assert _tested(scans) == 0, (
        f"the key filter tested rows for a join mode that emits probe rows "
        f"independently of matching: {statement}"
    )


def test_not_armed_under_range_settlement():
    """Settlement returns its predicate columns empty as filter-only; a key column the
    join reads must never be one of them, so a settled scan carries no key filter."""
    statement = ORACLE_STATEMENTS[3]
    _, scans = _run(statement, True)
    settled = [scan for scan in scans if scan.get("settled_rows", 0) > 0]
    assert settled, "the settlement shape did not settle — the test would assert nothing"
    for scan in settled:
        assert scan.get("key_filter_rows_in", 0) == 0, (
            "a key filter was armed on a range-settled scan"
        )


def test_positive_control_exact_set():
    """Scattered keys (one item category across store_sales): the min/max bound has
    nothing to prune, the key filter must reject most rows."""
    _, scans = _run(ORACLE_STATEMENTS[0], True)
    tested, kept = _tested(scans), _kept(scans)
    assert tested > 0, "the key filter did not fire on its own case"
    assert kept < tested / 2, f"the key filter kept {kept} of {tested} rows"


def test_positive_control_bloom():
    """A build side above the exact-set limit goes through the bloom and still rejects."""
    _, scans = _run(ORACLE_STATEMENTS[4], True)
    tested, kept = _tested(scans), _kept(scans)
    assert tested > 0, "the bloom-shaped key filter did not fire"
    assert kept < tested, "the bloom-shaped key filter rejected nothing"


def test_positive_control_with_pushed_predicate():
    """The prefilter path: the key bits AND into the predicate's survivor mask, so the
    rows tested are the predicate's survivors, and fewer come out."""
    _, scans = _run(ORACLE_STATEMENTS[2], True)
    tested, kept = _tested(scans), _kept(scans)
    assert tested > 0, "the key filter did not fire alongside a pushed predicate"
    assert kept < tested


if __name__ == "__main__":  # pragma: no cover
    for i, sql in enumerate(ORACLE_STATEMENTS):
        test_oracle_identical_results(sql)
        print(f"  oracle {i + 1:02d} OK")
    for i, sql in enumerate(REFUSED_STATEMENTS):
        test_refused_for_unsound_modes(sql)
        print(f"  refused {i + 1:02d} OK")
    test_not_armed_under_range_settlement()
    test_positive_control_exact_set()
    test_positive_control_bloom()
    test_positive_control_with_pushed_predicate()
    print("--- done")
