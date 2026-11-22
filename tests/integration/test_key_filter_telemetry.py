# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The runtime join KEY filter's per-row counts reach the scan's node stats.

A scan with no planned predicate can still emit far fewer rows than it decoded,
because a join's build keys are tested per row in its decode workers. The plan
view explains that from `key_filter_rows_in` / `key_filter_rows_out` on the scan,
so they must be present when the filter tested rows and absent when it did not.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

D = "testdata.tpch_1_rugo."

pytestmark = pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_1_rugo"), reason="testdata/tpch_1_rugo not populated"
)

# TPC-H Q8: the customer and supplier scans each probe a selective build.
Q8 = f"""SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume)
FROM (SELECT EXTRACT(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
      FROM {D}part, {D}supplier, {D}lineitem, {D}orders, {D}customer, {D}nation n1, {D}nation n2, {D}region
      WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY o_year ORDER BY o_year"""


def test_key_filter_counts_on_scan_stats():
    session = opteryx.session()
    for _ in session.execute_to_morsels(Q8):
        pass
    telemetry = session.telemetry
    scans = [r for r in telemetry["operations"].values() if r.get("operator") == "TABLE SCAN"]

    tested = [r for r in scans if "key_filter_rows_in" in r]
    assert tested, "no scan reported key-filter counts"
    for r in tested:
        assert 0 < r["key_filter_rows_out"] < r["key_filter_rows_in"] <= r["parquet_rows_before_filter"]

    # The per-scan totals are the same counters the IO diagnostics report.
    diag_in = sorted(d["key_filter_rows_in"] for d in telemetry["io_scan_diagnostics"] if d["key_filter_rows_in"])
    assert sorted(r["key_filter_rows_in"] for r in tested) == diag_in
