"""
TPC-H RESULT-checking battery (value correctness, not just execution).

Runs each verified TPC-H query against the SF=0.01 dataset (testdata/tpch_001)
and asserts the result matches a checked-in golden generated from DuckDB
(dev/tpch/generate_slt_golden.py). Comparison is order-insensitive and
decimal/float-tolerant (see _tpch_golden.normalize_rows).

This complements test_battery_tpch.py (execution-only). It is the first
value-checking coverage of the join / GROUP BY / aggregate / decimal paths.

Only queries Opteryx currently computes CORRECTLY (verified == DuckDB) are
asserted here — that set is `VERIFIED`. The remaining TPC-H queries are
EXCLUDED below with the reason; as each is fixed, regenerate the golden and
move the query id into VERIFIED so it becomes protected.

A fresh session is used per query, and only the verified set runs, to avoid a
pre-existing cumulative-memory instability that segfaults after ~14 TPC-H
queries in one process (tracked separately).

EXCLUDED (known-wrong / unsupported at SF=0.01, as of writing — NOT asserted):
    query02, query07           : return empty / wrong row set
    query03                    : date column rendered as raw int, not a date
    query06, query08, query14  : wrong aggregate / arithmetic value
    query10                    : wrong values / row count (incl. ORDER BY not sorting)
    query16                    : raises KeyError ('s_suppkey')
    query11, query22           : Opteryx-dialect SQL DuckDB can't parse (no oracle golden)


query13 moved into VERIFIED 2026-07-26: the previous WHERE-clause rewrite of the
LEFT OUTER JOIN filter silently dropped the c_count=0 bucket (customers with no
qualifying orders); fixed by pre-filtering orders in a derived table before the
join instead (query13.sql), verified == DuckDB oracle.

query17 and query20 ADDED to VERIFIED 2026-07-26: canonical Q17/Q20 use a
correlated scalar subquery, which is now decorrelated. That moved OFF the
syntactic pre-bind rewriter and INTO the optimizer, where the plan is bound —
the binder resolves a subquery's names against its own scope first and the
enclosing one second, so the correlation's orientation is a resolved fact rather
than a guess from whether the author wrote a table qualifier. The old pre-bind
rewrite could not decide `WHERE l_partkey = p_partkey` (both sides bare) at all.
See opteryx/planner/optimizer/strategies/decorrelate_subquery.py.

query18 ADDED to VERIFIED 2026-07-26: canonical Q18 needed two independent fixes,
both landed. (1) `rename_relations` in the IN-subquery→join rewrite: the subquery
re-scans `lineitem`, which after the plan merge collided with the outer scan and
raised AmbiguousDatasetError — the sibling EXISTS rewrite already did this, the IN
one did not. (2) HAVING pass-through columns: its
`GROUP BY l_orderkey HAVING SUM(l_quantity) > 300` aggregates a column the
subquery never selects, so nothing computed it. Verified == DuckDB oracle.

query04 ADDED to VERIFIED 2026-07-26: canonical `EXISTS (SELECT * ... WHERE
l_orderkey = o.o_orderkey ...)` now runs. Two gaps were fixed in
exists_subquery_to_join.py — an unqualified inner correlation column was
misclassified as an outer reference, and a `SELECT *` subquery carries no Project
node for the rewriter to overwrite (one is now synthesized). Result verified ==
DuckDB oracle.

query21 ADDED to VERIFIED 2026-07-26: canonical Q21 correlates with a NON-equality
predicate (`l2.l_suppkey <> l1.l_suppkey`) alongside the equi key. That used to be
left inside the subquery, where the outer column goes out of scope once the
subquery becomes the join's right-hand relation. It is now split off and evaluated
per candidate (build, probe) pair INSIDE the filter join's existence test — see
SemiAntiProbeOperator in native_join2.hpp. Verified == DuckDB oracle (numwait=9;
the previous CTE-based rewrite of this query returned 15).
"""

import os
import re
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _tpch_golden

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

from _tpch_golden import (
    TPCH_SCALE,
    load_golden,
    normalize_rows,
    opteryx_result_rows,
)

# Queries Opteryx computes correctly at SF=0.01 (cross-checked vs DuckDB).
VERIFIED = [
    "query01",
    "query04",
    "query05",
    "query09",
    "query12",
    "query13",
    "query15",
    "query17",
    "query18",
    "query19",
    "query20",
    "query21",
]

_QUERY_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "performance", "tpch", "opteryx", "queries",
)

_GOLDEN = load_golden()


@pytest.mark.parametrize("query_id", VERIFIED)
def test_tpch_result(query_id):
    assert query_id in _GOLDEN, (
        f"no golden for {query_id} — run dev/tpch/generate_slt_golden.py"
    )
    path = os.path.join(_QUERY_DIR, f"{query_id}.sql")
    sql = re.sub(r"testdata\.tpch\.", f"testdata.tpch_{TPCH_SCALE}.", open(path).read())

    actual = normalize_rows(opteryx_result_rows(opteryx.session(), sql))
    expected = _GOLDEN[query_id]

    assert actual == expected, (
        f"{query_id}: result mismatch\n"
        f"  rows: opteryx={len(actual)} golden={len(expected)}"
    )


if __name__ == "__main__":  # pragma: no cover
    passed = failed = 0
    for qid in VERIFIED:
        try:
            test_tpch_result(qid)
            print(f"  {qid}: PASS")
            passed += 1
        except AssertionError as err:
            print(f"  {qid}: FAIL — {err}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
