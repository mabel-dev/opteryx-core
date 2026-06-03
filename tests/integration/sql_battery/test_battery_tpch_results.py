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

EXCLUDED (known-wrong at SF=0.01, as of writing — NOT asserted):
    query02, query07, query20 : return empty / wrong row set
    query03                   : date column rendered as raw int, not a date
    query06, query08, query14 : wrong aggregate / arithmetic value
    query10, query13, query21 : wrong values / row count (incl. ORDER BY not sorting)
    query16                   : raises KeyError ('s_suppkey')
    query18                   : cumulative-run segfault (runs correctly in isolation)
    query04, query11, query22 : Opteryx-dialect SQL DuckDB can't parse (no oracle golden)
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
    "query05",
    "query09",
    "query12",
    "query15",
    "query17",
    "query19",
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
