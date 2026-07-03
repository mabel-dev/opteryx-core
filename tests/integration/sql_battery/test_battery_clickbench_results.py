"""
ClickBench RESULT-checking battery (value correctness, not just execution).

Runs each ClickBench query against a checked-in golden generated from DuckDB
(dev/clickbench/generate_golden.py) over the same parquet. Comparison is
order-insensitive and decimal/float-tolerant (see _clickbench_golden.normalize_rows).

This complements tests/performance/clickbench/opteryx/runner.py (timing-only).
The Opteryx-dialect query text is the single source of truth in that runner's
STATEMENTS; the DuckDB-dialect equivalents live in the duckdb runner. The two
are index-aligned (qNN), so a golden keyed by qNN checks the matching query.

Dataset is selected by the CLICKBENCH_DATASET env var ("tiny" default, "full"
for scratch/hits). Only the "tiny" golden is checked in; regenerate "full" with
the generator when running against scratch/hits.

Only queries Opteryx currently computes CORRECTLY (verified == DuckDB) are
asserted here — that set is VERIFIED. As each EXCLUDED query is fixed,
move its id into VERIFIED so it becomes protected.

A fresh session is used per query (mirrors the timing runner and avoids the
cumulative-memory instability seen in long-lived sessions).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _clickbench_golden
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "performance", "clickbench", "opteryx"))

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

from _clickbench_golden import (
    DATASETS,
    DEFAULT_DATASET,
    load_golden,
    normalize_rows,
    opteryx_result_rows,
    query_key,
)
from runner import STATEMENTS  # opteryx-dialect ClickBench statements

DATASET = os.environ.get("CLICKBENCH_DATASET", DEFAULT_DATASET)
RELATION, _ = DATASETS[DATASET]

# qNN -> opteryx SQL (dataset placeholder substituted).
QUERIES = {
    query_key(stmt): stmt.replace("{DATASET}", RELATION) for stmt, _ in STATEMENTS
}

# Queries Opteryx computes correctly on the "tiny" dataset (cross-checked vs
# DuckDB). Set empirically; see EXCLUDED for the rest. As each EXCLUDED query is
# fixed, move its id here so it becomes a protected regression.
#
# NOTE: on "tiny", several VERIFIED queries return zero rows on both sides
# (filters that match nothing in the small slice: q08, q20, q22, q23, q24,
# q37-q43). They still assert Opteryx neither errors nor invents rows, but their
# coverage is weak until run against "full".
VERIFIED = [
    "q01", "q02", "q03", "q04", "q05", "q06", "q08", "q09",
    "q10", "q13", "q14", "q15", "q16", "q20", "q21", "q22",
    "q23", "q24", "q25", "q26", "q27", "q28", "q29", "q30",
    "q34", "q35", "q36", "q37", "q38", "q39", "q40",
    "q41", "q42", "q43",
]

# EXCLUDED (NOT asserted on "tiny"). All remaining exclusions are comparison or
# query-equivalence artifacts — NOT engine bugs. The real engine bugs this battery
# found have been fixed: grouped ORDER BY..LIMIT top-N (heap_sort inversion);
# high-cardinality dict-column corruption (rugo parquet dict fallback); MIN/MAX
# swap+order on stats-only queries (q07 values); 90-column SUM output order (q30);
# AVG int64 sum overflow (q04, now accumulated in double).
#
#   q07 : MIN/MAX values now correct; remaining diff is only EventDate rendered as
#         raw int days vs DuckDB's make_date() — query/golden equivalence (like
#         q19), NOT an engine bug; Opteryx reads EventDate as INT64.
#   NOT engine bugs — comparison can't assert them on "tiny":
#       q11, q12, q17, q31, q32, q33 : ORDER BY <count> DESC LIMIT 10 with ties
#             straddling the boundary. Opteryx's order-key multiset MATCHES
#             DuckDB's; only the tied rows kept at rank 10 differ. A valid top-N,
#             not a wrong answer. (q32/q33 group on WatchID, which is unique per
#             row in tiny — every count is 1, so the kept 10 are arbitrary. The
#             earlier WatchID grouping-collapse bug — a parquet dictionary-
#             fallback decode bug in rugo — is now FIXED; see decode_column.cpp
#             place_plain_dict_codes. q31: rank 10 is a THREE-WAY tie at c=31 —
#             (2,1153450028)/(2,425344525)/(2,-330702184); ranks 1-9 verified
#             identical to DuckDB, and the golden's boundary pick is one of the
#             three. Oracle-checked 2026-07-02: per-group values match exactly.)
#       q19 : the opteryx-dialect text extracts minute from EventTime::TIMESTAMP[ms]
#             while the duckdb-dialect golden uses toDateTime() (epoch seconds*1000).
#             Different EventTime unit => different minute grouping. Query-text
#             equivalence issue, not an engine result bug.
#       q18 : LIMIT 10 with NO ORDER BY — row set is legitimately nondeterministic.
EXCLUDED = [
    "q07", "q11", "q12", "q17", "q18", "q19", "q31", "q32", "q33",
]

_GOLDEN = load_golden(DATASET)


@pytest.mark.parametrize("query_id", VERIFIED)
def test_clickbench_result(query_id):
    assert _GOLDEN, (
        f"no golden for dataset '{DATASET}' — run dev/clickbench/generate_golden.py --dataset {DATASET}"
    )
    assert query_id in _GOLDEN, f"no golden for {query_id}"

    actual = normalize_rows(opteryx_result_rows(opteryx.session(), QUERIES[query_id]))
    expected = _GOLDEN[query_id]

    assert actual == expected, (
        f"{query_id}: result mismatch\n"
        f"  rows: opteryx={len(actual)} golden={len(expected)}"
    )


if __name__ == "__main__":  # pragma: no cover
    # Triage mode: run EVERY golden query, categorise pass / mismatch / error.
    passed, mismatched, errored = [], [], []
    for qid in sorted(_GOLDEN):
        try:
            actual = normalize_rows(opteryx_result_rows(opteryx.session(), QUERIES[qid]))
            if actual == _GOLDEN[qid]:
                passed.append(qid)
                print(f"  {qid}: PASS ({len(actual)} rows)")
            else:
                mismatched.append(qid)
                print(f"  {qid}: MISMATCH (opteryx={len(actual)} golden={len(_GOLDEN[qid])})")
        except Exception as err:
            errored.append(qid)
            print(f"  {qid}: ERROR — {str(err).splitlines()[0][:80]}")
    print(f"\ndataset={DATASET}  pass={len(passed)} mismatch={len(mismatched)} error={len(errored)}")
    if mismatched:
        print("  mismatched:", ", ".join(mismatched))
    if errored:
        print("  errored:   ", ", ".join(errored))
