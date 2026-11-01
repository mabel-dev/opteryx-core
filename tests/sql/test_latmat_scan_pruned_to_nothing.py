"""
A late-materialising top-N scan whose predicate prunes every row group answers 0 rows.

`WHERE <pred> ORDER BY <col> LIMIT n` over parquet takes the two-pass LatmatScanSource.
When statistics pruning removed every row group, open_native_scan_plan returned before
sizing the submission window, leaving it 0, and the source refused the scan:
"LatmatScanSource: in_flight_limit must be at least 1". With no work items the empty
answer is the answer. Found by the single-table fuzzer (testdata.missions has no
negative Price, so the BETWEEN below prunes everything).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def _rows(sql):
    return [morsel[i] for morsel in opteryx.session().execute_to_morsels(sql) for i in range(len(morsel))]


def test_topn_scan_pruned_to_nothing_returns_no_rows():
    sql = (
        'SELECT "Mission", "Company" FROM testdata.missions '
        'WHERE "Price" BETWEEN -129375.4 AND -18635.2 ORDER BY "Mission" LIMIT 3'
    )
    assert _rows(sql) == []


def test_topn_scan_with_survivors_still_returns_them():
    sql = 'SELECT "Mission" FROM testdata.missions WHERE "Price" BETWEEN 1 AND 50 ORDER BY "Mission" LIMIT 2'
    assert len(_rows(sql)) == 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
