"""Local parquet scans decode on a process-lifetime pool shared by every query.

`open_native_scan_plan` used to construct and join a decode pool per query; local
scans now reach one long-lived pool through ParquetIOPipeline's injection path.
These tests pin that sharing it never changes an answer: repeated queries reuse it,
and concurrent queries in one process interleave their tickets on it.
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

QUERIES = [
    "SELECT COUNT(*), SUM(ResolutionWidth) FROM testdata.clickbench_tiny WHERE AdvEngineID = 0",
    "SELECT RegionID, COUNT(*) AS c FROM testdata.clickbench_tiny GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 10",
    "SELECT COUNT(DISTINCT UserID) FROM testdata.clickbench_tiny",
    "SELECT SearchPhrase FROM testdata.clickbench_tiny WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 5",
]


def _run(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            rows.append(tuple(morsel[i]))
    sources = session.telemetry.get("scan_sources") or {}
    session.close()
    assert sources and set(sources.values()) == {"NativeParquetScanSource"}, sources
    return rows


def test_repeated_queries_reuse_the_pool_and_agree():
    expected = [_run(sql) for sql in QUERIES]
    for _ in range(5):
        assert [_run(sql) for sql in QUERIES] == expected


def test_concurrent_queries_share_the_pool_and_agree():
    expected = {sql: _run(sql) for sql in QUERIES}
    work = QUERIES * 8
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_run, work))
    for sql, rows in zip(work, results):
        assert rows == expected[sql], sql
