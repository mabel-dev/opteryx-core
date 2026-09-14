# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
File-backed scans and the scan-absorbing pushdowns.

Two things changed for parquet alongside the remote-SQL work and both need a
regression here:

  - a LIMIT may now be pushed onto a scan that already absorbed a predicate
    (`supports_filtered_limit_pushdown`). The parquet reader decrements its
    LIMIT against EMITTED rows, so the rows returned must all satisfy the
    predicate and be exactly LIMIT many;

  - `TopNScanPushdownStrategy` moved after ProjectFusion. Before, the Project a
    SELECT list leaves between the HeapSort and the Scan blocked it for every
    query except `SELECT *`; it now fires for an explicit projection, and the
    result must be identical to the un-pushed plan (the HeapSort makes the
    canonical cut either way).

The aggregate/DISTINCT pushdowns must NOT fire on a file scan — the file
connectors do not (and cannot) promise a complete aggregate from one read.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config
from opteryx import register_workspace
from opteryx.connectors import DiskConnector

register_workspace("testdata", DiskConnector)

TABLE = "testdata.flat.formats.parquet"


def run(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return rows


def run_without(flag, sql):
    setattr(config.features, flag, True)
    try:
        return run(sql)
    finally:
        setattr(config.features, flag, False)


def explain_text(sql):
    parts = []
    for row in run("EXPLAIN " + sql):
        parts.append(
            " ".join(v.decode() if isinstance(v, (bytes, bytearray)) else str(v) for v in row)
        )
    return "\n".join(parts)


def test_filtered_limit_is_pushed_and_every_row_satisfies_the_predicate():
    sql = f"SELECT tweet_id, followers FROM {TABLE} WHERE followers > 100 LIMIT 5"
    rows = run(sql)
    assert len(rows) == 5
    assert all(followers > 100 for _tweet_id, followers in rows)
    text = explain_text(sql)
    assert "limit pushdown" in text
    assert "predicate pushdown into sc" in text
    assert "Limit" not in text  # absorbed, not relocated


def test_filtered_limit_larger_than_the_match_count_returns_every_match():
    matches = run(f"SELECT tweet_id FROM {TABLE} WHERE followers > 5000000")
    assert 0 < len(matches) < 1000
    limited = run(f"SELECT tweet_id FROM {TABLE} WHERE followers > 5000000 LIMIT 1000")
    assert sorted(limited) == sorted(matches)


def test_topn_fires_with_an_explicit_projection_and_matches_unpushed():
    sql = f"SELECT tweet_id, user_name FROM {TABLE} ORDER BY tweet_id DESC LIMIT 5"
    pushed = run(sql)
    assert len(pushed) == 5
    assert pushed == run_without("disable_topn_scan_pushdown", sql)
    assert "topn scan pushdown" in explain_text(sql)


def test_topn_with_a_predicate_matches_unpushed():
    sql = f"SELECT tweet_id FROM {TABLE} WHERE followers > 3 ORDER BY tweet_id ASC LIMIT 10"
    pushed = run(sql)
    assert len(pushed) == 10
    assert pushed == run_without("disable_topn_scan_pushdown", sql)
    assert "topn scan pushdown" in explain_text(sql)


def test_multi_key_topn_is_declined_by_the_parquet_reader():
    sql = f"SELECT tweet_id, followers FROM {TABLE} ORDER BY followers DESC, tweet_id ASC LIMIT 5"
    assert run(sql) == run_without("disable_topn_scan_pushdown", sql)
    assert "topn scan pushdown" not in explain_text(sql)


def _operator_in_plan(text, name):
    return re.search(rf"(?:^|─ ){name}\b", text, flags=re.MULTILINE) is not None


def test_aggregate_and_distinct_are_not_absorbed_by_a_file_scan():
    agg = f"SELECT user_verified, COUNT(*) FROM {TABLE} GROUP BY user_verified"
    text = explain_text(agg)
    assert "aggregate scan pushdown" not in text
    assert _operator_in_plan(text, "Grouped Aggregate")
    distinct = f"SELECT DISTINCT user_verified FROM {TABLE}"
    text = explain_text(distinct)
    assert "distinct scan pushdown" not in text
    assert _operator_in_plan(text, "Distinction")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
