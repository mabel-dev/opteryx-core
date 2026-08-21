# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
End-to-end: TopNManifestPruningStrategy (manifest min/max file pruning for
`ORDER BY <col> [ASC|DESC] LIMIT n`).

Each test writes several real Parquet files under a throwaway `testdata/`
subdirectory, runs `ANALYZE TABLE` (needed for real lower_bounds/upper_bounds
via this connector - see filesystem_connector.py), then runs the SAME
`SELECT * ... ORDER BY ... LIMIT n` query with the optimization enabled and
again force-disabled (FEATURE flag), asserting the two runs return IDENTICAL
rows. That equality is the correctness invariant that matters; the telemetry
checks exist only to prove the strategy actually fired (a passing equality
check where the strategy silently never ran would be a false green - it very
nearly WAS one, see the note on query shape below).

Query shape note: TopNScanPushdownStrategy (which stamps the topn spec this
strategy consumes) only fires when the HeapSort reads DIRECTLY from the Scan
- `SELECT project, seq FROM t ORDER BY project LIMIT n` leaves a Project node
between them and is out of its scope, `SELECT * FROM t ORDER BY ... LIMIT n`
is not. These tests deliberately use `SELECT *` (the ask's own query shape)
for that reason - a first draft of this file used an explicit column list and
every test "passed" while the optimization silently never ran at all.
"""

import shutil
import sys
import os
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config
from rugo.parquet import write_parquet


def _write_dataset(dir_name, file_batches):
    """Write one Parquet file per (sql) entry in `file_batches` under
    testdata/<dir_name>/, then ANALYZE it. Returns the dataset name."""
    ds_dir = Path("testdata") / dir_name
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    session = opteryx.session()
    for i, sql in enumerate(file_batches):
        morsel = list(session.execute_to_morsels(sql))[0]
        with open(ds_dir / f"part-{i}.parquet", "wb") as f:
            f.write(write_parquet(morsel))
    dataset = f"testdata.{dir_name}"
    list(session.execute_to_morsels(f"ANALYZE TABLE {dataset}"))
    return dataset, ds_dir


def _rows(session, sql, columns):
    morsels = list(session.execute_to_morsels(sql))
    cols = [[] for _ in columns]
    for m in morsels:
        for i, c in enumerate(columns):
            cols[i].extend(m.column(c.encode()).to_pylist())
    return list(zip(*cols)) if cols[0] else []


def _run_on_and_off(session, sql, columns):
    on_rows = _rows(session, sql, columns)
    on_telemetry = dict(session.telemetry)

    original = config.features.disable_topn_manifest_pruning
    try:
        config.features.disable_topn_manifest_pruning = True
        off_rows = _rows(session, sql, columns)
    finally:
        config.features.disable_topn_manifest_pruning = original

    return on_rows, on_telemetry, off_rows


def test_desc_pruning_matches_baseline_and_actually_prunes():
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_desc",
        [
            "SELECT * FROM (VALUES ('apple',1),('avocado',2),('banana',3)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('carrot',4),('durian',5),('eggplant',6)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('fig',7),('grape',8),('jackfruit',9)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('kiwi',10),('lemon',11),('melon',12)) AS t(project, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = f"SELECT * FROM {dataset} ORDER BY project DESC LIMIT 5"
        on_rows, on_telemetry, off_rows = _run_on_and_off(session, sql, ["project", "seq"])

        all_rows = [
            ("apple", 1), ("avocado", 2), ("banana", 3), ("carrot", 4), ("durian", 5),
            ("eggplant", 6), ("fig", 7), ("grape", 8), ("jackfruit", 9),
            ("kiwi", 10), ("lemon", 11), ("melon", 12),
        ]
        expected = sorted(all_rows, key=lambda r: r[0], reverse=True)[:5]

        assert on_rows == expected, on_rows
        assert on_rows == off_rows, (on_rows, off_rows)
        # The top file (kiwi/lemon/melon) alone has 3 rows, short of LIMIT 5,
        # so this needs the top TWO files (6 rows) and prunes the bottom two.
        assert on_telemetry.get("files_pruned", 0) == 2, on_telemetry
        assert on_telemetry.get("optimization_topn_manifest_pruning", 0) >= 1, on_telemetry
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_asc_pruning_matches_baseline_and_actually_prunes():
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_asc",
        [
            "SELECT * FROM (VALUES ('apple',1),('avocado',2),('banana',3)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('carrot',4),('durian',5),('eggplant',6)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('fig',7),('grape',8),('jackfruit',9)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('kiwi',10),('lemon',11),('melon',12)) AS t(project, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = f"SELECT * FROM {dataset} ORDER BY project ASC LIMIT 5"
        on_rows, on_telemetry, off_rows = _run_on_and_off(session, sql, ["project", "seq"])

        all_rows = [
            ("apple", 1), ("avocado", 2), ("banana", 3), ("carrot", 4), ("durian", 5),
            ("eggplant", 6), ("fig", 7), ("grape", 8), ("jackfruit", 9),
            ("kiwi", 10), ("lemon", 11), ("melon", 12),
        ]
        expected = sorted(all_rows, key=lambda r: r[0])[:5]

        assert on_rows == expected, on_rows
        assert on_rows == off_rows, (on_rows, off_rows)
        assert on_telemetry.get("files_pruned", 0) == 2, on_telemetry
        assert on_telemetry.get("optimization_topn_manifest_pruning", 0) >= 1, on_telemetry
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_tied_boundary_value_across_two_files_keeps_both():
    # 'grape' is the max of TWO separate files. A threshold computed as a
    # strict `>` (instead of the required inclusive `>=`) would wrongly drop
    # one of them and silently lose a row that belongs in the top-N.
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_tie",
        [
            "SELECT * FROM (VALUES ('grape',1),('apple',2)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('grape',3),('banana',4)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('carrot',5),('date',6),('eggplant',7),('fig',8)) AS t(project, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = f"SELECT * FROM {dataset} ORDER BY project DESC LIMIT 3"
        on_rows, on_telemetry, off_rows = _run_on_and_off(session, sql, ["project", "seq"])

        assert sorted(on_rows) == sorted(off_rows), (on_rows, off_rows)
        assert [r[0] for r in on_rows] == ["grape", "grape", "fig"], on_rows
        assert {r[1] for r in on_rows if r[0] == "grape"} == {1, 3}, on_rows
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_declined_predicate_disables_the_optimization():
    """A predicate the connector DECLINED still cuts rows, and this optimization
    must not fire under one.

    The guard inside the strategy reads `node.predicates`, which is populated ONLY
    when `can_push` accepted — so it looks like a declined predicate (left as a
    Filter NODE above the scan) slips past it. It does not, and the protection is
    UPSTREAM: TopNScanPushdownStrategy stamps the top-N spec only when the HeapSort
    reads directly from the Scan, so a surviving Filter node between them means
    this strategy is never armed. That is an invariant of another strategy, held in
    another file, with nothing here depending on it visibly — which is exactly the
    kind of thing that breaks silently and returns too few rows. This test is the
    thing that would notice.

    `project = 'melon' OR seq < 3` is the vehicle: a disjunction over two columns is
    outside `PredicatePushable._SIMPLE_NODE_TYPES`, so it is declined and left as a
    Filter. It matches melon(12) in the TOP file by `project` and avocado(2),
    apple(1) in the BOTTOM one — so pruning to the top files by sort-key bounds
    would lose two of the three answer rows rather than merely read too much.
    """
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_declined",
        [
            "SELECT * FROM (VALUES ('apple',1),('avocado',2),('banana',3)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('carrot',4),('durian',5),('eggplant',6)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('fig',7),('grape',8),('jackfruit',9)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('kiwi',10),('lemon',11),('melon',12)) AS t(project, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = (f"SELECT * FROM {dataset} WHERE project = 'melon' OR seq < 3 "
               "ORDER BY project DESC LIMIT 5")
        on_rows, on_telemetry, off_rows = _run_on_and_off(session, sql, ["project", "seq"])

        # The premise: this predicate really is declined, so it really is a Filter
        # node sitting between the HeapSort and the Scan. Without this the test
        # would still pass if pushdown started accepting ORs, while no longer
        # exercising the declined path at all.
        assert on_telemetry.get("optimization_predicate_pushdown_into_scan", 0) == 0, (
            "the OR was pushed into the scan — this test no longer exercises the "
            "declined-predicate path: %s" % on_telemetry)

        assert on_rows == [("melon", 12), ("avocado", 2), ("apple", 1)], on_rows
        assert on_rows == off_rows, (on_rows, off_rows)
        assert on_telemetry.get("files_pruned", 0) == 0, (
            "files were pruned under a residual Filter — the top-N threshold "
            "counted rows the filter removes: %s" % on_telemetry)
        assert on_telemetry.get("optimization_topn_manifest_pruning", 0) == 0, on_telemetry
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_null_in_sort_column_disables_the_optimization_entirely():
    # v1 scope gate: any NULL anywhere in the sort column and this strategy
    # must not touch the file list at all (see module docstring on
    # TopNManifestPruningStrategy for why - NULL/ASC top-n pruning is exactly
    # the bug class apply_topn_null_asc_bug already burned once).
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_null",
        [
            "SELECT * FROM (VALUES ('apple',1),('avocado',2)) AS t(project, seq)",
            "SELECT * FROM (VALUES ('melon',4),(NULL,3)) AS t(project, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = f"SELECT * FROM {dataset} ORDER BY project DESC LIMIT 2"
        rows = _rows(session, sql, ["project", "seq"])
        telemetry = dict(session.telemetry)

        assert telemetry.get("optimization_topn_manifest_pruning", 0) == 0, telemetry
        assert rows == [("melon", 4), ("avocado", 2)], rows
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_float_sort_column_disables_the_optimization():
    dataset, ds_dir = _write_dataset(
        "_tmp_topn_float",
        [
            "SELECT * FROM (VALUES (1.5,1),(2.5,2)) AS t(x, seq)",
            "SELECT * FROM (VALUES (100.5,3),(200.5,4)) AS t(x, seq)",
        ],
    )
    try:
        session = opteryx.session()
        sql = f"SELECT * FROM {dataset} ORDER BY x DESC LIMIT 2"
        rows = _rows(session, sql, ["x", "seq"])
        telemetry = dict(session.telemetry)

        assert telemetry.get("optimization_topn_manifest_pruning", 0) == 0, telemetry
        assert rows == [(200.5, 4), (100.5, 3)], rows
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
