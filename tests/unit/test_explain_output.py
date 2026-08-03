# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-9: EXPLAIN renders a readable operator tree + optimizer-decision trace.

Previously plain EXPLAIN emitted an opaque ``identity / bytes_in / bytes_out``
table with no operator names or structure. It now emits:
  * a ``tree`` column — the indented operator tree (with ├─/└─ branches),
  * a ``details`` column — each operator's config,
  * an ``est_rows`` column — the planner's row-count estimate (statistics_refresh),
    available even without ANALYZE since it needs no execution,
  * an ``est_bytes`` column — the planner's total-byte-size estimate, same
    availability as est_rows; 0 when no column at that node carried a known
    size (see ColumnStatistics.total_bytes),
  * an OPTIMIZATIONS section listing which optimizer rules fired,
  * and, for EXPLAIN ANALYZE, ``rows`` and ``time_ms`` columns -- the actual
    numbers to compare est_rows against.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _explain(sql):
    """Return (column_names, {col: [values]}) for the first EXPLAIN morsel."""
    morsel = list(opteryx.session().execute_to_morsels(sql))[0]
    names = [c.decode() if isinstance(c, bytes) else c for c in morsel.column_names]
    data = {
        n: [v.decode() if isinstance(v, bytes) else v for v in morsel.column(morsel.column_names[i]).to_pylist()]
        for i, n in enumerate(names)
    }
    return names, data


_JOIN_QUERY = (
    "SELECT r_name, COUNT(*) FROM testdata.tpch_001.region r "
    "JOIN testdata.tpch_001.nation n ON r.r_regionkey = n.n_regionkey "
    "WHERE n.n_nationkey > 2 GROUP BY r_name"
)


def test_explain_has_tree_and_details_columns():
    names, _ = _explain("EXPLAIN SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1")
    assert names[:2] == ["tree", "details"]
    # plain EXPLAIN must NOT carry analyze-only columns
    assert "time_ms" not in names


def test_explain_tree_shows_operator_names_and_branches():
    _, data = _explain("EXPLAIN " + _JOIN_QUERY)
    tree = data["tree"]
    joined = "\n".join(tree)
    # operator names appear (not opaque identity hashes)
    assert any("Parquet Read" in line for line in tree), tree
    assert any("Join" in line for line in tree), tree
    assert any("Aggregate" in line for line in tree), tree
    # the tree is actually indented with branch characters
    assert "├─ " in joined or "└─ " in joined, joined


def test_explain_lists_optimizations():
    _, data = _explain("EXPLAIN " + _JOIN_QUERY)
    tree = data["tree"]
    assert "OPTIMIZATIONS" in tree, tree
    # at least one named optimizer rule under the section (predicate pushdown fires here)
    idx = tree.index("OPTIMIZATIONS")
    rules = tree[idx + 1 :]
    assert any("predicate pushdown" in r for r in rules), rules


def test_explain_analyze_adds_stats_columns():
    names, data = _explain("EXPLAIN ANALYZE SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1")
    assert names == ["tree", "details", "est_rows", "est_bytes", "rows", "time_ms", "self_ms"]
    # the single scan's row count surfaces (5 nations in region 1)
    assert max(data["rows"]) == 5, data["rows"]


def test_explain_has_est_rows_without_analyze():
    # est_rows is a planning-time number (statistics_refresh's estimate) --
    # available without running the query, unlike rows/time_ms/self_ms.
    names, data = _explain("EXPLAIN SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1")
    assert names == ["tree", "details", "est_rows", "est_bytes"]
    scan_idx = next(i for i, line in enumerate(data["tree"]) if "Parquet Read" in line)
    assert data["est_rows"][scan_idx] > 0, data["est_rows"]


def test_explain_has_est_bytes_without_analyze():
    # Same planning-time availability as est_rows; testdata.tpch_001.nation
    # is a real Parquet manifest so the scan's est_bytes comes from measured
    # per-column uncompressed size, not just a fixed-width guess.
    names, data = _explain("EXPLAIN SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1")
    scan_idx = next(i for i, line in enumerate(data["tree"]) if "Parquet Read" in line)
    assert data["est_bytes"][scan_idx] > 0, data["est_bytes"]


def test_explain_est_rows_can_be_compared_against_actual_rows():
    # The whole point: est_rows (planner's guess) vs rows (what actually
    # happened) sitting side by side, to catch a bad estimate at a glance.
    names, data = _explain(
        "EXPLAIN ANALYZE SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1"
    )
    scan_idx = next(i for i, line in enumerate(data["tree"]) if "Parquet Read" in line)
    assert data["est_rows"][scan_idx] > 0
    assert data["rows"][scan_idx] == 5


def test_explain_analyze_filter_applies_predicate():
    # EXPLAIN ANALYZE runs the wrapped query on the native engine (the
    # ExitNode ExplainNode wraps is extracted and handed to execute_native —
    # see serial_engine.explain()), the same as any other SELECT; row counts
    # come from telemetry._reading["native_op_stats"], keyed by node identity.
    # Regression guard for a bug where FilterNode had no _push_impl and (on
    # the legacy push-pipeline this used to run on) silently dropped every row.
    names, data = _explain("EXPLAIN ANALYZE SELECT * FROM $planets WHERE id > 2")
    tree = data["tree"]
    rows = data["rows"]
    filter_idx = next(i for i, line in enumerate(tree) if "Filter" in line)
    reader_idx = next(i for i, line in enumerate(tree) if "Reader" in line)
    assert rows[reader_idx] == 9, rows
    assert rows[filter_idx] == 7, rows


def test_explain_mermaid_unchanged():
    names, data = _explain("EXPLAIN ANALYZE FORMAT MERMAID SELECT name FROM $planets")
    assert names == ["plan"]
    assert data["plan"][0].startswith("flowchart")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
