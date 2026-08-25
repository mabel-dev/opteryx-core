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
    availability as est_rows; NULL when no column at that node carried a known
    size (see ColumnStatistics.total_bytes),
  * an OPTIMIZATIONS section listing which optimizer rules fired,
  * and, for EXPLAIN ANALYZE, ``rows``/``time_ms``/``cpu_ms``/``self_ms``/``dop``
    -- the actual numbers to compare est_rows against, plus the wall-vs-CPU split
    that says whether a slow node was expensive or starved.

Architect rulings D1/D3 (2026-08-25), see docs/EXECUTION_PROFILING_IMPALA_GAP.md:
an estimate that was never computed renders NULL, never 0 — 0 is a legitimate
estimate and cannot also mean "unknown".
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
    assert names == [
        "tree", "details", "est_rows", "est_bytes",
        "rows", "time_ms", "cpu_ms", "merge_ms", "self_ms", "dop",
    ]
    # the single scan's row count surfaces (5 nations in region 1). Filtered for
    # None because the OPTIMIZATIONS/REWRITE TRACE rows are not plan nodes and
    # every numeric column on them is NULL, not 0 -- see _append_no_reading.
    assert max(v for v in data["rows"] if v is not None) == 5, data["rows"]


def test_explain_analyze_has_cpu_and_dop():
    """cpu_ms/dop are the D1 active-vs-wait surface: time_ms is WALL, cpu_ms is
    CPU actually burned, and both are summed across `dop` workers. The engine has
    always recorded cpu_ns (OpStats, always-on) -- this asserts it is rendered."""
    _, data = _explain("EXPLAIN ANALYZE SELECT n_name FROM testdata.tpch_001.nation WHERE n_regionkey = 1")
    scan_idx = next(i for i, line in enumerate(data["tree"]) if "Parquet Read" in line)
    assert data["dop"][scan_idx] >= 1, data["dop"]
    # a scan that read real rows burned real CPU
    assert data["cpu_ms"][scan_idx] > 0.0, data["cpu_ms"]


def test_explain_unknown_estimate_is_null_not_zero():
    """D3: `refresh_statistics` runs opportunistically, so a node it never reached
    has NO estimate -- distinct from an estimate OF zero. The non-plan-node filler
    rows (OPTIMIZATIONS / REWRITE TRACE headings) are the always-available case of
    a row that genuinely has no estimate, so they must render NULL."""
    _, data = _explain("EXPLAIN " + _JOIN_QUERY)
    opt_idx = data["tree"].index("OPTIMIZATIONS")
    assert data["est_rows"][opt_idx] is None, data["est_rows"]
    assert data["est_bytes"][opt_idx] is None, data["est_bytes"]


def test_explain_analyze_non_plan_rows_are_null_in_every_column():
    """A section heading / optimizer-rule row is not a plan node: it has no
    estimate, ran nothing and produced nothing. Every numeric column on it is
    NULL, not 0 -- 0 is a legitimate reading (a node can emit zero rows in zero
    measurable time) so it cannot also mean "no reading here"."""
    names, data = _explain("EXPLAIN ANALYZE " + _JOIN_QUERY)
    numeric = [n for n in names if n not in ("tree", "details")]
    for heading in ("OPTIMIZATIONS", "REWRITE TRACE"):
        if heading not in data["tree"]:
            continue
        idx = data["tree"].index(heading)
        for column in numeric:
            assert data[column][idx] is None, (heading, column, data[column][idx])


def test_explain_analyze_forces_estimate_refresh():
    """D3, second half: ANALYZE forces refresh_statistics so every explained plan
    node carries an estimate -- a column that is blank half the time cannot serve
    as the est-vs-actual cardinality audit that is the whole point of ANALYZE."""
    _, data = _explain("EXPLAIN ANALYZE " + _JOIN_QUERY)
    # every row that is a real plan node (i.e. above the OPTIMIZATIONS heading)
    # must have an estimate
    end = data["tree"].index("OPTIMIZATIONS")
    assert all(v is not None for v in data["est_rows"][:end]), data["est_rows"][:end]


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


def test_explain_analyze_reports_breaker_cost():
    """P2: combine() and finalize() -- the two Sink calls that run after the morsels
    stop -- used to be timed by nothing, so a breaker's merge and result construction
    were real work charged to no plan node. merge_ms is that cost. It is zero on a
    scan (a Source has neither call) and non-zero on a sink that actually merges."""
    _, data = _explain(
        "EXPLAIN ANALYZE SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY planetId"
    )
    scan_idx = next(i for i, line in enumerate(data["tree"]) if "Reader" in line or "Read" in line)
    assert data["merge_ms"][scan_idx] == 0.0, data["merge_ms"]
    # some node in the plan is a breaker and paid a measurable breaker cost
    plan_end = data["tree"].index("OPTIMIZATIONS") if "OPTIMIZATIONS" in data["tree"] else len(data["tree"])
    assert any(v is not None and v > 0.0 for v in data["merge_ms"][:plan_end]), data["merge_ms"]


def test_pipeline_stats_report_barrier_skew():
    """P3: worker skew at the pipeline barrier. exec_ns is SUMMED across workers and
    so cannot distinguish "all sixteen busy briefly" from "one busy for a long time";
    the spread of worker finish times can, and nothing recorded it before."""
    import opteryx

    session = opteryx.session()
    for _ in session.execute_to_morsels(
        "SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY planetId"
    ):
        pass
    pipelines = session._telemetry._reading["native_pipeline_stats"]
    assert pipelines, "no pipeline stats harvested"
    for row in pipelines:
        # present, well-formed, and bounded by the pipeline's own wall clock --
        # a skew wider than the run itself would be a clock or lifetime bug
        assert "skew_time" in row and "barrier_idle_time" in row, row
        assert row["skew_time"] >= 0, row
        assert row["skew_time"] <= row["wall_time"], row
        # every worker idles for (last_finish - own_finish), so the total is
        # bounded by workers * skew
        assert row["barrier_idle_time"] <= row["dop"] * row["skew_time"] + 1, row
