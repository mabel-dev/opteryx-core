"""Join build-side consolidation: the decision, and why, on the telemetry surface.

`Join2BuildSink` decides ONCE, at finalize, whether to consolidate its retained build
payload into a single block. That decision picks which emit path the probe takes for
the build half of every output row:

    consolidated  -> share the block, materialise 4 bytes of code per column per row
    declined      -> gather one physical value per column per OUTPUT row

On a string-carrying payload that is an 8-13x difference in bytes moved, and the gate
is driven by a CARDINALITY ESTIMATE — so it can silently flip between two runs of a
BYTE-IDENTICAL plan. It did: a join estimated at 3 rows emitted 2.55 billion, took the
dense gather for all of them, and ran 70.6s where the same plan with a corrected
estimate ran 18.3s. Same operators, same order, same row counts. Nothing in EXPLAIN,
EXPLAIN ANALYZE or telemetry distinguished the two; it had to be inferred from the
gate's arithmetic and a stopwatch.

These tests pin the decision record that closes that hole. They assert the DISTINCT
signature of each branch, because the branches are not diagnostically equivalent:
`declined_no_estimate` / `declined_not_amortised` are ESTIMATOR facts (fixable
upstream), while `declined_single_morsel` / `declined_payload_too_narrow` /
`declined_array_payload` are by-design refusals no estimate would move.

Observability only — none of this changes the decision, and these tests deliberately
assert nothing about which queries consolidate beyond what the gate already did.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

TPCH = "testdata.tpch_1"

_EXPECTED_KEYS = {
    "identity",
    "node",
    "outcome",
    "consolidated",
    "estimated_rows",
    "build_rows",
    "build_morsels",
    "payload_bytes",
    "dense_bytes_per_row",
    "code_bytes_per_row",
}


def _diagnostics(sql):
    """Run `sql` to completion and return its join build-side decision records."""
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    return session.telemetry.get("join_build_diagnostics")


def test_every_join_reports_a_decision():
    """A query with a Join2 build sink ALWAYS carries a record. Silence is the bug
    this exists to remove, so 'no key' and 'declined' must not look alike."""
    diags = _diagnostics(
        "SELECT a.name, b.name FROM $planets a INNER JOIN $planets b ON a.id = b.id"
    )
    assert diags, "a join query reported no build-side decision at all"
    assert len(diags) == 1
    row = diags[0]
    assert set(row) == _EXPECTED_KEYS
    assert row["identity"], "the decision is not attributable to a plan node"
    # NotReached is what a build sink whose finalize never ran would report (a worker
    # errored first). A query that returned rows must never show it.
    assert row["outcome"] != "not_reached"


def test_no_join_reports_nothing():
    """No build sink, no record — the key is absent rather than an empty list, so it
    costs nothing on the overwhelming majority of queries."""
    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT name FROM $planets"):
        pass
    assert session.telemetry.get("join_build_diagnostics") is None


def test_single_morsel_build_declines_by_design():
    """A build side already in one block: consolidating would copy it for nothing.
    Reported as its own reason so it is never mistaken for an estimate problem."""
    diags = _diagnostics(
        "SELECT a.name, b.name FROM $planets a INNER JOIN $planets b ON a.id = b.id"
    )
    row = diags[0]
    assert row["outcome"] == "declined_single_morsel"
    assert row["consolidated"] is False
    assert row["build_morsels"] == 1
    assert row["build_rows"] == 9
    # The gate returned before measuring — reported as 0, not as a fabricated number.
    assert row["payload_bytes"] == 0
    assert row["dense_bytes_per_row"] == 0.0


def test_wide_string_payload_consolidates_and_reports_the_ratio():
    """The shape this optimization exists for: a string-carrying payload against a
    large fanout. The record must carry BOTH per-row costs, because their ratio is
    the whole argument for taking the path."""
    diags = _diagnostics(
        f"""SELECT c.c_name, c.c_address, c.c_comment, o.o_orderkey
            FROM {TPCH}.customer c INNER JOIN {TPCH}.orders o
            ON c.c_custkey = o.o_custkey"""
    )
    row = diags[0]
    assert row["outcome"] == "consolidated"
    assert row["consolidated"] is True
    assert row["build_morsels"] > 1
    assert row["build_rows"] == 149999
    assert row["payload_bytes"] > 0
    # 3 payload columns -> 12 bytes of code per output row, against a dense cost this
    # test asserts only as "much larger" (the exact byte count is data, not contract).
    assert row["code_bytes_per_row"] == 12.0
    assert row["dense_bytes_per_row"] > 8 * row["code_bytes_per_row"]


def test_narrow_payload_declines_with_the_measurement_that_refused_it():
    """Same join, same estimate, fixed-width payload — declined because codes are not
    a real saving over 8-byte values. The record must show the measured ratio that
    made the call, so the decline is checkable rather than assertable."""
    diags = _diagnostics(
        f"""SELECT c.c_custkey, c.c_nationkey, o.o_orderkey
            FROM {TPCH}.customer c INNER JOIN {TPCH}.orders o
            ON c.c_custkey = o.o_custkey"""
    )
    row = diags[0]
    assert row["outcome"] == "declined_payload_too_narrow"
    assert row["consolidated"] is False
    # Measured, because this branch is reached AFTER the measurement.
    assert row["payload_bytes"] > 0
    assert row["dense_bytes_per_row"] > 0.0
    assert row["dense_bytes_per_row"] < 4.0 * row["code_bytes_per_row"]


def test_estimate_is_reported_next_to_the_actual_build_size():
    """The pair that makes an estimate-driven decline readable AS one. `estimated_rows`
    is the number the gate was handed; `build_rows` is what the sink actually held.
    Reporting one without the other is what made the original 70.6s query undiagnosable."""
    diags = _diagnostics(
        f"""SELECT c.c_name, c.c_address, c.c_comment, o.o_orderkey
            FROM {TPCH}.customer c INNER JOIN {TPCH}.orders o
            ON c.c_custkey = o.o_custkey"""
    )
    row = diags[0]
    assert row["estimated_rows"] > 0
    assert row["build_rows"] == 149999


def test_missing_estimate_is_its_own_reason():
    """SEMI/ANTI joins are deliberately not tagged by JoinBuildShapeStrategy (they drop
    the build payload, so there is no gather to improve). They therefore decline for
    'no estimate' — and that must be spelled differently from a decline that MADE a
    comparison, since only the latter says anything about the byte model."""
    diags = _diagnostics(
        f"""SELECT o_orderkey FROM {TPCH}.orders
            WHERE o_custkey IN (SELECT c_custkey FROM {TPCH}.customer WHERE c_nationkey = 3)"""
    )
    row = diags[0]
    assert row["outcome"] == "declined_no_estimate"
    assert row["estimated_rows"] == -1
    # The estimate was missing; the BUILD side still was not, and is still reported.
    assert row["build_rows"] > 0


def test_decision_is_reported_per_join_not_per_query():
    """Two joins, two records. A per-query scalar would average away exactly the join
    that took the wrong path."""
    diags = _diagnostics(
        f"""SELECT c.c_name, n.n_name, o.o_orderkey
            FROM {TPCH}.orders o
            INNER JOIN {TPCH}.customer c ON c.c_custkey = o.o_custkey
            INNER JOIN {TPCH}.nation n ON n.n_nationkey = c.c_nationkey"""
    )
    assert len(diags) == 2
    assert len({r["identity"] for r in diags}) == 2


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
