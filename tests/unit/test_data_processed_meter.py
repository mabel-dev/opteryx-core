# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The `data_processed` billing meter records dense LOGICAL bytes.

The commercial definition is the logical uncompressed bytes entering the
system: a zstd column bills its uncompressed bytes, a dictionary-encoded column
bills the equivalent dense bytes. The meter used to be `bytes_fetched` — the
COMPRESSED volume the rugo IO pipeline measured at transfer — which differs
from that definition by the whole compression ratio.

See opteryx/planner/data_processed.py for the definition and the plan-time
ruling these tests pin.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.managers.billing import BillingEventType


def _run(sql: str):
    """Execute `sql` to exhaustion, return its telemetry."""
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    return session._telemetry


def test_meter_is_not_the_compressed_fetch_volume():
    """The regression this whole change exists for.

    A real parquet scan is compressed on disk, so the logical figure has to
    come out ABOVE the fetched one. Equality would mean the meter had silently
    gone back to reporting `bytes_fetched`.
    """
    telemetry = _run("SELECT * FROM testdata.astronauts")

    logical = telemetry.bytes_processed
    fetched = telemetry._reading.get("io_bytes_fetched", 0)

    assert fetched > 0, "no IO measured — the scan did not read the file"
    assert logical > fetched, (
        f"logical bytes ({logical:,}) must exceed compressed bytes off storage "
        f"({fetched:,}); equal means the meter is reporting bytes_fetched again"
    )


def test_meter_is_dense_for_a_fixed_width_column():
    """A fixed-width column's dense size is exactly rows * itemsize.

    No estimation is involved, so this is an equality, not a bound — and it is
    the assertion that fails if the source order ever goes back to preferring
    the parquet footer's encoded size over the native width table.
    """
    # $planets.id is INT8 over 9 rows.
    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT id FROM $planets"):
        pass
    assert session._telemetry.bytes_processed == 9 * 1


def test_projection_narrows_the_meter():
    """Only the columns the query REFERENCES are billed."""
    wide = _run("SELECT * FROM testdata.astronauts").bytes_processed
    narrow = _run("SELECT name FROM testdata.astronauts").bytes_processed

    assert 0 < narrow < wide


def test_a_predicate_does_not_reduce_the_meter():
    """Bytes ENTERING the system, so a filter that discards rows after they
    are read changes nothing. The scan's own statistics are narrowed by
    predicate selectivity on the way up the plan, which is exactly why the
    meter reads `scan_base_statistics` rather than `node.statistics`.
    """
    unfiltered = _run("SELECT id FROM $planets").bytes_processed
    filtered = _run("SELECT id FROM $planets WHERE id > 6").bytes_processed

    assert filtered == unfiltered


def test_a_filter_only_column_is_billed():
    """A column read solely to evaluate a predicate still enters the system."""
    projection_only = _run("SELECT id FROM $planets").bytes_processed
    plus_predicate = _run(
        "SELECT id FROM $planets WHERE mean_temperature > 0"
    ).bytes_processed

    assert plus_predicate > projection_only


def test_explain_bills_nothing():
    """EXPLAIN plans the query and describes it. Nothing is read."""
    assert _run("EXPLAIN SELECT * FROM testdata.astronauts").bytes_processed == 0


def test_no_table_bills_nothing():
    """`$no_table` is the planner's stand-in for a statement with no FROM, and
    for a statistics-only answer. It is not a relation the user named."""
    assert _run("SELECT 1").bytes_processed == 0
    assert _run("SELECT COUNT(*) FROM testdata.astronauts").bytes_processed == 0


def test_a_shared_cte_is_billed_once():
    """A CTE referenced twice executes ONCE (its body is materialized off to
    the side, and its scans are not in the main plan graph). Billing it twice
    would charge for a read that never happens; billing it zero — the failure
    mode if the shared_ctes walk is dropped — would charge for none of it.
    """
    # The body's projection is narrowed to the column the outer query and the
    # join key actually need, so the comparison scan is that same one column —
    # NOT `SELECT name, year`, which the body no longer reads.
    direct = _run("SELECT name FROM testdata.astronauts").bytes_processed
    shared = _run(
        "WITH c AS (SELECT name, year FROM testdata.astronauts) "
        "SELECT a.name FROM c a INNER JOIN c b ON a.name = b.name"
    ).bytes_processed

    assert shared == direct, (
        f"shared CTE billed {shared:,}, one scan of the same columns is "
        f"{direct:,} — twice means the two refs were both billed, zero means "
        f"the shared_ctes walk was dropped"
    )


def test_each_leg_of_a_union_is_billed():
    """Two scans of the same relation read it twice and are billed twice.

    Compared against the FULL-WIDTH scan, not the one-column scan: projection
    pushdown does not currently reach through a UNION leg, so each leg really
    does decode all 19 columns (`native_scan_facts` reports columns_read=19).
    The meter reports what the engine reads — pinning it against a narrowed
    figure here would be asserting a pushdown that does not happen, and would
    make this test fail the day someone fixes it for the wrong reason.
    """
    whole = _run("SELECT * FROM testdata.astronauts").bytes_processed
    two_legs = _run(
        "SELECT name FROM testdata.astronauts "
        "UNION ALL SELECT name FROM testdata.astronauts"
    ).bytes_processed

    assert two_legs == 2 * whole


def test_billing_event_is_emitted_without_draining_the_result(monkeypatch):
    """The meter is a plan-time figure, so the event is emitted once execution
    is SUBMITTED — not once the result stream has been consumed. A caller that
    abandons the generator still bills; under the old runtime meter it did not.
    """
    events = []

    import opteryx.query_session as query_session

    monkeypatch.setattr(
        query_session,
        "write_billing_event",
        lambda **kwargs: events.append(kwargs),
    )

    session = opteryx.session()
    morsels = session.execute_to_morsels("SELECT * FROM testdata.astronauts")
    next(morsels)  # take ONE morsel, then walk away
    morsels.close()

    processed = [
        e
        for e in events
        if e["billing_event"] == BillingEventType.DATA_PROCESSED_BYTES
    ]
    assert len(processed) == 1, "expected exactly one DATA_PROCESSED_BYTES event"
    assert processed[0]["event_details"]["bytes_processed"] > 0


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
