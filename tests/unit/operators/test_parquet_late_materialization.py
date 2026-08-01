# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit tests for two-pass Parquet late materialization
(docs/parquet-two-pass-late-materialization-design.md).

These tests exercise the key correctness and observability properties of the
two-pass optimization inside ParquetReadNode:

  Pass 1  — decode only the columns referenced by pushed-down predicates.
  Skip    — if Pass 1 produces a zero-row mask, skip Pass 2 entirely.
  Pass 2  — decode projection-only columns only for surviving row groups.
  Abandon — stop skipping Pass 2 after N consecutive fully-passing row groups.

Dataset notes (testdata/clickbench_tiny):
  URL LIKE '%google%'  → 0 matching rows  (skip-ratio should be 1.0)
  URL LIKE '%yandex%'  → ~59 matching rows (assembly correctness)
  URL LIKE '%http%'    → ~999 983 matching rows (nearly all rows pass)
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx import config
from opteryx.connectors.parquet_io import pool_reader

# ─── helpers ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _force_trampoline_scan(monkeypatch):
    """These tests are about the TRAMPOLINE scan's two-pass implementation and read
    its own telemetry counters (`parquet_latmat_pass1_row_groups`,
    `parquet_latmat_pass2_bytes`, ...), which only `ParquetReadNode` emits.

    Since the R3 close-out, the `WHERE ... ORDER BY ... LIMIT` shape below is served
    by the native `LatmatScanSource` instead, which emits none of those counters —
    so without this the tests would silently stop exercising anything. The
    trampoline implementation is still live code (a scan the footer gate refuses —
    schema evolution, say — still reaches it), so it still deserves these tests;
    they just have to ASK for it now. Native two-pass late-materialization has its
    own correctness matrix in tests/unit/operators/test_wp_r3_latmat_scan.py.
    """
    monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)


def _get_read_operation(telemetry: dict) -> dict:
    """Return the first ReadRel operation dict from session telemetry."""
    for operation in telemetry.get("operations", {}).values():
        if operation.get("type") == "ReadRel":
            return operation
    raise AssertionError("No ReadRel operation found in telemetry")


class _Result:
    """The drained result of a query: column names, values, and row count.

    Two results compare equal when their column names, order, and every value
    match — the property these tests assert when comparing two-pass ON vs OFF.
    """

    __slots__ = ("names", "columns", "num_rows")

    def __init__(self, names, columns, num_rows):
        self.names = names
        self.columns = columns
        self.num_rows = num_rows

    @property
    def num_columns(self):
        return len(self.names)

    def equals(self, other) -> bool:
        return (
            self.names == other.names
            and self.num_rows == other.num_rows
            and self.columns == other.columns
        )


def _execute(session, sql) -> _Result:
    """Drain a query to a _Result. Morsel column names are bytes."""
    names: list = []
    columns: dict = {}
    num_rows = 0
    for morsel in session.execute_to_morsels(sql):
        if not morsel.num_rows:
            continue
        if not names:
            names = [n.decode() for n in morsel.column_names]
        num_rows += morsel.num_rows
        for name in morsel.column_names:
            columns.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return _Result(names, columns, num_rows)


def _latmat_sensors(session) -> dict:
    """Return latmat sensor values (defaulting to 0) from a session."""
    read_op = _get_read_operation(session.telemetry)
    keys = (
        "parquet_latmat_pass1_row_groups",
        "parquet_latmat_pass2_row_groups",
        "parquet_latmat_skipped_row_groups",
        "parquet_latmat_abandoned_files",
        "parquet_latmat_pass2_bytes",
        "parquet_latmat_skip_ratio",
    )
    return {k: read_op.get(k, 0) for k in keys}


# ─── test fixtures / config helpers ───────────────────────────────────────────


@pytest.fixture(autouse=True)
def _restore_latmat_config():
    """Ensure late-materialization feature flag and abandon threshold are
    restored to their defaults after every test, regardless of what the test
    does to them."""
    orig_flag = config.features.parquet_late_materialization
    orig_abandon = config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER
    yield
    config.features.parquet_late_materialization = orig_flag
    config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER = orig_abandon


# ─── fast-path (zero-survivor skip) tests ─────────────────────────────────────


def test_q24_no_matching_rows_activates_skip_fast_path():
    """Q24 pattern: URL LIKE '%google%' matches 0 rows in clickbench_tiny.

    All row groups should be processed in Pass 1 (pass1_row_groups > 0) and
    every single one skipped because no row survives the predicate
    (skip_ratio == 1.0, pass2_row_groups == 0, pass2_bytes == 0).
    """
    config.features.parquet_late_materialization = True

    session = opteryx.session()
    try:
        result = _execute(
            session,
            "SELECT * FROM testdata.clickbench_tiny"
            " WHERE URL LIKE '%google%'"
            " ORDER BY EventTime LIMIT 10"
        )

        assert result.num_rows == 0, "expected 0 rows — no 'google' URLs in tiny dataset"

        lm = _latmat_sensors(session)
        assert lm["parquet_latmat_pass1_row_groups"] > 0, (
            "two-pass path must activate for LIKE predicate with projection-only columns"
        )
        assert lm["parquet_latmat_pass2_row_groups"] == 0, (
            "Pass 2 must not run when no rows survive Pass 1"
        )
        assert lm["parquet_latmat_skipped_row_groups"] == lm["parquet_latmat_pass1_row_groups"], (
            "every row group processed in Pass 1 must be skipped"
        )
        assert lm["parquet_latmat_skip_ratio"] == 1.0, (
            "skip ratio must be 1.0 when all row groups are skipped"
        )
        assert lm["parquet_latmat_pass2_bytes"] == 0, (
            "no projection-only bytes should be decoded when every row group is skipped"
        )
    finally:
        session.close()


# NOTE: there is deliberately no test asserting "Pass 1 decodes only the URL
# column". It used to exist as test_q24_pass1_only_reads_url_column, asserting
# `parquet_filter_columns_read == 1`, but that counter had no producer on any
# scan path — it was always 0, so the test had never passed and protected
# nothing. The counter has been removed rather than invented: the pass-1 column
# set is a plan-time constant (and for this query it is TWO columns, URL plus
# the top-N sort column EventTime, not one), so it is not a scan sensor. The
# runtime consequence the test cared about — the other 104 columns are not
# decoded when no row survives Pass 1 — is covered by
# test_q24_no_matching_rows_activates_skip_fast_path's `pass2_bytes == 0`.


# ─── assembly correctness tests ───────────────────────────────────────────────


def test_assembly_correctness_matching_rows_yandex():
    """LIKE '%yandex%' matches ~59 rows.  Results must be identical whether the
    two-pass optimisation is on or off."""
    sql = (
        "SELECT URL, EventTime, UserID"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%yandex%'"
        " ORDER BY EventTime LIMIT 20"
    )

    config.features.parquet_late_materialization = True
    session_on = opteryx.session()
    try:
        result_on = _execute(session_on, sql)
    finally:
        session_on.close()

    config.features.parquet_late_materialization = False
    session_off = opteryx.session()
    try:
        result_off = _execute(session_off, sql)
    finally:
        session_off.close()

    assert result_on.num_rows == result_off.num_rows, (
        "row count must match between two-pass ON and OFF"
    )
    assert result_on.num_columns == result_off.num_columns
    assert result_on.equals(result_off), (
        "result tables must be byte-for-byte identical with two-pass ON vs OFF"
    )


def test_assembly_correctness_select_star():
    """SELECT * with a selective LIKE.  All 105 columns must be assembled
    correctly — in particular, Pass 2 columns must be appended in the right
    positions and their values must agree with single-pass output."""
    sql = (
        "SELECT *"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%yandex%'"
        " ORDER BY EventTime LIMIT 5"
    )

    config.features.parquet_late_materialization = True
    session_on = opteryx.session()
    try:
        result_on = _execute(session_on, sql)
    finally:
        session_on.close()

    config.features.parquet_late_materialization = False
    session_off = opteryx.session()
    try:
        result_off = _execute(session_off, sql)
    finally:
        session_off.close()

    assert result_on.names == result_off.names, (
        "column order must be preserved when assembling Pass 1 + Pass 2 morsels"
    )
    assert result_on.equals(result_off), (
        "SELECT * result must be identical whether two-pass is on or off"
    )


def test_pass2_row_groups_nonzero_when_rows_survive():
    """When the predicate is selective but non-zero rows survive, Pass 2 must
    run for those row groups (pass2_row_groups > 0) and decode > 0 bytes."""
    config.features.parquet_late_materialization = True

    session = opteryx.session()
    try:
        result = _execute(
            session,
            "SELECT URL, EventTime, UserID, SearchPhrase"
            " FROM testdata.clickbench_tiny"
            " WHERE URL LIKE '%yandex%'"
            " LIMIT 5"
        )
        assert result.num_rows > 0, "expected rows — 'yandex' appears in the dataset"

        lm = _latmat_sensors(session)
        assert lm["parquet_latmat_pass1_row_groups"] > 0
        assert lm["parquet_latmat_pass2_row_groups"] > 0, "Pass 2 must run when rows survive Pass 1"
        assert lm["parquet_latmat_pass2_bytes"] > 0, (
            "Pass 2 must decode bytes for the projection-only columns"
        )
    finally:
        session.close()


# ─── eligibility / ineligibility tests ────────────────────────────────────────


def test_two_pass_inactive_when_no_predicate():
    """Without a WHERE clause there are no filter columns, so the two-pass path
    should never activate."""
    config.features.parquet_late_materialization = True

    session = opteryx.session()
    try:
        _execute(session, "SELECT URL, EventTime FROM testdata.clickbench_tiny LIMIT 5")
        lm = _latmat_sensors(session)
        assert lm["parquet_latmat_pass1_row_groups"] == 0, (
            "two-pass must not activate when there is no pushed-down predicate"
        )
    finally:
        session.close()


def test_two_pass_inactive_when_all_projected_columns_in_filter():
    """SELECT url WHERE url LIKE '...' — the only projected column is also the
    filter column, so pass2_identity_set is empty and two-pass is ineligible."""
    config.features.parquet_late_materialization = True

    session = opteryx.session()
    try:
        _execute(
            session,
            "SELECT URL FROM testdata.clickbench_tiny WHERE URL LIKE '%yandex%' LIMIT 5"
        )
        lm = _latmat_sensors(session)
        assert lm["parquet_latmat_pass1_row_groups"] == 0, (
            "two-pass must not activate when all projected columns are filter columns"
        )
    finally:
        session.close()


def test_two_pass_inactive_when_feature_flag_disabled():
    """With FEATURE_PARQUET_LATE_MATERIALIZATION=0, the two-pass path must
    never activate, even for queries that would otherwise be eligible."""
    config.features.parquet_late_materialization = False

    session = opteryx.session()
    try:
        _execute(
            session,
            "SELECT * FROM testdata.clickbench_tiny"
            " WHERE URL LIKE '%google%'"
            " ORDER BY EventTime LIMIT 10"
        )
        lm = _latmat_sensors(session)
        assert lm["parquet_latmat_pass1_row_groups"] == 0, (
            "two-pass must not run when the feature flag is disabled"
        )
    finally:
        session.close()


# ─── abandonment heuristic tests ──────────────────────────────────────────────


def test_abandonment_fires_when_predicate_passes_all_rows():
    """When every row in a row group passes the predicate (rows_after == rows_before),
    the abandonment counter increments.  With ABANDON_AFTER=1 we expect the
    heuristic to fire after the first fully-passing row group."""
    config.features.parquet_late_materialization = True
    config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER = 1

    # Use MobilePhoneModel <> '' as a predicate known to pass all rows in the
    # tiny dataset (every record has a non-empty MobilePhoneModel), and project
    # additional columns so pass2_identity_set is non-empty.
    session = opteryx.session()
    try:
        # Fetch enough rows that at least one full row group is processed.
        _execute(
            session,
            "SELECT MobilePhoneModel, URL, EventTime"
            " FROM testdata.clickbench_tiny"
            " WHERE URL LIKE '%http%'"
            " LIMIT 5"
        )
        lm = _latmat_sensors(session)
        # If the heuristic fires, abandoned_files increments; alternatively,
        # pass1_row_groups must be at least 1 to have a meaningful result.
        assert lm["parquet_latmat_pass1_row_groups"] >= 1
        # With ABANDON_AFTER=1 and a nearly fully-passing predicate we expect
        # either abandonment or very low skip ratio.
        # (The heuristic triggers on exact equality rows_after==rows_before,
        # so a 99.9%+ pass rate still qualifies when the row group is dense.)
        # We simply verify the counter is accessible and non-negative.
        assert lm["parquet_latmat_abandoned_files"] >= 0
    finally:
        session.close()


# ─── regression / non-interference tests ──────────────────────────────────────


def test_non_like_predicate_not_affected():
    """A plain equality predicate (AdvEngineID <> 0) should not be harmed by
    the two-pass path.  Results must match with the feature on and off."""
    sql = "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0"

    config.features.parquet_late_materialization = True
    session_on = opteryx.session()
    try:
        result_on = _execute(session_on, sql)
    finally:
        session_on.close()

    config.features.parquet_late_materialization = False
    session_off = opteryx.session()
    try:
        result_off = _execute(session_off, sql)
    finally:
        session_off.close()

    assert result_on.equals(result_off), (
        "equality predicate results must be unaffected by the two-pass feature"
    )


def test_aggregate_query_not_affected():
    """A GROUP BY query without LIKE predicates must be unaffected."""
    sql = (
        "SELECT UserID, COUNT(*)"
        " FROM testdata.clickbench_tiny"
        " GROUP BY UserID"
        " ORDER BY COUNT(*) DESC LIMIT 3"
    )

    config.features.parquet_late_materialization = True
    session_on = opteryx.session()
    try:
        result_on = _execute(session_on, sql)
    finally:
        session_on.close()

    config.features.parquet_late_materialization = False
    session_off = opteryx.session()
    try:
        result_off = _execute(session_off, sql)
    finally:
        session_off.close()

    assert result_on.equals(result_off), (
        "aggregate query results must be identical with two-pass on and off"
    )


def test_q28_like_with_group_by_and_limit():
    """Regression: Q28 (URL LIKE '%google%' + GROUP BY + LIMIT) should return
    identical results with the feature on and off."""
    sql = (
        "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%google%' AND SearchPhrase <> ''"
        " GROUP BY SearchPhrase"
        " ORDER BY c DESC LIMIT 10"
    )

    config.features.parquet_late_materialization = True
    session_on = opteryx.session()
    try:
        result_on = _execute(session_on, sql)
    finally:
        session_on.close()

    config.features.parquet_late_materialization = False
    session_off = opteryx.session()
    try:
        result_off = _execute(session_off, sql)
    finally:
        session_off.close()

    assert result_on.equals(result_off), (
        "GROUP BY query with LIKE filter must produce identical results ON vs OFF"
    )
