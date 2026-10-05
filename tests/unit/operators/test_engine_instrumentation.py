"""WP-INSTR — native execution-engine instrumentation harness.

Two instruments (see dev/instrument_engine.py, docs/ENGINE_INSTRUMENTATION.md):

  * scan_sources       — per-parquet-scan Source selection (always on telemetry).
  * allocation harness — dev/instrument_engine.measure_query_allocations.

These tests assert that the admitted scan shapes select the native Source and that
a native scan's live footprint is O(morsels). They do not assert wall-clock
thresholds.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pytest

import opteryx
import instrument_engine as ie  # dev/instrument_engine.py

# A local parquet relation with both numeric (INT64) and string columns. Every
# query below selects the zero-Python native scan.
DATASET = "testdata/flat/formats/parquet"
NUMERIC_SQL = "SELECT user_id, followers, following FROM '%s'" % DATASET
STRING_SQL = "SELECT text FROM '%s'" % DATASET
# WP-02: a c-native pushed predicate RELOCATES to a native downstream ExprFilter
# and the scan goes native.
PREDICATE_SQL = "SELECT followers FROM '%s' WHERE followers > 100" % DATASET


def _run(sql):
    """Drain a query and return (telemetry_dict, row_count)."""
    session = opteryx.session()
    rows = 0
    for morsel in session.execute_to_morsels(sql):
        rows += morsel.num_rows
    return session.telemetry, rows


# --- scan-source logging (always on, no flag) --------------------------------

def test_scan_source_native_for_numeric():
    telemetry, rows = _run(NUMERIC_SQL)
    assert rows > 0
    sources = list(telemetry["scan_sources"].values())
    assert sources == ["NativeParquetScanSource"], sources


def test_scan_source_native_for_string():
    # WP-01: a bare string projection now selects the zero-Python native scan.
    telemetry, _ = _run(STRING_SQL)
    assert list(telemetry["scan_sources"].values()) == ["NativeParquetScanSource"]


def test_scan_source_native_for_cnative_predicate():
    # WP-02: a c-native pushed predicate relocates to a native ExprFilter; the
    # scan is native (was StreamingScanSource under WP-01).
    telemetry, _ = _run(PREDICATE_SQL)
    assert list(telemetry["scan_sources"].values()) == ["NativeParquetScanSource"]


# --- allocation harness ------------------------------------------------------

def test_alloc_harness_native_scan():
    result = ie.measure_query_allocations(NUMERIC_SQL)
    assert result["rows"] > 0
    assert result["scan_sources"] and set(result["scan_sources"].values()) == {
        "NativeParquetScanSource"
    }
    # A live footprint that is a small O(morsels) amount (fractional blocks per row).
    assert result["peak_block_delta"] >= 0
    assert result["blocks_per_row"] < 1.0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
