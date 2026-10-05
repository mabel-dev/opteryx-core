"""WP-INSTR — native execution-engine instrumentation harness.

Prerequisite measurement for later engine-performance work. Four instruments,
all off by default / behind the OPTERYX_INSTRUMENT_ENGINE flag (scan_sources is
always on):

  1. gil_held_ns        — per-query ns inside execution-time `with gil` bodies.
  2. scan_sources       — per-parquet-scan Source selection.
  3. allocation harness — dev/instrument_engine.measure_query_allocations.
  4. worker purity guard— dev/instrument_engine.assert_native_worker_purity.

These tests assert that a native scan (NativeParquetScanSource) reads as zero
execution-time Python on every instrument, and that the harness records nothing
when the flag is off. They do not assert wall-clock thresholds.

The Python per-morsel scan (StreamingScanSource) that used to be the POSITIVE
exemplar here — the one path that re-entered Python per morsel — was deleted
(ruling 2026-10-03), and its tests went with it. No SQL-reachable positive for
instruments 1, 3 and 4 is known today, so those instruments are currently pinned
by "never fires" assertions only.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pytest

import opteryx
import opteryx.config as config
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


@pytest.fixture
def armed(monkeypatch):
    """Arm the GIL instrumentation for one test (execute_native reads the config
    flag per-call, so monkeypatching the attribute is enough)."""
    monkeypatch.setattr(config, "OPTERYX_INSTRUMENT_ENGINE", True)
    yield


# --- instrument 2: scan-source logging (always on, no flag) ------------------

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


# --- instrument 1: gil_held_ns ----------------------------------------------

def test_gil_held_ns_zero_for_native(armed):
    telemetry, _ = _run(NUMERIC_SQL)
    # A native-gated scan touches NO execution-time Python: exactly zero.
    assert telemetry["gil_held_ns"] == 0
    assert telemetry["worker_gil_sites"] == []


# --- disabled-path: off by default, records nothing --------------------------

def test_instrumentation_off_by_default():
    # No monkeypatch: the flag is off, so execute_native never arms the sites and
    # never writes the readings — proving zero recording overhead when disabled.
    assert config.OPTERYX_INSTRUMENT_ENGINE is False
    telemetry, _ = _run(PREDICATE_SQL)
    assert "gil_held_ns" not in telemetry
    assert "worker_gil_sites" not in telemetry
    # scan_sources is a plan-time fact and remains available.
    assert telemetry["scan_sources"]


# --- instrument 4: worker-thread purity guard --------------------------------

def test_worker_purity_guard_passes_on_native(armed):
    telemetry, _ = _run(NUMERIC_SQL)
    assert list(telemetry["scan_sources"].values()) == ["NativeParquetScanSource"]
    # No un-whitelisted (indeed no) Python ran on a worker → guard passes, empty —
    # under the default whitelist AND with nothing whitelisted at all.
    assert ie.assert_native_worker_purity(telemetry) == []
    assert ie.assert_native_worker_purity(telemetry, whitelist=()) == []


# --- instrument 3: allocation harness ----------------------------------------

def test_alloc_harness_native_scan(armed):
    result = ie.measure_query_allocations(NUMERIC_SQL)
    assert result["rows"] > 0
    assert result["scan_sources"] and set(result["scan_sources"].values()) == {
        "NativeParquetScanSource"
    }
    # Native scan: zero execution-time Python, and a live footprint that is a
    # small O(morsels) amount (fractional blocks per row).
    assert result["gil_held_ns"] == 0
    assert result["peak_block_delta"] >= 0
    assert result["blocks_per_row"] < 1.0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
