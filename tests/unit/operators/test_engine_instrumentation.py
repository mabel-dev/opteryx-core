"""WP-INSTR — native execution-engine instrumentation harness.

Prerequisite measurement for later engine-performance work. Four instruments,
all off by default / behind the OPTERYX_INSTRUMENT_ENGINE flag:

  1. gil_held_ns        — per-query ns inside execution-time `with gil` bodies.
  2. scan_sources       — per-parquet-scan Source selection (native vs trampoline).
  3. allocation harness — dev/instrument_engine.measure_query_allocations.
  4. worker purity guard— dev/instrument_engine.assert_native_worker_purity.

These tests assert the instruments DISTINGUISH a native-gated numeric scan
(NativeParquetScanSource: zero execution Python) from a trampoline scan
(StreamingScanSource: per-morsel Python re-entry), and that the harness imposes
nothing when the flag is off. They do not assert wall-clock thresholds.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pytest

import opteryx
import opteryx.config as config
import instrument_engine as ie  # dev/instrument_engine.py

# A local parquet relation with both numeric (INT64) and string columns. The
# numeric projection is inside the native-scan gate (int/float, no predicate,
# local); as of WP-01 a bare string projection is ALSO native (VARCHAR decoded by
# NativeParquetScanSource). A pushed predicate is still out of the gate (predicate
# relocation is WP-02), so PREDICATE_SQL remains the trampoline exemplar these
# instruments use to exercise the per-morsel Python re-entry.
DATASET = "testdata/flat/formats/parquet"
NUMERIC_SQL = "SELECT user_id, followers, following FROM '%s'" % DATASET
STRING_SQL = "SELECT text FROM '%s'" % DATASET
# WP-02: a c-native pushed predicate now RELOCATES to a native downstream
# ExprFilter and the scan goes native — so this is a NATIVE exemplar now.
PREDICATE_SQL = "SELECT followers FROM '%s' WHERE followers > 100" % DATASET
# The trampoline exemplar: a pushed predicate that does NOT lower to a c-native
# span (regex) still fails closed to StreamingScanSource, exercising the
# per-morsel Python re-entry (its residual runs on the bytecode-VM fallback).
UNSUPPORTED_PREDICATE_SQL = "SELECT followers FROM '%s' WHERE text RLIKE 'a'" % DATASET
TRAMPOLINE_SQL = UNSUPPORTED_PREDICATE_SQL


def _run(sql):
    """Drain a query and return (telemetry_dict, row_count)."""
    session = opteryx.session()
    rows = 0
    for morsel in session.execute_to_morsels(sql):
        rows += morsel.num_rows
    return session._telemetry.as_dict(), rows


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


def test_scan_source_streaming_for_unsupported_predicate():
    # A predicate outside the c-native kernel set (regex) still fails closed.
    telemetry, _ = _run(UNSUPPORTED_PREDICATE_SQL)
    assert list(telemetry["scan_sources"].values()) == ["StreamingScanSource"]


# --- instrument 1: gil_held_ns ----------------------------------------------

def test_gil_held_ns_zero_for_native(armed):
    telemetry, _ = _run(NUMERIC_SQL)
    # A native-gated scan touches NO execution-time Python: exactly zero.
    assert telemetry["gil_held_ns"] == 0
    assert telemetry["worker_gil_sites"] == []


def test_gil_held_ns_nonzero_for_trampoline(armed):
    telemetry, _ = _run(TRAMPOLINE_SQL)
    # The trampoline re-enters Python per morsel per worker → clearly > 0. This is
    # the baseline number later work packages must drive back to ~0.
    assert telemetry["gil_held_ns"] > 0
    sites = telemetry["worker_gil_sites"]
    assert sites, "expected recorded worker GIL sites for the trampoline path"
    # Every recorded site must be the known scan-pull trampoline (or error stash).
    assert {s["site"] for s in sites} <= set(ie.DEFAULT_WORKER_WHITELIST)
    assert any(s["site"] == "_scan_pull_run" for s in sites)


# --- disabled-path: off by default, records nothing --------------------------

def test_instrumentation_off_by_default():
    # No monkeypatch: the flag is off, so execute_native never arms the sites and
    # never writes the readings — proving zero recording overhead when disabled.
    assert config.OPTERYX_INSTRUMENT_ENGINE is False
    telemetry, _ = _run(TRAMPOLINE_SQL)
    assert "gil_held_ns" not in telemetry
    assert "worker_gil_sites" not in telemetry
    # scan_sources is a plan-time fact and remains available.
    assert telemetry["scan_sources"]


# --- instrument 4: worker-thread purity guard --------------------------------

def test_worker_purity_guard_passes_on_native(armed):
    telemetry, _ = _run(NUMERIC_SQL)
    # No un-whitelisted (indeed no) Python ran on a worker → guard passes, empty.
    assert ie.assert_native_worker_purity(telemetry) == []


def test_worker_purity_guard_passes_trampoline_under_default_whitelist(armed):
    telemetry, _ = _run(TRAMPOLINE_SQL)
    # The trampoline's _scan_pull_run IS whitelisted today, so the guard passes and
    # returns the recorded sites.
    survived = ie.assert_native_worker_purity(telemetry)
    assert survived and all(s["site"] == "_scan_pull_run" for s in survived)


def test_worker_purity_guard_flags_trampoline_with_empty_whitelist(armed):
    # The deliberate flag: with nothing whitelisted, ANY execution-time Python on a
    # worker is a violation — this is how a future package proves a path went
    # native (the guard must then pass with an empty whitelist).
    telemetry, _ = _run(TRAMPOLINE_SQL)
    with pytest.raises(ie.WorkerPurityError):
        ie.assert_native_worker_purity(telemetry, whitelist=())
    # ...and the native scan still passes even with nothing whitelisted.
    native_telemetry, _ = _run(NUMERIC_SQL)
    assert ie.assert_native_worker_purity(native_telemetry, whitelist=()) == []


# --- instrument 3: allocation harness ----------------------------------------

def test_alloc_harness_native_has_zero_trampoline_calls(armed):
    result = ie.measure_query_allocations(NUMERIC_SQL)
    assert result["rows"] > 0
    assert result["scan_sources"] and set(result["scan_sources"].values()) == {
        "NativeParquetScanSource"
    }
    # Native scan: zero per-morsel Python re-entry, and a live footprint that is
    # a small O(morsels) amount (fractional blocks per row).
    assert result["trampoline_calls"] == 0
    assert result["gil_held_ns"] == 0
    assert result["peak_block_delta"] >= 0
    assert result["blocks_per_row"] < 1.0


def test_alloc_harness_trampoline_has_growing_reentry(armed):
    result = ie.measure_query_allocations(TRAMPOLINE_SQL)
    assert set(result["scan_sources"].values()) == {"StreamingScanSource"}
    # The trampoline re-enters Python O(morsels) times — non-zero, unlike native.
    assert result["trampoline_calls"] > 0
    assert result["gil_held_ns"] > 0
    # Live footprint is still O(morsels): blocks/row shrinks, not grows, with size.
    assert result["blocks_per_row"] < 1.0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
