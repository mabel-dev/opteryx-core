"""Chunk cache give-way on OS memory-compressor pressure (draken/core/mem_account.cpp).

On macOS the cache's limit is clamped while the memory compressor is busy, never
below a floor share of the budget, so a resident cache yields to a query the
machine is short of memory for (SF100 Q18 4.4 -> 1.9 s). What fires it is live OS
activity, which a test cannot drive deterministically; pinned here is what makes
the decision visible: the per-query compressor readings exist, are well formed,
are restarted at query start, and reach the query's telemetry.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.compiled.structures.memory_pool import chunk_cache_stats, reset_process_memory_peak

DATASET = "testdata.flat.formats.parquet_snappy"


def test_stats_carry_the_compressor_readings():
    stats = chunk_cache_stats()
    assert isinstance(stats["compress_rate_max"], int) and stats["compress_rate_max"] >= 0
    assert isinstance(stats["pressure_samples"], int) and stats["pressure_samples"] >= 0
    # the clamp only ever lowers the limit below the budget, never raises it
    assert stats["limit"] <= stats["budget"]


def test_readings_restart_with_the_query_peak():
    reset_process_memory_peak()
    # read immediately: a sample in this instant can register, but nothing older
    # than the reset can - so the count is at most a handful, not the history
    assert chunk_cache_stats()["pressure_samples"] <= 1


def test_query_telemetry_reports_compressor_pressure():
    session = opteryx.session()
    for _ in session.execute_to_morsels(f"SELECT COUNT(*) FROM {DATASET}"):
        pass
    tel = session.telemetry if isinstance(session.telemetry, dict) else session.telemetry.as_dict()
    assert tel["chunk_cache_compress_rate_max"] >= 0
    assert tel["chunk_cache_pressure_samples"] >= 0


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
