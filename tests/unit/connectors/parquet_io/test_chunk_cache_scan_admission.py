# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Scan-resistant chunk cache admission (C7).

A scan whose full fill would not fit in the chunk cache's remaining headroom
uses cache hits but does not fill. Before, every scan filled: one pass over a
table larger than the cache decompressed every chunk into a fresh image it
could never reuse, and on SF100 that fill cost was most of Q18's gap to skene.

Pinned here:
  * NativeScanPlan.cache_fill_bytes is the projected chunks' footer
    total_uncompressed_size, counting only chunks the decoder fills (compressed,
    or remote) - a local UNCOMPRESSED chunk counts nothing;
  * admission consumes headroom in scan order, and a declined scan consumes none;
  * the decision is reported in query telemetry.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan
from opteryx.managers.execution.compiler import _chunk_cache_admission

pytestmark = pytest.mark.usefixtures("allow_local_reads")

ROW_GROUP_SIZE = 4_000
N = ROW_GROUP_SIZE * 5


def _write(tmp, compression):
    path = os.path.join(tmp, f"admit_{compression}.parquet")
    table = pa.table({
        "x": pa.array(list(range(N)), type=pa.int64()),
        "s": pa.array([f"value-{i % 997:08d}" for i in range(N)]),
    })
    pq.write_table(table, path, row_group_size=ROW_GROUP_SIZE, compression=compression)
    return path


def _oracle(path, columns):
    md = pq.ParquetFile(path).metadata
    total = 0
    for r in range(md.num_row_groups):
        rg = md.row_group(r)
        for i in range(rg.num_columns):
            col = rg.column(i)
            if col.path_in_schema in columns and col.compression != "UNCOMPRESSED":
                total += col.total_uncompressed_size
    return total


def _fill_bytes(path, columns):
    plan = open_native_scan_plan([path], columns)
    try:
        return plan.cache_fill_bytes
    finally:
        plan.close()


def test_fill_bytes_are_the_projected_compressed_chunks():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp, "zstd")
        both = _fill_bytes(path, ["x", "s"])
        assert both == _oracle(path, {"x", "s"}) > 0
        # projection narrows it: only the chunks the scan reads would be filled
        assert _fill_bytes(path, ["x"]) == _oracle(path, {"x"}) < both


def test_local_uncompressed_chunks_count_nothing():
    # The decoder never caches a local UNCOMPRESSED chunk (the OS page cache
    # already holds those bytes), so such a scan adds nothing to the cache.
    with tempfile.TemporaryDirectory() as tmp:
        assert _fill_bytes(_write(tmp, "NONE"), ["x", "s"]) == 0


def test_admission_consumes_headroom_in_scan_order():
    # each scan that fits takes its bytes out of the headroom; the next is
    # judged against what is left
    assert _chunk_cache_admission([5, 3, 4], 8) == [True, True, False]
    # a declined scan consumes nothing, so a later smaller scan still fits
    assert _chunk_cache_admission([10, 2], 8) == [False, True]
    # exactly filling the headroom is admitted; a scan that fills nothing always is
    assert _chunk_cache_admission([8, 0], 8) == [True, True]
    # a full (or off) cache admits only scans that would add nothing
    assert _chunk_cache_admission([1, 0], 0) == [False, True]


def test_decision_is_reported_in_telemetry():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp, "zstd")
        session = opteryx.session()
        for _ in session.execute_to_morsels(f"SELECT x, s FROM READ_PARQUET('{path}')"):
            pass
        tel = session.telemetry if isinstance(session.telemetry, dict) else session.telemetry.as_dict()
        assert tel["chunk_cache_scans_declined"] >= 0
        assert tel["chunk_cache_declined_bytes"] >= 0
        # a few hundred KB fits any cache that is on, so this scan was admitted
        if tel["chunk_cache_limit"] > 0:
            assert tel["chunk_cache_scans_declined"] == 0
            assert tel["chunk_cache_declined_bytes"] == 0


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
