"""
Performance benchmark: Parquet row-group scheduler v1 vs v2.

Measures:
- time to first row group (first morsel)
- row groups per second
- wall-clock scan time

Run with:
    pytest -q tests/performance/benchmarks/bench_parquet_rowgroup_scheduler.py -s
"""

import glob
import os
import sys
import time

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.connectors.parquet_io import InMemoryParquetCache
from opteryx.connectors.parquet_io import fetch_footer
from opteryx.connectors.parquet_io import reader


DATASET_GLOBS = [
    os.path.join("testdata", "tpch", "lineitem", "*.parquet"),
    os.path.join("testdata", "flat", "formats", "parquet", "*.parquet"),
]


def _discover_paths(max_files=8):
    paths = []
    for pattern in DATASET_GLOBS:
        paths.extend(sorted(glob.glob(pattern)))
    deduped = list(dict.fromkeys(paths))
    return deduped[:max_files]


def _run_scheduler(scan_fn, filesystem, paths, columns):
    cache = InMemoryParquetCache()
    first_ns = None
    rowgroups = 0

    start_ns = time.monotonic_ns()
    for _ in scan_fn(filesystem, paths, columns, cache=cache, max_workers=32):
        rowgroups += 1
        if first_ns is None:
            first_ns = time.monotonic_ns()
    end_ns = time.monotonic_ns()

    elapsed_ns = end_ns - start_ns
    if first_ns is None:
        return {
            "rowgroups": 0,
            "time_to_first_ms": None,
            "elapsed_ms": elapsed_ns / 1_000_000,
            "rowgroups_per_s": 0.0,
        }

    elapsed_s = elapsed_ns / 1_000_000_000
    return {
        "rowgroups": rowgroups,
        "time_to_first_ms": (first_ns - start_ns) / 1_000_000,
        "elapsed_ms": elapsed_ns / 1_000_000,
        "rowgroups_per_s": (rowgroups / elapsed_s) if elapsed_s > 0 else 0.0,
    }


def test_parquet_rowgroup_scheduler_v1_vs_v2_prints():
    paths = _discover_paths(max_files=8)
    if not paths:
        pytest.skip("No parquet benchmark files found")

    filesystem = OpteryxLocalFileSystem()

    # Determine projected columns from first file metadata.
    footer = fetch_footer(filesystem, paths[0], cache=InMemoryParquetCache())
    if not footer["row_groups"]:
        pytest.skip("Benchmark file has no row groups")

    available = [c["name"] for c in footer["row_groups"][0]["columns"]]
    columns = available[: min(8, len(available))]
    if not columns:
        pytest.skip("Benchmark file has no columns")

    import opteryx.config as cfg

    cfg.PARQUET_FILES_IN_FLIGHT = 2
    cfg.PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT = 2
    cfg.PARQUET_GLOBAL_RANGE_READERS = 24
    cfg.PARQUET_RANGE_READERS_PER_ROWGROUP = 10

    print("\n=== Parquet row-group scheduler benchmark ===")
    print(f"files={len(paths)} columns={len(columns)}")

    v1 = _run_scheduler(reader._iter_row_groups_v1, filesystem, paths, columns)
    v2 = _run_scheduler(reader._iter_row_groups_v2, filesystem, paths, columns)

    print("\nv1:")
    print(f"  rowgroups: {v1['rowgroups']}")
    print(f"  first morsel: {v1['time_to_first_ms']:.2f} ms")
    print(f"  elapsed: {v1['elapsed_ms']:.2f} ms")
    print(f"  rowgroups/s: {v1['rowgroups_per_s']:.2f}")

    print("\nv2:")
    print(f"  rowgroups: {v2['rowgroups']}")
    print(f"  first morsel: {v2['time_to_first_ms']:.2f} ms")
    print(f"  elapsed: {v2['elapsed_ms']:.2f} ms")
    print(f"  rowgroups/s: {v2['rowgroups_per_s']:.2f}")

    # Correctness guard for benchmark runs.
    assert v1["rowgroups"] == v2["rowgroups"]
