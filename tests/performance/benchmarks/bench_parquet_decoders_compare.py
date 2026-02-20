"""
Performance benchmark: parquet decoders (PyArrow vs fastparquet vs rugo placeholder)

- Uses the TPCH `lineitem` parquet files in `testdata/tpch/lineitem/*.parquet`.
- Measures decode (read) time for PyArrow and fastparquet.
- Adds a skipped placeholder test for rugo decode (per request).

This file is intended as a benchmark (printed output) and not as a regression assertion.
Run with: pytest -q tests/performance/benchmarks/bench_parquet_decoders_compare.py
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../../mabel/orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.join(sys.path[0], "../../../pyiceberg-firestore-gcs"))

import glob
import os
import time
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import numpy as np

DATASET_GLOB = os.path.join("testdata", "tpch", "lineitem", "*.parquet")


def _get_parquet_files() -> List[str]:
    files = sorted(glob.glob(DATASET_GLOB))
    return files


def _read_with_pyarrow(files: List[str]) -> int:
    """Read all files with PyArrow and return total rows read."""
    total_rows = 0
    for f in files:
        table = pq.read_table(f)
        total_rows += table.num_rows
    return total_rows


def _read_with_fastparquet(files: List[str]):
    """Read all files with fastparquet and return total rows read — avoid pandas.

    Uses fastparquet's low-level reader to populate `numpy` arrays directly
    (no `pandas.DataFrame` allocation). This falls back to a conservative
    object-dtype array for unfamiliar dtypes.
    """
    import fastparquet as fp  # imported only when this helper is used
    from fastparquet import core

    total_rows = 0
    for f in files:
        pf = fp.ParquetFile(f)

        # iterate row-groups and decode into pre-allocated numpy arrays
        for rg in pf.row_groups:
            nrows = rg.num_rows
            assign = {}

            # prepare a numpy array for every data column
            for col in pf.columns:
                dtype = pf.dtypes.get(col, object)
                try:
                    arr = np.empty(nrows, dtype=dtype)
                except Exception:
                    # fallback to object for complicated/pandas dtypes
                    arr = np.empty(nrows, dtype=object)
                assign[col] = arr

            # open the underlying file for this row-group and decode into `assign`
            filename = pf.row_group_filename(rg)
            with pf.open(filename, 'rb') as fh:
                core.read_row_group(
                    fh,
                    rg,
                    pf.columns,
                    pf.categories,
                    pf.schema,
                    pf.cats,
                    selfmade=pf.selfmade,
                    index=None,
                    assign=assign,
                    scheme=pf.file_scheme,
                    partition_meta=pf.partition_meta,
                    row_filter=False,
                )

            # we don't materialize a DataFrame — just count rows
            total_rows += nrows

    return total_rows


def _read_with_rugo(files: List[str]) -> int:
    """Decode files using rugo.read_parquet and return total rows read.

    Requires `opteryx.compiled.io.disk_reader.read_file()` for I/O (no fallback).
    """
    import opteryx.rugo.parquet as rp
    try:
        from opteryx.compiled.io.disk_reader import read_file as _disk_read_file
    except Exception as exc:
        raise RuntimeError("compiled disk_reader.read_file() is required for rugo decode benchmark") from exc

    total_rows = 0
    for f in files:
        buf = _disk_read_file(f)
        res = rp.read_parquet(buf)
        if not res:
            # decoding failed for this file
            continue
        # derive row count from first non-empty column in first row group
        rg_list = res.get("row_groups") or []
        if not rg_list:
            continue
        first_rg = rg_list[0]
        row_count = 0
        for col in first_rg:
            if col is None:
                continue
            if isinstance(col, list):
                row_count = len(col)
                break
        if row_count == 0:
            # fallback to metadata for row count
            md = rp.read_metadata_from_memoryview(buf, schema_only=True, max_row_groups=1, include_statistics=False)
            if isinstance(md, dict):
                row_count = md.get("num_rows", 0) or 0
        total_rows += row_count
    return total_rows


def _timed(fn, *args, iterations: int = 3):
    # warm-up
    fn(*args)

    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        out = fn(*args)
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return out, times


def test_parquet_decode_pyarrow_vs_fastparquet_prints():
    """Compare decode (read) performance between PyArrow and fastparquet.

    Prints average/min/max timings and a simple ratio. Skips if dataset
    not available. Skips fastparquet part if the dependency is missing.
    """
    files = _get_parquet_files()
    if not files:
        pytest.skip(f"Parquet dataset not found: {DATASET_GLOB}")

    print("\n=== Parquet decoder decode benchmark (TPCH lineitem) ===\n")
    print(f"Files: {len(files)}  — example: {os.path.basename(files[0])}\n")

    # placeholders for rugo decode summary (so summary always reports rugo status/timings)
    rows_rugo = None
    rugo_avg = None

    # PyArrow
    rows_arrow, arrow_times = _timed(_read_with_pyarrow, files, iterations=3)
    arrow_avg = sum(arrow_times) / len(arrow_times)
    print("PyArrow read: ")
    for i, t in enumerate(arrow_times, 1):
        print(f"  Iter {i}: {t:.4f}s")
    print(f"  → rows: {rows_arrow:,d}, avg: {arrow_avg:.4f}s, min: {min(arrow_times):.4f}s, max: {max(arrow_times):.4f}s\n")

    # Rugo decode (use compiled disk_reader; expect this may fail to fully decode)
    try:
        import opteryx.rugo.parquet as rp
    except Exception:
        print("rugo.parquet not available — skipping rugo decode measurements\n")
    else:
        try:
            # timed decode using rugo (requires compiled disk_reader.read_file)
            rows_rugo, rugo_times = _timed(_read_with_rugo, files, iterations=3)
            rugo_avg = sum(rugo_times) / len(rugo_times)
            print("rugo read: ")
            for i, t in enumerate(rugo_times, 1):
                print(f"  Iter {i}: {t:.4f}s")
            print(f"  → rows: {rows_rugo:,d}, avg: {rugo_avg:.4f}s, min: {min(rugo_times):.4f}s, max: {max(rugo_times):.4f}s\n")

            # verification pass: ensure rugo actually decoded column data for each file
            # we expect this may fail (prove incomplete decoding)
            from opteryx.compiled.io.disk_reader import read_file as _disk_read_file
            for f in files:
                buf = _disk_read_file(f)
                res = rp.read_parquet(buf)
                if res is None or not res.get("success", False):
                    pytest.fail("rugo.read_parquet failed to return a successful result")

                # compare to PyArrow for row count and data presence
                arrow_table = pq.read_table(f)
                expected_rows = arrow_table.num_rows

                # check decoded rows from first non-empty column in first row_group
                if not res.get("row_groups"):
                    pytest.fail("rugo returned no row_groups for file")

                for rg in res["row_groups"]:
                    # ensure every column has data (not None/empty)
                    for col_data in rg:
                        if col_data is None or (isinstance(col_data, list) and len(col_data) == 0):
                            pytest.fail("rugo failed to decode one or more columns (incomplete decoding)")
        except RuntimeError as exc:
            # compiled disk_reader missing — treat as test error
            raise

    # fastparquet (optional) — skip only this section when dependency missing
    try:
        import fastparquet as fp  # type: ignore
    except Exception:  # pragma: no cover - environment may not have fastparquet
        print("fastparquet not installed — skipping fastparquet measurements\n")
        return

    rows_fp, fp_times = _timed(_read_with_fastparquet, files, iterations=3)
    fp_avg = sum(fp_times) / len(fp_times)
    print("fastparquet read: ")
    for i, t in enumerate(fp_times, 1):
        print(f"  Iter {i}: {t:.4f}s")
    print(f"  → rows: {rows_fp:,d}, avg: {fp_avg:.4f}s, min: {min(fp_times):.4f}s, max: {max(fp_times):.4f}s\n")

    # Summary
    ratio = fp_avg / arrow_avg if arrow_avg > 0 else float("inf")
    print("Summary:")
    print(f"  PyArrow avg:     {arrow_avg:.4f}s")
    if rugo_avg is not None:
        print(f"  rugo   avg:      {rugo_avg:.4f}s  (rugo / pyarrow = {rugo_avg/arrow_avg:.2f}x)")
    print(f"  fastparquet avg: {fp_avg:.4f}s")
    print(f"  fastparquet / pyarrow = {ratio:.2f}x\n")


def test_parquet_decode_rugo_placeholder():
    """Placeholder test for rugo decode performance (empty / TODO).

    Kept as a skipped test so CI/test runs show an explicit placeholder.
    """
    pytest.skip("TODO: implement rugo decode performance test")


# -----------------------------
# Metadata-only benchmarks
# -----------------------------
def _metadata_with_pyarrow(files: List[str]) -> tuple:
    """Read schema/metadata using PyArrow (fast footer/schema access).

    Returns tuple: (total_rows, sorted_unique_column_names)
    """
    total_rows = 0
    cols = set()
    for f in files:
        # read schema/metadata via pyarrow
        pf = pq.ParquetFile(f)
        md = pf.metadata
        total_rows += getattr(md, "num_rows", 0)

        try:
            # prefer Arrow schema names when available
            names = pq.read_schema(f).names
        except Exception:
            # fallback to metadata-derived column names
            names = [c.name for c in pf.metadata.schema]
        cols.update(names)

    return total_rows, sorted(cols)


def _metadata_with_fastparquet(files: List[str]) -> tuple:
    """Read schema/metadata using fastparquet without decoding data.

    Returns tuple: (total_rows, sorted_unique_column_names)
    """
    import fastparquet as fp  # type: ignore

    total = 0
    cols = set()
    for f in files:
        pf = fp.ParquetFile(f)
        try:
            total += int(pf.count())
        except Exception:
            total += sum(rg.num_rows for rg in pf.row_groups)
        # fastparquet exposes `columns` property
        cols.update(pf.columns)
    return total, sorted(cols)


def _metadata_with_rugo(files: List[str]) -> tuple:
    """Read schema-only metadata using rugo's metadata reader (memoryview input).

    REQUIRE compiled `disk_reader.read_file()` — do NOT fall back to Python I/O.
    Returns tuple: (total_rows, sorted_unique_column_names)
    """
    import opteryx.rugo.parquet as parquet_meta  # local rugo metadata reader

    # REQUIRE a compiled disk_reader that supports memory-mapping
    try:
        from opteryx.compiled.io.disk_reader import read_file_mmap as _disk_read_mmap
    except Exception as exc:
        raise RuntimeError(
            "compiled disk_reader.read_file_mmap() is required for rugo mmap benchmark"
        ) from exc

    total_rows = 0
    cols = set()
    for f in files:
        # Use memory-mapped file to avoid copy overhead and measure rugo's best case
        mm = _disk_read_mmap(f)
        buf = memoryview(mm)

        # schema_only should be the fastest path for rugo (no statistics)
        md = parquet_meta.read_metadata_from_memoryview(
            buf, schema_only=True, max_row_groups=1, include_statistics=False
        )
        if isinstance(md, dict):
            total_rows += md.get("num_rows", 0) or 0
            for c in md.get("schema_columns", []):
                cols.add(c.get("name"))
        # explicitly unmap to avoid resource leak
        try:
            from opteryx.compiled.io.disk_reader import unmap_memory as _unmap
            _unmap(mm)
        except Exception:
            pass
    return total_rows, sorted(cols)


def test_parquet_metadata_readers_prints():
    """Compare metadata/schema read performance across PyArrow, fastparquet and rugo.

    - Uses schema/metadata-only reads (no column data decoding).
    - Prints timings (avg/min/max) for each available reader.
    """
    files = _get_parquet_files()
    if not files:
        pytest.skip(f"Parquet dataset not found: {DATASET_GLOB}")

    print("\n=== Parquet metadata/schema benchmark (TPCH lineitem) ===\n")
    print(f"Files: {len(files)}  — example: {os.path.basename(files[0])}\n")

    # PyArrow schema/metadata
    out_arrow, arrow_times = _timed(_metadata_with_pyarrow, files, iterations=5)
    rows_arrow, cols_arrow = out_arrow
    arrow_avg = sum(arrow_times) / len(arrow_times)
    print("PyArrow metadata read:")
    for i, t in enumerate(arrow_times, 1):
        print(f"  Iter {i}: {t:.4f}s")
    print(f"  → rows(metadata): {rows_arrow:,d}, cols: {cols_arrow}, avg: {arrow_avg:.4f}s, min: {min(arrow_times):.4f}s, max: {max(arrow_times):.4f}s\n")

    # fastparquet (optional)
    try:
        import fastparquet  # type: ignore
    except Exception:  # pragma: no cover - environment may not have fastparquet
        print("fastparquet not installed — skipping fastparquet metadata measurements\n")
        rows_fp = None
        fp_avg = None
        cols_fp = None
    else:
        out_fp, fp_times = _timed(_metadata_with_fastparquet, files, iterations=5)
        rows_fp, cols_fp = out_fp
        fp_avg = sum(fp_times) / len(fp_times)
        print("fastparquet metadata read:")
        for i, t in enumerate(fp_times, 1):
            print(f"  Iter {i}: {t:.4f}s")
        print(f"  → rows(metadata): {rows_fp:,d}, cols: {cols_fp}, avg: {fp_avg:.4f}s, min: {min(fp_times):.4f}s, max: {max(fp_times):.4f}s\n")

    # Rugo metadata (schema-only)
    try:
        import opteryx.rugo.parquet as _tmp  # ensure importable
    except Exception:
        print("rugo parquet metadata reader not available — skipping rugo metadata measurements\n")
        rows_rugo = None
        rugo_avg = None
        cols_rugo = None
    else:
        out_rugo, rugo_times = _timed(_metadata_with_rugo, files, iterations=5)
        rows_rugo, cols_rugo = out_rugo
        rugo_avg = sum(rugo_times) / len(rugo_times)
        print("rugo metadata read (schema-only):")
        for i, t in enumerate(rugo_times, 1):
            print(f"  Iter {i}: {t:.4f}s")
        print(f"  → rows(metadata): {rows_rugo:,d}, cols: {cols_rugo}, avg: {rugo_avg:.4f}s, min: {min(rugo_times):.4f}s, max: {max(rugo_times):.4f}s\n")

    # Summary — show available comparisons
    print("Metadata Summary:")
    print(f"  PyArrow avg:     {arrow_avg:.4f}s")
    if fp_avg is not None:
        print(f"  fastparquet avg: {fp_avg:.4f}s  (fastparquet / pyarrow = {fp_avg/arrow_avg:.2f}x)")
    if rugo_avg is not None:
        print(f"  rugo   avg:      {rugo_avg:.4f}s  (rugo / pyarrow = {rugo_avg/arrow_avg:.2f}x)")
    print()


if __name__ == "__main__":
    # Run the benchmark tests directly (prints output) — not as regression assertions.
    test_parquet_decode_pyarrow_vs_fastparquet_prints()
    test_parquet_metadata_readers_prints()