# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-INSTR — native execution-engine instrumentation harness (developer tooling).

The runnable front-end for the engine's measurement instruments. It does NOT change
query behaviour; it only reads what the engine already records. See
``docs/ENGINE_INSTRUMENTATION.md``.

The instruments
---------------
* scan_sources        — per parquet scan, which native Source it selected
                        (NativeParquetScanSource / LatmatScanSource). Always on
                        telemetry (plan-time fact, ~0 cost).
* allocation harness  — ``measure_query_allocations`` / ``scaling_report`` below:
                        samples ``sys.getallocatedblocks()`` across a drained query
                        to show a scan allocates O(morsels), not O(rows).

(The execution-time GIL instrument — ``gil_held_ns``, ``worker_gil_sites`` and the
worker purity guard — was removed 2026-10-05. It was armed only for native runs,
whose execution never enters a Python operator body, and its sites ran only on the
serial engine, which never armed it: it could not record anything.)

CLI examples
------------
    # Readout for one query:
    python dev/instrument_engine.py --sql "SELECT followers FROM 'testdata/flat/formats/parquet'"

    # Allocation scaling: {n} is substituted with each --scale size as a LIMIT.
    python dev/instrument_engine.py \
        --sql "SELECT followers FROM 'testdata/flat/formats/parquet' LIMIT {n}" \
        --scale 10000,50000,250000
"""

from __future__ import annotations

import os
import sys
from typing import Callable
from typing import Iterable
from typing import Optional


def _telemetry_of(session) -> dict:
    """Read the drained query's telemetry dict from a session."""
    return session.telemetry


def run_and_report(sql: str, session_factory: Optional[Callable] = None) -> dict:
    """Execute ``sql`` to completion and return its row count and scan Sources."""
    import opteryx

    session = (session_factory or opteryx.session)()
    rows = 0
    for morsel in session.execute_to_morsels(sql):
        rows += morsel.num_rows
    telemetry = _telemetry_of(session)
    return {
        "sql": sql,
        "rows": rows,
        "scan_sources": telemetry.get("scan_sources", {}),
    }


def measure_query_allocations(
    sql: str,
    session_factory: Optional[Callable] = None,
    use_tracemalloc: bool = False,
) -> dict:
    """Run ``sql`` and measure allocation behaviour across the scan.

    ``peak_block_delta`` is the max of ``sys.getallocatedblocks()`` minus baseline,
    sampled after every yielded morsel: the largest live-block footprint the pipeline
    held at once. It is O(morsels) (bounded by morsel size), so ``blocks_per_row``
    falls toward zero as rows grow — the proof that native operators do not hold
    O(rows) memory. Morsels are counted then dropped, so this measures the engine's
    own footprint, not the caller hoarding results.

    With ``use_tracemalloc`` a peak byte figure from :mod:`tracemalloc` is added
    (heavier; off by default).
    """
    import gc

    import opteryx

    if use_tracemalloc:
        import tracemalloc

        tracemalloc.start()

    session = (session_factory or opteryx.session)()
    gen = session.execute_to_morsels(sql)

    gc.collect()
    baseline = sys.getallocatedblocks()
    peak_delta = 0
    rows = 0
    morsels = 0
    for morsel in gen:
        rows += morsel.num_rows
        morsels += 1
        delta = sys.getallocatedblocks() - baseline
        if delta > peak_delta:
            peak_delta = delta
        del morsel

    telemetry = _telemetry_of(session)
    result = {
        "sql": sql,
        "rows": rows,
        "morsels": morsels,
        "peak_block_delta": peak_delta,
        "blocks_per_row": (peak_delta / rows) if rows else 0.0,
        "blocks_per_morsel": (peak_delta / morsels) if morsels else 0.0,
        "scan_sources": telemetry.get("scan_sources", {}),
    }
    if use_tracemalloc:
        import tracemalloc

        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        result["tracemalloc_peak_bytes"] = peak_bytes
    return result


def scaling_report(
    sql_template: str,
    sizes: Iterable[int],
    session_factory: Optional[Callable] = None,
) -> list:
    """Run ``sql_template`` (with ``{n}`` substituted by each size) at several row
    counts and return the per-run allocation measurements. A flat ``blocks_per_row``
    trend that falls toward zero as ``n`` grows is the O(morsels) signature; a
    roughly constant (non-falling) ``blocks_per_row`` is the O(rows) signature.
    """
    out = []
    for n in sizes:
        out.append(measure_query_allocations(sql_template.format(n=n), session_factory))
    return out


def generate_dataset(
    base_dataset: str,
    columns: str,
    out_dir: str,
    multiplier: int,
    session_factory: Optional[Callable] = None,
) -> tuple:
    """Materialise ``SELECT columns FROM base_dataset`` repeated ``multiplier`` times
    (via UNION ALL) into a fresh parquet relation under ``out_dir``, using the native
    rugo writer (no pyarrow/numpy). Returns ``(dataset_path, rows)``.

    This exists so the allocation scaling demo can hold the projection/predicate
    shape fixed while growing the row count — the only way to separate O(rows) from
    O(morsels) — without a scan-pushed LIMIT changing the scan's shape.
    """
    import opteryx
    from opteryx.connectors.parquet_io.parquet_writer import write_morsel

    legs = " UNION ALL ".join("SELECT %s FROM '%s'" % (columns, base_dataset) for _ in range(multiplier))
    dataset_path = os.path.join(out_dir, "gen_%dx" % multiplier)
    os.makedirs(dataset_path, exist_ok=True)

    session = (session_factory or opteryx.session)()
    rows = 0
    for morsel in session.execute_to_morsels(legs):
        if morsel.num_rows == 0:
            continue
        write_morsel(morsel, dataset_path)
        rows += morsel.num_rows
    return dataset_path, rows


def demo_scaling(out_dir: str, multipliers=(1, 2, 4)) -> dict:
    """Generate numeric-only and string parquet relations at several sizes and run
    the allocation scaling for each. Returns ``{"numeric": [...], "string": [...]}``
    lists of :func:`measure_query_allocations` results. Prints two tables; both
    trends should show ``blocks_per_row`` falling toward zero (O(morsels)).
    """
    base = "testdata/flat/formats/parquet"
    results: dict = {"numeric": [], "string": []}
    specs = [
        ("numeric", "user_id, followers, following"),
        ("string", "text"),
    ]
    for label, cols in specs:
        label_dir = os.path.join(out_dir, label)
        for m in multipliers:
            ds, _ = generate_dataset(base, cols, label_dir, m)
            results[label].append(measure_query_allocations("SELECT %s FROM '%s'" % (cols, ds)))
    for label in ("numeric", "string"):
        print("== %s scaling ==" % label)
        print(
            "  %-9s %-8s %-12s %-12s %s"
            % ("rows", "morsels", "peak_blocks", "blocks/row", "source")
        )
        for r in results[label]:
            src = ",".join(sorted(set(r["scan_sources"].values()))) or "-"
            print(
                "  %-9d %-8d %-12d %-12.4f %s"
                % (r["rows"], r["morsels"], r["peak_block_delta"], r["blocks_per_row"], src)
            )
    return results


def _main(argv: list) -> int:
    import argparse

    # Run-from-tree: this file lives in dev/, so the repo root is one level up.
    sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))

    parser = argparse.ArgumentParser(description="Native engine instrumentation harness")
    parser.add_argument("--sql", default="", help="SQL to run; use {n} for --scale")
    parser.add_argument(
        "--scale",
        default="",
        help="comma-separated row counts to substitute for {n} in --sql (alloc scaling)",
    )
    parser.add_argument(
        "--tracemalloc",
        action="store_true",
        help="also report a tracemalloc peak-bytes figure in the alloc measurement",
    )
    parser.add_argument(
        "--demo-scaling",
        metavar="OUT_DIR",
        default="",
        help="generate sized numeric+string parquet under OUT_DIR and show both "
        "allocation trends (self-contained; ignores --sql)",
    )
    args = parser.parse_args(argv)

    if args.demo_scaling:
        demo_scaling(args.demo_scaling)
        return 0

    if not args.sql:
        parser.error("one of --sql or --demo-scaling is required")

    if args.scale:
        sizes = [int(x) for x in args.scale.split(",") if x.strip()]
        print("== allocation scaling ==")
        print(
            "  %-9s %-8s %-12s %-12s %s"
            % ("rows", "morsels", "peak_blocks", "blocks/row", "source")
        )
        for r in scaling_report(args.sql, sizes):
            src = ",".join(sorted(set(r["scan_sources"].values()))) or "-"
            print(
                "  %-9d %-8d %-12d %-12.4f %s"
                % (r["rows"], r["morsels"], r["peak_block_delta"], r["blocks_per_row"], src)
            )
        return 0

    report = run_and_report(args.sql)
    print("== instrumentation readout ==")
    print("  rows            :", report["rows"])
    print("  scan_sources    :", report["scan_sources"])
    alloc = measure_query_allocations(args.sql, use_tracemalloc=args.tracemalloc)
    print("  peak_block_delta:", alloc["peak_block_delta"], "(blocks/row %.4f)" % alloc["blocks_per_row"])
    if args.tracemalloc:
        print("  tracemalloc_peak:", alloc["tracemalloc_peak_bytes"], "bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
