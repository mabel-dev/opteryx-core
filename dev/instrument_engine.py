# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-INSTR — native execution-engine instrumentation harness (developer tooling).

This module is the runnable front-end for the four measurement instruments built
for the native execution engine. It does NOT change query behaviour; it only reads
what the engine already records. See ``docs/instrumentation.md`` for the full note.

The four instruments
--------------------
1. gil_held_ns          — per-query nanoseconds spent inside execution-time
                          ``with gil`` bodies (scan-pull trampoline + error stash).
                          Surfaced on telemetry when OPTERYX_INSTRUMENT_ENGINE=1.
2. scan_sources         — per parquet scan, which Source it selected
                          (NativeParquetScanSource vs StreamingScanSource).
                          Always on telemetry (plan-time fact, ~0 cost).
3. allocation harness   — ``measure_query_allocations`` / ``scaling_report`` below:
                          samples ``sys.getallocatedblocks()`` across a drained
                          query to show native scans allocate O(morsels) not
                          O(rows).
4. worker purity guard  — ``assert_native_worker_purity`` below: fails if any
                          non-whitelisted GIL site ran on a worker thread.

Enabling the GIL instrumentation (instruments 1 & 4)
----------------------------------------------------
Set the config flag before opteryx is imported::

    OPTERYX_INSTRUMENT_ENGINE=1 python dev/instrument_engine.py --sql "SELECT ..."

or, in a test, monkeypatch the already-imported flag::

    import opteryx.config as config
    config.OPTERYX_INSTRUMENT_ENGINE = True   # execute_native reads it per-call

CLI examples
------------
    # Full readout for one query (native-gated vs trampoline are distinguishable):
    OPTERYX_INSTRUMENT_ENGINE=1 python dev/instrument_engine.py \
        --sql "SELECT followers FROM 'testdata/flat/formats/parquet'"

    # Allocation scaling: prove flat blocks/row for a numeric scan, growing for a
    # string scan. {n} is substituted with each --scale size as a LIMIT.
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

# The two sites that are legitimately allowed to run Python on a worker thread
# TODAY. Each future work package that de-Pythons one of these removes it from the
# whitelist; the guard then fails until that path is genuinely native.
DEFAULT_WORKER_WHITELIST = ("_scan_pull_run", "_stash_exc")


def _telemetry_of(session) -> dict:
    """Read the drained query's telemetry dict from a session."""
    return session._telemetry.as_dict()


def run_and_report(sql: str, session_factory: Optional[Callable] = None) -> dict:
    """Execute ``sql`` to completion and return the instrumentation readings.

    Requires OPTERYX_INSTRUMENT_ENGINE=1 (or a monkeypatched config flag) for the
    ``gil_held_ns`` / ``worker_gil_sites`` readings to be populated; ``scan_sources``
    is present regardless.
    """
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
        "gil_held_ns": telemetry.get("gil_held_ns", 0),
        "worker_gil_sites": telemetry.get("worker_gil_sites", []),
    }


def measure_query_allocations(
    sql: str,
    session_factory: Optional[Callable] = None,
    use_tracemalloc: bool = False,
) -> dict:
    """Run ``sql`` and measure allocation behaviour across the scan.

    Two complementary readings:

    * ``peak_block_delta`` — max of ``sys.getallocatedblocks()`` minus baseline,
      sampled after every yielded morsel: the largest live-block footprint the
      pipeline held at once. For BOTH Sources this is O(morsels) (bounded by morsel
      size), so ``blocks_per_row`` falls toward zero as rows grow — the proof that
      native operators do not hold O(rows) memory. Morsels are counted then dropped,
      so this measures the engine's own footprint, not the caller hoarding results.

    * ``trampoline_calls`` — the number of per-morsel Python re-entries through
      ``_scan_pull_run`` (requires the GIL instrumentation armed). This is the
      allocation-bearing event the peak-block metric CANNOT see: each trampoline
      pull creates transient Python objects that are freed before the next
      morsel-boundary sample. It is 0 for a NativeParquetScanSource and grows with
      the scan (∝ morsels ∝ rows at fixed morsel size) for a StreamingScanSource —
      the honest O(morsels)-vs-zero discriminator between the two paths.

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
    trampoline_calls = sum(
        s.get("calls", 0)
        for s in (telemetry.get("worker_gil_sites", []) or [])
        if s.get("site") == "_scan_pull_run"
    )
    result = {
        "sql": sql,
        "rows": rows,
        "morsels": morsels,
        "peak_block_delta": peak_delta,
        "blocks_per_row": (peak_delta / rows) if rows else 0.0,
        "blocks_per_morsel": (peak_delta / morsels) if morsels else 0.0,
        # The per-morsel Python re-entry count — 0 for a native scan, growing with
        # the scan for the trampoline. This is the allocation-bearing event the
        # peak-live-block metric cannot see (its allocations are transient), so it
        # is the honest O(morsels)-vs-zero discriminator between the two Sources.
        "trampoline_calls": trampoline_calls,
        "gil_held_ns": telemetry.get("gil_held_ns", 0),
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
    O(morsels) — without a scan-pushed LIMIT (which would itself force the
    trampoline Source and defeat the native-vs-trampoline comparison).
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
    lists of :func:`measure_query_allocations` results. Prints two tables: the
    numeric (native) trend should show ``blocks_per_row`` falling toward zero
    (O(morsels)); the string (trampoline) trend should not fall as fast.
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
            "  %-9s %-8s %-12s %-12s %-14s %s"
            % ("rows", "morsels", "peak_blocks", "blocks/row", "trampoline_c", "source")
        )
        for r in results[label]:
            src = ",".join(sorted(set(r["scan_sources"].values()))) or "-"
            print(
                "  %-9d %-8d %-12d %-12.4f %-14d %s"
                % (
                    r["rows"],
                    r["morsels"],
                    r["peak_block_delta"],
                    r["blocks_per_row"],
                    r["trampoline_calls"],
                    src,
                )
            )
    return results


class WorkerPurityError(AssertionError):
    """Raised when a non-whitelisted GIL site executed on a worker thread."""


def assert_native_worker_purity(
    telemetry: dict,
    whitelist: Iterable[str] = DEFAULT_WORKER_WHITELIST,
) -> list:
    """Instrument 4 — the worker-thread purity guard.

    Inspects ``telemetry['worker_gil_sites']`` (populated only when the GIL
    instrumentation was armed) and raises :class:`WorkerPurityError` if any GIL site
    outside ``whitelist`` ran on a worker thread. Returns the list of offending site
    records when it passes (empty on a clean run).

    What it catches / limitations:
    * It counts entries into the INSTRUMENTED execution-time ``with gil`` bodies
      (currently ``_scan_pull_run`` and ``_stash_exc``). It is an *enumerated*
      guard, not a universal ``settrace`` — a worker that re-entered Python through
      some OTHER, not-yet-instrumented ``with gil`` body would not be seen until that
      body is added to the instrumentation. As each such body is discovered it must
      be wrapped (see ``_operators.pyx`` WP-INSTR block) so this guard covers it.
    * Passing ``whitelist=()`` turns any execution-time Python re-entry into a
      failure — this is how a test deliberately flags the trampoline path.
    * Requires OPTERYX_INSTRUMENT_ENGINE armed for the run; on an unarmed run
      ``worker_gil_sites`` is empty and the guard trivially passes.
    """
    allowed = set(whitelist)
    sites = telemetry.get("worker_gil_sites", []) or []
    offenders = [s for s in sites if s.get("site") not in allowed]
    if offenders:
        summary = ", ".join(
            "%s x%d on thread %s" % (o["site"], o["calls"], o["thread_id"]) for o in offenders
        )
        raise WorkerPurityError(
            "non-whitelisted Python ran on worker thread(s): " + summary
        )
    return sites


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
        "--guard",
        action="store_true",
        help="assert worker-thread purity (whitelist = scan-pull + error-stash)",
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

    import opteryx.config as config

    if args.demo_scaling:
        demo_scaling(args.demo_scaling)
        return 0

    if not args.sql:
        parser.error("one of --sql or --demo-scaling is required")

    if not config.OPTERYX_INSTRUMENT_ENGINE:
        print(
            "note: OPTERYX_INSTRUMENT_ENGINE is not set — gil_held_ns / worker_gil_sites "
            "will be empty. Re-run with OPTERYX_INSTRUMENT_ENGINE=1 for those readings.",
            file=sys.stderr,
        )

    if args.scale:
        sizes = [int(x) for x in args.scale.split(",") if x.strip()]
        print("== allocation scaling ==")
        print(
            "  %-9s %-8s %-12s %-12s %-14s %s"
            % ("rows", "morsels", "peak_blocks", "blocks/row", "trampoline_c", "source")
        )
        for r in scaling_report(args.sql, sizes):
            src = ",".join(sorted(set(r["scan_sources"].values()))) or "-"
            print(
                "  %-9d %-8d %-12d %-12.4f %-14d %s"
                % (
                    r["rows"],
                    r["morsels"],
                    r["peak_block_delta"],
                    r["blocks_per_row"],
                    r["trampoline_calls"],
                    src,
                )
            )
        return 0

    report = run_and_report(args.sql)
    print("== instrumentation readout ==")
    print("  rows            :", report["rows"])
    print("  scan_sources    :", report["scan_sources"])
    print("  gil_held_ns     :", report["gil_held_ns"])
    print("  worker_gil_sites:", report["worker_gil_sites"])
    alloc = measure_query_allocations(args.sql, use_tracemalloc=args.tracemalloc)
    print("  peak_block_delta:", alloc["peak_block_delta"], "(blocks/row %.4f)" % alloc["blocks_per_row"])
    if args.tracemalloc:
        print("  tracemalloc_peak:", alloc["tracemalloc_peak_bytes"], "bytes")

    if args.guard:
        import opteryx

        session = opteryx.session()
        for _ in session.execute_to_morsels(args.sql):
            pass
        assert_native_worker_purity(_telemetry_of(session))
        print("  worker purity   : PASS (only whitelisted sites ran)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
