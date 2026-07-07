"""M1-VALIDATE — prove/disprove that milestone M1 removed the scan-stage GIL
bottleneck the audit named as the most probable cause of the GIL performance
incident.

This is a MEASUREMENT + REPORTING harness. It changes NO engine behaviour. It
runs the SAME query twice — once on the default path (which now selects
``NativeParquetScanSource``, a pure-C++/nogil scan) and once forced onto the old
``StreamingScanSource`` (the per-morsel GIL trampoline) — and compares them across
a worker/dop sweep, reading only the instruments WP-INSTR already exposes.

Why this is observable on a GIL interpreter
-------------------------------------------
The deployed service runs standard-GIL CPython 3.14. On a GIL build, pure-C++
worker threads that never touch Python still run in parallel; the harm is threads
that RE-ATTACH the GIL per morsel — those serialise. ``NativeParquetScanSource``
does zero per-morsel Python; ``StreamingScanSource`` re-enters Python once per
morsel per worker through ``_scan_pull_run``. So the discriminator is visible on
a GIL interpreter: native should scale (or raise CPU-utilisation) with dop, the
trampoline should flatten.

This dev/test interpreter is a free-threaded 3.14 build whose GIL is toggled by
the ``PYTHON_GIL`` env var. We therefore reproduce the DEPLOYED regime exactly:

    PYTHON_GIL=1  ->  GIL enabled   (emulates the production standard-GIL service; PRIMARY)
    PYTHON_GIL=0  ->  GIL disabled  (free-threaded cross-check)

The harness detects ``sys._is_gil_enabled()`` and labels the run.

Force mechanism
---------------
Identical to the WP-01/02/11 parity tests: monkeypatch
``pool_reader.native_scan_supported`` to return False, which routes the scan to
``StreamingScanSource`` with the predicate on the old bytecode-VM path.

Run
---
    # PRIMARY (emulates deployed GIL service):
    PYTHON_GIL=1 PYENV_VERSION=3.14.5t pyenv exec python tests/performance/m1_validate.py

    # cross-check (free-threaded):
    PYTHON_GIL=0 PYENV_VERSION=3.14.5t pyenv exec python tests/performance/m1_validate.py

    # smaller/faster smoke (fewer rows, single dop):
    PYTHON_GIL=1 ... python tests/performance/m1_validate.py --rows 2000000 --dops 1,4 --repeats 2

Data is generated once (pyarrow, a test-only dep) under --data-dir and cached.
Results are printed as before/after tables and written as JSON next to the report.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import statistics
import sys
import time

# run-from-tree: this file is tests/performance/, repo root is two up.
_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, _REPO)
sys.path.insert(1, os.path.join(_REPO, "dev"))

import opteryx  # noqa: E402
import opteryx.config as config  # noqa: E402
from opteryx.connectors.parquet_io import pool_reader  # noqa: E402
import instrument_engine as IE  # noqa: E402


# ── dataset generation (pyarrow — test-only, never imported by the engine) ──────

def _gen_strings(path: str, rows: int, files: int, row_group_size: int) -> None:
    """ClickBench-like wide STRING table. Columns:

      id         int64, monotonically increasing 0..rows-1 (SORTED -> row-group
                 min/max pruning fires on `id` predicates)
      url        string, high cardinality (~rows/13 distinct)
      title      string, medium cardinality (~5000 distinct)
      referer    string, low cardinality (~1000 distinct)
      user_agent string, very low cardinality (~16 distinct)
      n          int64 = id % 1000 (UNSORTED within row groups -> no pruning on `n`)

    Written in row_group_size chunks so memory stays ~O(chunk), across `files`
    parquet files. Deterministic; cached via a .done marker.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    done = os.path.join(path, ".done")
    marker = f"{rows}:{files}:{row_group_size}:strings:v1"
    if os.path.exists(done) and open(done).read() == marker:
        return
    os.makedirs(path, exist_ok=True)

    ua_pool = [f"agent/{i}" for i in range(16)]
    ref_pool = [f"http://ref{i}.example/" for i in range(1000)]
    title_pool = [f"Title number {i} about things" for i in range(5000)]

    schema = pa.schema([
        ("id", pa.int64()), ("url", pa.string()), ("title", pa.string()),
        ("referer", pa.string()), ("user_agent", pa.string()), ("n", pa.int64()),
    ])
    per_file = rows // files
    base = 0
    for f in range(files):
        fn = os.path.join(path, f"part-{f:03d}.parquet")
        w = pq.ParquetWriter(fn, schema, use_dictionary=True)
        remaining = per_file if f < files - 1 else rows - base
        written = 0
        while written < remaining:
            g = min(row_group_size, remaining - written)
            ids = list(range(base, base + g))
            url = [f"http://site{v % 100000}.example/path/{v % 13}/page.html" for v in ids]
            title = [title_pool[v % 5000] for v in ids]
            referer = [ref_pool[v % 1000] for v in ids]
            ua = [ua_pool[v % 16] for v in ids]
            n = [v % 1000 for v in ids]
            tbl = pa.table({
                "id": pa.array(ids, pa.int64()), "url": pa.array(url, pa.string()),
                "title": pa.array(title, pa.string()), "referer": pa.array(referer, pa.string()),
                "user_agent": pa.array(ua, pa.string()), "n": pa.array(n, pa.int64()),
            }, schema=schema)
            w.write_table(tbl, row_group_size=row_group_size)
            base += g
            written += g
        w.close()
    with open(done, "w") as fh:
        fh.write(marker)


def _gen_dectime(path: str, rows: int, files: int, row_group_size: int) -> None:
    """DECIMAL + TIMESTAMP table. Columns:

      id       int64, SORTED (row-group pruning fires)
      price    decimal128(9,2)  -> DECIMAL64  (WP-11 admitted)
      amount   decimal128(18,2) -> DECIMAL128 (WP-11 admitted; p<=18 avoids the
               known FLBA p>18 display limit)
      event_ts timestamp[us]    (us unit avoids the known ns display limit)
      ev_date  date32
      n        int64 = id % 1000
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    done = os.path.join(path, ".done")
    marker = f"{rows}:{files}:{row_group_size}:dectime:v1"
    if os.path.exists(done) and open(done).read() == marker:
        return
    os.makedirs(path, exist_ok=True)

    schema = pa.schema([
        ("id", pa.int64()), ("price", pa.decimal128(9, 2)), ("amount", pa.decimal128(18, 2)),
        ("event_ts", pa.timestamp("us")), ("ev_date", pa.date32()), ("n", pa.int64()),
    ])
    base_ts = 1_600_000_000_000_000  # us since epoch, ~2020-09
    per_file = rows // files
    base = 0
    for f in range(files):
        fn = os.path.join(path, f"part-{f:03d}.parquet")
        w = pq.ParquetWriter(fn, schema, use_dictionary=True)
        remaining = per_file if f < files - 1 else rows - base
        written = 0
        while written < remaining:
            g = min(row_group_size, remaining - written)
            ids = list(range(base, base + g))
            price = [pa.scalar(v % 100000, pa.decimal128(9, 2)) for v in ids]  # replaced below
            tbl = pa.table({
                "id": pa.array(ids, pa.int64()),
                "price": pa.array([(v % 100000) for v in ids], pa.decimal128(9, 2)),
                "amount": pa.array([(v % 1000000) for v in ids], pa.decimal128(18, 2)),
                "event_ts": pa.array([base_ts + v * 1_000_000 for v in ids], pa.timestamp("us")),
                "ev_date": pa.array([(v % 3650) for v in ids], pa.date32()),
                "n": pa.array([v % 1000 for v in ids], pa.int64()),
            }, schema=schema)
            w.write_table(tbl, row_group_size=row_group_size)
            base += g
            written += g
        w.close()
    with open(done, "w") as fh:
        fh.write(marker)


# ── one measured run ────────────────────────────────────────────────────────

def _measure(sql: str, force_tramp: bool, dop: int, repeats: int) -> dict:
    """Run `sql` `repeats` times (plus one unmeasured warmup) at the given dop and
    path. Returns median wall/cpu/throughput plus the WP-INSTR readings and pruning
    facts from the LAST run (plan-time facts are run-invariant)."""
    config.MAX_EXECUTION_WORKERS = dop
    config.PARALLEL_MIN_ROWS = 0  # force the parallel scheduler to engage so dop is real
    config.OPTERYX_INSTRUMENT_ENGINE = True

    orig = pool_reader.native_scan_supported
    if force_tramp:
        pool_reader.native_scan_supported = lambda *a, **k: False
    try:
        walls, cpus, out_rows = [], [], 0
        telemetry = None
        for r in range(repeats + 1):  # r==0 is warmup (page-cache warm + JIT-ish)
            gc.collect()
            s = opteryx.session()
            w0 = time.perf_counter_ns()
            c0 = time.process_time_ns()
            rows = 0
            for m in s.execute_to_morsels(sql):
                rows += m.num_rows
            w = time.perf_counter_ns() - w0
            c = time.process_time_ns() - c0
            if r == 0:
                continue
            walls.append(w)
            cpus.append(c)
            out_rows = rows
            telemetry = s._telemetry.as_dict()
    finally:
        pool_reader.native_scan_supported = orig

    wall_ns = statistics.median(walls)
    cpu_ns = statistics.median(cpus)
    src = sorted(set(telemetry["scan_sources"].values()))
    tramp = sum(x.get("calls", 0) for x in (telemetry.get("worker_gil_sites") or [])
                if x.get("site") == "_scan_pull_run")
    # pruning facts (native path exposes native_scan_facts; both expose files_pruned).
    # native_scan_facts is intentionally stripped from as_dict() (it is overlaid onto
    # the scan's operation row by mermaid), so read it from the raw _reading dict.
    facts = s._telemetry._reading.get("native_scan_facts", {})
    rg_read = sum(v.get("row_groups_read", 0) for v in facts.values()) if facts else None
    rg_pruned = sum(v.get("row_groups_pruned", 0) for v in facts.values()) if facts else None
    # worker purity: whitelist=() flags ANY execution-time Python re-entry
    try:
        IE.assert_native_worker_purity(telemetry, whitelist=())
        purity = "PASS"
    except IE.WorkerPurityError as e:
        purity = f"FLAG ({str(e).split(':')[-1].strip()[:40]})"

    return {
        "wall_ms": wall_ns / 1e6,
        "cpu_ms": cpu_ns / 1e6,
        "cores": (cpu_ns / wall_ns) if wall_ns else 0.0,
        "rows": out_rows,
        "krows_s": (out_rows / (wall_ns / 1e9)) / 1000.0 if wall_ns else 0.0,
        "src": ",".join(x.replace("ParquetScanSource", "").replace("ScanSource", "") for x in src),
        "gil_ms": telemetry.get("gil_held_ns", 0) / 1e6,
        "tramp": tramp,
        "files_pruned": telemetry.get("files_pruned", 0),
        "rg_read": rg_read,
        "rg_pruned": rg_pruned,
        "purity": purity,
    }


# ── scenario driver ─────────────────────────────────────────────────────────

def _run_scenario(name: str, sql: str, dataset_rows: int, dops, repeats: int) -> dict:
    print(f"\n### {name}")
    print(f"    {sql}")
    hdr = (f"  {'path':<9}{'dop':>4}{'wall_ms':>10}{'krows/s':>10}{'cores':>7}"
           f"{'scan_krps':>11}{'gil_ms':>9}{'tramp':>7}{'rg_rd':>7}{'rg_pr':>7}  purity")
    print(hdr)
    rows = {"name": name, "sql": sql, "dataset_rows": dataset_rows, "native": [], "tramp": []}
    for force in (False, True):
        key = "tramp" if force else "native"
        for dop in dops:
            m = _measure(sql, force, dop, repeats)
            m["dop"] = dop
            # scan throughput = full dataset rows / wall (work the scan did), so
            # selective and non-selective are comparable across the A/B.
            scan_krps = (dataset_rows / (m["wall_ms"] / 1000.0)) / 1000.0 if m["wall_ms"] else 0.0
            m["scan_krows_s"] = scan_krps
            rows[key].append(m)
            print(f"  {key:<9}{dop:>4}{m['wall_ms']:>10.1f}{m['krows_s']:>10.0f}{m['cores']:>7.2f}"
                  f"{scan_krps:>11.0f}{m['gil_ms']:>9.1f}{m['tramp']:>7}"
                  f"{str(m['rg_read']):>7}{str(m['rg_pruned']):>7}  {m['purity']}")
    # cross-path correctness: identical survivor row count => predicate applied
    # exactly once (no double / dropped filter), pruning did not change the result.
    nat_rows = {r["rows"] for r in rows["native"]}
    tmp_rows = {r["rows"] for r in rows["tramp"]}
    rows["row_parity"] = (nat_rows == tmp_rows and len(nat_rows) == 1)
    rows["files_pruned_parity"] = (
        {r["files_pruned"] for r in rows["native"]} == {r["files_pruned"] for r in rows["tramp"]}
    )
    print(f"    row-parity(native==tramp): {'PASS' if rows['row_parity'] else 'FAIL'}  "
          f"(native rows={sorted(nat_rows)} tramp rows={sorted(tmp_rows)})  "
          f"files_pruned-parity: {'PASS' if rows['files_pruned_parity'] else 'FAIL'}")
    return rows


# ── concurrent-query scaling sweep (the scan-stage GIL money shot) ─────────────

def _run_query_once(sql: str) -> int:
    s = opteryx.session()
    r = 0
    for m in s.execute_to_morsels(sql):
        r += m.num_rows
    return r


def _concurrency_sweep(sql: str, dataset_rows: int, qs, per_query_dop: int, repeats: int) -> dict:
    """Run Q identical queries CONCURRENTLY (one per thread) and measure aggregate
    throughput + CPU-cores utilised, native vs forced-trampoline. This is the
    scan-stage GIL discriminator under load: on a GIL build the trampoline's
    per-morsel Python re-entry serialises across concurrent pull loops (aggregate
    cores plateau, throughput flattens); the native scan has no Python on the pull
    path so it scales with cores. Instrumentation is OFF here — the WP-INSTR GIL
    accumulators are single-query-only (module globals), so we rely on wall-clock /
    process-CPU, which are concurrency-safe."""
    import threading

    config.OPTERYX_INSTRUMENT_ENGINE = False
    config.MAX_EXECUTION_WORKERS = per_query_dop
    config.PARALLEL_MIN_ROWS = 0
    print(f"\n### CONCURRENCY  {sql}")
    print(f"    (per-query dop={per_query_dop}; aggregate over Q concurrent queries)")
    print(f"  {'path':<9}{'Q':>4}{'wall_ms':>10}{'agg_krows_s':>13}{'cores':>8}{'scale':>8}")
    out = {"sql": sql, "per_query_dop": per_query_dop, "native": [], "tramp": []}
    for force in (False, True):
        key = "tramp" if force else "native"
        base_krps = None
        orig = pool_reader.native_scan_supported
        if force:
            pool_reader.native_scan_supported = lambda *a, **k: False
        try:
            for q in qs:
                walls, cpus = [], []
                for r in range(repeats + 1):
                    gc.collect()
                    if r == 0:
                        _run_query_once(sql)  # warmup
                        continue
                    threads = [threading.Thread(target=_run_query_once, args=(sql,)) for _ in range(q)]
                    w0 = time.perf_counter_ns()
                    c0 = time.process_time_ns()
                    for t in threads:
                        t.start()
                    for t in threads:
                        t.join()
                    walls.append(time.perf_counter_ns() - w0)
                    cpus.append(time.process_time_ns() - c0)
                wall_ns = statistics.median(walls)
                cpu_ns = statistics.median(cpus)
                agg_krps = (dataset_rows * q / (wall_ns / 1e9)) / 1000.0
                if base_krps is None:
                    base_krps = agg_krps
                rec = {"q": q, "wall_ms": wall_ns / 1e6, "agg_krows_s": agg_krps,
                       "cores": cpu_ns / wall_ns, "scale": agg_krps / base_krps}
                out[key].append(rec)
                print(f"  {key:<9}{q:>4}{rec['wall_ms']:>10.1f}{agg_krps:>13.0f}"
                      f"{rec['cores']:>8.2f}{rec['scale']:>7.2f}x")
        finally:
            pool_reader.native_scan_supported = orig
    return out


def main(argv) -> int:
    ap = argparse.ArgumentParser(description="M1-VALIDATE scan-GIL benchmark")
    ap.add_argument("--rows", type=int, default=10_000_000, help="rows per dataset (>=10M for the deliverable)")
    ap.add_argument("--row-group-size", type=int, default=262_144)
    ap.add_argument("--files-multi", type=int, default=8, help="file count for the multi-file variant")
    ap.add_argument("--dops", default="1,2,4,8")
    ap.add_argument("--qs", default="1,2,4,8", help="concurrent-query counts for Part B")
    ap.add_argument("--conc-dop", type=int, default=2, help="per-query dop during the concurrency sweep")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--data-dir", default=os.path.join(_REPO, "tests", "performance", ".m1data"))
    ap.add_argument("--out", default=os.path.join(_REPO, "docs", "m1_validation_results.json"))
    ap.add_argument("--only", default="", help="substring filter on scenario names")
    args = ap.parse_args(argv)

    dops = [int(x) for x in args.dops.split(",") if x.strip()]
    args.qs = [int(x) for x in args.qs.split(",") if x.strip()]
    os.chdir(_REPO)  # so relative dataset paths in SQL resolve

    gil_on = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    regime = "GIL-ON (emulates deployed 3.14 service)" if gil_on else "GIL-OFF (free-threaded)"
    print("=" * 96)
    print(f"M1-VALIDATE  |  {regime}  |  cpus={os.cpu_count()}  |  rows={args.rows:,}  "
          f"rgs={args.row_group_size:,}  dops={dops}  repeats={args.repeats}")
    print("=" * 96)

    # generate / cache datasets
    d = args.data_dir
    print("generating datasets (cached)…", flush=True)
    _gen_strings(os.path.join(d, "strings_1f"), args.rows, 1, args.row_group_size)
    _gen_strings(os.path.join(d, "strings_mf"), args.rows, args.files_multi, args.row_group_size)
    _gen_dectime(os.path.join(d, "dectime_1f"), args.rows, 1, args.row_group_size)
    print("datasets ready.", flush=True)

    S1 = "'%s'" % os.path.join(d, "strings_1f")
    SM = "'%s'" % os.path.join(d, "strings_mf")
    DT = "'%s'" % os.path.join(d, "dectime_1f")
    # selective id threshold: keep ~1% -> prunes ~99% of sorted row groups.
    sel = max(1, args.rows // 100)

    scenarios = [
        # ── wide STRING, single file ──
        ("str/1f  no-predicate (scan+project)",        f"SELECT url, title, referer, user_agent FROM {S1}"),
        ("str/1f  selective (id<1%, prunes rgs)",      f"SELECT url, title FROM {S1} WHERE id < {sel}"),
        ("str/1f  non-selective (n>=0, keeps all)",    f"SELECT url, title FROM {S1} WHERE n >= 0"),
        ("str/1f  role-3 filter-only (id not projd)",  f"SELECT title FROM {S1} WHERE id < {sel}"),
        ("str/1f  string predicate (referer=)",        f"SELECT url FROM {S1} WHERE referer = 'http://ref7.example/'"),
        # ── wide STRING, multi file ──
        ("str/mf  no-predicate (scan+project)",        f"SELECT url, title, referer, user_agent FROM {SM}"),
        ("str/mf  selective (id<1%)",                  f"SELECT url, title FROM {SM} WHERE id < {sel}"),
        # ── DECIMAL + TIMESTAMP ──
        ("dt/1f   no-predicate (scan+project)",        f"SELECT price, amount, event_ts, ev_date FROM {DT}"),
        ("dt/1f   selective (id<1%, prunes rgs)",      f"SELECT price, event_ts FROM {DT} WHERE id < {sel}"),
        ("dt/1f   non-selective (n>=0)",               f"SELECT price, event_ts FROM {DT} WHERE n >= 0"),
        ("dt/1f   role-3 filter-only (id not projd)",  f"SELECT event_ts FROM {DT} WHERE id < {sel}"),
    ]

    print("\n" + "=" * 96)
    print("PART A — per-query dop sweep (instrumentation ON): trampoline_calls / gil_held_ns / purity / pruning")
    print("=" * 96)
    results = []
    for name, sql in scenarios:
        if args.only and args.only not in name:
            continue
        results.append(_run_scenario(name, sql, args.rows, dops, args.repeats))

    print("\n" + "=" * 96)
    print("PART B — concurrent-query scaling sweep (instrumentation OFF): the scan-stage GIL discriminator")
    print("=" * 96)
    conc = []
    if not args.only or "conc" in args.only:
        conc_scenarios = [
            ("str/1f no-predicate", f"SELECT url, title, referer, user_agent FROM {S1}"),
            ("str/1f string-predicate", f"SELECT url FROM {S1} WHERE referer = 'http://ref7.example/'"),
            ("dt/1f  no-predicate", f"SELECT price, amount, event_ts, ev_date FROM {DT}"),
        ]
        for cname, csql in conc_scenarios:
            r = _concurrency_sweep(csql, args.rows, args.qs, args.conc_dop, args.repeats)
            r["name"] = cname
            conc.append(r)

    payload = {
        "regime": regime, "gil_on": gil_on, "cpus": os.cpu_count(),
        "rows": args.rows, "row_group_size": args.row_group_size, "dops": dops,
        "repeats": args.repeats, "scenarios": results, "concurrency": conc,
        "conc_qs": args.qs, "conc_dop": args.conc_dop,
    }
    tag = "gilon" if gil_on else "giloff"
    out = args.out.replace(".json", f".{tag}.json")
    with open(out, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
