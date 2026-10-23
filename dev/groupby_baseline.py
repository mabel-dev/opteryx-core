#!/usr/bin/env python3
"""GROUP BY programme baseline — record and compare.

Records, in one JSON file per run under tests/performance/groupby_baseline/:

  * ClickBench GROUP BY targets (process per query, cold run kept separately,
    N hot runs): wall min/median, GROUP BY sink telemetry (hash/probe/apply CPU),
    finalize profile (merge/keys/lanes CPU), per-pipeline wall/CPU, peak RSS,
    result row count + content hash (a change there is a correctness bug, not a
    perf result).
  * CONTROLS: queries that never enter GroupBySink (scan / filter / LIKE page
    search / top-N / string sort). The redesign cannot move these; if they move
    between two runs by the same factor as the targets, the MACHINE moved.
  * MACHINE PROBES: engine-free CPU / memory work (Python sort, blake2b stream,
    page-touch) — a second control that does not even load the engine.
  * Environment: build identity (.so sha256), HEAD, host, power/thermal state,
    load average, preload library.
  * Optionally (--suites) the TPC-H SF10 and H2O groupby runners' own output,
    saved verbatim next to the JSON.

Compare two runs:   python dev/groupby_baseline.py --compare A.json B.json
  prints B/A per query, the control band, and flags targets whose movement is
  outside what the controls moved.

Design rules honoured: A/B is still the instrument for a change — this file
records the state of the tree on a day so a later A/B has a reference and so a
day-to-day drift can be told apart from the needle moving.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "tests" / "performance" / "groupby_baseline"

# ClickBench runner numbering (0-based, == STATEMENTS index).
TARGETS = [8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 27, 28, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41]
CONTROLS = [0, 1, 20, 23, 25]          # no GroupBySink on the path
DATASETS = {
    "rugo": "scratch.hits_rugo_262k",
    "canon": "scratch.hits",
}
CANON_SUBSET = [13, 15, 16, 17, 18, 19, 32, 33, 34, 35]   # the shapes the 2026-10-01 PoC regressed on
SAMPLE_QUERIES = [32, 33, 16]


def _preload_env() -> dict:
    env = dict(os.environ)
    try:
        import draken  # noqa: WPS433
        p = draken.preload_library_path() or ""
    except Exception:  # pragma: no cover - probe only
        p = ""
    if p and sys.platform != "darwin":
        env["LD_PRELOAD"] = p
        env["MIMALLOC_PURGE_DELAY"] = "1000"
    elif p:
        env["DYLD_INSERT_LIBRARIES"] = p
        env["MIMALLOC_PURGE_DELAY"] = "1000"
    env["OPTERYX_GB_FINALIZE_PROF"] = "1"
    env.pop("OPTERYX_DEBUG", None)
    return env


def _load_statements():
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))   # the runner imports opteryx from the tree
    spec = importlib.util.spec_from_file_location(
        "cb", REPO / "tests/performance/clickbench/opteryx/runner.py")
    cb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cb)
    return cb.STATEMENTS


# ---------------------------------------------------------------- child -----
def child(q: int, rel: str, runs: int) -> None:
    sys.path.insert(0, str(REPO))
    os.chdir(REPO)
    import gc
    import opteryx
    from opteryx.operators._operators import get_groupby_telemetry, reset_groupby_telemetry

    st = _load_statements()[q][0].replace("{DATASET}", rel)
    walls = []
    rows = 0
    digest = ""
    out_rows = []
    out_names = []
    tel = None
    gb = None
    for i in range(runs + 1):
        gc.collect()
        reset_groupby_telemetry()
        s = opteryx.session()
        t = time.perf_counter()
        out = []
        for m in s.execute_to_morsels(st):
            if i == 0:
                m.materialize()
                names = list(m.column_names)
                out_names = [n.decode() if isinstance(n, bytes) else n for n in names]
                cols = [list(m.column(n)) for n in names]
                for r in range(m.num_rows):
                    out.append(repr([c[r] for c in cols]))   # the digest's input — keep stable
                    out_rows.append([repr(c[r]) for c in cols])
        walls.append((time.perf_counter() - t) * 1000.0)
        if i == 0:
            rows = len(out)
            digest = hashlib.sha256("\n".join(sorted(out)).encode()).hexdigest()[:16]
        if i == runs:
            tel = s.telemetry
            gb = get_groupby_telemetry()
        s.close()
    pipes = {p["label"]: {"wall_ms": p["wall_time"] / 1e6, "cpu_ms": p["cpu_time"] / 1e6}
             for p in tel.get("native_pipeline_stats", [])}
    rec = {
        "cold_ms": walls[0],
        "hot_ms": walls[1:],
        "rows": rows,
        "digest": digest,
        "names": out_names,
        "result": out_rows,
        "gb": {k: (v * 1000.0 if isinstance(v, float) else v) for k, v in gb.items() if v},
        "pipelines": pipes,
        "time_planning_ms": tel.get("time_planning", 0) * 1000.0,
        "time_executing_ms": tel.get("time_executing", 0) * 1000.0,
        "ru_maxrss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1 << 20),
    }
    sys.stdout.write("@@JSON@@" + json.dumps(rec) + "\n")


def run_child(q: int, rel: str, runs: int, env: dict) -> dict:
    proc = subprocess.run(
        [sys.executable, __file__, "--child", str(q), rel, str(runs)],
        capture_output=True, text=True, env=env, cwd=REPO, timeout=600,
    )
    rec = None
    for line in proc.stdout.splitlines():
        if line.startswith("@@JSON@@"):
            rec = json.loads(line[8:])
    if rec is None:
        raise RuntimeError(f"Q{q:02d} {rel} child failed rc={proc.returncode}\n{proc.stderr[-2000:]}")
    # last finalize-prof line of the LAST (telemetry) run
    prof = [l for l in proc.stderr.splitlines() if l.startswith("[gb-finalize-prof]")]
    if prof:
        parts = prof[-1].split()
        rec["finalize_ms"] = {
            kv.split("=")[0]: float(kv.split("=")[1].rstrip("ms"))
            for kv in parts[1:] if "=" in kv and kv.endswith("ms")
        }
        rec["finalize_threads"] = int(parts[1].split("=")[1]) if parts[1].startswith("threads=") else None
    return rec


# ------------------------------------------------------------- probes -------
def machine_probes(reps: int = 5) -> dict:
    import random
    out = {}
    rnd = random.Random(1234)
    data = [rnd.getrandbits(62) for _ in range(5_000_000)]
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); sorted(data); ts.append((time.perf_counter() - t) * 1000)
    out["py_sort_5m_ms"] = min(ts)
    blob = bytes(range(256)) * (1 << 20)   # 256 MB
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); hashlib.blake2b(blob).hexdigest(); ts.append((time.perf_counter() - t) * 1000)
    out["blake2b_256mb_ms"] = min(ts)
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); b = bytearray(1 << 29); b[::4096] = b"\x01" * (len(b) // 4096); del b
        ts.append((time.perf_counter() - t) * 1000)
    out["page_touch_512mb_ms"] = min(ts)
    return out


def environment() -> dict:
    def sha(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()[:16] if p.exists() else "missing"
    # A benchmark tree made by dev/arch_tree.sh carries no .git — the .so hashes
    # below are its build identity then.
    head_path = REPO / ".git/HEAD"
    head = head_path.read_text().strip() if head_path.exists() else "no-git (benchmark tree)"
    ref = head.split(" ", 1)[1] if head.startswith("ref:") else None
    commit = None
    if ref and (REPO / ".git" / ref).exists():
        commit = (REPO / ".git" / ref).read_text().strip()
    so = {}
    for pat in ("opteryx/operators/_operators*.so", "opteryx/connectors/parquet_io/pool_reader*.so",
                "draken/draken_native*.so", "rugo/rugo_native*.so", "opteryx/compiled/thread_pool*.so"):
        for p in REPO.glob(pat):
            so[p.name] = {"sha256_16": sha(p), "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.stat().st_mtime))}
    def sh(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:
            return ""
    env = {
        "host": platform.node(), "machine": platform.machine(), "system": platform.platform(),
        "python": sys.version.split()[0], "head": commit or head, "so": so,
        "loadavg": os.getloadavg(), "cpu_count": os.cpu_count(),
    }
    if sys.platform == "darwin":
        env["cpu"] = sh(["sysctl", "-n", "machdep.cpu.brand_string"])
        env["mem_bytes"] = sh(["sysctl", "-n", "hw.memsize"])
        env["power"] = sh(["pmset", "-g", "batt"]).splitlines()[:2]
        env["thermal"] = sh(["pmset", "-g", "therm"]).splitlines()[:3]
    try:
        import opteryx
        env["opteryx_version"] = opteryx.__version__
    except Exception:
        pass
    return env


def sample_profile(q: int, rel: str, seconds: int, env: dict, out_dir: Path, label: str) -> str | None:
    if sys.platform != "darwin":
        return None
    loop = (
        "import os,sys,time,importlib.util\n"
        f"sys.path.insert(0,{str(REPO)!r}); os.chdir({str(REPO)!r})\n"
        "import opteryx\n"
        f"spec=importlib.util.spec_from_file_location('cb',{str(REPO / 'tests/performance/clickbench/opteryx/runner.py')!r})\n"
        "cb=importlib.util.module_from_spec(spec); spec.loader.exec_module(cb)\n"
        f"st=cb.STATEMENTS[{q}][0].replace('{{DATASET}}',{rel!r})\n"
        "for _ in range(2):\n    [None for _m in opteryx.session().execute_to_morsels(st)]\n"
        "print('READY',flush=True)\n"
        f"t0=time.time()\nwhile time.time()-t0<{seconds + 4}:\n    [None for _m in opteryx.session().execute_to_morsels(st)]\n"
    )
    p = subprocess.Popen([sys.executable, "-c", loop], stdout=subprocess.PIPE, text=True, env=env, cwd=REPO)
    for line in p.stdout:
        if line.startswith("READY"):
            break
    path = out_dir / f"{label}_sample_Q{q:02d}.txt"
    subprocess.run(["sample", str(p.pid), str(seconds), "-f", str(path)], capture_output=True, timeout=seconds + 60)
    p.wait(timeout=seconds + 60)
    # keep only the top-of-stack summary (the tree is 100k+ lines)
    txt = path.read_text()
    i = txt.find("Sort by top of stack")
    path.write_text(txt[i:] if i >= 0 else txt)
    return str(path.relative_to(REPO))


def run_suite(cmd: list[str], env: dict, out_path: Path) -> None:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=REPO, timeout=3600)
    out_path.write_text(proc.stdout + "\n--- stderr ---\n" + proc.stderr)


# ------------------------------------------------------------- record -------
def record(args) -> Path:
    env = _preload_env()
    label = time.strftime("%Y%m%d-%H%M%S") + (f"_{args.label}" if args.label else "")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {"label": label, "runs": args.runs, "env": environment(), "queries": {}, "probes": {}}
    print(f"baseline {label}: env recorded; probes...", flush=True)
    out["probes"]["before"] = machine_probes()
    stmts = _load_statements()
    plan = [("rugo", q, "target") for q in TARGETS] + [("rugo", q, "control") for q in CONTROLS]
    if not args.no_canon:
        plan += [("canon", q, "target") for q in CANON_SUBSET] + [("canon", q, "control") for q in (0, 20)]
    for ds, q, role in plan:
        rel = DATASETS[ds]
        key = f"{ds}:Q{q:02d}"
        rec = run_child(q, rel, args.runs, env)
        rec["role"] = role
        rec["sql"] = stmts[q][0]
        out["queries"][key] = rec
        hot = rec["hot_ms"]
        gb = rec.get("gb", {})
        fin = rec.get("finalize_ms", {})
        print(f"  {key:10} {role:7} min {min(hot):8.1f} med {statistics.median(hot):8.1f} cold {rec['cold_ms']:8.1f}"
              f"  probe {gb.get('probe_s', 0):7.0f} apply {gb.get('apply_s', 0):6.0f} merge {fin.get('merge', 0):7.0f}"
              f"  rows {rec['rows']} {rec['digest']}", flush=True)
    out["probes"]["after"] = machine_probes()
    out["env"]["loadavg_after"] = os.getloadavg()
    if args.samples:
        out["samples"] = {}
        for q in SAMPLE_QUERIES:
            out["samples"][f"rugo:Q{q:02d}"] = sample_profile(q, DATASETS["rugo"], 8, env, OUT_DIR, label)
            print(f"  sampled Q{q:02d}", flush=True)
    if args.suites:
        print("  running TPC-H SF10 + H2O groupby runners...", flush=True)
        run_suite([sys.executable, "tests/performance/tpch/runner.py", "--scale", "10", "--variant", "skene",
                   "--iterations", "3"], env, OUT_DIR / f"{label}_tpch_sf10.txt")
        run_suite([sys.executable, "tests/performance/h2o/runner.py", "--variant", "skene", "--size", "medium",
                   "--workload", "groupby", "--iterations", "2"], env, OUT_DIR / f"{label}_h2o_groupby.txt")
    path = OUT_DIR / f"{label}.json"
    path.write_text(json.dumps(out, indent=1, default=str))
    write_summary(out, OUT_DIR / f"{label}.md")
    print(f"written {path.relative_to(REPO)}")
    return path


def write_summary(out: dict, path: Path) -> None:
    e = out["env"]
    lines = [f"# GROUP BY baseline {out['label']}", "",
             f"host {e.get('host')} {e.get('cpu', e.get('machine'))} · python {e.get('python')} · opteryx {e.get('opteryx_version')} · HEAD {str(e.get('head'))[:12]}",
             f"load {e.get('loadavg')} → {e.get('loadavg_after')} · power {e.get('power')}",
             "", "| query | role | min ms | med ms | cold ms | hash | probe | apply | merge | keys | rows | digest |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for k, r in out["queries"].items():
        hot = r["hot_ms"]; gb = r.get("gb", {}); fin = r.get("finalize_ms", {})
        lines.append(f"| {k} | {r['role']} | {min(hot):.1f} | {statistics.median(hot):.1f} | {r['cold_ms']:.1f} | "
                     f"{gb.get('hash_s', 0):.0f} | {gb.get('probe_s', 0):.0f} | {gb.get('apply_s', 0):.0f} | "
                     f"{fin.get('merge', 0):.0f} | {fin.get('keys', 0):.0f} | {r['rows']} | {r['digest']} |")
    lines += ["", "hash/probe/apply/merge/keys = CPU ms summed over threads, last hot run.", "",
              "machine probes (min ms): " + json.dumps(out["probes"]), ""]
    if out.get("samples"):
        lines += ["samples: " + ", ".join(f"{k} → {v}" for k, v in out["samples"].items()), ""]
    path.write_text("\n".join(lines))


# ------------------------------------------------------------ compare -------
def _order_key(sql: str):
    """The leading ORDER BY expression's output name, or None when there is none."""
    m = re.search(r"ORDER BY\s+([^\s,]+)", sql)
    return m.group(1) if m else None


def classify_result(ra: dict, rb: dict) -> str:
    """same | ties | any_rows | changed | unverifiable.

    A LIMIT over tied ORDER BY values may legitimately return different tied rows on
    different runs (SQL leaves it unspecified) — the ORDER BY values themselves must
    still match. A LIMIT with no ORDER BY may return any rows."""
    if (ra["rows"], ra["digest"]) == (rb["rows"], rb["digest"]):
        return "same"
    if "result" not in ra or "result" not in rb:
        return "unverifiable"
    if ra["rows"] != rb["rows"]:
        return "changed"
    key = _order_key(ra["sql"])
    if key is None:
        return "any_rows" if "LIMIT" in ra["sql"].upper() else "changed"
    if key not in ra["names"] or key not in rb["names"]:
        return "changed"
    ia, ib = ra["names"].index(key), rb["names"].index(key)
    return "ties" if [r[ia] for r in ra["result"]] == [r[ib] for r in rb["result"]] else "changed"


def compare(a_path: str, b_path: str, tol: float) -> int:
    A = json.loads(Path(a_path).read_text()); B = json.loads(Path(b_path).read_text())
    def med(r): return statistics.median(r["hot_ms"])
    ctrl = []
    rows = []
    for k in A["queries"]:
        if k not in B["queries"]:
            continue
        ra, rb = A["queries"][k], B["queries"][k]
        ratio_min = min(rb["hot_ms"]) / min(ra["hot_ms"])
        ratio_med = med(rb) / med(ra)
        rows.append((k, ra["role"], ratio_min, ratio_med, classify_result(ra, rb)))
        if ra["role"] == "control":
            ctrl.append(ratio_med)
    pa, pb = A["probes"], B["probes"]
    probe_ratio = {k: pb["before"][k] / pa["before"][k] for k in pa["before"]}
    cmed = statistics.median(ctrl) if ctrl else 1.0
    cspread = (max(ctrl) - min(ctrl)) if ctrl else 0.0
    print(f"A {A['label']}  B {B['label']}")
    print(f"controls (engine, no GROUP BY): median B/A {cmed:.3f}, spread {cspread:.3f}  "
          f"machine probes B/A {json.dumps({k: round(v, 3) for k, v in probe_ratio.items()})}")
    band = max(tol, cspread)
    print(f"a target is called MOVED when |ratio - control median| > {band:.3f}\n")
    print(f"{'query':12} {'role':8} {'B/A min':>8} {'B/A med':>8}  verdict")
    moved = 0
    for k, role, rmin, rmed, result in rows:
        verdict = ""
        if result == "changed":
            verdict = "RESULT CHANGED — correctness, not perf"
        elif result == "unverifiable":
            verdict = "result differs; A has no stored rows — re-record A to classify"
        elif role == "target":
            d = rmed - cmed
            if abs(d) > band:
                verdict = "MOVED " + ("faster" if d < 0 else "SLOWER"); moved += 1
            else:
                verdict = "within control band"
        note = {"ties": "  [ties reordered: ORDER BY values identical]",
                "any_rows": "  [LIMIT without ORDER BY: any rows are valid]"}.get(result, "")
        print(f"{k:12} {role:8} {rmin:8.3f} {rmed:8.3f}  {verdict}{note}")
    print(f"\n{moved} targets moved beyond the control band.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--child", nargs=3, metavar=("Q", "REL", "RUNS"), help=argparse.SUPPRESS)
    ap.add_argument("--runs", type=int, default=7, help="hot runs per query (cold run recorded separately)")
    ap.add_argument("--label", default="", help="suffix for the output file name")
    ap.add_argument("--no-canon", action="store_true", help="skip the canon ClickBench subset")
    ap.add_argument("--samples", action="store_true", help="also `sample` Q32/Q33/Q16 hot loops (macOS)")
    ap.add_argument("--suites", action="store_true", help="also run TPC-H SF10 and H2O groupby runners")
    ap.add_argument("--compare", nargs=2, metavar=("A.json", "B.json"))
    ap.add_argument("--tol", type=float, default=0.05, help="minimum band for --compare")
    args = ap.parse_args()
    if args.child:
        child(int(args.child[0]), args.child[1], int(args.child[2]))
        return 0
    if args.compare:
        return compare(args.compare[0], args.compare[1], args.tol)
    record(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
