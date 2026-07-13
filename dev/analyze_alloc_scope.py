#!/usr/bin/env python3
"""DIAGNOSTIC-ONLY, TEMPORARY: offline analysis for the allocator-scope
measurement pass (see draken/core/alloc_trace.h).

Reads the per-thread binary trace files written by an OPTERYX_ALLOC_TRACE=1
run, matches alloc/free events by pointer, symbolicates each allocation's
call site (via addr2line/atos against the .so it came from), and reports
per-call-site peak concurrency, size/lifetime distributions and churn rate —
the data needed to judge whether allocator buffers are short-lived and
tightly scoped enough for an arena, and which kernels would dominate one.

This does NOT attribute allocations to a query-plan operator or morsel —
that data was not collected (see the plan this implements for why). It
attributes to a call site (file:line/function), which is a proxy for
"kernel that owns this allocation pattern".

Usage:
    python dev/analyze_alloc_scope.py [--dir DIR] [--out OUT.json] [--top N]
"""
import argparse
import glob
import json
import os
import shutil
import struct
import subprocess
import sys
from collections import defaultdict

EVENT_FMT = "=QQQIB3x"  # ts_ns, ptr, retaddr, size, kind, pad -> 32 bytes
EVENT_SIZE = struct.calcsize(EVENT_FMT)
assert EVENT_SIZE == 32, f"Event layout mismatch: expected 32 bytes, got {EVENT_SIZE}"

KIND_FREE = 0
KIND_ALLOC = 1


def parse_trace_file(path):
    """Yields (header_dict, list_of_event_dicts) for one thread's trace file."""
    with open(path, "rb") as f:
        header = {}
        while True:
            line = f.readline()
            if not line:
                raise ValueError(f"{path}: truncated before ---BINARY--- marker")
            text = line.decode("ascii", errors="replace").rstrip("\n")
            if text == "---BINARY---":
                break
            if "=" in text:
                k, _, v = text.partition("=")
                header[k] = v
        body = f.read()

    count = int(header.get("count", "0"))
    expected = count * EVENT_SIZE
    if len(body) < expected:
        print(
            f"WARNING: {path}: header says count={count} but only "
            f"{len(body) // EVENT_SIZE} events present (short file) — using what's there",
            file=sys.stderr,
        )
        count = len(body) // EVENT_SIZE

    events = []
    for i in range(count):
        ts_ns, ptr, retaddr, size, kind = struct.unpack_from(
            EVENT_FMT, body, i * EVENT_SIZE
        )[:5]
        events.append(
            {"ts_ns": ts_ns, "ptr": ptr, "retaddr": retaddr, "size": size, "kind": kind}
        )
    return header, events


def load_all(trace_dir):
    files = sorted(glob.glob(os.path.join(trace_dir, "alloc_trace.*.bin")))
    if not files:
        raise SystemExit(
            f"No alloc_trace.*.bin files found under {trace_dir!r}. "
            "Run with OPTERYX_ENABLE_ALLOC_TRACE=1 build + OPTERYX_ALLOC_TRACE=1 env first."
        )
    all_events = []
    truncated_files = []
    modules = {}  # so_path -> so_base (first seen; should be stable per process run)
    for path in files:
        header, events = parse_trace_file(path)
        so_path = header.get("so_path", "")
        so_base = int(header.get("so_base", "0x0"), 16)
        if so_path and so_path not in modules:
            modules[so_path] = so_base
        for e in events:
            e["so_path"] = so_path
            e["so_base"] = so_base
        all_events.extend(events)
        if header.get("truncated") == "1":
            truncated_files.append(path)
    return all_events, files, truncated_files, modules


def symbolicate(modules_to_addrs):
    """modules_to_addrs: {(so_path, so_base): set(retaddr)} -> {(so_path, retaddr): label}"""
    result = {}
    is_mac = sys.platform == "darwin"
    tool = "atos" if is_mac else "addr2line"
    if not shutil.which(tool):
        print(
            f"WARNING: {tool!r} not found on PATH — call sites will be reported as raw "
            "hex addresses, not file:line/function. Install binutils (addr2line) or use "
            "Xcode command line tools (atos).",
            file=sys.stderr,
        )
        for (so_path, _base), addrs in modules_to_addrs.items():
            for a in addrs:
                result[(so_path, a)] = f"{so_path}+0x{a:x}"
        return result

    for (so_path, so_base), addrs in modules_to_addrs.items():
        addr_list = sorted(addrs)
        if not so_path or not os.path.exists(so_path):
            for a in addr_list:
                result[(so_path, a)] = f"<unknown module>+0x{a:x}"
            continue
        try:
            if is_mac:
                labels = _symbolicate_atos(so_path, so_base, addr_list)
            else:
                labels = _symbolicate_addr2line(so_path, so_base, addr_list)
        except (subprocess.SubprocessError, OSError) as exc:
            print(f"WARNING: symbolication failed for {so_path}: {exc}", file=sys.stderr)
            labels = [f"{so_path}+0x{a:x}" for a in addr_list]
        for a, label in zip(addr_list, labels):
            result[(so_path, a)] = label
    return result


def _symbolicate_addr2line(so_path, so_base, addr_list):
    # addr2line resolves against file-relative offsets for PIE/PIC shared objects.
    offsets = [f"0x{(a - so_base):x}" for a in addr_list]
    out = subprocess.run(
        ["addr2line", "-f", "-C", "-e", so_path] + offsets,
        capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    labels = []
    for i in range(0, len(out), 2):
        func = out[i] if i < len(out) else "??"
        loc = out[i + 1] if i + 1 < len(out) else "??:?"
        labels.append(f"{func} ({loc})")
    return labels


def _symbolicate_atos(so_path, so_base, addr_list):
    hex_addrs = [f"0x{a:x}" for a in addr_list]
    out = subprocess.run(
        ["atos", "-o", so_path, "-l", f"0x{so_base:x}"] + hex_addrs,
        capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    return out if len(out) == len(addr_list) else [f"{so_path}+0x{a:x}" for a in addr_list]


def log_bucket(n):
    if n <= 0:
        return "0"
    b = 1
    while b * 2 <= n:
        b *= 2
    return f"{b}-{b * 2 - 1}"


def analyze(all_events):
    all_events.sort(key=lambda e: (e["ts_ns"], 0 if e["kind"] == KIND_ALLOC else 1))

    live = {}  # ptr -> alloc event
    matched = []  # (alloc_event, free_ts_ns)
    unmatched_frees = 0

    for e in all_events:
        if e["kind"] == KIND_ALLOC:
            if e["ptr"] in live:
                print(
                    f"WARNING: duplicate ALLOC for ptr=0x{e['ptr']:x} without an intervening "
                    "FREE seen in this trace (instrumentation gap or overlapping capture window)",
                    file=sys.stderr,
                )
            live[e["ptr"]] = e
        else:
            a = live.pop(e["ptr"], None)
            if a is None:
                unmatched_frees += 1
                continue
            matched.append((a, e["ts_ns"]))

    still_live = list(live.values())

    # Symbolicate every distinct (so_path, retaddr) that appears in a matched or still-live alloc.
    modules_to_addrs = defaultdict(set)
    for a, _ in matched:
        modules_to_addrs[(a["so_path"], a["so_base"])].add(a["retaddr"])
    for a in still_live:
        modules_to_addrs[(a["so_path"], a["so_base"])].add(a["retaddr"])
    labels = symbolicate(modules_to_addrs)

    def site_of(ev):
        return labels.get((ev["so_path"], ev["retaddr"]), f"0x{ev['retaddr']:x}")

    # Per-call-site aggregates.
    per_site = defaultdict(lambda: {
        "count": 0, "bytes": 0, "size_hist": defaultdict(int),
        "lifetime_hist": defaultdict(int), "lifetimes_ns": [],
    })
    for a, free_ts in matched:
        site = site_of(a)
        s = per_site[site]
        s["count"] += 1
        s["bytes"] += a["size"]
        s["size_hist"][log_bucket(a["size"])] += 1
        lifetime = free_ts - a["ts_ns"]
        s["lifetime_hist"][log_bucket(lifetime)] += 1
        s["lifetimes_ns"].append(lifetime)

    # Sweep-line for peak concurrent live bytes/count, globally and per call-site.
    timeline = []
    for a, free_ts in matched:
        site = site_of(a)
        timeline.append((a["ts_ns"], a["size"], site))
        timeline.append((free_ts, -a["size"], site))
    timeline.sort(key=lambda t: (t[0], 0 if t[1] > 0 else 1))

    global_live_bytes = global_live_count = 0
    global_peak_bytes = global_peak_count = 0
    site_live = defaultdict(int)
    site_peak = defaultdict(int)
    for _ts, delta, site in timeline:
        global_live_bytes += delta
        global_live_count += 1 if delta > 0 else -1
        global_peak_bytes = max(global_peak_bytes, global_live_bytes)
        global_peak_count = max(global_peak_count, global_live_count)
        site_live[site] += delta
        site_peak[site] = max(site_peak[site], site_live[site])

    if all_events:
        window_ns = max(e["ts_ns"] for e in all_events) - min(e["ts_ns"] for e in all_events)
    else:
        window_ns = 0
    window_s = window_ns / 1e9 if window_ns > 0 else float("nan")

    still_live_bytes = sum(a["size"] for a in still_live)

    return {
        "total_matched": len(matched),
        "unmatched_frees": unmatched_frees,
        "still_live_count": len(still_live),
        "still_live_bytes": still_live_bytes,
        "window_seconds": window_s,
        "global_peak_bytes": global_peak_bytes,
        "global_peak_count": global_peak_count,
        "per_site": {
            site: {
                "count": s["count"],
                "bytes": s["bytes"],
                "churn_per_sec": s["count"] / window_s if window_s > 0 else float("nan"),
                "peak_live_bytes": site_peak.get(site, 0),
                "size_hist": dict(s["size_hist"]),
                "lifetime_hist": dict(s["lifetime_hist"]),
                "median_lifetime_ns": sorted(s["lifetimes_ns"])[len(s["lifetimes_ns"]) // 2]
                if s["lifetimes_ns"] else None,
            }
            for site, s in per_site.items()
        },
    }


def print_report(result, truncated_files, top_n):
    if truncated_files:
        print(
            f"*** {len(truncated_files)} trace file(s) hit their capacity and were "
            "TRUNCATED — this run's totals are a lower bound, not complete. ***",
            file=sys.stderr,
        )
        for f in truncated_files:
            print(f"  truncated: {f}", file=sys.stderr)

    print(f"Observation window: {result['window_seconds']:.3f}s")
    print(f"Matched alloc/free pairs: {result['total_matched']}")
    print(f"Unmatched frees (alloc predates capture): {result['unmatched_frees']}")
    print(
        f"Still live at exit: {result['still_live_count']} allocations, "
        f"{result['still_live_bytes']} bytes "
        "(verify this matches the retained result set — if not, treat as a bug, not noise)"
    )
    print(f"Global peak concurrent live bytes: {result['global_peak_bytes']}")
    print(f"Global peak concurrent live count: {result['global_peak_count']}")
    print()

    ranked = sorted(result["per_site"].items(), key=lambda kv: kv[1]["bytes"], reverse=True)
    print(f"Top {top_n} call sites by total allocated bytes:")
    print(f"{'bytes':>14} {'count':>10} {'churn/s':>10} {'peak_live':>12} {'median_ns':>12}  site")
    for site, s in ranked[:top_n]:
        median = s["median_lifetime_ns"]
        median_str = f"{median}" if median is not None else "?"
        print(
            f"{s['bytes']:>14} {s['count']:>10} {s['churn_per_sec']:>10.1f} "
            f"{s['peak_live_bytes']:>12} {median_str:>12}  {site}"
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir", default=os.environ.get("OPTERYX_ALLOC_TRACE_DIR", "/tmp/opteryx_alloc_trace"),
        help="Directory containing alloc_trace.*.bin files",
    )
    ap.add_argument("--out", default="dev/alloc_scope_summary.json", help="JSON summary output path")
    ap.add_argument("--top", type=int, default=30, help="Number of call sites to print, ranked by bytes")
    args = ap.parse_args()

    all_events, files, truncated_files, _modules = load_all(args.dir)
    print(f"Loaded {len(all_events)} events from {len(files)} trace file(s) in {args.dir}")

    result = analyze(all_events)
    print_report(result, truncated_files, args.top)

    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
