"""Leak gate for the string-arena ownership channel.

Runs each probe query under the draken allocator trace and asserts that nothing
but the KNOWN, by-design globals is left outstanding. The globals are the shared
identity/zero selection arrays (CLAUDE.md §11: `selection` is a NON-OWNING
pointer into a process-wide array that is never draken_free'd), so they are
matched by allocation SITE, not by count.

Every probe pairs a shape whose string results all fit in a 12-byte inline slot
(arena_len == 0) with the same shape forced long. That pairing is the point: the
all-inline arm is what dropped a live arena on the floor, and it is invisible to
any correctness test because the RESULT is right — only the memory is lost.

Each probe runs in its OWN subprocess, which the gate respawns with
OPTERYX_FREE_TRACE set: draken resolves that variable exactly once per process
(draken/core/alloc.h), so it cannot be turned on from inside a running one.

Usage:  python3.14 dev/check_string_arena_leaks.py
Exits non-zero, naming each probe, when anything outside BY_DESIGN survives.
"""
import collections
import os
import re
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MALLOC = re.compile(r"^DRAKEN_(?:ALIGNED_)?MALLOC TRACE: ptr=(0x[0-9a-f]+) req=(\d+)")
FREE = re.compile(r"^DRAKEN_FREE TRACE: ptr=(0x[0-9a-f]+)")
FRAME0 = re.compile(r"^0\s+\S+\s+0x[0-9a-f]+\s+(\S+)")

# Allocation sites that are outstanding at exit BY DESIGN — process-wide shared
# buffers, not per-result ownership. Matched on the innermost frame's symbol.
BY_DESIGN = ("draken_identity_sel", "draken_zero_sel")

PROBES = [
    # (label, all-inline query, forced-long twin)
    ("concat",
     "SELECT LEFT(name, 2) || 'x' FROM testdata.satellites",
     "SELECT name || '----------------------------------' FROM testdata.satellites"),
    ("concat_chain",
     "SELECT LEFT(name, 2) || 'x' || 'y' || 'z' FROM testdata.satellites",
     "SELECT name || name || '----------------------------------' FROM testdata.satellites"),
    ("format_timestamp",
     "SELECT FORMAT_TIMESTAMP('%Y', Lauched_at) FROM testdata.missions",
     "SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', Lauched_at) FROM testdata.missions"),
    ("format_date",
     "SELECT FORMAT_DATE('%Y', birth_date) FROM testdata.astronauts",
     "SELECT FORMAT_DATE('%Y-%m-%d %H:%M:%S', birth_date) FROM testdata.astronauts"),
]


def run_query(sql):
    """Execute `sql` in a fresh traced process; return its stderr trace."""
    env = dict(os.environ, OPTERYX_FREE_TRACE="1", OPTERYX_FREE_TRACE_MIN="1")
    proc = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--run", sql],
        capture_output=True, text=True, env=env, cwd=REPO)
    if proc.returncode != 0:
        raise SystemExit(f"probe failed: {sql}\n{proc.stderr[-2000:]}")
    return proc.stderr


def outstanding(trace):
    """[(req_bytes, innermost_symbol)] for every pointer never freed."""
    blocks = collections.defaultdict(list)
    cur = None
    req = 0
    sym = "?"
    for line in trace.splitlines():
        m = MALLOC.match(line)
        if m:
            cur, req, sym = m.group(1), int(m.group(2)), "?"
            continue
        f = FREE.match(line)
        if f:
            if cur is not None:
                blocks[cur].append((req, sym))
                cur = None
            if blocks[f.group(1)]:
                blocks[f.group(1)].pop()
            continue
        if cur is not None:
            fr = FRAME0.match(line)
            if fr:
                sym = fr.group(1)
            elif line.startswith("-- end"):
                blocks[cur].append((req, sym))
                cur = None
    return [b for bs in blocks.values() for b in bs]


def main():
    failures = []
    for label, inline_sql, long_sql in PROBES:
        for shape, sql in (("inline", inline_sql), ("long", long_sql)):
            leaked = [(n, s) for n, s in outstanding(run_query(sql))
                      if not any(s.startswith(d) for d in BY_DESIGN)]
            total = sum(n for n, _ in leaked)
            status = "LEAK" if leaked else "ok"
            print(f"  {label:18s} {shape:6s} {status:4s} "
                  f"blocks={len(leaked)} bytes={total}")
            if leaked:
                for n, s in sorted(leaked, reverse=True)[:3]:
                    print(f"      {n:>9} B  {s}")
                failures.append(f"{label}/{shape}: {len(leaked)} blocks, {total} bytes")
    if failures:
        raise SystemExit("ARENA LEAK GATE FAILED:\n  " + "\n  ".join(failures))
    print("arena leak gate: clean")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--run":
        sys.path.insert(1, REPO)
        import opteryx
        for _morsel in opteryx.session().execute_to_morsels(sys.argv[2]):
            pass
    else:
        main()
