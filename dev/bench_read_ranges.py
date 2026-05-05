#!/usr/bin/env python3
"""
Microbenchmark for OpteryxLocalFileSystem.read_ranges().

Drives the *exact* range pattern that fetch_columns() produces against a real
parquet file (ClickBench hits_*.parquet). Compares:
- the existing implementation (os.pread + Python ThreadPoolExecutor)
- whatever read_ranges() resolves to after the change

Usage (from repo root):
    python3 dev/bench_read_ranges.py [parquet_path]

Default path: scratch/hits_mid/hits_2.parquet (220 MB)

The script reads the parquet footer with rugo, builds a coalesced range list
per row group, then for each row group calls read_ranges() many times and
records timings. Page cache state matters a lot for IO benchmarks; the script
runs a warm-cache pass and (best-effort) a cold-cache pass.

Cold-cache trick on macOS: there is no portable cache-drop syscall available
to a user, so we use `purge` if installed (requires sudo) and otherwise
fall back to "best-effort" by re-opening with F_NOCACHE on a sibling fd
before each iteration. Numbers in cold mode on macOS without `purge` should
be treated as warm-ish-but-not-identical-warm and labelled accordingly.

Baseline (recorded BEFORE adding read_file_ranges; current path = os.pread +
Python ThreadPoolExecutor with 48 workers, on Apple Silicon, hits_2.parquet
(219.9 MiB, 5 row groups, 525 coalesced ranges total):

  WARM   median  9.70 ms   (22.6 GiB/s)   min  9.24 ms
  COLD-* median 12.40 ms   (17.7 GiB/s)   min 12.20 ms
  (* macOS `purge` failed without sudo, so cold is partial)

22 GiB/s warm is already at memcpy-from-page-cache bandwidth, so any speedup
from saving Python overhead will be limited. Decision threshold (per plan):
microbench median speedup must be >= 1.15x to ship.
"""
from __future__ import annotations

import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple

# Make 'opteryx' / 'rugo' importable when running from the repo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DEFAULT_PARQUET = Path("scratch/hits_mid/hits_2.parquet")


def parse_args(argv: List[str]) -> Path:
    if len(argv) > 1:
        return Path(argv[1])
    return DEFAULT_PARQUET


def build_ranges_per_row_group(parquet_path: Path) -> List[List[Tuple[int, int]]]:
    """Return one coalesced range list per row group, mirroring fetch_columns."""
    from opteryx.connectors.parquet_io.reader import _coalesce_ranges
    from rugo.parquet_reader import read_metadata_from_memoryview

    with open(parquet_path, "rb") as f:
        data = f.read()
    meta = read_metadata_from_memoryview(memoryview(data), schema_only=False)

    ranges_by_rg: List[List[Tuple[int, int]]] = []
    for rg in meta["row_groups"]:
        raw_ranges: List[Tuple[int, int]] = []
        for col in rg["columns"]:
            dict_off = col.get("dictionary_page_offset")
            data_off = col["data_page_offset"]
            if dict_off is not None and dict_off >= 0 and dict_off < data_off:
                base = dict_off
            else:
                base = data_off
            raw_ranges.append((base, col["total_compressed_size"]))
        coalesced, _parts = _coalesce_ranges(raw_ranges)
        ranges_by_rg.append(coalesced)
    return ranges_by_rg


def try_drop_cache(path: Path) -> str:
    """Best-effort cache eviction. Returns label describing what happened."""
    if sys.platform == "linux":
        # Requires CAP_SYS_ADMIN or root; usually unavailable to a user.
        try:
            with open("/proc/sys/vm/drop_caches", "w") as f:
                f.write("1\n")
            return "linux:drop_caches=1"
        except (OSError, PermissionError):
            pass
        # Fallback: posix_fadvise DONTNEED on the file itself.
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
            return "linux:posix_fadvise(DONTNEED)"
        except (AttributeError, OSError):
            return "linux:no-eviction"
    if sys.platform == "darwin":
        # `purge` is the only reliable way on macOS, requires sudo.
        if subprocess.run(
            ["which", "purge"], capture_output=True
        ).returncode == 0:
            r = subprocess.run(["purge"], capture_output=True)
            if r.returncode == 0:
                return "darwin:purge"
            return f"darwin:purge-failed(rc={r.returncode})"
        return "darwin:no-purge-available"
    return f"{sys.platform}:no-eviction"


def warm_file(path: Path) -> int:
    """Read the whole file once to populate the page cache."""
    n = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(8 << 20)
            if not buf:
                break
            n += len(buf)
    return n


def time_read_ranges(
    fs,
    path: Path,
    ranges_by_rg: List[List[Tuple[int, int]]],
    iterations: int,
) -> dict:
    """Return per-row-group timing stats (ns) over `iterations` repeats.

    Each iteration reads ALL row groups once, sequentially.
    """
    str_path = str(path)
    timings_ns: List[int] = []
    bytes_per_iter = sum(length for rg in ranges_by_rg for _, length in rg)

    total_ranges = sum(len(rg) for rg in ranges_by_rg)

    for _ in range(iterations):
        t0 = time.monotonic_ns()
        for ranges in ranges_by_rg:
            buffers = fs.read_ranges(str_path, ranges)
            # Defeat any lazy-evaluation concerns
            if len(buffers) != len(ranges):
                raise RuntimeError("range count mismatch")
        timings_ns.append(time.monotonic_ns() - t0)

    return {
        "iterations": iterations,
        "row_groups": len(ranges_by_rg),
        "total_ranges": total_ranges,
        "bytes_per_iter": bytes_per_iter,
        "median_ns": int(statistics.median(timings_ns)),
        "min_ns": min(timings_ns),
        "p95_ns": int(statistics.quantiles(timings_ns, n=20)[-1])
            if len(timings_ns) >= 20 else max(timings_ns),
        "max_ns": max(timings_ns),
        "all_ns": timings_ns,
    }


def fmt_throughput(bytes_per_iter: int, ns: int) -> str:
    if ns <= 0:
        return "n/a"
    bps = bytes_per_iter * 1e9 / ns
    return f"{bps / (1 << 20):.1f} MiB/s"


def fmt_us(ns: int) -> str:
    return f"{ns / 1000:.1f} us"


def fmt_ms(ns: int) -> str:
    return f"{ns / 1e6:.2f} ms"


def main() -> int:
    parquet_path = parse_args(sys.argv)
    if not parquet_path.is_absolute():
        parquet_path = (Path(__file__).resolve().parent.parent / parquet_path).resolve()
    if not parquet_path.exists():
        print(f"ERROR: parquet file not found: {parquet_path}", file=sys.stderr)
        return 2

    print(f"file: {parquet_path}")
    print(f"size: {parquet_path.stat().st_size / (1 << 20):.1f} MiB")

    print("building range plan from parquet footer...")
    ranges_by_rg = build_ranges_per_row_group(parquet_path)
    n_rg = len(ranges_by_rg)
    n_ranges = sum(len(rg) for rg in ranges_by_rg)
    n_bytes = sum(l for rg in ranges_by_rg for _, l in rg)
    print(f"row groups: {n_rg}")
    print(f"coalesced ranges total: {n_ranges} (avg {n_ranges/max(n_rg,1):.1f} per row group)")
    print(f"bytes per full pass: {n_bytes / (1 << 20):.1f} MiB")
    print()

    from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
    fs_baseline = OpteryxLocalFileSystem()

    # Candidate path: call the new C++ batched read directly via a tiny shim
    # that exposes the same .read_ranges() shape.
    from opteryx.compiled.io import disk_reader

    class CandidateFs:
        @staticmethod
        def read_ranges(path, ranges):
            if not ranges:
                return []
            return disk_reader.read_file_ranges(path, ranges)

    fs_candidate = CandidateFs()

    def run_phase(label: str, fs):
        # Warm-cache
        print(f"--- {label}: WARM CACHE ---")
        warm_file(parquet_path)
        time_read_ranges(fs, parquet_path, ranges_by_rg, iterations=2)
        warm = time_read_ranges(fs, parquet_path, ranges_by_rg, iterations=25)
        print(f"iterations: {warm['iterations']}")
        print(f"min:    {fmt_ms(warm['min_ns']):>10}  ({fmt_throughput(warm['bytes_per_iter'], warm['min_ns'])})")
        print(f"median: {fmt_ms(warm['median_ns']):>10}  ({fmt_throughput(warm['bytes_per_iter'], warm['median_ns'])})")
        print(f"p95:    {fmt_ms(warm['p95_ns']):>10}  ({fmt_throughput(warm['bytes_per_iter'], warm['p95_ns'])})")
        print(f"max:    {fmt_ms(warm['max_ns']):>10}")
        print()

        # Cold-ish
        print(f"--- {label}: COLD CACHE (best-effort) ---")
        cold_runs: List[int] = []
        evict_label = ""
        for _ in range(5):
            evict_label = try_drop_cache(parquet_path)
            single = time_read_ranges(fs, parquet_path, ranges_by_rg, iterations=1)
            cold_runs.extend(single["all_ns"])
        print(f"eviction: {evict_label}")
        if cold_runs:
            print(f"min:    {fmt_ms(min(cold_runs)):>10}  ({fmt_throughput(n_bytes, min(cold_runs))})")
            print(f"median: {fmt_ms(int(statistics.median(cold_runs))):>10}  "
                  f"({fmt_throughput(n_bytes, int(statistics.median(cold_runs)))})")
            print(f"max:    {fmt_ms(max(cold_runs)):>10}")
        print()
        return warm["median_ns"], int(statistics.median(cold_runs)) if cold_runs else 0

    print("=" * 60)
    base_warm, base_cold = run_phase("BASELINE (os.pread + ThreadPool)", fs_baseline)
    print("=" * 60)
    cand_warm, cand_cold = run_phase("CANDIDATE (read_file_ranges)", fs_candidate)
    print("=" * 60)
    print()
    print("=== HEAD-TO-HEAD ===")
    if base_warm and cand_warm:
        speedup = base_warm / cand_warm
        print(f"WARM   median  baseline={fmt_ms(base_warm)}  candidate={fmt_ms(cand_warm)}  speedup={speedup:.3f}x")
    if base_cold and cand_cold:
        speedup_c = base_cold / cand_cold
        print(f"COLD   median  baseline={fmt_ms(base_cold)}  candidate={fmt_ms(cand_cold)}  speedup={speedup_c:.3f}x")
    print()
    print("decision rule: ship if warm speedup >= 1.15x or cold speedup >= 1.15x")
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
