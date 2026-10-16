"""Benchmark the rugo Avro writer (dev tooling; never imported).

Writes the 1,000,000-row bench dataset (testdata/avro_bench/events_1000000_null.avro,
from dev/generate_avro_bench.py — 20 columns: ints, floats, short/long strings, enum,
timestamp, date, decimal, bytes, a nested record and map as JSON text, ARRAY<string>)
back out through `rugo.avro.write_avro` and reports, per (codec, block_bytes, threads):

  write  — median wall time of write_avro over the rounds
  bytes  — file size
  read   — median wall time of rugo.avro.read_avro over the written file (all cores)
  read1  — the same with max_threads=1

Cases are run in a rotated order inside every round, so no case always runs first or
last (thermal drift / within-round order bias). Results are printed and, with --out,
saved as JSON so an A/B can compare two builds.

    python dev/bench_avro_writers.py --rounds 5 --out scratch/avro_writer_ab/base.json
    python dev/bench_avro_writers.py --threads 1 0 --out scratch/avro_writer_ab/par.json

`--threads` is passed to write_avro as max_threads; omit it for a build that predates
the parameter. `--rugo-root DIR` imports `rugo` from DIR (a frozen copy of an earlier
build's package, compiled module included) instead of the working tree, for an A/B
against that build; draken always comes from the working tree.
"""

import argparse
import json
import os
import statistics
import sys
import time

_ap = argparse.ArgumentParser()
_ap.add_argument("--rounds", type=int, default=5)
_ap.add_argument("--codecs", nargs="+", default=["null", "deflate", "zstandard"])
_ap.add_argument("--block-sizes", nargs="+", type=int,
                 default=[64 << 10, 256 << 10, 1 << 20, 4 << 20])
_ap.add_argument("--threads", nargs="+", type=int, default=None)
_ap.add_argument("--rugo-root")
_ap.add_argument("--out")
ARGS = _ap.parse_args()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if ARGS.rugo_root:
    sys.path.insert(0, os.path.abspath(ARGS.rugo_root))

from draken.morsels.morsel import Morsel  # noqa: E402
from rugo import avro  # noqa: E402

SOURCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "testdata", "avro_bench",
                      "events_1000000_null.avro")


def _load():
    with avro.read_avro(SOURCE) as r:
        return Morsel.combine(list(r))


def _read_time(data, threads):
    t = time.perf_counter()
    with avro.read_avro(data, max_threads=threads) as r:
        rows = sum(m.num_rows for m in r)
    return time.perf_counter() - t, rows


def main():
    args = ARGS
    print("rugo from", os.path.dirname(avro.__file__))
    morsel = _load()
    n = morsel.num_rows
    threads = args.threads if args.threads is not None else [None]
    cases = [(c, b, t) for c in args.codecs for b in args.block_sizes for t in threads]
    times = {case: [] for case in cases}
    files = {}
    for rnd in range(args.rounds):
        k = rnd % len(cases)
        for case in cases[k:] + cases[:k]:
            codec, block, thr = case
            kw = {"codec": codec, "block_bytes": block}
            if thr is not None:
                kw["max_threads"] = thr
            t = time.perf_counter()
            data = avro.write_avro(morsel, **kw)
            times[case].append(time.perf_counter() - t)
            files[case] = data

    rows = []
    for case in cases:
        codec, block, thr = case
        data = files[case]
        reads = [_read_time(data, 0) for _ in range(3)]
        reads1 = [_read_time(data, 1) for _ in range(3)]
        assert all(r[1] == n for r in reads + reads1)
        rows.append({
            "codec": codec, "block_bytes": block, "threads": thr,
            "write_s": statistics.median(times[case]),
            "write_all_s": times[case],
            "bytes": len(data),
            "read_s": statistics.median(r[0] for r in reads),
            "read1_s": statistics.median(r[0] for r in reads1),
        })
        print(f"{codec:9s} block={block >> 10:5d}K thr={str(thr):4s} "
              f"write={rows[-1]['write_s'] * 1000:8.1f} ms  bytes={len(data):>12,d}  "
              f"read={rows[-1]['read_s'] * 1000:7.1f} ms  read1={rows[-1]['read1_s'] * 1000:7.1f} ms")
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            json.dump({"rows": n, "rounds": args.rounds, "cases": rows}, f, indent=1)


if __name__ == "__main__":
    main()
