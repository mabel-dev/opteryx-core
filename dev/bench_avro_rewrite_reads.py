"""Benchmark READS of rugo-written Avro files (dev tooling; never imported).

Rewrites the 1,000,000-row bench dataset (testdata/avro_bench/events_1000000_null.avro,
see dev/generate_avro_bench.py) with `rugo.avro.write_avro` at its defaults, then times
`rugo.avro.read_avro` on the result — all columns and a narrow projection — at one
thread and on every core, per codec. It measures what a writer's choices (field
nullability, enums, fixed vs bytes encodings) cost or save the reader.

Each case is timed `--rounds` times in a rotated order. Use `--rugo-root DIR` to import
a frozen copy of an earlier build's `rugo` package (writer AND reader come from it), for
an A/B in alternating processes:

    python dev/bench_avro_rewrite_reads.py --rugo-root scratch/x/base_pkg --out a.json
    python dev/bench_avro_rewrite_reads.py --out b.json
"""

import argparse
import json
import os
import statistics
import sys
import time

_ap = argparse.ArgumentParser()
_ap.add_argument("--rounds", type=int, default=5)
_ap.add_argument("--codecs", nargs="+", default=["null", "deflate"])
_ap.add_argument("--narrow", nargs="+", default=["id", "amount", "country", "event"])
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


def _read(data, columns, threads):
    t = time.perf_counter()
    with avro.read_avro(data, columns=columns, max_threads=threads) as r:
        rows = sum(m.num_rows for m in r)
    return time.perf_counter() - t, rows


def main():
    print("rugo from", os.path.dirname(avro.__file__))
    with avro.read_avro(SOURCE) as r:
        morsel = Morsel.combine(list(r))
    n = morsel.num_rows
    files = {}
    write_s = {}
    for codec in ARGS.codecs:
        t = time.perf_counter()
        files[codec] = avro.write_avro(morsel, codec=codec)
        write_s[codec] = time.perf_counter() - t
    cases = [(c, proj, thr) for c in ARGS.codecs for proj in ("all", "narrow") for thr in (1, 0)]
    times = {case: [] for case in cases}
    for rnd in range(ARGS.rounds):
        k = rnd % len(cases)
        for case in cases[k:] + cases[:k]:
            codec, proj, thr = case
            secs, rows = _read(files[codec], None if proj == "all" else ARGS.narrow, thr)
            assert rows == n
            times[case].append(secs)
    out = []
    for case in cases:
        codec, proj, thr = case
        med = statistics.median(times[case])
        out.append({"codec": codec, "projection": proj, "threads": thr, "read_s": med,
                    "read_all_s": times[case], "bytes": len(files[codec]), "write_s": write_s[codec]})
        print(f"{codec:8s} {proj:6s} thr={thr}  read={med * 1000:8.1f} ms  min={min(times[case]) * 1000:8.1f}  "
              f"bytes={len(files[codec]):>12,d}")
    schema = avro.read_metadata(files[ARGS.codecs[0]]).schema
    if ARGS.out:
        os.makedirs(os.path.dirname(os.path.abspath(ARGS.out)), exist_ok=True)
        with open(ARGS.out, "w") as f:
            json.dump({"rows": n, "cases": out, "schema": schema}, f, indent=1)


if __name__ == "__main__":
    main()
