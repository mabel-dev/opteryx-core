"""Compare Avro readers: rugo vs fastavro vs Apache `avro` (dev tooling; never imported).

Input: the dataset from dev/generate_avro_bench.py (same rows, one file per codec).

Arms, all decoding the SAME in-memory bytes (file IO is outside the timing):

  rugo           rugo.rugo_native.read_avro -> Draken vectors (native, no Python per value)
  rugo+pylist    the same, then every vector .to_pylist() — the cost of ending in Python
                 objects, which is what the other two produce
  fastavro       fastavro.reader -> one dict per record (Cython)
  apache         avro.datafile.DataFileReader -> one dict per record (pure Python;
                 the reference implementation — expect minutes per pass at 1M rows)

Shapes:

  all            every column
  narrow         id, amount, country. rugo selects columns; fastavro and Apache are
                 given a reader schema of just those fields, which is how they skip.

Method (memory: interleaved A/B, rotated order, no A/A arms): each round runs every
arm once for each (codec, shape), in an order rotated per round so no arm always goes
first or last. The median per arm is reported, with the speedup of rugo vs each arm.

    python dev/bench_avro_readers.py                           # everything, 3 rounds
    python dev/bench_avro_readers.py --rounds 5 --readers rugo fastavro
    python dev/bench_avro_readers.py --codecs null --shapes narrow

Results also go to dev/bench_results/avro_readers_<timestamp>.json (git-ignored).
"""

import argparse
import io
import json
import os
import platform
import statistics
import sys
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)

import fastavro  # noqa: E402
from avro.datafile import DataFileReader  # noqa: E402
from avro.io import DatumReader  # noqa: E402
from avro.schema import parse as avro_parse  # noqa: E402

from rugo.rugo_native import read_avro, read_avro_metadata  # noqa: E402

DATA_DIR = os.path.join(ROOT, "testdata", "avro_bench")
RESULTS_DIR = os.path.join(ROOT, "dev", "bench_results")
NARROW = ["id", "amount", "country"]


def narrow_schema(writer_schema: dict) -> dict:
    return {**writer_schema, "fields": [f for f in writer_schema["fields"] if f["name"] in NARROW]}


def arm_rugo(data, shape, _schema):
    res = read_avro(data, NARROW if shape == "narrow" else None)
    return res["num_rows"]


def arm_rugo_pylist(data, shape, _schema):
    res = read_avro(data, NARROW if shape == "narrow" else None)
    for batch in res["batches"]:
        for vec in batch:
            vec.to_pylist()
    return res["num_rows"]


def arm_fastavro(data, shape, schema):
    reader_schema = narrow_schema(schema) if shape == "narrow" else None
    n = 0
    for _ in fastavro.reader(io.BytesIO(data), reader_schema=reader_schema):
        n += 1
    return n


def arm_apache(data, shape, schema):
    rs = avro_parse(json.dumps(narrow_schema(schema))) if shape == "narrow" else None
    reader = DataFileReader(io.BytesIO(data), DatumReader(readers_schema=rs))
    n = 0
    for _ in reader:
        n += 1
    reader.close()
    return n


ARMS = {"rugo": arm_rugo, "rugo+pylist": arm_rugo_pylist, "fastavro": arm_fastavro, "apache": arm_apache}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=1_000_000, help="which generated dataset")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--readers", nargs="+", default=list(ARMS), choices=list(ARMS))
    ap.add_argument("--codecs", nargs="+", default=["null", "deflate", "zstandard"])
    ap.add_argument("--shapes", nargs="+", default=["all", "narrow"], choices=["all", "narrow"])
    args = ap.parse_args()

    files = {}
    for codec in args.codecs:
        path = os.path.join(DATA_DIR, f"events_{args.rows}_{codec}.avro")
        if not os.path.exists(path):
            sys.exit(f"missing {path} — run: python dev/generate_avro_bench.py --rows {args.rows}")
        with open(path, "rb") as f:
            data = f.read()
        files[codec] = (data, json.loads(read_avro_metadata(data)["schema"]))

    cases = [(c, s) for c in args.codecs for s in args.shapes]
    times = {(c, s, a): [] for c, s in cases for a in args.readers}
    print(f"{args.rows:,} rows · {args.rounds} rounds · readers {args.readers}")
    for rnd in range(args.rounds):
        for ci, (codec, shape) in enumerate(cases):
            data, schema = files[codec]
            k = (rnd + ci) % len(args.readers)
            order = args.readers[k:] + args.readers[:k]  # rotate: no arm always first
            for arm in order:
                t0 = time.perf_counter()
                n = ARMS[arm](data, shape, schema)
                dt = time.perf_counter() - t0
                if n != args.rows:
                    sys.exit(f"{arm} read {n} rows from the {codec} file, expected {args.rows}")
                times[(codec, shape, arm)].append(dt)
                print(f"  round {rnd + 1}  {codec:10s} {shape:6s} {arm:12s} {dt:8.3f}s", flush=True)

    print()
    print(f"{'codec':10s} {'shape':6s} {'reader':12s} {'median s':>9s} {'Mrows/s':>8s} {'rugo x':>8s}")
    summary = []
    for codec, shape in cases:
        base = statistics.median(times[(codec, shape, "rugo")]) if "rugo" in args.readers else None
        for arm in args.readers:
            med = statistics.median(times[(codec, shape, arm)])
            ratio = med / base if base else None
            summary.append({"codec": codec, "shape": shape, "reader": arm, "median_s": med,
                            "runs_s": times[(codec, shape, arm)], "rugo_speedup": ratio})
            print(f"{codec:10s} {shape:6s} {arm:12s} {med:9.3f} {args.rows / med / 1e6:8.2f} "
                  f"{(f'{ratio:7.1f}x') if ratio else '':>8s}")
        print()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out = os.path.join(RESULTS_DIR, f"avro_readers_{time.strftime('%Y%m%d_%H%M%S')}.json")
    with open(out, "w") as f:
        json.dump({"rows": args.rows, "rounds": args.rounds, "machine": platform.platform(),
                   "python": platform.python_version(), "results": summary}, f, indent=2)
    print(f"results: {out}")


if __name__ == "__main__":
    main()
