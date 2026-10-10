"""Generate the Avro reader comparison dataset (dev tooling; never imported).

Writes the SAME rows (seeded, deterministic) once per codec, so the readers can be
compared on decode alone (null), and with decompression (deflate, zstandard):

    testdata/avro_bench/events_<rows>_null.avro
    testdata/avro_bench/events_<rows>_deflate.avro
    testdata/avro_bench/events_<rows>_zstandard.avro

fastavro is the writer (rugo has no Avro writer). The output is git-ignored
(testdata/avro_bench/**) — regenerate it rather than commit it:

    python dev/generate_avro_bench.py                 # 1,000,000 rows
    python dev/generate_avro_bench.py --rows 200000

The schema is 20 columns chosen to exercise every decode path a comparison should
see, in roughly the mix of an event / log stream:

  - fixed-width scalars: long, int, double, float, boolean
  - strings short (<= 12 bytes, inline in rugo) and long (> 12, arena), fixed(16)
  - nullable unions in both branch orders, at different null rates
  - enum (country, device.os), timestamp-micros, date, decimal(12,2) on bytes
  - nested: a record (device), array<string> (tags), map<double> (metrics)

so a reader can be timed reading everything, and reading a narrow projection
(e.g. `id, amount, country`) where rugo skips the rest without materialising it.
"""

import argparse
import datetime
import decimal
import os
import random
import time

import fastavro

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "testdata", "avro_bench")
CODECS = ("null", "deflate", "zstandard")
SEED = 20261010

COUNTRIES = ["AU", "BR", "CA", "CN", "DE", "ES", "FR", "GB", "IN", "IT",
             "JP", "KR", "MX", "NL", "NZ", "PL", "SE", "SG", "US", "ZA"]
EVENTS = ["click", "view", "purchase", "signup", "logout", "search", "share"]
OSES = ["android", "ios", "linux", "macos", "windows"]
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
         "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"]

SCHEMA = {
    "type": "record",
    "name": "Event",
    "namespace": "opteryx.bench",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "user_id", "type": "long"},
        {"name": "small", "type": "int"},
        {"name": "amount", "type": "double"},
        {"name": "ratio", "type": "float"},
        {"name": "flag", "type": "boolean"},
        {"name": "event", "type": "string"},
        {"name": "country", "type": {"type": "enum", "name": "Country", "symbols": COUNTRIES}},
        {"name": "name", "type": "string"},
        {"name": "url", "type": "string"},
        {"name": "note", "type": ["null", "string"]},
        {"name": "score", "type": ["double", "null"]},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "event_date", "type": {"type": "int", "logicalType": "date"}},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
        {"name": "session", "type": {"type": "fixed", "name": "Session", "size": 16}},
        {"name": "counter", "type": ["null", "long"]},
        {"name": "device", "type": {"type": "record", "name": "Device", "fields": [
            {"name": "os", "type": {"type": "enum", "name": "Os", "symbols": OSES}},
            {"name": "version", "type": "string"},
            {"name": "model", "type": ["null", "string"]},
        ]}},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "metrics", "type": {"type": "map", "values": "double"}},
    ],
}

EPOCH = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
DAY0 = datetime.date(2026, 1, 1)


def records(n):
    rnd = random.Random(SEED)
    for i in range(n):
        words = rnd.randint(1, 4)
        yield {
            "id": i,
            "user_id": rnd.randrange(1, 5_000_000),
            "small": rnd.randrange(-1000, 1000),
            "amount": round(rnd.uniform(0, 10_000), 4),
            "ratio": rnd.random(),
            "flag": rnd.random() < 0.3,
            "event": rnd.choice(EVENTS),
            "country": rnd.choice(COUNTRIES),
            "name": " ".join(rnd.choice(WORDS) for _ in range(words)),
            "url": f"https://example.com/{rnd.choice(WORDS)}/{rnd.randrange(1_000_000)}?ref={rnd.choice(WORDS)}",
            "note": None if rnd.random() < 0.7 else " ".join(rnd.choice(WORDS) for _ in range(rnd.randint(1, 8))),
            "score": None if rnd.random() < 0.1 else rnd.gauss(50, 15),
            "created_at": EPOCH + datetime.timedelta(microseconds=i * 31_536 + rnd.randrange(1_000_000)),
            "event_date": DAY0 + datetime.timedelta(days=rnd.randrange(365)),
            "price": decimal.Decimal(rnd.randrange(-10_000_000, 100_000_000)).scaleb(-2),
            "session": rnd.randbytes(16),
            "counter": None if rnd.random() < 0.5 else rnd.randrange(1 << 40),
            "device": {
                "os": rnd.choice(OSES),
                "version": f"{rnd.randint(1, 17)}.{rnd.randint(0, 9)}.{rnd.randint(0, 20)}",
                "model": None if rnd.random() < 0.2 else f"model-{rnd.randrange(500)}",
            },
            "tags": [rnd.choice(WORDS) for _ in range(rnd.randint(0, 3))],
            "metrics": {rnd.choice(WORDS): rnd.random() for _ in range(rnd.randint(0, 3))},
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rows", type=int, default=1_000_000)
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    parsed = fastavro.parse_schema(SCHEMA)
    for codec in CODECS:
        path = os.path.join(OUT_DIR, f"events_{args.rows}_{codec}.avro")
        t0 = time.perf_counter()
        with open(path, "wb") as f:
            fastavro.writer(f, parsed, records(args.rows), codec=codec)
        size = os.path.getsize(path)
        print(f"{path}: {args.rows:,} rows, {size / 1e6:.1f} MB, written in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
