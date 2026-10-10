"""Seed corpus for tests/fuzzing/native/fuzz_avro.cpp (dev tooling, never imported).

Small valid Avro container files that reach every part of the decoder: each codec,
nullable unions in both orders, enum, fixed, decimal on bytes and fixed, dates and
timestamps, arrays/maps (including nested), a nullable nested record, and a real
PyIceberg manifest list + manifest. The fuzzer mutates from these.

    python dev/generate_avro_fuzz_corpus.py
"""

import datetime
import decimal
import io
import shutil
import sys
import tempfile
from pathlib import Path

import fastavro

OUT = Path(__file__).resolve().parent.parent / "tests" / "fuzzing" / "native" / "corpus" / "avro"

SCHEMA = {"type": "record", "name": "r", "fields": [
    {"name": "c", "type": "long"},
    {"name": "s", "type": ["null", "string"]},
    {"name": "t", "type": ["double", "null"]},
    {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["a", "b"]}},
    {"name": "fx", "type": {"type": "fixed", "name": "F", "size": 4}},
    {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 2}},
    {"name": "decf", "type": {"type": "fixed", "name": "G", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 0}},
    {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "arr", "type": {"type": "array", "items": ["null", "long"]}},
    {"name": "m", "type": {"type": "map", "values": {"type": "array", "items": "string"}}},
    {"name": "data_file", "type": ["null", {"type": "record", "name": "D", "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "lower_bounds", "type": {"type": "array", "items": {"type": "record", "name": "KV", "fields": [
            {"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}},
    ]}]},
]}


def records(n):
    for i in range(n):
        yield {
            "c": i, "s": None if i % 3 == 0 else "s" * i, "t": None if i % 2 else i / 3,
            "e": "ab"[i % 2], "fx": bytes([i, 1, 2, 3]),
            "dec": decimal.Decimal(i * 101).scaleb(-2), "decf": decimal.Decimal(-i),
            "dt": datetime.date(2000, 1, 1 + i), "ts": datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
            "arr": [None, i], "m": {"k": ["v"] * i},
            "data_file": None if i % 4 == 0 else {"file_path": f"p{i}", "lower_bounds": [{"key": i, "value": b"\x00"}]},
        }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for codec in ["null", "deflate", "snappy", "zstandard"]:
        buf = io.BytesIO()
        fastavro.writer(buf, SCHEMA, records(6), codec=codec, sync_interval=200)
        (OUT / f"mixed_{codec}.avro").write_bytes(buf.getvalue())

    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    d = tempfile.mkdtemp()
    try:
        cat = SqlCatalog("t", uri=f"sqlite:///{d}/c.db", warehouse=f"file://{d}/wh")
        cat.create_namespace("n")
        t = cat.create_table("n.t", schema=Schema(NestedField(1, "id", LongType(), required=False),
                                                   NestedField(2, "s", StringType(), required=False)))
        t.append(pa.table({"id": pa.array([1, 2], pa.int64()), "s": pa.array(["a", None])}))
        snap = t.current_snapshot()
        (OUT / "iceberg_manifest_list.avro").write_bytes(Path(snap.manifest_list[7:]).read_bytes())
        m = snap.manifests(t.io)[0]
        (OUT / "iceberg_manifest.avro").write_bytes(Path(m.manifest_path[7:]).read_bytes())
    finally:
        shutil.rmtree(d)
    print(f"wrote {len(list(OUT.iterdir()))} files to {OUT}")


if __name__ == "__main__":
    sys.exit(main())
