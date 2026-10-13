"""
Produce data into the local Confluent stack (dev/kafka_avro/docker-compose.yml), register
S3 sink connectors that write it out as Avro container files, wait for the sinks to
settle, and download every object the bucket holds into an output directory.

One-off fixture generator for the Kafka Connect Avro read tests — the point is files
written by the REAL connector, not files we think look like them. Run via run.sh.

    uv run --with 'confluent-kafka[avro,schemaregistry]' --with boto3 --with cramjam \
        python dev/kafka_avro/generate.py --rows 500 --out testdata/avro/kafka_connect/confluent

Connector failures (an unsupported codec, a partitioner that refuses a field) are
reported and recorded in MANIFEST.json — they are results, not errors to hide.
"""

import argparse
import base64
import datetime
import decimal
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
import uuid

import boto3
import fastavro
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = "localhost:9092"
REGISTRY = "http://localhost:8081"
CONNECT = "http://localhost:8083"
S3_ENDPOINT = "http://localhost:8333"
S3_USER = "kafkaavro"
S3_PASSWORD = "kafkaavro-local-only"
BUCKET = "kafka-avro"
PARTITIONS = 2
SEED = 20261013

EVENT_FIELDS_V1 = [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "day", "type": {"type": "int", "logicalType": "date"}},
    {"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "ACTIVE", "CLOSED"]}},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "attrs", "type": {"type": "map", "values": "string"}},
    {
        "name": "address",
        "type": [
            "null",
            {
                "type": "record",
                "name": "Address",
                "fields": [{"name": "street", "type": "string"}, {"name": "city", "type": "string"}],
            },
        ],
        "default": None,
    },
    {"name": "maybe_int", "type": ["null", "int"], "default": None},
    {"name": "score", "type": "double"},
    {"name": "ratio", "type": "float"},
    {"name": "flag", "type": "boolean"},
    {"name": "raw", "type": "bytes"},
    {"name": "code", "type": {"type": "fixed", "name": "Code", "size": 4}},
]
# v2 = v1 + an optional field; the sink rotates to a new file at the schema change.
EVENT_FIELDS_V2 = EVENT_FIELDS_V1 + [{"name": "note", "type": ["null", "string"], "default": None}]


def event_schema(fields):
    return json.dumps({"type": "record", "name": "Event", "namespace": "opteryx.kafka", "fields": fields})


CITIES = ["Auckland", "Lisbon", "Osaka", "Quito", "Tromsø", "Zürich"]
BASE_TS = datetime.datetime(2026, 10, 1, tzinfo=datetime.timezone.utc)


def event_row(rng, i, v2):
    ts = BASE_TS + datetime.timedelta(seconds=rng.randrange(0, 3 * 86400))
    row = {
        "id": i,
        "name": f"event-{i}",
        "amount": decimal.Decimal(rng.randrange(-10**9, 10**9)).scaleb(-2),
        "ts": ts,
        "ts_us": ts + datetime.timedelta(microseconds=rng.randrange(0, 10**6)),
        "day": ts.date(),
        "t_ms": datetime.time(rng.randrange(24), rng.randrange(60), rng.randrange(60), rng.randrange(1000) * 1000),
        "uid": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "status": rng.choice(["NEW", "ACTIVE", "CLOSED"]),
        "tags": [f"t{rng.randrange(10)}" for _ in range(rng.randrange(4))],
        "attrs": {f"k{j}": f"v{rng.randrange(100)}" for j in range(rng.randrange(3))},
        "address": None if i % 5 == 0 else {"street": f"{i} Main St", "city": rng.choice(CITIES)},
        "maybe_int": None if i % 3 == 0 else rng.randrange(-1000, 1000),
        "score": rng.random() * 100,
        "ratio": rng.random(),
        "flag": i % 2 == 0,
        "raw": rng.randbytes(rng.randrange(0, 8)),
        "code": rng.randbytes(4),
    }
    if v2:
        row["note"] = None if i % 4 == 0 else f"note {i}"
    return row


def connect_field(field_name, type_, optional=False, **extra):
    return {"field": field_name, "type": type_, "optional": optional, **extra}


def decimal_bytes(value, scale):
    unscaled = int(value.scaleb(scale))
    return base64.b64encode(unscaled.to_bytes((unscaled.bit_length() + 8) // 8 or 1, "big", signed=True)).decode()


# Connect-native types via JsonConverter: here AvroData BUILDS the Avro schema itself,
# so this topic shows what Connect emits for its own logical types (decimal with no
# declared precision, int-keyed map, optional struct) rather than echoing ours.
CONNECT_TYPES_SCHEMA = {
    "type": "struct",
    "name": "opteryx.kafka.ConnectTypes",
    "optional": False,
    "fields": [
        connect_field("id", "int64"),
        connect_field("small", "int8"),
        connect_field("medium", "int16"),
        connect_field("amount", "bytes", name="org.apache.kafka.connect.data.Decimal", version=1, parameters={"scale": "2"}),
        connect_field("ts", "int64", name="org.apache.kafka.connect.data.Timestamp", version=1),
        connect_field("day", "int32", name="org.apache.kafka.connect.data.Date", version=1),
        connect_field("t", "int32", name="org.apache.kafka.connect.data.Time", version=1),
        connect_field("ratio", "float"),
        connect_field("flag", "boolean"),
        connect_field("blob", "bytes", optional=True),
        connect_field("label", "string", optional=True),
        connect_field("nums", "array", items={"type": "int64", "optional": False}),
        connect_field("by_code", "map", keys={"type": "int32", "optional": False}, values={"type": "string", "optional": False}),
        connect_field(
            "detail",
            "struct",
            optional=True,
            name="opteryx.kafka.Detail",
            fields=[connect_field("a", "string"), connect_field("b", "int32", optional=True)],
        ),
    ],
}


def connect_types_row(rng, i):
    ts = BASE_TS + datetime.timedelta(seconds=rng.randrange(0, 3 * 86400))
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return {
        "id": i,
        "small": rng.randrange(-128, 128),
        "medium": rng.randrange(-32768, 32768),
        "amount": decimal_bytes(decimal.Decimal(rng.randrange(-10**9, 10**9)).scaleb(-2), 2),
        "ts": int((ts - epoch).total_seconds() * 1000),
        "day": (ts.date() - epoch.date()).days,
        "t": rng.randrange(0, 86400000),
        "ratio": rng.random(),
        "flag": i % 2 == 1,
        "blob": None if i % 4 == 0 else base64.b64encode(rng.randbytes(3)).decode(),
        "label": None if i % 3 == 0 else f"label-{i}",
        "nums": [rng.randrange(1000) for _ in range(rng.randrange(4))],
        "by_code": [[rng.randrange(100), f"c{j}"] for j in range(rng.randrange(3))],
        "detail": None if i % 5 == 0 else {"a": f"a{i}", "b": None if i % 2 else i},
    }


# Debezium-shaped change envelope (before/after/source/op), with Debezium's own
# semantic types: precise Decimal with connect.decimal.precision, VariableScaleDecimal,
# MicroTimestamp, ZonedTimestamp, Enum.
DBZ_VALUE = {
    "type": "struct",
    "name": "dbserver1.inventory.customers.Value",
    "optional": True,
    "fields": [
        connect_field("id", "int32"),
        connect_field("email", "string"),
        connect_field(
            "balance",
            "bytes",
            name="org.apache.kafka.connect.data.Decimal",
            version=1,
            parameters={"scale": "2", "connect.decimal.precision": "10"},
        ),
        connect_field(
            "raw_num",
            "struct",
            optional=True,
            name="io.debezium.data.VariableScaleDecimal",
            version=1,
            fields=[connect_field("scale", "int32"), connect_field("value", "bytes")],
        ),
        connect_field("created", "int64", name="io.debezium.time.MicroTimestamp", version=1),
        connect_field("updated", "string", optional=True, name="io.debezium.time.ZonedTimestamp", version=1),
        connect_field("birth", "int32", optional=True, name="io.debezium.time.Date", version=1),
    ],
}
DBZ_SCHEMA = {
    "type": "struct",
    "name": "dbserver1.inventory.customers.Envelope",
    "optional": False,
    "fields": [
        {**DBZ_VALUE, "field": "before"},
        {**DBZ_VALUE, "field": "after"},
        connect_field(
            "source",
            "struct",
            name="io.debezium.connector.postgresql.Source",
            fields=[
                connect_field("version", "string"),
                connect_field("connector", "string"),
                connect_field("name", "string"),
                connect_field("ts_ms", "int64"),
                connect_field(
                    "snapshot",
                    "string",
                    optional=True,
                    name="io.debezium.data.Enum",
                    version=1,
                    parameters={"allowed": "true,last,false,incremental"},
                ),
                connect_field("db", "string"),
                connect_field("table", "string"),
            ],
        ),
        connect_field("op", "string"),
        connect_field("ts_ms", "int64", optional=True),
    ],
}


def dbz_rows(rng, n):
    state = {}
    epoch_us = int(BASE_TS.timestamp() * 10**6)
    for i in range(n):
        cid = rng.randrange(1, max(2, n // 3))
        before = state.get(cid)
        if before is None:
            op = "c"
        else:
            op = "d" if rng.random() < 0.15 else "u"
        after = None
        if op != "d":
            after = {
                "id": cid,
                "email": f"user{cid}.{i}@example.com",
                "balance": decimal_bytes(decimal.Decimal(rng.randrange(0, 10**8)).scaleb(-2), 2),
                "raw_num": None if i % 4 == 0 else {"scale": 3, "value": decimal_bytes(decimal.Decimal(rng.randrange(10**6)), 0)},
                "created": epoch_us + i * 1_000_123,
                "updated": None if op == "c" else (BASE_TS + datetime.timedelta(minutes=i)).isoformat().replace("+00:00", "Z"),
                "birth": None if i % 6 == 0 else rng.randrange(-5000, 15000),
            }
        state[cid] = after
        yield str(cid), {
            "before": before,
            "after": after,
            "source": {
                "version": "2.7.0.Final",
                "connector": "postgresql",
                "name": "dbserver1",
                "ts_ms": int(BASE_TS.timestamp() * 1000) + i,
                "snapshot": "false",
                "db": "inventory",
                "table": "customers",
            },
            "op": op,
            "ts_ms": int(BASE_TS.timestamp() * 1000) + i + 5,
        }


# The Confluent sink accepts exactly these (kafka-connect-storage-common
# AVRO_SUPPORTED_CODECS); zstandard is not among them.
CODECS = ["null", "deflate", "snappy", "bzip2"]


def connectors(flush_size):
    common = {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "s3.bucket.name": BUCKET,
        "s3.region": "us-east-1",
        "store.url": "http://s3:8333",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
        "flush.size": str(flush_size),
        # Flush partial tails so every produced record lands in a file.
        "rotate.schedule.interval.ms": "15000",
        "timezone": "UTC",
        "locale": "en-US",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        # Each open output file holds an s3.part.size buffer (default 25 MiB); the hourly
        # partitioner keeps ~150 open at once, which ran Connect out of heap.
        "s3.part.size": "5242880",
    }
    json_value = {
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
    }
    out = {}
    for codec in CODECS:
        out[f"s3-events-{codec}"] = {**common, "topics": "events", "topics.dir": f"codec_{codec}", "avro.codec": codec}
    out["s3-events-hourly"] = {
        **common,
        "topics": "events",
        "topics.dir": "time_partitioned",
        "avro.codec": "deflate",
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
        "partition.duration.ms": "3600000",
        "timestamp.extractor": "RecordField",
        "timestamp.field": "ts",
    }
    out["s3-events-by-status"] = {
        **common,
        "topics": "events",
        "topics.dir": "field_partitioned",
        "avro.codec": "snappy",
        "partitioner.class": "io.confluent.connect.storage.partitioner.FieldPartitioner",
        "partition.field.name": "status",
    }
    out["s3-connect-types"] = {**common, **json_value, "topics": "connect_types", "topics.dir": "connect_types", "avro.codec": "deflate"}
    out["s3-dbz-customers"] = {**common, **json_value, "topics": "dbz_customers", "topics.dir": "debezium", "avro.codec": "snappy"}
    return out


def http(method, url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read()
    return json.loads(payload) if payload else None


def wait_for(url, what, timeout=600):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return http("GET", url)
        except (urllib.error.URLError, ConnectionError) as err:
            last = err
            time.sleep(3)
    raise SystemExit(f"{what} did not come up at {url} within {timeout}s: {last}")


def wait_for_s3(timeout=300):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(S3_ENDPOINT + "/", timeout=10) as resp:
                return resp.status
        except urllib.error.HTTPError as err:
            # Any HTTP answer means the server is up; anonymous requests get 403.
            return err.code
        except (urllib.error.URLError, ConnectionError) as err:
            last = err
            time.sleep(3)
    raise SystemExit(f"S3 did not come up at {S3_ENDPOINT} within {timeout}s: {last}")


def s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_USER, aws_secret_access_key=S3_PASSWORD, region_name="us-east-1"
    )


def list_objects(client):
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return sorted(keys)


def produce(rows):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    futures = admin.create_topics([NewTopic(t, PARTITIONS, 1) for t in ("events", "connect_types", "dbz_customers")])
    for topic, fut in futures.items():
        fut.result()
        print(f"created topic {topic}")

    rng = random.Random(SEED)
    registry = SchemaRegistryClient({"url": REGISTRY})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    half = rows // 2
    for version, fields, start, stop in ((1, EVENT_FIELDS_V1, 0, half), (2, EVENT_FIELDS_V2, half, rows)):
        serializer = AvroSerializer(registry, event_schema(fields), conf={"auto.register.schemas": True})
        ctx = SerializationContext("events", MessageField.VALUE)
        for i in range(start, stop):
            producer.produce("events", key=str(i), value=serializer(event_row(rng, i, version == 2), ctx))
            producer.poll(0)
        producer.flush()
        print(f"produced events v{version}: {stop - start} rows")

    for i in range(rows):
        msg = {"schema": CONNECT_TYPES_SCHEMA, "payload": connect_types_row(rng, i)}
        producer.produce("connect_types", key=str(i), value=json.dumps(msg).encode())
        producer.poll(0)
    for key, payload in dbz_rows(rng, rows):
        producer.produce("dbz_customers", key=key, value=json.dumps({"schema": DBZ_SCHEMA, "payload": payload}).encode())
        producer.poll(0)
    producer.flush()
    print(f"produced connect_types + dbz_customers: {rows} rows each")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--flush-size", type=int, default=100)
    parser.add_argument("--out", required=True)
    parser.add_argument("--settle-seconds", type=int, default=45)
    args = parser.parse_args()

    if os.path.exists(args.out) and os.listdir(args.out):
        raise SystemExit(f"{args.out} is not empty — refusing to mix runs")

    wait_for(f"{REGISTRY}/subjects", "schema registry")
    wait_for(f"{CONNECT}/connectors", "kafka connect")
    wait_for_s3()
    client = s3()
    client.create_bucket(Bucket=BUCKET)

    plugins = http("GET", f"{CONNECT}/connector-plugins")
    s3_plugin = [p for p in plugins if p["class"] == "io.confluent.connect.s3.S3SinkConnector"]
    if not s3_plugin:
        raise SystemExit(f"S3 sink connector is not installed in Connect: {plugins}")

    produce(args.rows)

    configs = connectors(args.flush_size)
    for name, config in configs.items():
        http("PUT", f"{CONNECT}/connectors/{name}/config", config)
        print(f"registered {name}")

    # Settle: object count unchanged across two polls past the rotate interval.
    previous = None
    while True:
        time.sleep(args.settle_seconds)
        keys = list_objects(client)
        print(f"{len(keys)} objects in s3://{BUCKET}")
        if keys == previous:
            break
        previous = keys

    status = {name: http("GET", f"{CONNECT}/connectors/{name}/status") for name in configs}
    for name, st in status.items():
        tasks = st.get("tasks", [])
        states = [t["state"] for t in tasks]
        print(f"{name}: connector={st['connector']['state']} tasks={states}")
        for t in tasks:
            if t["state"] == "FAILED":
                for line in t["trace"].splitlines():
                    if line.startswith("Caused by"):
                        print("  " + line[:300])

    os.makedirs(args.out, exist_ok=True)
    for key in keys:
        dest = os.path.join(args.out, key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        client.download_file(BUCKET, key, dest)

    # Writer schemas + codec straight from each file header (fastavro is the oracle
    # here, in dev/ tooling only).
    files = {}
    for key in keys:
        with open(os.path.join(args.out, key), "rb") as fh:
            reader = fastavro.reader(fh)
            files[key] = {
                "codec": reader.codec,
                "writer_schema": reader.metadata["avro.schema"],
                "records": sum(1 for _ in reader),
            }
    manifest = {
        "generated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "rows_per_topic": args.rows,
        "seed": SEED,
        "s3_sink_plugin": s3_plugin[0],
        "connectors": configs,
        "connector_status": status,
        "files": files,
    }
    with open(os.path.join(args.out, "MANIFEST.json"), "w") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    print(f"wrote {len(keys)} files + MANIFEST.json to {args.out}")
    failed = [n for n, st in status.items() if any(t["state"] == "FAILED" for t in st.get("tasks", []))]
    if failed:
        print(f"connectors FAILED: {failed}", file=sys.stderr)


if __name__ == "__main__":
    main()
