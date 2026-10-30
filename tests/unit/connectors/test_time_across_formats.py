# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A TIME is parquet's TIME in every format: INT64 microseconds since midnight.

The engine is a parquet engine first and other formats fold into its convention (architect ruling,
2026-10-26): READ_PARQUET types a time column INT64 and returns the microseconds, whatever unit the file
stores (ms scaled up) and declares it TIME; READ_AVRO (time-micros, time-millis) and READ_ORC (a long marked
`iceberg.long-type=TIME`) therefore return the same integer, as do an Avro and an ORC folder. Every
expected value is the value the test wrote."""

import datetime
import io
import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../rugo"))

os.environ["TZ"] = "UTC"
time.tzset()

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from test_orc_uuid_fixed import _file as orc_file

import opteryx

pytestmark = pytest.mark.usefixtures("allow_local_reads")

TIMES = [datetime.time(0, 0, 1), datetime.time(12, 30, 45, 123456), datetime.time(23, 59, 59, 999999)]
MICROS = [((t.hour * 60 + t.minute) * 60 + t.second) * 1_000_000 + t.microsecond for t in TIMES]
MILLIS_TIMES = [datetime.time(0, 0, 1), datetime.time(12, 30, 45, 123000), datetime.time(23, 59, 59, 999000)]
MILLIS_MICROS = [((t.hour * 60 + t.minute) * 60 + t.second) * 1_000_000 + t.microsecond for t in MILLIS_TIMES]


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    root = tmp_path_factory.mktemp("time")
    ids = pa.array([1, 2, 3], pa.int64())
    pq.write_table(pa.table({"id": ids, "t": pa.array(TIMES, pa.time64("us"))}), root / "us.parquet")
    pq.write_table(pa.table({"id": ids, "t": pa.array(MILLIS_TIMES, pa.time32("ms"))}), root / "ms.parquet")
    for name, logical, values in (("us", "time-micros", TIMES), ("ms", "time-millis", MILLIS_TIMES)):
        schema = {
            "type": "record",
            "name": "R",
            "fields": [{"name": "id", "type": "long"}, {"name": "t", "type": {"type": "long" if name == "us" else "int", "logicalType": logical}}],
        }
        with open(root / f"{name}.avro", "wb") as f:
            fastavro.writer(f, schema, [{"id": i + 1, "t": values[i]} for i in range(3)])
    data = orc_file(pa.table({"id": ids, "t": pa.array(MICROS, pa.int64())}), {"t": {"iceberg.long-type": "TIME"}})
    (root / "us.orc").write_bytes(data)
    for kind in ("avro", "orc"):
        (root / f"{kind}_dir").mkdir()
        os.link(root / f"us.{kind}", root / f"{kind}_dir" / f"us.{kind}")
    return root


def _read(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return sorted(out)


@pytest.mark.parametrize(
    "source, expected",
    [
        ("READ_PARQUET('{r}/us.parquet')", MICROS),
        ("READ_PARQUET('{r}/ms.parquet')", MILLIS_MICROS),
        ("READ_AVRO('{r}/us.avro')", MICROS),
        ("READ_AVRO('{r}/ms.avro')", MILLIS_MICROS),
        ("READ_ORC('{r}/us.orc')", MICROS),
        ("'{r}/avro_dir'", MICROS),
        ("'{r}/orc_dir'", MICROS),
    ],
)
def test_a_time_column_is_int64_microseconds_in_every_format(files, source, expected):
    got = _read(f"SELECT id, t FROM {source.format(r=files)}")
    assert got == [(i + 1, expected[i]) for i in range(3)]


def test_every_format_types_it_the_way_parquet_does(files):
    types = {}
    for name, source in (("parquet", "READ_PARQUET('{r}/us.parquet')"), ("avro", "READ_AVRO('{r}/us.avro')"), ("orc", "READ_ORC('{r}/us.orc')")):
        session = opteryx.session()
        for morsel in session.execute_to_morsels(f"SELECT t FROM {source.format(r=files)}"):
            types[name] = str(morsel.schema["t"])
    assert len(set(types.values())) == 1, types


def test_time_arithmetic_and_aggregates_work_on_the_integer(files):
    for source in ("READ_PARQUET('{r}/us.parquet')", "READ_AVRO('{r}/us.avro')", "READ_ORC('{r}/us.orc')"):
        assert _read(f"SELECT MAX(t), MIN(t) FROM {source.format(r=files)}") == [(MICROS[2], MICROS[0])]


def test_a_time_column_casts_to_text_in_every_format(files):
    # The TIME column is an INT64 vector of microseconds (parquet's convention); CAST(... AS VARCHAR) must
    # format it, not demand a TIME64-tagged vector.
    for source in ("READ_PARQUET('{r}/us.parquet')", "READ_AVRO('{r}/us.avro')", "READ_ORC('{r}/us.orc')"):
        assert _read(f"SELECT id, CAST(t AS VARCHAR) FROM {source.format(r=files)}") == [
            (1, "00:00:01.000000"),
            (2, "12:30:45.123456"),
            (3, "23:59:59.999999"),
        ]
