# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Avro writer with a caller-supplied schema, header metadata and JSON-as-nested
columns (docs/AVRO_WRITER_DESIGN.md §15) — the surface Iceberg manifests and manifest
lists are written through. Oracles: rugo's reader, fastavro, and PyIceberg's own Avro
reader (field-id resolution). fastavro / PyIceberg are test-only (CLAUDE.md §4).
"""

import base64
import io
import json
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import fastavro  # noqa: E402

from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo import avro  # noqa: E402

SYNC = bytes(range(16))


def _morsel(**columns):
    """{name: (dtype, values)} -> Morsel."""
    names, vectors = [], []
    for name, (dtype, values) in columns.items():
        names.append(name)
        vectors.append(vector_from_sequence(values, dtype=dtype))
    return Morsel.from_vectors(names, vectors)


def _native_morsel(**columns):
    """{name: native draken Vector} -> Morsel."""
    from draken.vectors.vector import Vector

    return Morsel.from_vectors(list(columns), [Vector(v) for v in columns.values()])


def _fastavro_rows(data):
    return list(fastavro.reader(io.BytesIO(data)))


def _rugo_columns(data):
    out = {}
    with avro.read_avro(data) as r:
        for morsel in r:
            for name in morsel.column_names:
                out.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return out


def _j(v):
    return None if v is None else json.dumps(v)


def _b64(b):
    return base64.b64encode(b).decode()


# An Iceberg v2 manifest_entry, trimmed: data_file is a nested record holding a nested
# partition record, a map (array of {key:int, value:bytes} with logicalType map) and an
# array of required longs — every field carries its field-id / element-id.
DATA_FILE = {
    "type": "record", "name": "r2", "fields": [
        {"name": "content", "field-id": 134, "type": "int"},
        {"name": "file_path", "field-id": 100, "type": "string"},
        {"name": "file_format", "field-id": 101, "type": "string"},
        {"name": "partition", "field-id": 102, "type": {
            "type": "record", "name": "r102", "fields": [
                {"name": "region", "field-id": 1000, "type": ["null", "string"], "default": None}]}},
        {"name": "record_count", "field-id": 103, "type": "long"},
        {"name": "file_size_in_bytes", "field-id": 104, "type": "long"},
        {"name": "lower_bounds", "field-id": 125, "default": None, "type": ["null", {
            "type": "array", "logicalType": "map", "items": {
                "type": "record", "name": "k126_v127", "fields": [
                    {"name": "key", "type": "int", "field-id": 126},
                    {"name": "value", "type": "bytes", "field-id": 127}]}}]},
        {"name": "split_offsets", "field-id": 132, "default": None,
         "type": ["null", {"type": "array", "element-id": 133, "items": "long"}]},
    ]}
MANIFEST_ENTRY = {
    "type": "record", "name": "manifest_entry", "fields": [
        {"name": "status", "field-id": 0, "type": "int"},
        {"name": "snapshot_id", "field-id": 1, "type": ["null", "long"], "default": None},
        {"name": "data_file", "field-id": 2, "type": DATA_FILE},
    ]}

BIG = 2**62 + 1  # beyond 2^53: a double cannot hold it

DATA_FILES = [
    {"content": 0, "file_path": "s3://b/data/a.parquet", "file_format": "PARQUET",
     "partition": {"region": "eu"}, "record_count": BIG, "file_size_in_bytes": 4096,
     "lower_bounds": [{"key": 1, "value": _b64(b"\x00\x01\xff")}, {"key": 2, "value": _b64(b"")}],
     "split_offsets": [4, BIG]},
    {"content": 0, "file_path": "s3://b/data/b.parquet", "file_format": "PARQUET",
     "partition": {"region": None}, "record_count": 7, "file_size_in_bytes": 100,
     "lower_bounds": None, "split_offsets": []},
    # Nullable fields left out are null.
    {"content": 1, "file_path": "s3://b/data/c.parquet", "file_format": "PARQUET",
     "partition": {}, "record_count": 0, "file_size_in_bytes": 1},
]


def _manifest_morsel():
    return _morsel(
        status=("INT32", [1, 1, 2]),
        snapshot_id=("INT64", [BIG, None, -5]),
        data_file=("VARCHAR", [json.dumps(d) for d in DATA_FILES]),
    )


def _manifest(**kw):
    return avro.write_avro(
        _manifest_morsel(), schema=MANIFEST_ENTRY, json_columns=["data_file"], json_bytes="base64",
        metadata={"format-version": "2", "content": "data", "partition-spec-id": b"0"},
        sync_marker=SYNC, **kw)


def _expected_data_file(d):
    full = {"lower_bounds": None, "split_offsets": None, **d}
    full["partition"] = {"region": None, **d["partition"]}
    if full["lower_bounds"] is not None:
        full["lower_bounds"] = [{"key": e["key"], "value": base64.b64decode(e["value"])} for e in full["lower_bounds"]]
    return full


def test_manifest_entry_reads_in_fastavro():
    rows = _fastavro_rows(_manifest())
    assert [r["status"] for r in rows] == [1, 1, 2]
    assert [r["snapshot_id"] for r in rows] == [BIG, None, -5]
    assert [r["data_file"] for r in rows] == [_expected_data_file(d) for d in DATA_FILES]


def test_manifest_entry_round_trips_through_rugo():
    got = _rugo_columns(_manifest())
    assert got["status"] == [1, 1, 2]
    assert got["snapshot_id"] == [BIG, None, -5]
    # rugo reads a nested record back as JSON text, bytes as base64: the input again.
    back = [json.loads(t) for t in got["data_file"]]
    assert back[0]["record_count"] == BIG and back[0]["split_offsets"] == [4, BIG]
    assert back[0]["lower_bounds"] == DATA_FILES[0]["lower_bounds"]
    assert back[1]["partition"] == {"region": None} and back[1]["lower_bounds"] is None
    assert back[2]["lower_bounds"] is None and back[2]["split_offsets"] is None


def test_writer_schema_is_verbatim_with_field_ids_and_required_fields():
    meta = avro.read_metadata(_manifest())
    assert meta.schema == MANIFEST_ENTRY  # field-id / element-id / logicalType map kept
    assert meta.schema["fields"][0]["type"] == "int"  # required: no union
    assert meta.metadata["format-version"] == b"2"
    assert meta.metadata["content"] == b"data"
    assert meta.metadata["partition-spec-id"] == b"0"
    assert meta.metadata["avro.codec"] == b"deflate"


def test_field_id_resolution_by_rugo_reader_schema():
    # A reader schema with other names but the same field-ids resolves by id.
    reader = {"type": "record", "name": "x", "fields": [
        {"name": "st", "field-id": 0, "type": "int"},
        {"name": "snap", "field-id": 1, "type": ["null", "long"]}]}
    with avro.read_avro(_manifest(), reader_schema=reader) as r:
        rows = {}
        for m in r:
            for n in m.column_names:
                rows.setdefault(n.decode(), []).extend(m.column(n).to_pylist())
    assert rows == {"st": [1, 1, 2], "snap": [BIG, None, -5]}


def test_streaming_writes_the_same_bytes_as_one_shot():
    chunks = []
    with avro.open_avro_writer(chunks.append, schema=MANIFEST_ENTRY, json_columns=["data_file"],
                               json_bytes="base64", sync_marker=SYNC,
                               metadata={"format-version": "2", "content": "data",
                                         "partition-spec-id": b"0"}) as w:
        w.write_batch(_manifest_morsel())
    assert b"".join(chunks) == _manifest()


def test_streaming_row_numbers_count_across_batches():
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "j", "type": {"type": "record", "name": "s", "fields": [{"name": "a", "type": "int"}]}}]}
    with pytest.raises(ValueError, match=r"row 2, column 'j'\.a: expected an integer"):
        with avro.open_avro_writer(lambda b: None, schema=schema, json_columns=["j"]) as w:
            w.write_batch(_morsel(j=("VARCHAR", ['{"a": 1}', '{"a": 2}'])))
            w.write_batch(_morsel(j=("VARCHAR", ['{"a": "3"}'])))


def test_columns_match_by_name_in_schema_order():
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "b", "type": "string"}, {"name": "a", "type": "long"}]}
    data = avro.write_avro(_morsel(a=("INT64", [1, 2]), b=("VARCHAR", ["x", "y"])), schema=schema)
    assert _fastavro_rows(data) == [{"b": "x", "a": 1}, {"b": "y", "a": 2}]


# ---- manifest list: plain required scalars, a JSON array of records, nullable bytes ----

MANIFEST_FILE = {"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "field-id": 500, "type": "string"},
    {"name": "manifest_length", "field-id": 501, "type": "long"},
    {"name": "partition_spec_id", "field-id": 502, "type": "int"},
    {"name": "content", "field-id": 517, "type": "int"},
    {"name": "sequence_number", "field-id": 515, "type": "long"},
    {"name": "min_sequence_number", "field-id": 516, "type": "long"},
    {"name": "added_snapshot_id", "field-id": 503, "type": "long"},
    {"name": "added_files_count", "field-id": 504, "type": "int"},
    {"name": "existing_files_count", "field-id": 505, "type": "int"},
    {"name": "deleted_files_count", "field-id": 506, "type": "int"},
    {"name": "added_rows_count", "field-id": 512, "type": "long"},
    {"name": "existing_rows_count", "field-id": 513, "type": "long"},
    {"name": "deleted_rows_count", "field-id": 514, "type": "long"},
    {"name": "partitions", "field-id": 507, "default": None, "type": ["null", {
        "type": "array", "element-id": 508, "items": {
            "type": "record", "name": "r508", "fields": [
                {"name": "contains_null", "field-id": 509, "type": "boolean"},
                {"name": "contains_nan", "field-id": 518, "type": ["null", "boolean"], "default": None},
                {"name": "lower_bound", "field-id": 510, "type": ["null", "bytes"], "default": None},
                {"name": "upper_bound", "field-id": 511, "type": ["null", "bytes"], "default": None}]}}]},
    {"name": "key_metadata", "field-id": 519, "type": ["null", "bytes"], "default": None},
]}

PARTITIONS = [
    [{"contains_null": False, "contains_nan": None, "lower_bound": _b64(b"eu"), "upper_bound": _b64(b"us")}],
    None,
]


def _manifest_list():
    m = _morsel(
        manifest_path=("VARCHAR", ["s3://b/m/1.avro", "s3://b/m/2.avro"]),
        manifest_length=("INT64", [1234, 99]),
        partition_spec_id=("INT32", [0, 0]),
        content=("INT32", [0, 1]),
        sequence_number=("INT64", [1, 2]),
        min_sequence_number=("INT64", [1, 1]),
        added_snapshot_id=("INT64", [BIG, 3]),
        added_files_count=("INT32", [1, 0]),
        existing_files_count=("INT32", [0, 2]),
        deleted_files_count=("INT32", [0, 0]),
        added_rows_count=("INT64", [10, 0]),
        existing_rows_count=("INT64", [0, 20]),
        deleted_rows_count=("INT64", [0, 0]),
        partitions=("VARCHAR", [_j(p) for p in PARTITIONS]),
        key_metadata=("VARBINARY", [None, b"\x01\x02"]),
    )
    return avro.write_avro(m, schema=MANIFEST_FILE, json_columns=["partitions"], json_bytes="base64",
                           metadata={"format-version": "2", "snapshot-id": str(BIG)})


def test_manifest_list_reads_in_fastavro():
    rows = _fastavro_rows(_manifest_list())
    assert [r["added_snapshot_id"] for r in rows] == [BIG, 3]
    assert rows[0]["partitions"] == [{"contains_null": False, "contains_nan": None,
                                      "lower_bound": b"eu", "upper_bound": b"us"}]
    assert rows[1]["partitions"] is None
    assert [r["key_metadata"] for r in rows] == [None, b"\x01\x02"]


def test_manifest_list_and_manifest_read_in_pyiceberg():
    pytest.importorskip("pyiceberg")
    from pyiceberg.avro.file import AvroFile
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.manifest import (
        DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus, ManifestFile,
        MANIFEST_ENTRY_SCHEMAS, MANIFEST_LIST_FILE_SCHEMAS, PartitionFieldSummary,
        data_file_with_partition, manifest_entry_schema_with_data_file)
    from pyiceberg.types import NestedField, StringType, StructType
    from pyiceberg.utils.schema_conversion import AvroSchemaConversion
    import tempfile

    io_ = PyArrowFileIO()
    with tempfile.TemporaryDirectory() as d:
        # Manifest list, read the way PyIceberg reads one (field-id resolution).
        path = os.path.join(d, "snap.avro")
        with open(path, "wb") as f:
            f.write(_manifest_list())
        with AvroFile[ManifestFile](io_.new_input(path), MANIFEST_LIST_FILE_SCHEMAS[2],
                                    read_types={-1: ManifestFile, 508: PartitionFieldSummary}) as r:
            files = list(r)
        assert [f.manifest_path for f in files] == ["s3://b/m/1.avro", "s3://b/m/2.avro"]
        assert files[0].added_snapshot_id == BIG
        assert files[0].partitions[0].lower_bound == b"eu" and files[0].partitions[0].upper_bound == b"us"
        assert files[1].partitions is None and files[1].key_metadata == b"\x01\x02"

        # A manifest written against PyIceberg's own generated v2 schema (every data_file
        # field, logicalType map arrays, element-ids), read back by PyIceberg.
        pt = StructType(NestedField(1000, "region", StringType(), required=False))
        schema = AvroSchemaConversion().iceberg_to_avro(
            manifest_entry_schema_with_data_file(2, data_file_with_partition(pt, 2)), schema_name="manifest_entry")
        df = [{**d, "file_format": "PARQUET"} for d in DATA_FILES]
        m = _morsel(status=("INT32", [1, 1, 2]), snapshot_id=("INT64", [BIG, None, -5]),
                    sequence_number=("INT64", [None, None, None]),
                    file_sequence_number=("INT64", [None, None, None]),
                    data_file=("VARCHAR", [json.dumps(x) for x in df]))
        path = os.path.join(d, "manifest.avro")
        with open(path, "wb") as f:
            f.write(avro.write_avro(m, schema=schema, json_columns=["data_file"], json_bytes="base64"))
        with AvroFile[ManifestEntry](io_.new_input(path), MANIFEST_ENTRY_SCHEMAS[2],
                                     read_types={-1: ManifestEntry, 2: DataFile},
                                     read_enums={0: ManifestEntryStatus, 101: FileFormat,
                                                 134: DataFileContent}) as r:
            entries = list(r)
    assert [e.status for e in entries] == [ManifestEntryStatus.ADDED, ManifestEntryStatus.ADDED,
                                           ManifestEntryStatus.DELETED]
    assert entries[0].snapshot_id == BIG
    assert entries[0].data_file.record_count == BIG
    assert entries[0].data_file.lower_bounds == {1: b"\x00\x01\xff", 2: b""}
    assert entries[0].data_file.split_offsets == [4, BIG]
    assert entries[2].data_file.content == DataFileContent.POSITION_DELETES


# ---- every Avro shape through JSON, against fastavro ----

def test_json_maps_enums_fixed_unions_and_numbers():
    schema = {"type": "record", "name": "r", "fields": [{"name": "j", "type": {
        "type": "record", "name": "s", "fields": [
            {"name": "m", "type": {"type": "map", "values": ["null", "double"]}},
            {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]}},
            {"name": "f", "type": {"type": "fixed", "name": "F", "size": 2}},
            {"name": "u", "type": ["string", "null"]},
            {"name": "fl", "type": "float"},
            {"name": "b", "type": "boolean"},
            {"name": "n", "type": "null"},
            {"name": "nested", "type": {"type": "array", "items": {"type": "array", "items": "int"}}},
        ]}}]}
    rows = [
        {"m": {"x": 1.5, "y": None, "z": 3}, "e": "B", "f": _b64(b"\xab\xcd"), "u": "s", "fl": 0.5,
         "b": True, "n": None, "nested": [[1, -2], [], [2147483647]]},
        {"m": {}, "e": "A", "f": _b64(b"\x00\x00"), "u": None, "fl": -2, "b": False, "n": None, "nested": []},
    ]
    data = avro.write_avro(_morsel(j=("VARCHAR", [json.dumps(r) for r in rows])), schema=schema,
                           json_columns=["j"], json_bytes="base64")
    got = [r["j"] for r in _fastavro_rows(data)]
    assert got[0] == {**rows[0], "f": b"\xab\xcd", "m": {"x": 1.5, "y": None, "z": 3.0}}
    assert got[1] == {**rows[1], "f": b"\x00\x00", "fl": -2.0}


def test_json_null_rows_and_top_level_union():
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "j", "type": ["null", {"type": "array", "items": "long"}]}]}
    data = avro.write_avro(_morsel(j=("VARCHAR", ["[1, 2]", None, "null"])), schema=schema, json_columns=["j"])
    assert [r["j"] for r in _fastavro_rows(data)] == [[1, 2], None, None]


def test_long_extremes_are_exact():
    schema = {"type": "record", "name": "r", "fields": [{"name": "j", "type": {"type": "array", "items": "long"}}]}
    vals = [-(2**63), 2**63 - 1, 2**53 + 1, -(2**53) - 1]
    data = avro.write_avro(_morsel(j=("VARCHAR", [json.dumps(vals)])), schema=schema, json_columns=["j"])
    assert _fastavro_rows(data)[0]["j"] == vals


# ---- plain columns against the schema ----

def test_plain_columns_required_and_nullable():
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "i", "type": "long"},  # INT32 into long: the same varint
        {"name": "s", "type": ["null", "string"]},
        {"name": "a", "type": ["null", {"type": "array", "items": ["null", "long"]}]},
        {"name": "n", "type": ["null", "int"]},
    ]}
    import draken.draken_native as dn

    m = _native_morsel(i=dn.vector_int32_from_sequence([1, -1]),
                       s=vector_from_sequence(["x", None], dtype="VARCHAR"),
                       a=dn.vector_array_from_sequence([[1, None], None]),
                       n=dn.vector_null_from_length(2))
    data = avro.write_avro(m, schema=schema)
    assert _fastavro_rows(data) == [{"i": 1, "s": "x", "a": [1, None], "n": None},
                                    {"i": -1, "s": None, "a": None, "n": None}]
    assert _rugo_columns(data)["i"] == [1, -1]


def test_no_options_is_byte_identical():
    m = _morsel(a=("INT64", [1, None]), s=("VARCHAR", ["x", "y"]))
    assert avro.write_avro(m, sync_marker=SYNC) == avro.write_avro(m, sync_marker=SYNC, metadata=None,
                                                                   schema=None, json_columns=None)


# ---- refusals ----

SIMPLE = {"type": "record", "name": "r", "fields": [{"name": "a", "type": "long"}]}


@pytest.mark.parametrize("columns, kwargs, match", [
    ({"a": ("INT64", [1]), "b": ("INT64", [1])}, {}, r"column 'b' is not a field of the schema"),
    ({"b": ("INT64", [1])}, {}, r"the schema's field 'a' has no column"),
    ({"a": ("VARCHAR", ["1"])}, {}, r"column 'a' is VARCHAR .* but the schema field is long"),
    ({"a": ("INT64", [None])}, {}, r"column 'a' is written as required .* has a null"),
    ({"a": ("INT64", [1])}, {"metadata": {"avro.codec": "x"}}, r"metadata key 'avro.codec' is reserved"),
    ({"a": ("INT64", [1])}, {"metadata": {"rugo.created_by": "x"}}, r"rugo.created_by"),
    ({"a": ("INT64", [1])}, {"json_columns": ["zz"]}, r"json_columns names 'zz'"),
    ({"a": ("INT64", [1])}, {"json_columns": ["a"]}, r"column 'a' is a JSON column but is INT64"),
    ({"a": ("INT64", [1])}, {"json_bytes": "hex"}, r"json_bytes must be 'base64'"),
    ({"a": ("INT64", [1])}, {"record_name": "x"}, r"record_name does not apply with a schema"),
    ({"a": ("INT64", [1])}, {"dictionary": True}, r"dictionary does not apply with a schema"),
])
def test_schema_refusals(columns, kwargs, match):
    with pytest.raises(ValueError, match=match):
        avro.write_avro(_morsel(**columns), schema=SIMPLE, **kwargs)


def test_non_record_and_invalid_schemas_are_refused():
    m = _morsel(a=("INT64", [1]))
    with pytest.raises(ValueError, match=r"must be a record"):
        avro.write_avro(m, schema='"long"')
    with pytest.raises(ValueError, match=r"not valid JSON"):
        avro.write_avro(m, schema="{")
    with pytest.raises(ValueError, match=r"unknown type"):
        avro.write_avro(m, schema={"type": "record", "name": "r", "fields": [{"name": "a", "type": "nope"}]})


def test_json_without_schema_and_reversed_union_plain_are_refused():
    with pytest.raises(ValueError, match=r"json_columns needs a schema"):
        avro.write_avro(_morsel(a=("VARCHAR", ["{}"])), json_columns=["a"])
    with pytest.raises(ValueError, match=r"json_bytes was given but there is no schema"):
        avro.write_avro(_morsel(a=("VARCHAR", ["{}"])), json_bytes="base64")
    rev = {"type": "record", "name": "r", "fields": [{"name": "a", "type": ["long", "null"]}]}
    with pytest.raises(ValueError, match=r"flag it as a JSON column"):
        avro.write_avro(_morsel(a=("INT64", [1])), schema=rev)
    req_items = {"type": "record", "name": "r", "fields": [{"name": "a", "type": {"type": "array", "items": "long"}}]}
    with pytest.raises(ValueError, match=r"plain ARRAY column writes \[\"null\", T\] items"):
        import draken.draken_native as dn

        avro.write_avro(_native_morsel(a=dn.vector_array_from_sequence([[1]])), schema=req_items)


def test_metadata_header_order_and_bytes_values():
    data = avro.write_avro(_morsel(a=("INT64", [1])), metadata={"z": b"\x00\xff", "a": "text"})
    meta = avro.read_metadata(data)
    assert meta.metadata["z"] == b"\x00\xff" and meta.metadata["a"] == b"text"
    assert meta.schema["name"] == "record"  # derived schema still derived


BYTES_SCHEMA = {"type": "record", "name": "r", "fields": [{"name": "j", "type": {
    "type": "record", "name": "s", "fields": [
        {"name": "i", "type": "int"},
        {"name": "b", "type": ["null", "bytes"]},
        {"name": "f", "type": {"type": "fixed", "name": "F", "size": 2}},
        {"name": "l", "type": {"type": "array", "items": "long"}},
        {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A"]}},
        {"name": "m", "type": {"type": "map", "values": "int"}}]}}]}
GOOD = {"i": 1, "b": None, "f": "AAA=", "l": [], "e": "A", "m": {}}


@pytest.mark.parametrize("row, match", [
    ({**GOOD, "i": "1"}, r"row 0, column 'j'\.i: expected an integer, got JSON string"),
    ({**GOOD, "i": 1.0}, r"\.i: expected an integer, got a number with a fraction"),
    ({**GOOD, "i": 2**31}, r"\.i: 2147483648 is beyond the range of an int"),
    ({**GOOD, "l": [1, 2**64]}, r"\.l\[1\]: expected an integer"),
    ({**GOOD, "l": [2**63]}, r"\.l\[0\]: 9223372036854775808 is beyond the range of a long"),
    ({k: v for k, v in GOOD.items() if k != "i"}, r"\.i: the required field is missing"),
    ({**GOOD, "extra": 1}, r"'extra' is not a field of record s"),
    ({**GOOD, "b": "@@@@"}, r"\.b: is not base64"),
    ({**GOOD, "b": "AAA"}, r"\.b: is not base64 \(its length"),
    ({**GOOD, "b": "A=AA"}, r"\.b: is not base64"),
    ({**GOOD, "f": "AA=="}, r"\.f: is 1 bytes but the field is fixed\(2\)"),
    ({**GOOD, "e": "Z"}, r"\.e: 'Z' is not a symbol of enum E"),
    ({**GOOD, "m": {"k": "v"}}, r"\.m\.k: expected an integer"),
    ({**GOOD, "m": []}, r"\.m: expected an object \(map\)"),
    ({**GOOD, "l": None}, r"\.l: expected an array, got JSON null"),
])
def test_json_strict_refusals(row, match):
    with pytest.raises(ValueError, match=match):
        avro.write_avro(_morsel(j=("VARCHAR", [json.dumps(row)])), schema=BYTES_SCHEMA,
                        json_columns=["j"], json_bytes="base64")


def test_json_text_level_refusals():
    with pytest.raises(ValueError, match=r"row 1, column 'j': is not valid JSON"):
        avro.write_avro(_morsel(j=("VARCHAR", [json.dumps(GOOD), "{"])), schema=BYTES_SCHEMA,
                        json_columns=["j"], json_bytes="base64")
    with pytest.raises(ValueError, match=r"row 0, column 'j': is null but its schema field is required"):
        avro.write_avro(_morsel(j=("VARCHAR", [None])), schema=BYTES_SCHEMA,
                        json_columns=["j"], json_bytes="base64")
    with pytest.raises(ValueError, match=r"field 'i' appears twice"):
        avro.write_avro(_morsel(j=("VARCHAR", ['{"i":1,"i":2,"f":"AAA=","l":[],"e":"A","m":{}}'])),
                        schema=BYTES_SCHEMA, json_columns=["j"], json_bytes="base64")
    with pytest.raises(ValueError, match=r"map key 'k' appears twice"):
        avro.write_avro(_morsel(j=("VARCHAR", ['{"i":1,"f":"AAA=","l":[],"e":"A","m":{"k":1,"k":2}}'])),
                        schema=BYTES_SCHEMA, json_columns=["j"], json_bytes="base64")


def test_bytes_in_json_needs_json_bytes():
    with pytest.raises(ValueError, match=r"holds bytes / fixed at 'j\.b'.*json_bytes='base64'"):
        avro.write_avro(_morsel(j=("VARCHAR", [json.dumps(GOOD)])), schema=BYTES_SCHEMA, json_columns=["j"])


def test_long_base64_uses_simd_path_correctly():
    # >= 64 base64 chars takes the NEON / AVX2 decoder; padding still exact.
    for n in (48, 49, 50, 200, 1001):
        raw = bytes((i * 37) & 0xFF for i in range(n))
        schema = {"type": "record", "name": "r", "fields": [{"name": "j", "type": {
            "type": "array", "items": "bytes"}}]}
        data = avro.write_avro(_morsel(j=("VARCHAR", [json.dumps([_b64(raw)])])), schema=schema,
                               json_columns=["j"], json_bytes="base64")
        assert _fastavro_rows(data)[0]["j"] == [raw]


def test_many_rows_across_length_tasks_and_blocks():
    schema = {"type": "record", "name": "r", "fields": [
        {"name": "k", "type": "long"},
        {"name": "j", "type": {"type": "record", "name": "s", "fields": [{"name": "v", "type": "long"}]}}]}
    n = 150_000  # > 2 length tasks of 65,536 rows
    m = _morsel(k=("INT64", list(range(n))), j=("VARCHAR", [f'{{"v": {i * 3}}}' for i in range(n)]))
    data = avro.write_avro(m, schema=schema, json_columns=["j"], block_bytes=1 << 16, max_threads=4,
                           sync_marker=SYNC)
    assert data == avro.write_avro(m, schema=schema, json_columns=["j"], block_bytes=1 << 16,
                                 max_threads=1, sync_marker=SYNC)
    rows = _fastavro_rows(data)
    assert len(rows) == n and rows[-1] == {"k": n - 1, "j": {"v": (n - 1) * 3}}
