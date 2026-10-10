# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Iceberg manifest lists and manifests written by PyIceberg (deflate, its default
codec), read by rugo's Avro reader and compared entry-for-entry with PyIceberg's own
manifest decoder — the oracle. Scope A of docs/AVRO_READER_DESIGN.md: this is the
read the backend will make in place of `plan_files()`.

PyIceberg and PyArrow are test-only here (CLAUDE.md §4).
"""

import base64
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # noqa: E402
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from pyiceberg.expressions import EqualTo  # noqa: E402
from pyiceberg.partitioning import PartitionField, PartitionSpec  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.transforms import IdentityTransform  # noqa: E402
from pyiceberg.types import DoubleType, LongType, NestedField, StringType  # noqa: E402

from rugo.rugo_native import read_avro, read_avro_metadata  # noqa: E402

ENTRY_COLUMNS = [
    "status",
    "snapshot_id",
    "data_file.content",
    "data_file.file_path",
    "data_file.file_format",
    "data_file.partition",
    "data_file.record_count",
    "data_file.file_size_in_bytes",
    "data_file.null_value_counts",
    "data_file.lower_bounds",
    "data_file.upper_bounds",
]


def _local(uri):
    return uri[len("file://"):] if uri.startswith("file://") else uri


def _read(path, columns=None):
    with open(_local(path), "rb") as f:
        data = f.read()
    res = read_avro(data, columns)
    rows = {n: [] for n in res["column_names"]}
    for batch in res["batches"]:
        for n, v in zip(res["column_names"], batch):
            rows[n].extend(v.to_pylist())
    return data, rows


def _kv(text, decode_bytes=False):
    """An Iceberg map<int,*> (array<record{key,value}> on disk) rendered as JSON."""
    if text is None:
        return None
    return {e["key"]: (base64.b64decode(e["value"]) if decode_bytes else e["value"]) for e in json.loads(text)}


@pytest.fixture(params=["1", "2"], ids=["format-v1", "format-v2"])
def table(request, tmp_path):
    cat = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("n")
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "cat", StringType(), required=False),
        NestedField(3, "v", DoubleType(), required=False),
    )
    spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "cat"))
    t = cat.create_table("n.t", schema=schema, partition_spec=spec,
                         properties={"format-version": request.param})
    for k in range(4):
        n = 25
        t.append(pa.table({
            "id": pa.array([k * 100 + j for j in range(n)], pa.int64()),
            "cat": pa.array([None if j % 11 == 0 else f"c{j % 3}" for j in range(n)]),
            "v": pa.array([None if j % 4 == 0 else j / 3 for j in range(n)]),
        }))
    # A copy-on-write delete rewrites files: manifests then carry DELETED (2) and
    # EXISTING (0) entries as well as ADDED (1).
    t.delete(EqualTo("cat", "c1"))
    return t


def test_manifest_list_matches_pyiceberg(table):
    snap = table.current_snapshot()
    data, got = _read(snap.manifest_list)
    meta = read_avro_metadata(data)["metadata"]
    assert meta["format-version"] == table.metadata.format_version.__str__().encode()

    oracle = snap.manifests(table.io)
    assert got["manifest_path"] == [m.manifest_path for m in oracle]
    assert got["manifest_length"] == [m.manifest_length for m in oracle]
    assert got["partition_spec_id"] == [m.partition_spec_id for m in oracle]
    assert got["added_snapshot_id"] == [m.added_snapshot_id for m in oracle]
    if table.metadata.format_version >= 2:
        assert got["content"] == [int(m.content) for m in oracle]
        assert got["sequence_number"] == [m.sequence_number for m in oracle]
        assert got["min_sequence_number"] == [m.min_sequence_number for m in oracle]


def test_manifest_entries_match_pyiceberg(table):
    snap = table.current_snapshot()
    statuses = set()
    for manifest in snap.manifests(table.io):
        # v1 manifests have no data_file.content (a v2 field). Once the reader schema
        # with defaults is built (docs §4.2/§16) this becomes one read for both.
        v2 = table.metadata.format_version >= 2
        cols = ENTRY_COLUMNS if v2 else [c for c in ENTRY_COLUMNS if c != "data_file.content"]
        _, got = _read(manifest.manifest_path, cols)
        oracle = manifest.fetch_manifest_entry(table.io, discard_deleted=False)
        assert len(got["status"]) == len(oracle)
        for i, e in enumerate(oracle):
            f = e.data_file
            statuses.add(int(e.status))
            assert got["status"][i] == int(e.status)
            assert got["snapshot_id"][i] == e.snapshot_id
            if v2:
                assert got["data_file.content"][i] == int(f.content)
            assert got["data_file.file_path"][i] == f.file_path
            assert got["data_file.file_format"][i] == str(f.file_format.value)
            assert json.loads(got["data_file.partition"][i]) == {"cat": f.partition[0]}
            assert got["data_file.record_count"][i] == f.record_count
            assert got["data_file.file_size_in_bytes"][i] == f.file_size_in_bytes
            assert _kv(got["data_file.null_value_counts"][i]) == f.null_value_counts
            assert _kv(got["data_file.lower_bounds"][i], True) == f.lower_bounds
            assert _kv(got["data_file.upper_bounds"][i], True) == f.upper_bounds
    # The delete must leave EXISTING and DELETED entries, or the test proves less.
    assert statuses >= {0, 2}
