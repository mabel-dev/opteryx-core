# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

import os
import sys

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.aggregations.key_codec import (
    decode_multi_payload_keys,
    decode_single_payload_key,
    smoke_test_native_group_key_codec,
    smoke_test_native_single_encoded_key_codec,
    smoke_test_native_single_fixed_key_codec,
)
from draken.morsels.morsel import Morsel

from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation

KEY_MULTI_FIXED_INT = 1
KEY_MULTI_FIXED_DATE32 = 2
KEY_MULTI_FIXED_TIME32 = 3
KEY_MULTI_FIXED_TIME64 = 4
KEY_MULTI_FIXED_TIMESTAMP64 = 5
KEY_MULTI_ENCODED_STRING = 6


def _normalize_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _rows_by_key(rows, key_columns):
    if isinstance(key_columns, str):
        key_columns = [key_columns]

    out = {}
    for row in rows:
        key = tuple(_normalize_value(row[column]) for column in key_columns)
        out[key] = {k: _normalize_value(v) for k, v in row.items()}
    return out


def _finalize_rows(group_by_columns, aggregations, table):
    morsel = Morsel.from_arrow(table)
    op = ShuffleGroupByOperation(
        group_by_columns=group_by_columns,
        aggregations=aggregations,
    )
    op.ingest(morsel)
    return op.finalize().to_arrow().to_pylist()


def test_native_single_fixed_key_codec_smoke_round_trip():
    decoded_value, decoded_valid_flag, payload_offsets, payload_bytes = (
        smoke_test_native_single_fixed_key_codec()
    )

    assert decoded_value == 123456789
    assert decoded_valid_flag == 1
    assert payload_offsets == [0, 9]
    assert len(payload_bytes) == 9
    assert payload_bytes[0] == 1


def test_native_single_encoded_key_codec_smoke_round_trip():
    decoded_value, decoded_valid_flag, payload_offsets, payload_bytes = (
        smoke_test_native_single_encoded_key_codec()
    )

    assert decoded_value == "hello"
    assert decoded_valid_flag == 1
    assert payload_offsets == [0, 10]
    assert len(payload_bytes) == 10
    assert payload_bytes[0] == 1


def test_native_multi_key_codec_smoke_round_trip():
    (
        decoded_fixed_values,
        decoded_fixed_valids,
        decoded_strings,
        decoded_encoded_valids,
        payload_offsets,
        payload_bytes,
    ) = smoke_test_native_group_key_codec()

    assert decoded_fixed_values == [123456789, 0]
    assert decoded_fixed_valids == [1, 0]
    assert decoded_strings == ["hello", None]
    assert decoded_encoded_valids == [1, 0]
    assert payload_offsets == [0, len(payload_bytes)]
    assert payload_offsets[0] == 0
    assert payload_offsets[-1] == len(payload_bytes)


def test_decode_single_payload_key_fixed_smoke_shape():
    decoded_value, decoded_valid_flag, payload_offsets, payload_bytes = (
        smoke_test_native_single_fixed_key_codec()
    )

    value, valid = decode_single_payload_key(payload_bytes, payload_offsets, 0, KEY_MULTI_FIXED_INT)

    assert decoded_value == value
    assert decoded_valid_flag == valid


def test_decode_single_payload_key_encoded_smoke_shape():
    decoded_value, decoded_valid_flag, payload_offsets, payload_bytes = (
        smoke_test_native_single_encoded_key_codec()
    )

    value, valid = decode_single_payload_key(
        payload_bytes, payload_offsets, 0, KEY_MULTI_ENCODED_STRING
    )

    assert decoded_value == value
    assert decoded_valid_flag == valid


def test_decode_multi_payload_keys_smoke_shape():
    (
        _decoded_fixed_values,
        _decoded_fixed_valids,
        _decoded_strings,
        _decoded_encoded_valids,
        payload_offsets,
        payload_bytes,
    ) = smoke_test_native_group_key_codec()

    values, valids = decode_multi_payload_keys(
        payload_bytes,
        payload_offsets,
        [
            KEY_MULTI_FIXED_INT,
            KEY_MULTI_FIXED_INT,
            KEY_MULTI_ENCODED_STRING,
            KEY_MULTI_ENCODED_STRING,
        ],
        0,
    )

    assert values == (123456789, None, "hello", None)
    assert valids == (1, 0, 1, 0)


def test_single_int64_group_key_storage_rewrite_bypasses_payload_arena_and_groups_correctly():
    table = pa.table(
        {
            "k": pa.array([10, 20, 10, None, 20, 30], type=pa.int64()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, "k")

    assert by_key[(10,)]["cnt"] == 2
    assert by_key[(20,)]["cnt"] == 2
    assert by_key[(30,)]["cnt"] == 1
    assert by_key[(None,)]["cnt"] == 1


def test_multi_fixed_group_key_storage_rewrite_round_trips_int64_and_date32_shapes():
    table = pa.table(
        {
            "planetId": pa.array([3, 3, 4, 4, 4, None], type=pa.int64()),
            "event_date": pa.array([1, 1, 2, 2, 3, 3], type=pa.date32()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["planetId", "event_date"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, ["planetId", "event_date"])

    assert by_key[(3, 1)]["cnt"] == 2
    assert by_key[(4, 2)]["cnt"] == 2
    assert by_key[(4, 3)]["cnt"] == 1
    assert by_key[(None, 3)]["cnt"] == 1


def test_multi_fixed_group_key_storage_rewrite_round_trips_timestamp_shape():
    table = pa.table(
        {
            "ts": pa.array(
                [
                    1_700_000_000_000_000,
                    1_700_000_000_000_000,
                    1_700_000_100_000_000,
                    None,
                ],
                type=pa.timestamp("us"),
            ),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["ts"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, "ts")

    assert len(rows) == 3
    assert sum(row["cnt"] for row in rows) == 4
    assert sorted(row["cnt"] for row in rows) == [1, 1, 2]
    assert sum(1 for row in rows if row["ts"] is None) == 1
    assert sum(1 for row in rows if row["ts"] is not None) == 2


def test_multi_fixed_group_key_storage_rewrite_round_trips_time32_and_time64_shapes():
    table = pa.table(
        {
            "t32": pa.array([1, 1, 2, None], type=pa.time32("s")),
            "t64": pa.array([1000, 1000, 2000, 3000], type=pa.time64("us")),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["t32", "t64"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, ["t32", "t64"])

    assert len(rows) == 3
    assert sum(row["cnt"] for row in rows) == 4
    assert sorted(row["cnt"] for row in rows) == [1, 1, 2]
    assert sum(1 for row in rows if row["t32"] is None) == 1
    assert sum(1 for row in rows if row["t32"] is not None) == 2
    assert sum(1 for row in rows if row["t64"] is None) == 0


def test_multi_mixed_group_key_storage_rewrite_round_trips_fixed_and_encoded_shapes():
    table = pa.table(
        {
            "planetId": pa.array([3, 3, 4, 4, None], type=pa.int64()),
            "name": pa.array(["Moon", "Moon", "Phobos", "Deimos", "Moon"], type=pa.string()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["planetId", "name"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, ["planetId", "name"])

    assert by_key[(3, "Moon")]["cnt"] == 2
    assert by_key[(4, "Phobos")]["cnt"] == 1
    assert by_key[(4, "Deimos")]["cnt"] == 1
    assert by_key[(None, "Moon")]["cnt"] == 1


def test_group_key_storage_rewrite_preserves_empty_and_non_empty_encoded_values():
    table = pa.table(
        {
            "company": pa.array(["A", "A", "B", "B", None], type=pa.string()),
            "rocket": pa.array(["", "Falcon", "", None, ""], type=pa.string()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["company", "rocket"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, ["company", "rocket"])

    assert by_key[("A", "")]["cnt"] == 1
    assert by_key[("A", "Falcon")]["cnt"] == 1
    assert by_key[("B", "")]["cnt"] == 1
    assert by_key[("B", None)]["cnt"] == 1
    assert by_key[(None, "")]["cnt"] == 1


def test_group_key_storage_rewrite_preserves_null_group_keys():
    table = pa.table(
        {
            "company": pa.array(["A", None, "A", None, "B"], type=pa.string()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["company"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, "company")

    assert by_key[("A",)]["cnt"] == 2
    assert by_key[("B",)]["cnt"] == 1
    assert by_key[(None,)]["cnt"] == 2


def test_group_key_storage_rewrite_preserves_nulls_in_multi_key_groups():
    table = pa.table(
        {
            "company": pa.array(["A", "A", None, None, "B"], type=pa.string()),
            "rocket": pa.array([None, "Falcon", None, "Falcon", None], type=pa.string()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["company", "rocket"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    by_key = _rows_by_key(rows, ["company", "rocket"])

    assert by_key[("A", None)]["cnt"] == 1
    assert by_key[("A", "Falcon")]["cnt"] == 1
    assert by_key[(None, None)]["cnt"] == 1
    assert by_key[(None, "Falcon")]["cnt"] == 1
    assert by_key[("B", None)]["cnt"] == 1


def test_group_key_storage_rewrite_groupby_results_are_stable_across_repeated_execution():
    table = pa.table(
        {
            "planetId": pa.array([3, 3, 4, 4, None], type=pa.int64()),
            "name": pa.array(["Moon", "Moon", "Phobos", "Deimos", "Moon"], type=pa.string()),
        }
    )

    first = _finalize_rows(
        group_by_columns=["planetId", "name"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )
    second = _finalize_rows(
        group_by_columns=["planetId", "name"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        table=table,
    )

    assert _rows_by_key(first, ["planetId", "name"]) == _rows_by_key(second, ["planetId", "name"])


def test_group_key_storage_rewrite_multi_aggregate_outputs_match_expected_types_and_values():
    table = pa.table(
        {
            "k": pa.array([1, 1, 2, 2, None], type=pa.int64()),
            "v": pa.array([10.0, 20.0, 5.0, None, 7.0], type=pa.float64()),
        }
    )

    rows = _finalize_rows(
        group_by_columns=["k"],
        aggregations=[
            AggregationSpec(alias="cnt", function="count", column="*"),
            AggregationSpec(alias="sum_v", function="sum", column="v"),
            AggregationSpec(alias="avg_v", function="avg", column="v"),
            AggregationSpec(alias="min_v", function="min", column="v"),
            AggregationSpec(alias="max_v", function="max", column="v"),
        ],
        table=table,
    )
    by_key = _rows_by_key(rows, "k")

    assert by_key[(1,)]["cnt"] == 2
    assert by_key[(1,)]["sum_v"] == 30.0
    assert by_key[(1,)]["avg_v"] == 15.0
    assert by_key[(1,)]["min_v"] == 10.0
    assert by_key[(1,)]["max_v"] == 20.0

    assert by_key[(2,)]["cnt"] == 2
    assert by_key[(2,)]["sum_v"] == 5.0
    assert by_key[(2,)]["avg_v"] == 5.0
    assert by_key[(2,)]["min_v"] == 5.0
    assert by_key[(2,)]["max_v"] == 5.0

    assert by_key[(None,)]["cnt"] == 1
    assert by_key[(None,)]["sum_v"] == 7.0
    assert by_key[(None,)]["avg_v"] == 7.0
    assert by_key[(None,)]["min_v"] == 7.0
    assert by_key[(None,)]["max_v"] == 7.0
