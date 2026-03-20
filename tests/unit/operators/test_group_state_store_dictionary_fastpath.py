import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.morsels.morsel import Morsel
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
from opteryx.operators.shuffle import AggregationSpec


def _normalize_key(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _rows_by_key(rows, key_name):
    out = {}
    for row in rows:
        out[_normalize_key(row[key_name])] = row
    return out


def _materialize_dictionary_column(array, value_type):
    return pa.compute.cast(array, value_type)


def test_dictionary_groupby_count_star_fastpath_hit():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None, 1, 2], type=pa.int8()),
        pa.array(["a", "b", "c"], type=pa.string()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )
    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["cnt"] == 2
    assert rows["b"]["cnt"] == 2
    assert rows["c"]["cnt"] == 1
    assert rows[None]["cnt"] == 1
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_dictionary_groupby_count_distinct_fastpath_hit():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 0, 1, 1, 1, None], type=pa.int8()),
        pa.array(["g1", "g2"], type=pa.string()),
    )
    value = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 0, 1, 1], type=pa.int8()),
        pa.array(["x", "y"], type=pa.string()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["g1"]["cd"] == 2
    assert rows["g2"]["cd"] == 2
    assert rows[None]["cd"] == 1
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_dictionary_groupby_count_distinct_raises_for_unsupported_value_type():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 1], type=pa.int8()),
        pa.array(["a", "b"], type=pa.string()),
    )
    value = pa.array([1.0, 1.0, 2.0, None], type=pa.float64())
    morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )

    with pytest.raises(UnsupportedSyntaxError):
        op.ingest(morsel)


def test_numeric_dictionary_groupby_count_star_fastpath_hit():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None, 1, 2], type=pa.int8()),
        pa.array([10, 20, 30], type=pa.int32()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )
    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows[10]["cnt"] == 2
    assert rows[20]["cnt"] == 2
    assert rows[30]["cnt"] == 1
    assert rows[None]["cnt"] == 1
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_float_dictionary_groupby_count_star_raises_for_unsupported_key_type():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None, 1, 2], type=pa.int8()),
        pa.array([10.5, 20.5, 30.5], type=pa.float64()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )

    with pytest.raises(UnsupportedSyntaxError):
        op.ingest(morsel)


def test_numeric_dictionary_groupby_count_distinct_fastpath_hit():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 0, 1, 1, 1, None], type=pa.int8()),
        pa.array([10, 20], type=pa.int32()),
    )
    value = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 0, 1, 1], type=pa.int8()),
        pa.array([1, 2], type=pa.int32()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows[10]["cd"] == 2
    assert rows[20]["cd"] == 2
    assert rows[None]["cd"] == 1
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_float_dictionary_groupby_count_distinct_raises_for_unsupported_key_type():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 0, 1, 1, 1, None], type=pa.int8()),
        pa.array([10.5, 20.5], type=pa.float64()),
    )
    value = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, 0, 1, 1], type=pa.int8()),
        pa.array([1.25, 2.25], type=pa.float64()),
    )
    morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )

    with pytest.raises(UnsupportedSyntaxError):
        op.ingest(morsel)


def test_numeric_dictionary_groupby_aggregate_correctness_all_functions():
    key = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0, None, 1, 2], type=pa.int8()),
        pa.array([10, 20, 30], type=pa.int32()),
    )
    value = pa.array([1.0, 2.0, 3.0, 4.0, None, 6.0], type=pa.float64())
    morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[
            AggregationSpec(alias="cnt", function="count", column="*"),
            AggregationSpec(alias="sum_v", function="sum", column="v"),
            AggregationSpec(alias="avg_v", function="avg", column="v"),
            AggregationSpec(alias="min_v", function="min", column="v"),
            AggregationSpec(alias="max_v", function="max", column="v"),
        ],
    )
    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows[10]["cnt"] == 2
    assert rows[10]["sum_v"] == 4.0
    assert rows[10]["avg_v"] == 2.0
    assert rows[10]["min_v"] == 1.0
    assert rows[10]["max_v"] == 3.0

    assert rows[20]["cnt"] == 2
    assert rows[20]["sum_v"] == 2.0
    assert rows[20]["avg_v"] == 2.0
    assert rows[20]["min_v"] == 2.0
    assert rows[20]["max_v"] == 2.0

    assert rows[30]["cnt"] == 1
    assert rows[30]["sum_v"] == 6.0
    assert rows[30]["avg_v"] == 6.0
    assert rows[30]["min_v"] == 6.0
    assert rows[30]["max_v"] == 6.0

    assert rows[None]["cnt"] == 1
    assert rows[None]["sum_v"] == 4.0
    assert rows[None]["avg_v"] == 4.0
    assert rows[None]["min_v"] == 4.0
    assert rows[None]["max_v"] == 4.0


def test_dictionary_groupby_count_distinct_large_duplicate_codes_parity():
    rows = 10_000
    key_cardinality = 8
    value_cardinality = 256
    key = pa.DictionaryArray.from_arrays(
        pa.array([i % key_cardinality for i in range(rows)], type=pa.int32()),
        pa.array([f"k{i:02d}" for i in range(key_cardinality)], type=pa.string()),
    )
    value = pa.DictionaryArray.from_arrays(
        pa.array([(i * 7) % value_cardinality for i in range(rows)], type=pa.int32()),
        pa.array([f"v{i:03d}" for i in range(value_cardinality)], type=pa.string()),
    )

    dictionary_morsel = Morsel.from_arrow(pa.table({"k": key, "v": value}))
    materialized_morsel = Morsel.from_arrow(
        pa.table(
            {
                "k": _materialize_dictionary_column(key, pa.string()),
                "v": _materialize_dictionary_column(value, pa.string()),
            }
        )
    )

    dict_op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    dict_op.ingest(dictionary_morsel)
    dict_rows = _rows_by_key(dict_op.finalize().to_arrow().to_pylist(), "k")

    materialized_op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    materialized_op.ingest(materialized_morsel)
    materialized_rows = _rows_by_key(materialized_op.finalize().to_arrow().to_pylist(), "k")

    assert dict_rows == materialized_rows
    for i in range(key_cardinality):
        assert dict_rows[f"k{i:02d}"]["cd"] == 32
    assert dict_op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert dict_op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_dictionary_groupby_cross_morsel_local_code_remap_correctness():
    morsel_one = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.DictionaryArray.from_arrays(
                    pa.array([0, 1, 0, None], type=pa.int8()),
                    pa.array(["north", "south"], type=pa.string()),
                )
            }
        )
    )
    # Same logical values, reversed local dictionary codes.
    morsel_two = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.DictionaryArray.from_arrays(
                    pa.array([0, 1, 1], type=pa.int8()),
                    pa.array(["south", "north"], type=pa.string()),
                )
            }
        )
    )

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )
    op.ingest(morsel_one)
    op.ingest(morsel_two)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["north"]["cnt"] == 4
    assert rows["south"]["cnt"] == 2
    assert rows[None]["cnt"] == 1
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0


def test_dictionary_groupby_count_distinct_cross_file_code_remap_correctness():
    # Simulate file A dictionaries.
    morsel_file_a = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.DictionaryArray.from_arrays(
                    pa.array([0, 0, 1], type=pa.int8()),
                    pa.array(["g1", "g2"], type=pa.string()),
                ),
                "v": pa.DictionaryArray.from_arrays(
                    pa.array([0, 1, 0], type=pa.int8()),
                    pa.array(["x", "y"], type=pa.string()),
                ),
            }
        )
    )
    # Simulate file B dictionaries with independent code assignments.
    morsel_file_b = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.DictionaryArray.from_arrays(
                    pa.array([0, 1, 1], type=pa.int8()),
                    pa.array(["g2", "g1"], type=pa.string()),
                ),
                "v": pa.DictionaryArray.from_arrays(
                    pa.array([0, 1, 0], type=pa.int8()),
                    pa.array(["y", "x"], type=pa.string()),
                ),
            }
        )
    )

    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cd", function="count_distinct", column="v")],
    )
    op.ingest(morsel_file_a)
    op.ingest(morsel_file_b)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["g1"]["cd"] == 2
    assert rows["g2"]["cd"] == 2
    assert op.readings["draken_dict_groupby_fastpath_hits"] == 0
    assert op.readings["draken_dict_groupby_fastpath_fallbacks"] == 0
