import datetime
import random

import pyarrow as pa
import pytest

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.morsels.morsel import Morsel
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.operators.shuffle import AggregationSpec
from opteryx.operators.shuffle import ShuffleGroupByOperation


_UNSET = object()


def _decode_name(name: str | bytes | None) -> str | None:
    if name is None:
        return None
    if isinstance(name, bytes):
        return name.decode("utf-8")
    return str(name)


def _normalize_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _normalize_spec(spec: AggregationSpec) -> tuple[str, str, str | None]:
    function = spec.function.lower()
    column = _decode_name(spec.column)
    if column == "*":
        column = None
    return spec.alias, function, column


def _rows_to_table(rows: list[dict], required_columns: list[str]) -> pa.Table:
    if not rows:
        return pa.table({column: [] for column in required_columns})
    return pa.table({column: [row.get(column) for row in rows] for column in required_columns})


def _run_operation(
    rows: list[dict],
    group_by_columns: list[str | bytes],
    aggregations: list[AggregationSpec],
    chunk_size: int | None = None,
):
    group_names = [_decode_name(column) for column in group_by_columns]
    agg_columns = [_decode_name(spec.column) for spec in aggregations if _decode_name(spec.column)]
    required_columns = sorted(set([*group_names, *agg_columns]))

    operation = ShuffleGroupByOperation(
        group_by_columns=group_by_columns,
        aggregations=aggregations,
    )

    if chunk_size is None:
        table = _rows_to_table(rows, required_columns)
        operation.ingest(Morsel.from_arrow(table))
    else:
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            table = _rows_to_table(chunk, required_columns)
            operation.ingest(Morsel.from_arrow(table))

    result = operation.finalize().to_arrow().to_pylist()
    output = {}
    for row in result:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        key = tuple(normalized_row[group_name] for group_name in group_names) if group_names else ()
        output[key] = normalized_row
    return output


def _reference_group_by(
    rows: list[dict],
    group_by_columns: list[str | bytes],
    aggregations: list[AggregationSpec],
):
    group_names = [_decode_name(column) for column in group_by_columns]
    normalized = [_normalize_spec(spec) for spec in aggregations]

    states: dict[tuple, dict[str, object]] = {}
    for row in rows:
        key = tuple(row.get(group_name) for group_name in group_names) if group_names else ()
        if key not in states:
            key_state = {}
            for alias, function, _column in normalized:
                if function == "count":
                    key_state[alias] = 0
                elif function in ("sum", "min", "max"):
                    key_state[alias] = None
                elif function in ("mean", "avg"):
                    key_state[alias] = [0, 0]
                elif function in ("count_distinct", "distinct"):
                    key_state[alias] = set()
                elif function == "hash_one":
                    key_state[alias] = _UNSET
                else:  # pragma: no cover
                    raise ValueError(f"unsupported function in reference path: {function}")
            states[key] = key_state

        key_state = states[key]
        for alias, function, column in normalized:
            value = _UNSET if column is None else row.get(column)
            if function == "count":
                if value is _UNSET or value is not None:
                    key_state[alias] += 1
            elif function == "sum":
                if value is None:
                    continue
                key_state[alias] = value if key_state[alias] is None else key_state[alias] + value
            elif function == "min":
                if value is None:
                    continue
                if key_state[alias] is None or value < key_state[alias]:
                    key_state[alias] = value
            elif function == "max":
                if value is None:
                    continue
                if key_state[alias] is None or value > key_state[alias]:
                    key_state[alias] = value
            elif function in ("mean", "avg"):
                if value is None:
                    continue
                key_state[alias][0] += value
                key_state[alias][1] += 1
            elif function in ("count_distinct", "distinct"):
                if value is None:
                    continue
                key_state[alias].add(value)
            elif function == "hash_one" and key_state[alias] is _UNSET and value is not None:
                key_state[alias] = value

    if not states:
        empty_key = ()
        states[empty_key] = {}
        for alias, function, _column in normalized:
            if function == "count":
                states[empty_key][alias] = 0
            elif function in ("sum", "min", "max"):
                states[empty_key][alias] = None
            elif function in ("mean", "avg"):
                states[empty_key][alias] = [0, 0]
            elif function in ("count_distinct", "distinct"):
                states[empty_key][alias] = set()
            elif function == "hash_one":
                states[empty_key][alias] = _UNSET

    result = {}
    for key, key_state in states.items():
        row = {}
        for alias, function, _column in normalized:
            state = key_state[alias]
            if function in ("mean", "avg"):
                row[alias] = None if state[1] == 0 else state[0] / state[1]
            elif function in ("count_distinct", "distinct"):
                row[alias] = len(state)
            elif function == "hash_one":
                row[alias] = None if state is _UNSET else state
            else:
                row[alias] = state

        for idx, group_name in enumerate(group_names):
            row[group_name] = key[idx]
        result[key] = row
    return result


def _assert_group_by_matches_reference(
    rows: list[dict],
    group_by_columns: list[str | bytes],
    aggregations: list[AggregationSpec],
    chunk_sizes: list[int] | None = None,
):
    expected = _reference_group_by(rows, group_by_columns, aggregations)

    actual_single = _run_operation(rows, group_by_columns, aggregations, chunk_size=None)
    assert actual_single == expected

    for chunk_size in chunk_sizes or []:
        actual_chunked = _run_operation(rows, group_by_columns, aggregations, chunk_size=chunk_size)
        assert actual_chunked == expected

    # also exercise the phase1 prototype (V2) if available
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    def _run_v2(rows, group_by_columns, aggregations, chunk_size=None):
        # replicate required_columns calculation from _run_operation
        group_names = [_decode_name(column) for column in group_by_columns]
        agg_columns = [_decode_name(spec.column) for spec in aggregations if _decode_name(spec.column)]
        required_columns = sorted(set([*group_names, *agg_columns]))

        operation = ShuffleGroupByOperationV2(
            group_by_columns=group_by_columns,
            aggregations=aggregations,
        )
        if chunk_size is None:
            table = _rows_to_table(rows, required_columns)
            operation.ingest(Morsel.from_arrow(table))
        else:
            for start in range(0, len(rows), chunk_size):
                chunk = rows[start : start + chunk_size]
                table = _rows_to_table(chunk, required_columns)
                operation.ingest(Morsel.from_arrow(table))
        result = operation.finalize().to_arrow().to_pylist()
        output = {}
        for row in result:
            normalized_row = {name: _normalize_value(value) for name, value in row.items()}
            if group_names:
                key = tuple(normalized_row[group_name] for group_name in group_names)
            else:
                key = ()
            output[key] = normalized_row
        return output

    try:
        actual_v2 = _run_v2(rows, group_by_columns, aggregations)
    except UnsupportedSyntaxError:
        return
    assert actual_v2 == expected


def test_phase1_golden_multi_key_multi_aggregate():
    rows = [
        {"k1": 1, "k2": "a", "v": 10, "t": "x"},
        {"k1": 1, "k2": "a", "v": None, "t": "x"},
        {"k1": 1, "k2": "b", "v": 5, "t": "y"},
        {"k1": 2, "k2": "b", "v": 7, "t": "y"},
        {"k1": 2, "k2": "b", "v": 3, "t": None},
        {"k1": None, "k2": "z", "v": 2, "t": "m"},
        {"k1": None, "k2": "z", "v": 8, "t": "n"},
        {"k1": None, "k2": "z", "v": None, "t": "n"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="cnt_v", function="count", column="v"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="min_v", function="min", column="v"),
        AggregationSpec(alias="max_v", function="max", column="v"),
        AggregationSpec(alias="avg_v", function="avg", column="v"),
        AggregationSpec(alias="distinct_t", function="count_distinct", column="t"),
        AggregationSpec(alias="one_t", function="hash_one", column="t"),
    ]
    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k1", "k2"],
        aggregations=aggregations,
        chunk_sizes=[1, 2, 3],
    )


def test_phase1_carchar_multi_key_fixed_width_stays_compiled():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k1": 1, "k2": 10, "v": 5, "t": "a"},
        {"k1": 1, "k2": 10, "v": 7, "t": "b"},
        {"k1": 1, "k2": 11, "v": None, "t": "a"},
        {"k1": 2, "k2": 10, "v": 3, "t": "a"},
        {"k1": 2, "k2": 10, "v": 4, "t": "c"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="distinct_t", function="count_distinct", column="t"),
    ]

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k1", "k2"],
        aggregations=aggregations,
    )
    table = _rows_to_table(rows, ["k1", "k2", "v", "t"])
    operation.ingest(Morsel.from_arrow(table))
    result = operation.finalize().to_arrow().to_pylist()

    normalized = {
        (row["k1"], row["k2"]): {name: _normalize_value(value) for name, value in row.items()}
        for row in result
    }
    expected = _reference_group_by(rows, ["k1", "k2"], aggregations)
    assert normalized == expected
    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0
    assert operation.readings["feature_groupby_engine_multi_key_fixed"] == 1


def test_phase1_carchar_multi_key_temporal_fixed_width_stays_compiled():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"ts": datetime.datetime(2024, 1, 1, 12, 0, 0), "k2": 1, "v": 5},
        {"ts": datetime.datetime(2024, 1, 1, 12, 0, 0), "k2": 1, "v": 7},
        {"ts": datetime.datetime(2024, 1, 1, 12, 1, 0), "k2": 1, "v": 3},
        {"ts": datetime.datetime(2024, 1, 1, 12, 1, 0), "k2": 2, "v": 4},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
    ]

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["ts", "k2"],
        aggregations=aggregations,
    )
    table = _rows_to_table(rows, ["ts", "k2", "v"])
    operation.ingest(Morsel.from_arrow(table))
    result = operation.finalize().to_arrow().to_pylist()

    normalized = {
        (_normalize_value(row["ts"]), row["k2"]): {name: _normalize_value(value) for name, value in row.items()}
        for row in result
    }
    expected = _reference_group_by(rows, ["ts", "k2"], aggregations)
    assert normalized == expected
    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0
    assert operation.readings["feature_groupby_engine_multi_key_fixed"] == 1


def test_phase1_global_aggregate_non_empty_and_empty():
    non_empty_rows = [
        {"v": 4, "t": "a"},
        {"v": None, "t": "b"},
        {"v": 8, "t": "a"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="avg_v", function="mean", column="v"),
        AggregationSpec(alias="distinct_t", function="distinct", column="t"),
        AggregationSpec(alias="one_t", function="hash_one", column="t"),
    ]
    _assert_group_by_matches_reference(
        non_empty_rows,
        group_by_columns=[],
        aggregations=aggregations,
        chunk_sizes=[1, 2],
    )

    _assert_group_by_matches_reference(
        [],
        group_by_columns=[],
        aggregations=aggregations,
        chunk_sizes=[],
    )


def test_phase1_randomized_chunking_invariance():
    rng = random.Random(9223372036854775807)
    rows = []
    for index in range(1200):
        key_value = None if rng.random() < 0.1 else rng.randint(0, 127)
        metric_value = None if rng.random() < 0.35 else rng.randint(-25, 250)
        tag_value = None if rng.random() < 0.2 else f"tag_{rng.randint(0, 31)}"
        rows.append({"k": key_value, "v": metric_value, "t": tag_value, "i": index})

    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="cnt_v", function="count", column="v"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="avg_v", function="avg", column="v"),
        AggregationSpec(alias="min_v", function="min", column="v"),
        AggregationSpec(alias="max_v", function="max", column="v"),
        AggregationSpec(alias="distinct_t", function="count_distinct", column="t"),
        AggregationSpec(alias="one_t", function="hash_one", column="t"),
    ]

    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k"],
        aggregations=aggregations,
        chunk_sizes=[1, 3, 17, 64, 255],
    )


def test_phase1_high_cardinality_groups():
    row_count = 5000
    rows = [{"k": index, "v": index % 17} for index in range(row_count)]

    aggregations = [AggregationSpec(alias="cnt", function="count", column="*")]
    actual = _run_operation(rows, group_by_columns=["k"], aggregations=aggregations, chunk_size=97)

    assert len(actual) == row_count
    assert all(row["cnt"] == 1 for row in actual.values())


def test_phase1_null_heavy_semantics():
    rng = random.Random(1977)
    rows = []
    for _ in range(2000):
        key = None if rng.random() < 0.6 else rng.randint(1, 8)
        value = None if rng.random() < 0.8 else rng.randint(1, 20)
        rows.append({"k": key, "v": value, "t": None if value is None else f"v{value}"})

    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="cnt_v", function="count", column="v"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="min_v", function="min", column="v"),
        AggregationSpec(alias="max_v", function="max", column="v"),
        AggregationSpec(alias="avg_v", function="mean", column="v"),
        AggregationSpec(alias="distinct_t", function="count_distinct", column="t"),
    ]
    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k"],
        aggregations=aggregations,
        chunk_sizes=[11, 128],
    )


def test_phase1_from_legacy_aggregate_functions_matches_reference():
    rows = [
        {"k": 1, "v": 5, "tag": "a"},
        {"k": 1, "v": 7, "tag": "a"},
        {"k": 2, "v": None, "tag": "b"},
        {"k": 2, "v": 3, "tag": "c"},
    ]

    aggregate_functions = [
        ("*", "count", None),
        ("v", "sum", None),
        ("tag", "count_distinct", None),
    ]
    aliases = ["cnt_all", "sum_v", "distinct_tag"]

    operation = ShuffleGroupByOperation.from_legacy_aggregate_functions(
        group_by_columns=["k"],
        aggregate_functions=aggregate_functions,
        aliases=aliases,
    )

    required_columns = ["k", "v", "tag"]
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns)))

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {(row["k"],): row for row in actual_rows}

    expected = _reference_group_by(
        rows,
        group_by_columns=["k"],
        aggregations=[
            AggregationSpec(alias="cnt_all", function="count", column="*"),
            AggregationSpec(alias="sum_v", function="sum", column="v"),
            AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag"),
        ],
    )
    assert actual == expected


def test_phase1_v2_single_int64_count_star_with_null_keys():
    rows = [
        {"k": 1},
        {"k": 2},
        {"k": 1},
        {"k": None},
        {"k": 2},
        {"k": None},
    ]
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k"],
        aggregations=aggregations,
        chunk_sizes=[1, 2, 4],
    )


def test_phase1_v2_single_string_key_uses_compiled_carchar_mode():
    rows = [
        {"k": "a", "v": 1.5},
        {"k": "a", "v": 2.0},
        {"k": "b", "v": None},
        {"k": "b", "v": 5.0},
    ]
    aggregations = [AggregationSpec(alias="sum_v", function="sum", column="v")]
    from opteryx.operators.group_state_store import ShuffleGroupByOperationV2

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "v"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {
        (_normalize_value(row["k"]),): {name: _normalize_value(value) for name, value in row.items()}
        for row in actual_rows
    }
    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_unsupported_count_value_type_errors_when_carchar_path_is_selected():
    rows = [
        {"k": 1, "v": "x"},
        {"k": 1, "v": None},
        {"k": 2, "v": "y"},
        {"k": 2, "v": "z"},
    ]
    aggregations = [AggregationSpec(alias="cnt_v", function="count", column="v")]
    from opteryx.operators.group_state_store import ShuffleGroupByOperationV2

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    with pytest.raises(UnsupportedSyntaxError):
        operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "v"])))


def test_phase1_v2_single_sum_matches_reference():
    rows = [
        {"k": 1, "v": 1.25},
        {"k": 2, "v": 3.0},
        {"k": 1, "v": 2.75},
        {"k": 3, "v": None},
    ]
    aggregations = [AggregationSpec(alias="sum_v", function="sum", column="v")]
    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k"],
        aggregations=aggregations,
        chunk_sizes=[1, 2],
    )


def test_phase1_v2_single_key_multi_aggregate_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 10},
        {"k": 1, "v": 5},
        {"k": 2, "v": 3},
        {"k": 2, "v": None},
        {"k": None, "v": 7},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="min_v", function="min", column="v"),
        AggregationSpec(alias="max_v", function="max", column="v"),
        AggregationSpec(alias="avg_v", function="avg", column="v"),
    ]

    group_names = ["k"]
    table = _rows_to_table(rows, required_columns=["k", "v"])
    operation = ShuffleGroupByOperationV2(group_by_columns=group_names, aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(table))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=group_names, aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_single_key_multi_aggregate_with_count_distinct_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 10, "tag": "a"},
        {"k": 1, "v": 5, "tag": "a"},
        {"k": 2, "v": 3, "tag": "b"},
        {"k": 2, "v": None, "tag": None},
        {"k": None, "v": 7, "tag": "z"},
        {"k": None, "v": 9, "tag": "q"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag"),
    ]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "v", "tag"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_dictionary_key_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    key_dict = pa.array(["alpha", "beta", "gamma"], type=pa.string())
    key_indices = pa.array([0, 1, 0, 2, 1, 2], type=pa.int32())
    key_array = pa.DictionaryArray.from_arrays(key_indices, key_dict)
    value_array = pa.array([10, 5, 7, 1, None, 4], type=pa.int64())
    morsel = Morsel.from_arrow(pa.table({"k": key_array, "v": value_array}))

    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
    ]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [
            {"k": "alpha", "v": 10},
            {"k": "beta", "v": 5},
            {"k": "alpha", "v": 7},
            {"k": "gamma", "v": 1},
            {"k": "beta", "v": None},
            {"k": "gamma", "v": 4},
        ],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_numeric_dictionary_key_count_star_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    key_dict = pa.array([101, 202, 303], type=pa.int64())
    key_indices = pa.array([0, 1, 0, 2, 1, 2, 2], type=pa.int32())
    key_array = pa.DictionaryArray.from_arrays(key_indices, key_dict)
    morsel = Morsel.from_arrow(pa.table({"k": key_array}))

    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [
            {"k": 101},
            {"k": 202},
            {"k": 101},
            {"k": 303},
            {"k": 202},
            {"k": 303},
            {"k": 303},
        ],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_dictionary_key_multi_aggregate_with_count_distinct_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    key_dict = pa.array(["alpha", "beta", "gamma"], type=pa.string())
    key_indices = pa.array([0, 1, 0, 2, 1, 2, 2], type=pa.int32())
    key_array = pa.DictionaryArray.from_arrays(key_indices, key_dict)
    value_array = pa.array([10, 5, 7, 1, None, 4, 9], type=pa.int64())
    tag_dict = pa.array(["red", "blue", "green"], type=pa.string())
    tag_indices = pa.array([0, 1, 0, 2, 1, 2, 0], type=pa.int32())
    tag_array = pa.DictionaryArray.from_arrays(tag_indices, tag_dict)
    morsel = Morsel.from_arrow(pa.table({"k": key_array, "v": value_array, "tag": tag_array}))

    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag"),
    ]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [
            {"k": "alpha", "v": 10, "tag": "red"},
            {"k": "beta", "v": 5, "tag": "blue"},
            {"k": "alpha", "v": 7, "tag": "red"},
            {"k": "gamma", "v": 1, "tag": "green"},
            {"k": "beta", "v": None, "tag": "blue"},
            {"k": "gamma", "v": 4, "tag": "green"},
            {"k": "gamma", "v": 9, "tag": "red"},
        ],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_mixed_numeric_dictionary_and_string_dictionary_multi_key_stays_compiled():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    user_dict = pa.array([101, 202, 303], type=pa.int64())
    user_indices = pa.array([0, 1, 0, 2, 1, 2, 2], type=pa.int32())
    user_array = pa.DictionaryArray.from_arrays(user_indices, user_dict)

    phrase_dict = pa.array(["alpha", "beta", "gamma"], type=pa.string())
    phrase_indices = pa.array([0, 1, 0, 2, 1, 2, 0], type=pa.int32())
    phrase_array = pa.DictionaryArray.from_arrays(phrase_indices, phrase_dict)

    minute_array = pa.array([1, 2, 1, 3, 2, 3, 1], type=pa.int64())
    morsel = Morsel.from_arrow(pa.table({"uid": user_array, "m": minute_array, "phrase": phrase_array}))

    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    operation = ShuffleGroupByOperationV2(
        group_by_columns=["uid", "m", "phrase"],
        aggregations=aggregations,
    )
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["uid"], normalized_row["m"], normalized_row["phrase"])] = normalized_row

    expected = _reference_group_by(
        [
            {"uid": 101, "m": 1, "phrase": "alpha"},
            {"uid": 202, "m": 2, "phrase": "beta"},
            {"uid": 101, "m": 1, "phrase": "alpha"},
            {"uid": 303, "m": 3, "phrase": "gamma"},
            {"uid": 202, "m": 2, "phrase": "beta"},
            {"uid": 303, "m": 3, "phrase": "gamma"},
            {"uid": 303, "m": 1, "phrase": "alpha"},
        ],
        group_by_columns=["uid", "m", "phrase"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_single_count_distinct_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "tag": "a"},
        {"k": 1, "tag": "a"},
        {"k": 1, "tag": "b"},
        {"k": 2, "tag": "b"},
        {"k": 2, "tag": None},
        {"k": None, "tag": "z"},
        {"k": None, "tag": "z"},
        {"k": None, "tag": "q"},
    ]
    aggregations = [AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag")]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "tag"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_dictionary_key_count_distinct_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    key_dict = pa.array(["alpha", "beta", "gamma"], type=pa.string())
    key_indices = pa.array([0, 1, 0, 2, 1, 2, 2], type=pa.int32())
    key_array = pa.DictionaryArray.from_arrays(key_indices, key_dict)

    value_dict = pa.array(["red", "blue", "green"], type=pa.string())
    value_indices = pa.array([0, 1, 0, 2, 1, 2, 0], type=pa.int32())
    value_array = pa.DictionaryArray.from_arrays(value_indices, value_dict)
    morsel = Morsel.from_arrow(pa.table({"k": key_array, "v": value_array}))

    aggregations = [AggregationSpec(alias="distinct_v", function="count_distinct", column="v")]
    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [
            {"k": "alpha", "v": "red"},
            {"k": "beta", "v": "blue"},
            {"k": "alpha", "v": "red"},
            {"k": "gamma", "v": "green"},
            {"k": "beta", "v": "blue"},
            {"k": "gamma", "v": "green"},
            {"k": "gamma", "v": "red"},
        ],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_single_count_star_string_key_matches_reference():
    rows = [
        {"k": "a"},
        {"k": "b"},
        {"k": "a"},
        {"k": None},
        {"k": "b"},
        {"k": "b"},
    ]
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    _assert_group_by_matches_reference(
        rows,
        group_by_columns=["k"],
        aggregations=aggregations,
        chunk_sizes=[1, 2, 3],
    )


def test_phase1_v2_single_count_star_string_key_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": "a"},
        {"k": "b"},
        {"k": "a"},
        {"k": None},
        {"k": "b"},
        {"k": "b"},
    ]
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_multi_key_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k1": 1, "k2": "a", "v": 10, "tag": "x"},
        {"k1": 1, "k2": "a", "v": None, "tag": "x"},
        {"k1": 1, "k2": "b", "v": 5, "tag": "y"},
        {"k1": 2, "k2": "b", "v": 7, "tag": "y"},
        {"k1": 2, "k2": "b", "v": 3, "tag": None},
        {"k1": None, "k2": "z", "v": 2, "tag": "m"},
        {"k1": None, "k2": "z", "v": 8, "tag": "n"},
        {"k1": None, "k2": "z", "v": None, "tag": "n"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="sum_v", function="sum", column="v"),
        AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag"),
    ]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k1", "k2"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k1", "k2", "v", "tag"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k1"], normalized_row["k2"])] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k1", "k2"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_single_large_string_key_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.array(["alpha", "beta", "alpha", None, "beta"], type=pa.large_string()),
            }
        )
    )
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [{"k": "alpha"}, {"k": "beta"}, {"k": "alpha"}, {"k": None}, {"k": "beta"}],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_single_timestamp_key_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    morsel = Morsel.from_arrow(
        pa.table(
            {
                "k": pa.array([1_000_000, 1_000_000, 2_000_000, None], type=pa.timestamp("us")),
            }
        )
    )
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]
    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(
        [{"k": 1_000_000}, {"k": 1_000_000}, {"k": 2_000_000}, {"k": None}],
        group_by_columns=["k"],
        aggregations=aggregations,
    )
    assert actual == expected


def test_phase1_v2_constant_key_uses_compiled_constant_mode():
    try:
        from opteryx.draken.interop.arrow import vector_from_arrow
        from opteryx.draken.vectors.constant_vector import from_scalar as constant_from_scalar
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    value_arr = pa.array([1, 2, 3, 4], type=pa.int64())
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            constant_from_scalar("g", 4, dtype=pa.string()),
            vector_from_arrow(value_arr),
        ],
    )

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt_all", function="count", column="*")],
    )
    operation.ingest(morsel)

    assert operation.readings["feature_groupby_engine_constant"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0
    rows = operation.finalize().to_arrow().to_pylist()
    normalized = [{name: _normalize_value(value) for name, value in row.items()} for row in rows]
    assert normalized == [{"cnt_all": 4, "k": "g"}]


def test_phase1_v2_finalize_morsels_chunking_matches_finalize():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": "a", "v": 1},
        {"k": "b", "v": 2},
        {"k": "a", "v": 3},
        {"k": "c", "v": 4},
        {"k": "d", "v": 5},
    ]
    aggregations = [AggregationSpec(alias="cnt_all", function="count", column="*")]

    table = _rows_to_table(rows, required_columns=["k"])
    morsel = Morsel.from_arrow(table)

    op_single = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    op_single.ingest(morsel)
    single = op_single.finalize().to_arrow().to_pylist()

    op_chunked = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    op_chunked.ingest(morsel)
    chunked_tables = [m.to_arrow() for m in op_chunked.finalize_morsels(chunk_size=2)]
    chunked_rows = []
    for t in chunked_tables:
        chunked_rows.extend(t.to_pylist())

    assert sorted(single, key=lambda r: r["k"]) == sorted(chunked_rows, key=lambda r: r["k"])


def test_phase1_v2_empty_groupby_does_not_fallback_to_legacy():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    morsel = Morsel.from_arrow(_rows_to_table([], required_columns=["k", "v"]))
    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt_all", function="count", column="*")],
    )
    operation.ingest(morsel)

    rows = operation.finalize().to_arrow().to_pylist()

    assert rows == []
    assert operation.readings["feature_groupby_engine_legacy"] == 0


def test_phase1_v2_single_avg_uses_fast_columns_when_null_free():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 1.0},
        {"k": 1, "v": 3.0},
        {"k": 2, "v": 10.0},
        {"k": 2, "v": 14.0},
    ]
    table = _rows_to_table(rows, required_columns=["k", "v"])
    morsel = Morsel.from_arrow(table)

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="avg_v", function="avg", column="v")],
    )
    operation.ingest(morsel)
    fast_columns = operation._backend.finalize_fast_columns()

    assert fast_columns is not None
    keys, values = fast_columns
    actual = {int(k): float(v) for k, v in zip(keys, values)}
    assert actual == {1: 2.0, 2: 12.0}


def test_phase1_v2_single_avg_disables_fast_columns_with_all_null_group():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 2.0},
        {"k": 1, "v": 4.0},
        {"k": 2, "v": None},
    ]
    table = _rows_to_table(rows, required_columns=["k", "v"])
    morsel = Morsel.from_arrow(table)

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="avg_v", function="avg", column="v")],
    )
    operation.ingest(morsel)
    assert operation._backend.finalize_fast_columns() is None

    rows = operation.finalize().to_arrow().to_pylist()
    rows_by_key = {row["k"]: row for row in rows}
    assert rows_by_key[1]["avg_v"] == 3.0
    assert rows_by_key[2]["avg_v"] is None


def test_phase1_v2_single_sum_uses_fast_columns_when_null_free():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 2},
        {"k": 1, "v": 3},
        {"k": 2, "v": 10},
        {"k": 2, "v": 20},
    ]
    table = _rows_to_table(rows, required_columns=["k", "v"])
    morsel = Morsel.from_arrow(table)

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )
    operation.ingest(morsel)
    fast_columns = operation._backend.finalize_fast_columns()

    assert fast_columns is not None
    keys, values = fast_columns
    actual = {int(k): int(v) for k, v in zip(keys, values)}
    assert actual == {1: 5, 2: 30}


def test_phase1_v2_single_sum_disables_fast_columns_with_all_null_group():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": 1, "v": 2},
        {"k": 1, "v": 3},
        {"k": 2, "v": None},
    ]
    table = _rows_to_table(rows, required_columns=["k", "v"])
    morsel = Morsel.from_arrow(table)

    operation = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )
    operation.ingest(morsel)
    assert operation._backend.finalize_fast_columns() is None

    rows = operation.finalize().to_arrow().to_pylist()
    rows_by_key = {row["k"]: row for row in rows}
    assert rows_by_key[1]["sum_v"] == 5
    assert rows_by_key[2]["sum_v"] is None


def test_phase1_v2_string_min_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": "alpha", "v": "zeta"},
        {"k": "alpha", "v": "beta"},
        {"k": "beta", "v": "delta"},
        {"k": "beta", "v": None},
        {"k": None, "v": "omega"},
        {"k": None, "v": "eta"},
    ]
    aggregations = [AggregationSpec(alias="min_v", function="min", column="v")]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "v"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


def test_phase1_v2_string_min_multi_aggregate_uses_compiled_carchar_mode():
    try:
        from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
    except ImportError:
        return

    rows = [
        {"k": "alpha", "v": "zeta", "tag": "x"},
        {"k": "alpha", "v": "beta", "tag": "x"},
        {"k": "beta", "v": "delta", "tag": "y"},
        {"k": "beta", "v": None, "tag": None},
        {"k": None, "v": "omega", "tag": "m"},
        {"k": None, "v": "eta", "tag": "n"},
    ]
    aggregations = [
        AggregationSpec(alias="cnt_all", function="count", column="*"),
        AggregationSpec(alias="min_v", function="min", column="v"),
        AggregationSpec(alias="distinct_tag", function="count_distinct", column="tag"),
    ]

    operation = ShuffleGroupByOperationV2(group_by_columns=["k"], aggregations=aggregations)
    operation.ingest(Morsel.from_arrow(_rows_to_table(rows, required_columns=["k", "v", "tag"])))

    assert operation.readings["feature_groupby_engine_carchar"] == 1
    assert operation.readings["feature_groupby_engine_legacy"] == 0

    actual_rows = operation.finalize().to_arrow().to_pylist()
    actual = {}
    for row in actual_rows:
        normalized_row = {name: _normalize_value(value) for name, value in row.items()}
        actual[(normalized_row["k"],)] = normalized_row

    expected = _reference_group_by(rows, group_by_columns=["k"], aggregations=aggregations)
    assert actual == expected


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
