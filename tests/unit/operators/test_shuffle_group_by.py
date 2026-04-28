import pyarrow as pa
from draken.morsels.morsel import Morsel

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import (
    AggregationSpec,
    ShuffleGroupByOperation,
    group_by_post_shuffle,
)
from opteryx.operators.shuffle_node import ShuffleNode


def _morsel_from_dict(values: dict):
    return Morsel.from_arrow(pa.table(values))


def _group_by_to_rows(morsel):
    return morsel.to_arrow().to_pylist()


def test_shuffle_group_by_single_morsel_multiple_aggregates():
    morsel = _morsel_from_dict(
        {
            "k": [1, 1, 2, 2, 2],
            "v": [10, 20, 7, None, 8],
            "c": ["a", "a", "b", "c", "b"],
        }
    )
    operation = ShuffleGroupByOperation(
        group_by_columns=["k"],
        aggregations=[
            AggregationSpec(alias="cnt_all", function="count", column="*"),
            AggregationSpec(alias="cnt_v", function="count", column="v"),
            AggregationSpec(alias="sum_v", function="sum", column="v"),
            AggregationSpec(alias="min_v", function="min", column="v"),
            AggregationSpec(alias="max_v", function="max", column="v"),
            AggregationSpec(alias="avg_v", function="mean", column="v"),
            AggregationSpec(alias="distinct_c", function="count_distinct", column="c"),
        ],
    )
    operation.ingest(morsel)

    rows = _group_by_to_rows(operation.finalize())
    rows_by_key = {row["k"]: row for row in rows}

    assert rows_by_key[1]["cnt_all"] == 2
    assert rows_by_key[1]["cnt_v"] == 2
    assert rows_by_key[1]["sum_v"] == 30
    assert rows_by_key[1]["min_v"] == 10
    assert rows_by_key[1]["max_v"] == 20
    assert rows_by_key[1]["avg_v"] == 15
    assert rows_by_key[1]["distinct_c"] == 1

    assert rows_by_key[2]["cnt_all"] == 3
    assert rows_by_key[2]["cnt_v"] == 2
    assert rows_by_key[2]["sum_v"] == 15
    assert rows_by_key[2]["min_v"] == 7
    assert rows_by_key[2]["max_v"] == 8
    assert rows_by_key[2]["avg_v"] == 7.5
    assert rows_by_key[2]["distinct_c"] == 2


def test_shuffle_group_by_multiple_morsels_merge_state():
    morsel_a = _morsel_from_dict({"k": [1, 2], "v": [5, 10]})
    morsel_b = _morsel_from_dict({"k": [1, 3], "v": [7, 2]})

    operation = ShuffleGroupByOperation(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )
    operation.ingest_many([morsel_a, morsel_b])
    rows = _group_by_to_rows(operation.finalize())
    rows_by_key = {row["k"]: row for row in rows}

    assert rows_by_key[1]["sum_v"] == 12
    assert rows_by_key[2]["sum_v"] == 10
    assert rows_by_key[3]["sum_v"] == 2


def test_shuffle_group_by_global_empty_input_returns_single_row():
    operation = ShuffleGroupByOperation(
        group_by_columns=[],
        aggregations=[
            AggregationSpec(alias="cnt", function="count", column="*"),
            AggregationSpec(alias="sum_v", function="sum", column="v"),
            AggregationSpec(alias="avg_v", function="mean", column="v"),
            AggregationSpec(alias="one_v", function="any_value", column="v"),
        ],
    )
    rows = _group_by_to_rows(operation.finalize())

    assert len(rows) == 1
    assert rows[0]["cnt"] == 0
    assert rows[0]["sum_v"] is None
    assert rows[0]["avg_v"] is None
    assert rows[0]["one_v"] is None


def test_group_by_post_shuffle_on_shuffle_output():
    table = pa.table(
        {
            "k": [1, 1, 2, 2, 2, 3],
            "v": [10, 15, 1, 2, 7, 4],
        }
    )
    properties = QueryProperties(query_id="gb-after-shuffle", variables={})
    shuffle = ShuffleNode(properties, columns=["k"], num_bins=4, spill_enabled=False)

    for _ in shuffle.execute(table):
        pass

    shuffled_morsels = []
    for output in shuffle.execute(EOS):
        if output is None or output is EOS:
            continue
        shuffled_morsels.append(output)

    result = group_by_post_shuffle(
        shuffled_morsels,
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )
    rows = _group_by_to_rows(result)
    rows_by_key = {row["k"]: row["sum_v"] for row in rows}

    assert rows_by_key == {1: 25, 2: 10, 3: 4}
