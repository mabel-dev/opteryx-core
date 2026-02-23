"""
Performance benchmark: existing SQL group-by vs shuffle+post-shuffle group-by.

This benchmark intentionally does not wire new operators into the planner.
It compares:
1) Existing engine path: SQL GROUP BY
2) Prototype path: read -> ShuffleNode -> post-shuffle group_by
3) Optional sort step (disabled by default)

Query shape selected from ClickBench Q16 family:
    SELECT <group_column>, <aggregate> AS agg
    FROM <dataset>
    GROUP BY <group_column>
    [ORDER BY agg DESC, <group_column> ASC]

Run with:
    pytest -q tests/performance/benchmarks/bench_clickbench_shuffle_groupby_compare.py
"""

import os
import sys
import time
from typing import Callable

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
from opteryx.operators.shuffle import AggregationSpec
from opteryx.operators.shuffle import ShuffleGroupByOperation
from opteryx.operators.shuffle import ShuffleMergeSortOperation
from opteryx.operators.shuffle import SortKey
from opteryx.operators.shuffle_node import ShuffleNode


def _resolve_aggregate_config(session, dataset: str, group_column: str) -> tuple[str, str | None]:
    """
    Returns:
        (aggregate_function, aggregate_column)
    """
    aggregate_function = os.environ.get("CLICKBENCH_GB_AGG", "count").strip().lower()
    if aggregate_function not in ("count", "sum"):
        raise ValueError("CLICKBENCH_GB_AGG must be one of: count, sum")

    if aggregate_function == "count":
        return "count", None

    # SUM aggregate: default to summing the same key column unless overridden.
    sum_column = os.environ.get("CLICKBENCH_GB_SUM_COLUMN", group_column).strip()
    try:
        session.execute_to_arrow(f"SELECT {sum_column} FROM {dataset} LIMIT 1;")
    except Exception as err:
        raise ValueError(
            f"Invalid CLICKBENCH_GB_SUM_COLUMN '{sum_column}' for dataset '{dataset}'"
        ) from err
    return "sum", sum_column


def _resolve_dataset(session) -> str | None:
    candidates = [
        os.environ.get("CLICKBENCH_DATASET"),
        "scratch.parquet",
        "scratch.hits",
        "testdata.clickbench_tiny",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            session.execute_to_arrow(f"SELECT COUNT(*) FROM {candidate};")
            return candidate
        except Exception:
            continue
    return None


def _resolve_group_column(session, dataset: str) -> str:
    candidates = ("UserID", "user_id")
    for column in candidates:
        try:
            session.execute_to_arrow(f"SELECT {column} FROM {dataset} LIMIT 1;")
            return column
        except Exception:
            continue
    raise ValueError(f"No supported group-by column found in {dataset}; tried {candidates}")


def _rows_from_table(table):
    if table is None:
        return []
    as_dict = table.to_pydict()
    keys = list(as_dict.keys())
    if not keys:
        return []
    row_count = len(as_dict[keys[0]])
    rows = []
    for i in range(row_count):
        rows.append({k: as_dict[k][i] for k in keys})
    return rows


def _timed(
    fn: Callable[[], object],
    iterations: int,
):
    fn()  # warm-up
    times = []
    result = None
    for _ in range(iterations):
        start = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - start)
    return result, times


def _timed_pipeline(
    fn: Callable[[], tuple],
    iterations: int,
):
    # fn may return (result, stages) or (result, stages, metadata)
    fn()  # warm-up
    times = []
    stage_accumulator: dict[str, list[float]] = {}
    result = None
    metadata_list: list = []

    for _ in range(iterations):
        start = time.perf_counter()
        output = fn()
        total = time.perf_counter() - start
        times.append(total)

        # unpack the returned tuple
        if len(output) == 2:
            result, stages = output
            meta = None
        else:
            result, stages, meta = output
        metadata_list.append(meta)

        for name, stage_time in stages.items():
            stage_accumulator.setdefault(name, []).append(stage_time)

    stage_averages = {
        name: (sum(samples) / len(samples))
        for name, samples in stage_accumulator.items()
        if samples
    }

    # return first metadata item (could be None)
    combined_meta = metadata_list[0] if metadata_list else None

    return result, times, stage_averages, combined_meta


def _legacy_group_by(
    session,
    dataset: str,
    group_column: str,
    aggregate_function: str,
    aggregate_column: str | None,
):
    if aggregate_function == "count":
        sql = f"SELECT {group_column}, COUNT(*) AS agg FROM {dataset} GROUP BY {group_column}"
    elif aggregate_function == "sum":
        sql = (
            f"SELECT {group_column}, SUM(CAST({aggregate_column} AS DOUBLE)) AS agg "
            f"FROM {dataset} GROUP BY {group_column}"
        )
    else:
        raise ValueError(f"unsupported aggregate_function '{aggregate_function}'")
    return session.execute_to_arrow(sql)


def _shuffle_group_by_pipeline(
    session,
    dataset: str,
    group_column: str,
    aggregate_function: str,
    aggregate_column: str | None,
    group_by_class,
    include_sort: bool,
):
    stages: dict[str, float] = {}
    sum_input_alias = "__sum_input"

    t0 = time.perf_counter()
    if aggregate_function == "sum" and aggregate_column:
        source_sql = (
            f"SELECT {group_column}, CAST({aggregate_column} AS DOUBLE) AS {sum_input_alias} "
            f"FROM {dataset};"
        )
    else:
        source_sql = f"SELECT {group_column} FROM {dataset};"
    source = session.execute_to_arrow(source_sql)
    stages["read"] = time.perf_counter() - t0

    t1 = time.perf_counter()
    props = QueryProperties(query_id=f"bench-shuffle-{time.time_ns()}", variables={})
    shuffle = ShuffleNode(
        props,
        columns=[group_column],
        num_bins=4,
        spill_enabled=False,
    )
    for _ in shuffle.execute(source):
        pass
    shuffled_morsels = []
    for output in shuffle.execute(EOS):
        if output is None or output is EOS:
            continue
        shuffled_morsels.append(output)
    stages["partition"] = time.perf_counter() - t1

    t2 = time.perf_counter()
    group_by_operation = group_by_class(
        group_by_columns=[group_column],
        aggregations=[
            AggregationSpec(
                alias="agg",
                function=aggregate_function,
                column="*" if aggregate_function == "count" else sum_input_alias,
            )
        ],
    )
    group_by_operation.ingest_many(shuffled_morsels)
    grouped = group_by_operation.finalize()
    # capture the number of distinct groups produced
    group_count = int(getattr(grouped, "num_rows", 0))
    stages["group_by_total"] = time.perf_counter() - t2

    if hasattr(group_by_operation, "timings_seconds"):
        group_timings = group_by_operation.timings_seconds()
        stages["group"] = group_timings.get("group", 0.0)
        stages["agg"] = group_timings.get("agg", 0.0) + group_timings.get("finalize", 0.0)
    else:
        stages["group"] = stages["group_by_total"]
        stages["agg"] = 0.0

    if include_sort:
        t3 = time.perf_counter()
        sorter = ShuffleMergeSortOperation(
            order_by=[
                SortKey(column="agg", direction="DESC"),
                SortKey(column=group_column, direction="ASC"),
            ]
        )
        sorted_morsel = sorter.sort_single_stream([grouped])
        stages["merge"] = time.perf_counter() - t3
        return sorted_morsel, stages, group_count

    return grouped, stages, group_count


def test_clickbench_groupby_shuffle_vs_legacy_prints():
    session = opteryx.session()
    dataset = _resolve_dataset(session)
    if not dataset:
        pytest.skip("No ClickBench dataset found (tried $CLICKBENCH_DATASET, scratch.parquet, scratch.hits, testdata.clickbench_tiny)")

    iterations = int(os.environ.get("CLICKBENCH_GB_BENCH_ITERS", "3"))
    print("\n=== ClickBench GroupBy Benchmark (Q16 shape) ===")
    print(f"dataset: {dataset}")
    print(f"iterations: {iterations}")
    include_sort = os.environ.get("CLICKBENCH_GB_INCLUDE_SORT", "0").lower() in ("1", "true", "yes")
    group_column = _resolve_group_column(session, dataset)
    aggregate_function, aggregate_column = _resolve_aggregate_config(session, dataset, group_column)
    print(f"group column: {group_column}")
    if aggregate_function == "count":
        print("aggregate: count(*)")
    else:
        print(f"aggregate: sum({aggregate_column})")
    print(f"include sort: {include_sort}")

    # run the legacy SQL path and capture timing
    legacy_result, legacy_times = _timed(
        lambda: _legacy_group_by(
            session, dataset, group_column, aggregate_function, aggregate_column
        ),
        iterations=iterations,
    )

    # also ask SQL for the total number of groups (distinct UserID) as a reference
    groups_table = session.execute_to_arrow(
        f"SELECT COUNT(DISTINCT {group_column}) AS groups FROM {dataset};"
    )
    legacy_group_count = int(groups_table.to_pydict()["groups"][0])

    pipeline_v2_result, pipeline_v2_times, pipeline_v2_stages, pipeline_v2_meta = _timed_pipeline(
        lambda: _shuffle_group_by_pipeline(
            session,
            dataset,
            group_column,
            aggregate_function,
            aggregate_column,
            ShuffleGroupByOperationV2,
            include_sort,
        ),
        iterations=iterations,
    )

    if isinstance(pipeline_v2_meta, dict):
        pipeline_v2_group_count = pipeline_v2_meta.get("group_count", None)
    else:
        pipeline_v2_group_count = pipeline_v2_meta

    legacy_rows = legacy_result.num_rows if legacy_result else None
    pipeline_v2_rows = pipeline_v2_result.num_rows if pipeline_v2_result else None
    assert legacy_rows == pipeline_v2_rows, f"V2 pipeline result differs from SQL: {pipeline_v2_rows} != {legacy_rows}"

    legacy_avg = sum(legacy_times) / len(legacy_times)
    pipeline_v2_avg = sum(pipeline_v2_times) / len(pipeline_v2_times)

    # print group counts for sanity
    print(f"legacy group count: {legacy_group_count}")
    print(f"pipeline v2 group count: {pipeline_v2_group_count}")

    assert pipeline_v2_group_count == legacy_group_count, "V2 pipeline group count differs from SQL"

    print("\nlegacy SQL path:")
    for i, t in enumerate(legacy_times, 1):
        print(f"  iter {i}: {t:.4f}s")
    print(
        f"  avg: {legacy_avg:.4f}s min: {min(legacy_times):.4f}s max: {max(legacy_times):.4f}s"
    )

    print("\nshuffle + group_by pipeline v2:")
    for i, t in enumerate(pipeline_v2_times, 1):
        print(f"  iter {i}: {t:.4f}s")
    print(
        f"  avg: {pipeline_v2_avg:.4f}s min: {min(pipeline_v2_times):.4f}s max: {max(pipeline_v2_times):.4f}s"
    )
    print("  stage avg:")
    stage_order = ("read", "partition", "group", "agg", "group_by_total", "merge")
    for stage_name in stage_order:
        if stage_name == "merge" and not include_sort:
            continue
        if stage_name in pipeline_v2_stages:
            print(f"    {stage_name}: {pipeline_v2_stages[stage_name]:.4f}s")

    if pipeline_v2_avg > 0:
        print(f"ratio (legacy/pipeline_v2): {legacy_avg / pipeline_v2_avg:.2f}x")
        print(f"ratio (pipeline_v2/legacy): {pipeline_v2_avg / legacy_avg:.2f}x")

if __name__ == "__main__":
    test_clickbench_groupby_shuffle_vs_legacy_prints()
