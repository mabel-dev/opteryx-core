"""
Performance benchmark: existing SQL group-by vs shuffle+post-shuffle group-by.

This benchmark intentionally does not wire new operators into the planner.
It compares:
1) Existing engine path: SQL GROUP BY
2) Prototype path: read -> ShuffleNode -> post-shuffle group_by -> merge-sort -> limit

Query shape selected from ClickBench Q16 family:
    SELECT UserID, COUNT(*) AS c
    FROM <dataset>
    GROUP BY UserID
    ORDER BY c DESC, UserID ASC
    LIMIT 10

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
from opteryx.operators.shuffle import AggregationSpec
from opteryx.operators.shuffle import ShuffleGroupByOperation
from opteryx.operators.shuffle import ShuffleMergeSortOperation
from opteryx.operators.shuffle import SortKey
from opteryx.operators.shuffle_node import ShuffleNode


def _resolve_dataset(session) -> str | None:
    candidates = [
        "testdata.clickbench_tiny",
        os.environ.get("CLICKBENCH_DATASET"),
        "scratch.hits",
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
    fn: Callable[[], tuple[object, dict[str, float]]],
    iterations: int,
):
    fn()  # warm-up
    times = []
    stage_accumulator: dict[str, list[float]] = {}
    result = None

    for _ in range(iterations):
        start = time.perf_counter()
        result, stages = fn()
        total = time.perf_counter() - start
        times.append(total)
        for name, stage_time in stages.items():
            stage_accumulator.setdefault(name, []).append(stage_time)

    stage_averages = {
        name: (sum(samples) / len(samples))
        for name, samples in stage_accumulator.items()
        if samples
    }
    return result, times, stage_averages


def _legacy_group_by(session, dataset: str):
    sql = (
        f"SELECT UserID, COUNT(*) AS c "
        f"FROM {dataset} "
        f"GROUP BY UserID "
        f"ORDER BY c DESC, UserID ASC "
        f"LIMIT 10;"
    )
    return session.execute_to_arrow(sql)


def _shuffle_group_by_pipeline(session, dataset: str):
    stages: dict[str, float] = {}

    t0 = time.perf_counter()
    source = session.execute_to_arrow(f"SELECT UserID FROM {dataset};")
    stages["read"] = time.perf_counter() - t0

    t1 = time.perf_counter()
    props = QueryProperties(query_id=f"bench-shuffle-{time.time_ns()}", variables={})
    shuffle = ShuffleNode(
        props,
        columns=["UserID"],
        num_bins=8,
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
    group_by_operation = ShuffleGroupByOperation(
        group_by_columns=["UserID"],
        aggregations=[AggregationSpec(alias="c", function="count", column="*")],
    )
    group_by_operation.ingest_many(shuffled_morsels)
    grouped = group_by_operation.finalize()
    stages["group_by_total"] = time.perf_counter() - t2

    group_timings = group_by_operation.timings_seconds()
    stages["group"] = group_timings.get("group", 0.0)
    stages["agg"] = group_timings.get("agg", 0.0) + group_timings.get("finalize", 0.0)

    t3 = time.perf_counter()
    sorter = ShuffleMergeSortOperation(
        order_by=[
            SortKey(column="c", direction="DESC"),
            SortKey(column="UserID", direction="ASC"),
        ]
    )
    sorted_morsel = sorter.sort_single_stream([grouped])
    if sorted_morsel is None:
        result = session.execute_to_arrow("SELECT 1 WHERE FALSE;")
    else:
        result = sorted_morsel.to_arrow().slice(0, 10)
    stages["merge"] = time.perf_counter() - t3

    return result, stages


def test_clickbench_groupby_shuffle_vs_legacy_prints():
    session = opteryx.session()
    dataset = _resolve_dataset(session)
    if not dataset:
        pytest.skip("No ClickBench dataset found (tried $CLICKBENCH_DATASET, scratch.hits, testdata.clickbench_tiny)")

    iterations = int(os.environ.get("CLICKBENCH_GB_BENCH_ITERS", "3"))
    print("\n=== ClickBench GroupBy Benchmark (Q16 shape) ===")
    print(f"dataset: {dataset}")
    print(f"iterations: {iterations}")

    legacy_result, legacy_times = _timed(
        lambda: _legacy_group_by(session, dataset),
        iterations=iterations,
    )
    pipeline_result, pipeline_times, stage_averages = _timed_pipeline(
        lambda: _shuffle_group_by_pipeline(session, dataset),
        iterations=iterations,
    )

    legacy_rows = _rows_from_table(legacy_result)
    pipeline_rows = _rows_from_table(pipeline_result)
    assert legacy_rows == pipeline_rows, "Pipeline result differs from legacy SQL result"

    legacy_avg = sum(legacy_times) / len(legacy_times)
    pipeline_avg = sum(pipeline_times) / len(pipeline_times)

    print("\nlegacy SQL path:")
    for i, t in enumerate(legacy_times, 1):
        print(f"  iter {i}: {t:.4f}s")
    print(
        f"  avg: {legacy_avg:.4f}s min: {min(legacy_times):.4f}s max: {max(legacy_times):.4f}s"
    )

    print("\nshuffle + group_by pipeline:")
    for i, t in enumerate(pipeline_times, 1):
        print(f"  iter {i}: {t:.4f}s")
    print(
        f"  avg: {pipeline_avg:.4f}s min: {min(pipeline_times):.4f}s max: {max(pipeline_times):.4f}s"
    )
    print("  stage avg:")
    for stage_name in ("read", "partition", "group", "agg", "merge", "group_by_total"):
        if stage_name in stage_averages:
            print(f"    {stage_name}: {stage_averages[stage_name]:.4f}s")

    if pipeline_avg > 0:
        print(f"\nratio (legacy/pipeline): {legacy_avg / pipeline_avg:.2f}x")
        print(f"ratio (pipeline/legacy): {pipeline_avg / legacy_avg:.2f}x")

if __name__ == "__main__":
    test_clickbench_groupby_shuffle_vs_legacy_prints()
