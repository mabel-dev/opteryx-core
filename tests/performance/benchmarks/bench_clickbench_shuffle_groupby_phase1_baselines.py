"""
Phase 1 baseline benchmark for shuffle group-by correctness/performance tracking.

This benchmark is intentionally standalone:
- no planner wiring
- compares legacy SQL group-by against shuffle + post-shuffle group-by operation
- runs multiple ClickBench-shaped group-by query forms when columns exist

Run with:
    /Users/justin/.pyenv/versions/3.13.5/bin/python tests/performance/benchmarks/bench_clickbench_shuffle_groupby_phase1_baselines.py
"""

from __future__ import annotations

import os
import sys
import time

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
        os.environ.get("CLICKBENCH_DATASET"),
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


def _available_columns(session, dataset: str) -> set[str]:
    sample = session.execute_to_arrow(f"SELECT * FROM {dataset} LIMIT 0;")
    return set(sample.column_names)


def _timed(fn, iterations: int):
    fn()
    samples = []
    result = None
    for _ in range(iterations):
        start = time.perf_counter()
        result = fn()
        samples.append(time.perf_counter() - start)
    return result, samples


def _legacy_groupby_count(session, dataset: str, group_columns: list[str]):
    group_sql = ", ".join(group_columns)
    order_sql = ", ".join(["c DESC", *[f"{column} ASC" for column in group_columns]])
    sql = (
        f"SELECT {group_sql}, COUNT(*) AS c "
        f"FROM {dataset} "
        f"GROUP BY {group_sql} "
        f"ORDER BY {order_sql}"
    )
    return session.execute_to_arrow(sql)


def _shuffle_pipeline_count(session, dataset: str, group_columns: list[str]):
    source_sql = f"SELECT {', '.join(group_columns)} FROM {dataset};"
    source = session.execute_to_arrow(source_sql)

    props = QueryProperties(query_id=f"phase1-shuffle-{time.time_ns()}", variables={})
    shuffle = ShuffleNode(
        props,
        columns=group_columns,
        num_bins=8,
        spill_enabled=False,
    )

    for _ in shuffle.execute(source):
        pass

    shuffled = []
    for output in shuffle.execute(EOS):
        if output is None or output is EOS:
            continue
        shuffled.append(output)

    group_by = ShuffleGroupByOperation(
        group_by_columns=group_columns,
        aggregations=[AggregationSpec(alias="c", function="count", column="*")],
    )
    group_by.ingest_many(shuffled)
    grouped = group_by.finalize()

    order_by = [SortKey(column="c", direction="DESC")]
    order_by.extend(SortKey(column=column, direction="ASC") for column in group_columns)
    sorter = ShuffleMergeSortOperation(order_by=order_by)
    return sorter.sort_single_stream([grouped])


def _rows(table):
    if table is None:
        return []
    return table.to_pylist()


def run_phase1_groupby_baselines(iterations: int = 3):
    session = opteryx.session()
    dataset = _resolve_dataset(session)
    if not dataset:
        print("No ClickBench-like dataset found. Set CLICKBENCH_DATASET or provide scratch.hits.")
        return

    columns = _available_columns(session, dataset)
    shapes = [
        ("group_userid", ["UserID"]),
        ("group_url", ["URL"]),
        ("group_userid_url", ["UserID", "URL"]),
    ]
    runnable = [(name, cols) for name, cols in shapes if set(cols).issubset(columns)]
    if not runnable:
        print(f"Dataset '{dataset}' missing required columns for configured shapes.")
        print(f"Available columns: {sorted(columns)}")
        return

    print("\n=== Phase 1 GroupBy Baselines ===")
    print(f"dataset: {dataset}")
    print(f"iterations: {iterations}")
    print(f"shapes: {', '.join(name for name, _ in runnable)}")

    for shape_name, group_cols in runnable:
        legacy_result, legacy_times = _timed(
            lambda: _legacy_groupby_count(session, dataset, group_cols),
            iterations=iterations,
        )
        pipeline_result, pipeline_times = _timed(
            lambda: _shuffle_pipeline_count(session, dataset, group_cols),
            iterations=iterations,
        )

        legacy_rows = _rows(legacy_result)
        pipeline_rows = _rows(pipeline_result.to_arrow() if hasattr(pipeline_result, "to_arrow") else pipeline_result)
        same = legacy_rows == pipeline_rows

        legacy_avg = sum(legacy_times) / len(legacy_times)
        pipeline_avg = sum(pipeline_times) / len(pipeline_times)

        print(f"\nshape: {shape_name} ({', '.join(group_cols)})")
        print(f"  correctness: {'PASS' if same else 'FAIL'}")
        print(
            f"  legacy avg/min/max: {legacy_avg:.4f}s / {min(legacy_times):.4f}s / {max(legacy_times):.4f}s"
        )
        print(
            f"  shuffle avg/min/max: {pipeline_avg:.4f}s / {min(pipeline_times):.4f}s / {max(pipeline_times):.4f}s"
        )
        if pipeline_avg > 0:
            print(f"  ratio (legacy/shuffle): {legacy_avg / pipeline_avg:.2f}x")


if __name__ == "__main__":
    iterations = int(os.environ.get("CLICKBENCH_GB_BASELINE_ITERS", "3"))
    run_phase1_groupby_baselines(iterations=iterations)

