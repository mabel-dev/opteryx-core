from __future__ import annotations

from dataclasses import dataclass
import math

import pytest

import opteryx
from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import AggregationSpec
from opteryx.operators.shuffle import ShuffleGroupByOperation
from opteryx.operators.shuffle_node import ShuffleNode


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


def _sql_expr(spec: AggregationSpec) -> str:
    function = spec.function.lower()
    column = _decode_name(spec.column)
    alias = spec.alias

    if function == "count":
        if column in (None, "*"):
            return f"COUNT(*) AS {alias}"
        return f"COUNT({column}) AS {alias}"
    if function == "sum":
        return f"SUM({column}) AS {alias}"
    if function == "min":
        return f"MIN({column}) AS {alias}"
    if function == "max":
        return f"MAX({column}) AS {alias}"
    if function in ("avg", "mean"):
        return f"AVG({column}) AS {alias}"
    if function in ("count_distinct", "distinct"):
        return f"COUNT(DISTINCT {column}) AS {alias}"
    if function == "hash_one":
        return f"ANY_VALUE({column}) AS {alias}"
    raise ValueError(f"unsupported function for SQL projection: {function}")


def _sort_rows(rows: list[dict], columns: list[str]) -> list[dict]:
    def _sortable(value):
        if value is None:
            return (1, "")
        return (0, value)

    return sorted(rows, key=lambda row: tuple(_sortable(row.get(column)) for column in columns))


def _assert_rows_equal(actual: list[dict], expected: list[dict], ordered_columns: list[str]):
    assert len(actual) == len(expected), f"row count mismatch: {len(actual)} != {len(expected)}"

    sorted_actual = _sort_rows(actual, ordered_columns)
    sorted_expected = _sort_rows(expected, ordered_columns)

    for actual_row, expected_row in zip(sorted_actual, sorted_expected):
        assert set(actual_row.keys()) == set(expected_row.keys())
        for column in expected_row.keys():
            a = _normalize_value(actual_row[column])
            e = _normalize_value(expected_row[column])
            if isinstance(e, float) or isinstance(a, float):
                if isinstance(a, float) and isinstance(e, float) and math.isnan(a) and math.isnan(e):
                    continue
                assert a == pytest.approx(e, rel=1e-9, abs=1e-9)
            else:
                assert a == e


def _run_shuffle_groupby(
    session,
    dataset: str,
    group_by_columns: list[str],
    aggregations: list[AggregationSpec],
    chunk_size: int | None,
) -> list[dict]:
    projection_columns = list(group_by_columns)
    for aggregation in aggregations:
        column = _decode_name(aggregation.column)
        if column not in (None, "*") and column not in projection_columns:
            projection_columns.append(column)

    source_sql = f"SELECT {', '.join(projection_columns)} FROM {dataset};"
    source = session.execute_to_arrow(source_sql)

    props = QueryProperties(query_id=f"it-shuffle-groupby-{dataset}-{chunk_size}", variables={})
    shuffle = ShuffleNode(
        props,
        columns=group_by_columns,
        num_bins=4,
        spill_enabled=False,
    )

    if chunk_size is None:
        for _ in shuffle.execute(source):
            pass
    else:
        for start in range(0, source.num_rows, chunk_size):
            for _ in shuffle.execute(source.slice(start, chunk_size)):
                pass

    shuffled_morsels = []
    for output in shuffle.execute(EOS):
        if output is None or output is EOS:
            continue
        shuffled_morsels.append(output)

    op = ShuffleGroupByOperation(group_by_columns=group_by_columns, aggregations=aggregations)
    op.ingest_many(shuffled_morsels)
    return op.finalize().to_arrow().to_pylist()


def _run_sql_groupby(
    session,
    dataset: str,
    group_by_columns: list[str],
    aggregations: list[AggregationSpec],
) -> list[dict]:
    sql_aggs = ", ".join(_sql_expr(spec) for spec in aggregations)
    sql = (
        f"SELECT {', '.join(group_by_columns)}, {sql_aggs} "
        f"FROM {dataset} "
        f"GROUP BY {', '.join(group_by_columns)}"
    )
    return session.execute_to_arrow(sql).to_pylist()


@dataclass(frozen=True)
class _Case:
    name: str
    dataset: str
    group_by: tuple[str, ...]
    aggregations: tuple[AggregationSpec, ...]


_CASES = (
    _Case(
        name="satellites_planetid_radius_rollup",
        dataset="testdata.satellites",
        group_by=("planetId",),
        aggregations=(
            AggregationSpec(alias="cnt_all", function="count", column="*"),
            AggregationSpec(alias="sum_radius", function="sum", column="radius"),
            AggregationSpec(alias="min_radius", function="min", column="radius"),
            AggregationSpec(alias="max_radius", function="max", column="radius"),
            AggregationSpec(alias="avg_radius", function="avg", column="radius"),
            AggregationSpec(alias="cnt_distinct_name", function="count_distinct", column="name"),
        ),
    ),
    _Case(
        name="satellites_planetid_name_count",
        dataset="testdata.satellites",
        group_by=("planetId", "name"),
        aggregations=(
            AggregationSpec(alias="cnt_all", function="count", column="*"),
        ),
    ),
)


@pytest.mark.parametrize("chunk_size", [None, 13])
@pytest.mark.parametrize("case", _CASES, ids=[case.name for case in _CASES])
def test_shuffle_groupby_matches_sql_engine(case: _Case, chunk_size: int | None):
    session = opteryx.session(memberships=["Apollo 11", "opteryx"])

    expected = _run_sql_groupby(
        session=session,
        dataset=case.dataset,
        group_by_columns=list(case.group_by),
        aggregations=list(case.aggregations),
    )
    actual = _run_shuffle_groupby(
        session=session,
        dataset=case.dataset,
        group_by_columns=list(case.group_by),
        aggregations=list(case.aggregations),
        chunk_size=chunk_size,
    )

    compare_columns = [*case.group_by, *[spec.alias for spec in case.aggregations]]
    _assert_rows_equal(actual=actual, expected=expected, ordered_columns=compare_columns)


def test_shuffle_groupby_nullable_numeric_semantics_matches_sql():
    case = _Case(
        name="missions_company_nullable_price_gap",
        dataset="testdata.missions",
        group_by=("Company",),
        aggregations=(
            AggregationSpec(alias="cnt_all", function="count", column="*"),
            AggregationSpec(alias="cnt_price", function="count", column="Price"),
            AggregationSpec(alias="sum_price", function="sum", column="Price"),
            AggregationSpec(alias="avg_price", function="avg", column="Price"),
        ),
    )
    session = opteryx.session(memberships=["Apollo 11", "opteryx"])
    expected = _run_sql_groupby(
        session=session,
        dataset=case.dataset,
        group_by_columns=list(case.group_by),
        aggregations=list(case.aggregations),
    )
    actual = _run_shuffle_groupby(
        session=session,
        dataset=case.dataset,
        group_by_columns=list(case.group_by),
        aggregations=list(case.aggregations),
        chunk_size=13,
    )

    compare_columns = [*case.group_by, *[spec.alias for spec in case.aggregations]]
    _assert_rows_equal(actual=actual, expected=expected, ordered_columns=compare_columns)
