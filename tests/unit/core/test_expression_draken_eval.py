import os
import sys
from types import SimpleNamespace

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from orso.types import OrsoTypes

from opteryx.draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.expression.evaluator import _eval_value
from opteryx.expression.evaluator import evaluate_and_append_draken
from opteryx.models import Node


def _schema(identity: str, value_type):
    return SimpleNamespace(identity=identity, type=value_type, name=identity)


def test_draken_evaluate_and_append_native_integer_binary_operator():
    morsel = Morsel.from_arrow(pa.table({"clientip": pa.array([10, 11, 12], type=pa.int64())}))

    clientip = Node(
        NodeType.IDENTIFIER,
        value="clientip",
        schema_column=_schema("clientip", OrsoTypes.INTEGER),
    )
    one = Node(
        NodeType.LITERAL,
        value=1,
        schema_column=_schema("one", OrsoTypes.INTEGER),
    )
    minus_one = Node(
        NodeType.BINARY_OPERATOR,
        value="Minus",
        left=clientip,
        right=one,
        schema_column=_schema("clientip_minus_1", OrsoTypes.INTEGER),
    )

    out = evaluate_and_append_draken([minus_one], morsel)

    assert out.column(b"clientip_minus_1").to_pylist() == [9, 10, 11]


def test_draken_eval_value_expression_list_materializes_children():
    morsel = Morsel.from_arrow(pa.table({"clientip": pa.array([10, 11, 12], type=pa.int64())}))

    clientip = Node(
        NodeType.IDENTIFIER,
        value="clientip",
        schema_column=_schema("clientip", OrsoTypes.INTEGER),
    )
    literal = Node(
        NodeType.LITERAL,
        value=1,
        schema_column=_schema("one", OrsoTypes.INTEGER),
    )
    expression_list = Node(
        NodeType.EXPRESSION_LIST,
        parameters=[clientip, literal],
        schema_column=_schema("expression_list", OrsoTypes.INTEGER),
    )

    out = _eval_value(expression_list, morsel)

    assert len(out) == 2
    assert out[0].to_pylist() == [10, 11, 12]
    assert out[1] == 1


@pytest.mark.parametrize(
    ("sql", "expected_rows"),
    [
        (
            """
            SELECT
                TraficSourceID,
                SearchEngineID,
                AdvEngineID,
                CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src,
                URL AS Dst,
                COUNT(*) AS PageViews
            FROM testdata.clickbench_tiny
            WHERE IsRefresh = 0
            GROUP BY
                TraficSourceID,
                SearchEngineID,
                AdvEngineID,
                CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END,
                URL
            ORDER BY PageViews DESC
            LIMIT 10
            """,
            10,
        ),
        (
            """
            SELECT TRUNC(EventTime, 'minute') AS M, COUNT(*) AS PageViews
            FROM testdata.clickbench_tiny
            WHERE IsRefresh = 0
            GROUP BY TRUNC(EventTime, 'minute')
            ORDER BY M
            LIMIT 10
            """,
            10,
        ),
    ],
)
def test_grouped_clickbench_style_expressions_stay_native(sql, expected_rows):
    session = opteryx.session()
    try:
        result = session.execute_to_arrow(sql)
        ops = session.telemetry.get("operations", {})
        agg = next((v for v in ops.values() if v.get("type") == "AggregateRel"), {})
        assert result.num_rows == expected_rows
        assert agg.get("feature_groupby_engine_group_state_store", 0) == 0
        assert agg.get("feature_groupby_draken_eval_arrow_fallback", 0) == 0
        assert agg.get("feature_groupby_draken_eval_native", 0) >= 1
    finally:
        session.close()
