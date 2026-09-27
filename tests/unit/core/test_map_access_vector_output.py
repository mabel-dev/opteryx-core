import os
import sys

import draken.draken_native as dn
from opteryx.compiled.structures.expressions import ExtractionOperator
from opteryx.compiled.structures.expressions import Literal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.types.logical_type import INT64, VARCHAR
import opteryx
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.planner.plan_context import PlanContext
from opteryx.expression.formatter import ExpressionColumn


def test_map_access_string_projection_returns_draken_vector():
    # one query's columns and expressions
    context = PlanContext()
    arena = context.expressions
    user_name_column = context.columns.relation_column("t", "user_name", column_type=VARCHAR)
    first_char_column = context.columns.computed(ExpressionColumn, "a", column_type=VARCHAR)

    morsel = Morsel.from_vectors(
        [user_name_column.identity], [dn.vector_from_string_sequence([b"alice", b"bob", None])]
    )

    user_name = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="user_name", schema_column=user_name_column, arena=arena)
    zero = Literal(
        value=0,
        type=INT64,
        schema_column=context.columns.constant("zero", column_type=INT64, value=0),
        arena=arena,
    )
    first_char = ExtractionOperator(
        value="MapAccess",
        left=user_name,
        right=zero,
        schema_column=first_char_column,
        arena=arena,
    )

    out = execute_and_append(compile_eval_nodes([first_char]), morsel)
    values = out.column(first_char_column.identity).to_pylist()
    normalized = [v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v for v in values]

    assert normalized == ["a", "b", None]


def test_hex_encode_projection_returns_draken_vector():
    session = opteryx.session()
    try:
        morsels = list(
            session.execute_to_morsels(
                "SELECT COUNT(*), a FROM (SELECT HEX_ENCODE(name) AS a FROM $planets) GROUP BY a"
            )
        )
        assert len(morsels) > 0
        assert sum(m.num_rows for m in morsels) > 0
    finally:
        session.close()


def test_hex_encode_index_projection_returns_draken_vector():
    session = opteryx.session()
    try:
        morsels = list(
            session.execute_to_morsels(
                "SELECT COUNT(*), a FROM (SELECT HEX_ENCODE(name)[0] AS a FROM $planets) GROUP BY a"
            )
        )
        assert len(morsels) > 0
        assert sum(m.num_rows for m in morsels) > 0
    finally:
        session.close()
