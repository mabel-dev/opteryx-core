import os
import sys

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from orso.schema import ConstantColumn
from orso.schema import FlatColumn
from orso.schema import FunctionColumn
from orso.types import OrsoTypes

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import Node
from opteryx.models import QueryProperties
from opteryx.operators.projection_node import ProjectionNode
from opteryx.operators.group_state_store import DRAKEN_ENCODING_CONSTANT


def _literal_node(identity: str, value, value_type: OrsoTypes) -> Node:
    schema_column = ConstantColumn(name=identity, value=value, type=value_type)
    schema_column.identity = identity
    return Node(NodeType.LITERAL, type=value_type, value=value, schema_column=schema_column)


def test_evaluate_and_append_morsel_literal_emits_typed_constant_vector():
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])
    literal = _literal_node("k", 42, OrsoTypes.INTEGER)

    result = evaluate_and_append([literal], morsel)

    assert result.__class__.__name__ == "Morsel"
    assert result.num_rows == 3
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [42, 42, 42]
    assert result.column(b"x").to_pylist() == [1, 2, 3]


def test_projection_node_keeps_typed_constant_vector_on_morsel_literal_projection():
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])
    literal = _literal_node("k", 7, OrsoTypes.INTEGER)
    node = ProjectionNode(QueryProperties("projection-constant-morsel", {}), projection=[literal])

    result = next(node.execute(morsel))

    assert result.__class__.__name__ == "Morsel"
    assert result.num_rows == 3
    assert result.column_names == [b"k"]
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [7, 7, 7]
    assert node.readings["draken_constant_columns_emitted"] == 1


def test_evaluate_and_append_morsel_mixed_non_literal_and_literal_keeps_typed_constant():
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])

    x_col = FlatColumn(name="x", type=OrsoTypes.INTEGER)
    x_col.identity = "x"
    x_node = Node(NodeType.IDENTIFIER, type=OrsoTypes.INTEGER, value="x", schema_column=x_col)

    one_literal = _literal_node("one", 1, OrsoTypes.INTEGER)
    plus_schema = FunctionColumn(name="y", type=OrsoTypes.INTEGER)
    plus_schema.identity = "y"
    plus = Node(
        NodeType.BINARY_OPERATOR,
        value="Plus",
        left=x_node,
        right=one_literal,
        schema_column=plus_schema,
    )
    k_literal = _literal_node("k", 99, OrsoTypes.INTEGER)

    result = evaluate_and_append([plus, k_literal], morsel)

    assert result.__class__.__name__ == "Morsel"
    assert result.column(b"y").to_pylist() == [2, 3, 4]
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [99, 99, 99]


def test_projection_node_mixed_non_literal_and_literal_keeps_typed_constant():
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])

    x_col = FlatColumn(name="x", type=OrsoTypes.INTEGER)
    x_col.identity = "x"
    x_node = Node(NodeType.IDENTIFIER, type=OrsoTypes.INTEGER, value="x", schema_column=x_col)

    one_literal = _literal_node("one", 1, OrsoTypes.INTEGER)
    plus_schema = FunctionColumn(name="y", type=OrsoTypes.INTEGER)
    plus_schema.identity = "y"
    plus = Node(
        NodeType.BINARY_OPERATOR,
        value="Plus",
        left=x_node,
        right=one_literal,
        schema_column=plus_schema,
    )
    k_literal = _literal_node("k", 5, OrsoTypes.INTEGER)

    node = ProjectionNode(
        QueryProperties("projection-mixed-constant-morsel", {}),
        projection=[plus, k_literal],
    )
    result = next(node.execute(morsel))

    assert result.__class__.__name__ == "Morsel"
    assert result.column_names == [b"y", b"k"]
    assert result.column(b"y").to_pylist() == [2, 3, 4]
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [5, 5, 5]
    assert node.readings["draken_constant_columns_emitted"] == 1


def test_projection_node_arrow_input_literal_uses_typed_constant_path():
    table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
    literal = _literal_node("k", 7, OrsoTypes.INTEGER)
    node = ProjectionNode(QueryProperties("projection-constant-arrow", {}), projection=[literal])

    result = next(node.execute(table))

    assert result.__class__.__name__ == "Morsel"
    assert result.num_rows == 3
    assert result.column_names == [b"k"]
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [7, 7, 7]
    assert node.readings["draken_constant_columns_emitted"] == 1


def test_projection_node_arrow_input_mixed_preserves_typed_constant_literal():
    table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})

    x_col = FlatColumn(name="x", type=OrsoTypes.INTEGER)
    x_col.identity = "x"
    x_node = Node(NodeType.IDENTIFIER, type=OrsoTypes.INTEGER, value="x", schema_column=x_col)

    one_literal = _literal_node("one", 1, OrsoTypes.INTEGER)
    plus_schema = FunctionColumn(name="y", type=OrsoTypes.INTEGER)
    plus_schema.identity = "y"
    plus = Node(
        NodeType.BINARY_OPERATOR,
        value="Plus",
        left=x_node,
        right=one_literal,
        schema_column=plus_schema,
    )
    k_literal = _literal_node("k", 5, OrsoTypes.INTEGER)

    node = ProjectionNode(
        QueryProperties("projection-mixed-constant-arrow", {}),
        projection=[plus, k_literal],
    )
    result = next(node.execute(table))

    assert result.__class__.__name__ == "Morsel"
    assert result.column_names == [b"y", b"k"]
    assert result.column(b"y").to_pylist() == [2, 3, 4]
    assert result.column(b"k").__class__.__name__ == "IntegerVector"
    assert result.column(b"k").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"k").to_pylist() == [5, 5, 5]
    assert node.readings["draken_constant_columns_emitted"] == 1


def test_evaluate_and_append_morsel_date_literal_emits_typed_constant_vector():
    import datetime

    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])
    literal = _literal_node("d", datetime.date(2024, 1, 2), OrsoTypes.DATE)

    result = evaluate_and_append([literal], morsel)

    assert result.column(b"d").__class__.__name__ == "Date32Vector"
    assert result.column(b"d").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"d").to_arrow().to_pylist() == [datetime.date(2024, 1, 2)] * 3


def test_evaluate_and_append_morsel_timestamp_literal_emits_typed_constant_vector():
    import datetime

    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3])])
    literal_value = datetime.datetime(2024, 1, 2, 3, 4, 5)
    literal = _literal_node("ts", literal_value, OrsoTypes.TIMESTAMP)

    result = evaluate_and_append([literal], morsel)

    assert result.column(b"ts").__class__.__name__ == "TimestampVector"
    assert result.column(b"ts").encoding == DRAKEN_ENCODING_CONSTANT
    assert result.column(b"ts").to_arrow().to_pylist() == [literal_value] * 3
