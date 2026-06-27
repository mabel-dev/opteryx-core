import os
import sys
from types import SimpleNamespace

from opteryx.types.logical_type import LogicalCategory

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.vector_types import is_numeric_vector_type
from opteryx.vector_types import node_is_numeric_vector


def test_is_numeric_vector_type_recognizes_vector_type():
    assert is_numeric_vector_type(LogicalCategory.VECTOR, LogicalCategory.FLOAT)
    assert is_numeric_vector_type(LogicalCategory.VECTOR, LogicalCategory.INTEGER)
    assert is_numeric_vector_type(LogicalCategory.VECTOR, LogicalCategory.DECIMAL)


def test_is_numeric_vector_type_rejects_arrays():
    assert not is_numeric_vector_type(LogicalCategory.ARRAY, LogicalCategory.FLOAT)
    assert not is_numeric_vector_type(LogicalCategory.ARRAY, LogicalCategory.VARCHAR)
    assert not is_numeric_vector_type(LogicalCategory.VARCHAR, None)
    assert not is_numeric_vector_type(LogicalCategory.ARRAY, None)


def test_node_is_numeric_vector_prefers_schema_column_types():
    node = SimpleNamespace(
        type=None,
        element_type=None,
        schema_column=SimpleNamespace(
            category=LogicalCategory.VECTOR, element_type=LogicalCategory.FLOAT
        ),
    )

    assert node_is_numeric_vector(node)


def test_node_is_numeric_vector_rejects_text_arrays():
    node = SimpleNamespace(
        type=LogicalCategory.ARRAY,
        element_type=LogicalCategory.VARCHAR,
        schema_column=SimpleNamespace(
            category=LogicalCategory.ARRAY, element_type=LogicalCategory.VARCHAR
        ),
    )

    assert not node_is_numeric_vector(node)
