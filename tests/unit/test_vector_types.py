import os
import sys
from types import SimpleNamespace

from opteryx.types import OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.vector_types import is_numeric_vector_type
from opteryx.vector_types import node_is_numeric_vector


def test_is_numeric_vector_type_recognizes_vector_type():
    assert is_numeric_vector_type(OrsoTypes.VECTOR, OrsoTypes.DOUBLE)
    assert is_numeric_vector_type(OrsoTypes.VECTOR, OrsoTypes.INTEGER)
    assert is_numeric_vector_type(OrsoTypes.VECTOR, OrsoTypes.DECIMAL)


def test_is_numeric_vector_type_rejects_arrays():
    assert not is_numeric_vector_type(OrsoTypes.ARRAY, OrsoTypes.DOUBLE)
    assert not is_numeric_vector_type(OrsoTypes.ARRAY, OrsoTypes.VARCHAR)
    assert not is_numeric_vector_type(OrsoTypes.VARCHAR, None)
    assert not is_numeric_vector_type(OrsoTypes.ARRAY, None)


def test_node_is_numeric_vector_prefers_schema_column_types():
    node = SimpleNamespace(
        type=None,
        element_type=None,
        schema_column=SimpleNamespace(type=OrsoTypes.VECTOR, element_type=OrsoTypes.DOUBLE),
    )

    assert node_is_numeric_vector(node)


def test_node_is_numeric_vector_rejects_text_arrays():
    node = SimpleNamespace(
        type=OrsoTypes.ARRAY,
        element_type=OrsoTypes.VARCHAR,
        schema_column=SimpleNamespace(type=OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR),
    )

    assert not node_is_numeric_vector(node)
