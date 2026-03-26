from array import array

import pyarrow as pa

from opteryx.compiled.draken import Morsel
from opteryx.compiled.draken import Vector
from opteryx.compiled.draken.vectors.array_vector import ArrayVector
from opteryx.compiled.draken.vectors.vector_vector import VectorVector


def test_fixed_size_numeric_lists_promote_to_vectorvector():
    arrow_array = pa.array(
        [[1.0, 2.0], [3.0, 4.0], None],
        type=pa.list_(pa.float64(), 2),
    )

    vector = Vector.from_arrow(arrow_array)

    assert isinstance(vector, VectorVector)
    assert vector.dimensions == 2
    assert vector.to_pylist() == arrow_array.to_pylist()
    assert vector.to_arrow().type == arrow_array.type
    assert vector.to_arrow().equals(arrow_array)


def test_vectorvector_take_preserves_type_and_fixed_width():
    arrow_array = pa.array(
        [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]],
        type=pa.list_(pa.int64(), 3),
    )

    vector = Vector.from_arrow(arrow_array)
    taken = vector.take(array("i", [3, 0, 2]))

    assert isinstance(taken, VectorVector)
    assert taken.dimensions == 3
    assert taken.to_arrow().type == arrow_array.type
    assert taken.to_arrow().to_pylist() == [[7, 8, 9], [1, 2, 3], None]


def test_morsel_roundtrip_preserves_vectorvector_arrow_type():
    embeddings = pa.array(
        [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
        type=pa.list_(pa.float64(), 2),
    )
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "embedding": embeddings,
        }
    )

    morsel = Morsel.from_arrow(table)
    roundtrip = morsel.to_arrow()

    assert isinstance(morsel.column(b"embedding"), VectorVector)
    assert roundtrip.schema.field("embedding").type == embeddings.type
    assert roundtrip.column("embedding").combine_chunks().equals(embeddings)


def test_uniform_numeric_list_promotes_to_vectorvector():
    arrow_array = pa.array(
        [[1.0, 2.0], [3.0, 4.0], None],
        type=pa.list_(pa.float64()),
    )

    vector = Vector.from_arrow(arrow_array)

    assert isinstance(vector, VectorVector)
    assert vector.dimensions == 2
    assert vector.to_arrow().type == pa.list_(pa.float64(), 2)
    assert vector.to_arrow().to_pylist() == arrow_array.to_pylist()


def test_non_uniform_numeric_list_stays_arrayvector():
    arrow_array = pa.array(
        [[1.0, 2.0], [3.0], None],
        type=pa.list_(pa.float64()),
    )

    vector = Vector.from_arrow(arrow_array)

    assert isinstance(vector, ArrayVector)
    assert not isinstance(vector, VectorVector)
    assert vector.to_arrow().type == arrow_array.type
    assert vector.to_arrow().equals(arrow_array)
