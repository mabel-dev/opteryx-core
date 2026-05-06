"""VectorVector tests.

VectorVector is the FP16-only embedding column type. These tests pin down
the routing rules at the Arrow boundary, the round-trip fidelity, and the
distance kernel behaviour (including null propagation).
"""

from array import array

import pyarrow as pa
import pytest

from draken import Morsel, Vector
from draken.vectors.array_vector import ArrayVector
from draken.vectors.float32_vector import Float32Vector
from draken.vectors.vector_vector import VectorVector


# --- Routing at the Arrow boundary ----------------------------------------


def test_fixed_size_list_fp16_lands_in_vectorvector():
    arr = pa.array([[1.0, 2.0], [3.0, 4.0], None], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, VectorVector)
    assert vec.dimensions == 2
    assert len(vec) == 3
    assert vec.to_pylist() == [[1.0, 2.0], [3.0, 4.0], None]
    assert vec.to_arrow().equals(arr)


def test_uniform_list_fp16_auto_promotes_to_vectorvector():
    arr = pa.array([[1.0, 2.0], [3.0, 4.0], None], type=pa.list_(pa.float16()))
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, VectorVector)
    assert vec.dimensions == 2
    assert vec.to_arrow().type == pa.list_(pa.float16(), 2)


def test_ragged_list_fp16_stays_arrayvector():
    arr = pa.array([[1.0, 2.0], [3.0], None], type=pa.list_(pa.float16()))
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, ArrayVector)
    assert not isinstance(vec, VectorVector)


@pytest.mark.parametrize(
    "child_type", [pa.float32(), pa.float64(), pa.int32(), pa.int64()]
)
def test_non_fp16_fixed_size_list_routes_to_arrayvector(child_type):
    arr = pa.array([[1, 2], [3, 4]], type=pa.list_(child_type, 2))
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, ArrayVector)
    assert not isinstance(vec, VectorVector)


@pytest.mark.parametrize("child_type", [pa.float32(), pa.float64(), pa.int64()])
def test_uniform_list_non_fp16_does_not_promote(child_type):
    arr = pa.array([[1, 2], [3, 4]], type=pa.list_(child_type))
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, ArrayVector)
    assert not isinstance(vec, VectorVector)


def test_top_level_fp16_widens_to_float32():
    arr = pa.array([1.0, 2.0, 3.0], type=pa.float16())
    vec = Vector.from_arrow(arr)

    assert isinstance(vec, Float32Vector)


# --- take ------------------------------------------------------------------


def test_take_preserves_dimensions_and_nulls():
    arr = pa.array(
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], None, [7.0, 8.0, 9.0]],
        type=pa.list_(pa.float16(), 3),
    )
    vec = Vector.from_arrow(arr)

    taken = vec.take(array("i", [3, 0, 2]))

    assert isinstance(taken, VectorVector)
    assert taken.dimensions == 3
    assert taken.to_pylist() == [[7.0, 8.0, 9.0], [1.0, 2.0, 3.0], None]
    assert taken.is_null_at(2)
    assert taken.to_arrow().type == pa.list_(pa.float16(), 3)


def test_take_out_of_range_raises():
    arr = pa.array([[1.0, 2.0]], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)

    with pytest.raises(IndexError):
        vec.take(array("i", [5]))


# --- Distance kernels ------------------------------------------------------


def _q(values):
    return memoryview(array("f", values))


def test_dot_product_basic():
    arr = pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], type=pa.list_(pa.float16(), 3))
    vec = Vector.from_arrow(arr)

    out = vec.dot(_q([1.0, 2.0, 3.0]))

    assert isinstance(out, Float32Vector)
    assert out.to_pylist() == [14.0, 32.0]


def test_dot_propagates_null_rows():
    arr = pa.array([[1.0, 2.0], None, [3.0, 4.0]], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)

    out = vec.dot(_q([1.0, 1.0]))
    values = out.to_pylist()

    assert values[0] == 3.0
    assert values[1] is None
    assert values[2] == 7.0


def test_cosine_similarity_self_match_is_one():
    arr = pa.array([[1.0, 0.0, 0.0]], type=pa.list_(pa.float16(), 3))
    vec = Vector.from_arrow(arr)

    out = vec.cosine_similarity(_q([1.0, 0.0, 0.0]))
    assert out.to_pylist()[0] == pytest.approx(1.0, abs=1e-6)


def test_cosine_similarity_zero_norm_is_zero():
    arr = pa.array([[0.0, 0.0]], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)

    out = vec.cosine_similarity(_q([1.0, 0.0]))
    assert out.to_pylist()[0] == 0.0


def test_l2_distance_self_match_is_zero():
    arr = pa.array([[1.0, 2.0, 3.0]], type=pa.list_(pa.float16(), 3))
    vec = Vector.from_arrow(arr)

    out = vec.l2_distance(_q([1.0, 2.0, 3.0]))
    assert out.to_pylist()[0] == pytest.approx(0.0, abs=1e-6)


def test_distance_kernels_reject_query_dimension_mismatch():
    arr = pa.array([[1.0, 2.0]], type=pa.list_(pa.float16(), 2))
    vec = Vector.from_arrow(arr)

    with pytest.raises(ValueError):
        vec.dot(_q([1.0, 2.0, 3.0]))


# --- Morsel round-trip -----------------------------------------------------


def test_morsel_roundtrip_preserves_fp16_embedding_column():
    embeddings = pa.array(
        [[0.5, 0.25], [0.125, 0.0625], [1.0, 1.0]],
        type=pa.list_(pa.float16(), 2),
    )
    table = pa.table(
        {"id": pa.array([1, 2, 3], type=pa.int64()), "embedding": embeddings}
    )

    morsel = Morsel.from_arrow(table)
    assert isinstance(morsel.column(b"embedding"), VectorVector)

    roundtrip = morsel.to_arrow()
    assert roundtrip.schema.field("embedding").type == embeddings.type
    assert roundtrip.column("embedding").combine_chunks().equals(embeddings)
