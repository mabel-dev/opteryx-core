from opteryx.draken.vectors.int64_vector import Int64Vector


def test_dense_vector_encoding():
    vec = Int64Vector(3)
    assert vec.encoding == 0


def test_dictionary_vector_encoding():
    vec = Int64Vector.from_dict([0, 1, 0], [10, 20])
    assert vec.encoding == 1
