import hashlib
from opteryx.compiled import vector_ops


def _compare(func, algo, inputs):
    expected = []
    for v in inputs:
        if v is None:
            expected.append(None)
        else:
            expected.append(getattr(hashlib, algo)(str(v).encode()).hexdigest())
    return list(func(inputs)) == expected


def test_md5():
    assert _compare(vector_ops.vector_md5, 'md5', ['hello', None, 123])

def test_sha1():
    assert _compare(vector_ops.vector_sha1, 'sha1', ['hello', None, 123])

def test_sha256():
    assert _compare(vector_ops.vector_sha256, 'sha256', ['hello', None, 123])

def test_sha512():
    assert _compare(vector_ops.vector_sha512, 'sha512', ['hello', None, 123])
