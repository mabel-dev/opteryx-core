import hashlib
import pytest

from opteryx.compiled import vector_ops


def test_list_md5_simple():
    vals = ["hello", "world", ""]
    result = vector_ops.vector_md5(vals)
    expected = [hashlib.md5(v.encode()).hexdigest() for v in vals]
    assert list(result) == expected


def test_list_md5_none_and_numbers():
    vals = [None, 123, 45.6]
    result = vector_ops.vector_md5(vals)
    expected = []
    for v in vals:
        if v is None:
            expected.append(None)
        else:
            expected.append(hashlib.md5(str(v).encode()).hexdigest())
    assert list(result) == expected
