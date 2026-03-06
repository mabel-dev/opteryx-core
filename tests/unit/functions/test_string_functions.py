import os
import sys

import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken import Vector
from opteryx.draken.interop.arrow import vector_from_arrow
from opteryx.compiled import list_ops as compiled_list_ops
from opteryx.functions import string_functions

list_initcap = getattr(compiled_list_ops, "list_initcap")
list_regex_replace = getattr(compiled_list_ops, "list_regex_replace")
list_replace = getattr(compiled_list_ops, "list_replace")
list_string_slice_right = getattr(compiled_list_ops, "list_string_slice_right")
list_string_slice_left = getattr(compiled_list_ops, "list_string_slice_left")


def _to_sv(lst):
    """Convert a Python list to StringVector via PyArrow."""
    return vector_from_arrow(pyarrow.array(lst, type=pyarrow.string()))


def _sv_to_list(sv):
    """Convert StringVector result back to Python list, decoding bytes to str."""
    arr = sv.to_arrow()
    if pyarrow.types.is_binary(arr.type):
        arr = arr.cast(pyarrow.string())
    return arr.to_pylist()


def test_slice_left():
    slicer = lambda arr, n: _sv_to_list(list_string_slice_left(_to_sv(arr), n))

    # fmt:off
    assert slicer(["abcdef"], 3) == ["abc"]
    assert slicer(["abcdef", "ghijklm"], 3) == ["abc", "ghi"]
    assert slicer([], 3) == []
    assert slicer([None], 3) == [None]
    assert slicer([""], 0) == [""]
    assert slicer(["abc", "abcdefghijklmnopqrstuvwxyz"], 5) == ["abc", "abcde"]
    assert slicer([None, "", "abcdef", "a"], 2) == [None, "", "ab", "a"]
    # fmt:on


def test_slice_right():
    slicer = lambda arr, n: _sv_to_list(list_string_slice_right(_to_sv(arr), n))

    # fmt:off
    assert slicer(["abcdef"], 3) == ["def"]
    assert slicer(["abcdef", "ghijklm"], 3) == ["def", "klm"]
    assert slicer([], 3) == []
    assert slicer([None], 3) == [None]
    assert slicer([""], 0) == [""]
    assert slicer(["abc", "abcdefghijklmnopqrstuvwxyz"], 5) == ["abc", "vwxyz"]
    assert slicer([None, "", "abcdef", "a"], 2) == [None, "", "ef", "a"]
    # fmt:on


def test_random_string():
    from orso.tools import random_string

    seen = set()
    for _ in range(100):
        rs = random_string()
        # we shouldn't see the same string twice
        assert rs not in seen
        seen.add(rs)
        # we shouldn't see padding in the string
        assert rs.count("=") == 0


def test_compiled_replace():
    data = _to_sv(["hello world", "banana", None])
    result = _sv_to_list(list_replace(data, b"l", b"L"))
    assert result == ["heLLo worLd", "banana", None]


def test_compiled_replace_bytes():
    data = vector_from_arrow(pyarrow.array([b"abcabc", b"", None], type=pyarrow.binary()))
    result = _sv_to_list(list_replace(data, b"abc", b"x"))
    assert result == ["xx", "", None]


def test_compiled_initcap():
    data = _to_sv(["hello world", "AmiGoS", "o'connor", "3rd street", None])
    result = _sv_to_list(list_initcap(data))
    assert result == ["Hello World", "Amigos", "O'Connor", "3rd Street", None]


def test_compiled_initcap_bytes():
    data = vector_from_arrow(pyarrow.array([b"mixed CASE"], type=pyarrow.binary()))
    result = _sv_to_list(list_initcap(data))
    assert result == ["Mixed Case"]


def test_re2_list_regex_replace_strings():
    """Test regex replace with string data (stored as bytes in Draken)"""
    data = Vector.from_arrow(pyarrow.array(["abc123", "xyz789", None]))
    pattern = rb"\d+"
    replacement = b""

    result = list_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"abc", b"xyz", None]


def test_re2_list_regex_replace_bytes():
    data = Vector.from_arrow(pyarrow.array([b"http://a.example", b"https://b.example"], type=pyarrow.binary()))
    pattern = b"^https?"
    replacement = b""

    result = list_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"://a.example", b"://b.example"]


def test_regex_replace_python_wrapper_returns_arrow():
    """Test that the Python wrapper returns PyArrow arrays with bytes"""
    data = pyarrow.array(["Earth", "Europa"])
    pattern = numpy.array(["^E"], dtype=object)
    replacement = numpy.array(["G"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, pyarrow.Array)
    # Result is binary (bytes) because Draken works with bytes
    assert result.to_pylist() == [b"Garth", b"Guropa"]


def test_regex_replace_invalid_pattern_raises():
    from opteryx.exceptions import InvalidFunctionParameterError

    data = pyarrow.array(["test"])
    pattern = numpy.array(["("], dtype=object)
    replacement = numpy.array([""], dtype=object)

    try:
        string_functions.regex_replace(data, pattern, replacement)
    except InvalidFunctionParameterError:
        pass
    else:
        assert False, "Expected InvalidFunctionParameterError to be raised"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
