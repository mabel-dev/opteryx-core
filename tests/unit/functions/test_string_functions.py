import os
import sys

import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken import Vector
from opteryx.draken.interop.arrow import vector_from_arrow
from opteryx.compiled import vector_ops as compiled_vector_ops
from opteryx.expression.functions.implementations import text as string_functions

vector_initcap = getattr(compiled_vector_ops, "vector_initcap")
vector_regex_replace = getattr(compiled_vector_ops, "vector_regex_replace")
vector_replace = getattr(compiled_vector_ops, "vector_replace")
vector_trim = getattr(compiled_vector_ops, "vector_trim")
vector_ltrim = getattr(compiled_vector_ops, "vector_ltrim")
vector_rtrim = getattr(compiled_vector_ops, "vector_rtrim")
vector_string_slice_right = getattr(compiled_vector_ops, "vector_string_slice_right")
vector_string_slice_left = getattr(compiled_vector_ops, "vector_string_slice_left")


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
    slicer = lambda arr, n: _sv_to_list(vector_string_slice_left(_to_sv(arr), n))

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
    slicer = lambda arr, n: _sv_to_list(vector_string_slice_right(_to_sv(arr), n))

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
    result = _sv_to_list(vector_replace(data, b"l", b"L"))
    assert result == ["heLLo worLd", "banana", None]


def test_compiled_replace_bytes():
    data = vector_from_arrow(pyarrow.array([b"abcabc", b"", None], type=pyarrow.binary()))
    result = _sv_to_list(vector_replace(data, b"abc", b"x"))
    assert result == ["xx", "", None]


def test_compiled_initcap():
    data = _to_sv(["hello world", "AmiGoS", "o'connor", "3rd street", None])
    result = _sv_to_list(vector_initcap(data))
    assert result == ["Hello World", "Amigos", "O'Connor", "3rd Street", None]


def test_compiled_initcap_bytes():
    data = vector_from_arrow(pyarrow.array([b"mixed CASE"], type=pyarrow.binary()))
    result = _sv_to_list(vector_initcap(data))
    assert result == ["Mixed Case"]


def test_compiled_trim_kernels():
    data = _to_sv(["  hello  ", "xxE" , None])

    assert _sv_to_list(vector_trim(data)) == ["hello", "xxE", None]
    assert _sv_to_list(vector_trim(data, "x")) == ["  hello  ", "E", None]

    assert _sv_to_list(vector_ltrim(data)) == ["hello  ", "xxE", None]
    assert _sv_to_list(vector_ltrim(data, " x")) == ["hello  ", "E", None]

    assert _sv_to_list(vector_rtrim(data)) == ["  hello", "xxE", None]
    assert _sv_to_list(vector_rtrim(data, " x")) == ["  hello", "xxE", None]


def test_compiled_trim_from_literal():
    from opteryx.draken.vectors.string_vector import StringVector
    from opteryx.draken.vectors.scalar_constructors import from_scalar
    import pyarrow as pa

    arr = StringVector.from_arrow(pa.array(["xxxhelloxxx"]))
    assert _sv_to_list(vector_trim(arr, "x")) == ["hello"]
    assert _sv_to_list(vector_trim(arr, from_scalar("x", 1))) == ["hello"]
    assert _sv_to_list(vector_trim(arr, StringVector.from_arrow(pa.array(["x"])))) == ["hello"]
    assert _sv_to_list(vector_trim(arr, [b"x"])) == ["hello"]
    assert _sv_to_list(vector_rtrim(arr, " x")) == ["xxxhello"]


def test_re2_list_regex_replace_strings():
    """Test regex replace with string data (stored as bytes in Draken)"""
    data = Vector.from_arrow(pyarrow.array(["abc123", "xyz789", None]))
    pattern = rb"\d+"
    replacement = b""

    result = vector_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"abc", b"xyz", None]


def test_re2_list_regex_replace_bytes():
    data = Vector.from_arrow(pyarrow.array([b"http://a.example", b"https://b.example"], type=pyarrow.binary()))
    pattern = b"^https?"
    replacement = b""

    result = vector_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"://a.example", b"://b.example"]


def test_regex_replace_python_wrapper_returns_arrow():
    """Test that the Python wrapper returns PyArrow arrays with bytes"""
    data = pyarrow.array(["Earth", "Europa"])
    pattern = numpy.array(["^E"], dtype=object)
    replacement = numpy.array(["G"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, pyarrow.Array)
    # Result is binary (bytes) because Draken works with bytes
    # But the scalar wrapper may return unicode; accept both formats.
    assert [x if isinstance(x, bytes) else x.encode("utf-8") for x in result.to_pylist()] == [b"Garth", b"Guropa"]


def test_regex_replace_python_wrapper_dictionary_input():
    data = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1, 0, None], type=pyarrow.int8()),
        pyarrow.array(["http://a.example", "https://b.example"], type=pyarrow.string()),
    )
    pattern = numpy.array([r"^https?".encode("utf8")], dtype=object)
    replacement = numpy.array([b""], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, pyarrow.Array)
    assert result.to_pylist() == [b"://a.example", b"://b.example", b"://a.example", None]


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
