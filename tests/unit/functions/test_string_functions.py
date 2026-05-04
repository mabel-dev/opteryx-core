import os
import sys

import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.interop.arrow import vector_from_arrow
from draken.vectors.string_vector import StringVector

from opteryx.compiled import vector_ops as compiled_vector_ops
from draken import Vector
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
    from opteryx.utils import random_string

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
    # Create constant-encoded vectors (repeated for all rows)
    search = _to_sv(["l", "l", "l"])
    replace = _to_sv(["L", "L", "L"])
    result = _sv_to_list(vector_replace(data, search, replace))
    assert result == ["heLLo worLd", "banana", None]


def test_compiled_replace_bytes():
    data = vector_from_arrow(pyarrow.array([b"abcabc", b"", None], type=pyarrow.binary()))
    # Create constant-encoded vectors (repeated for all rows)
    search = _to_sv(["abc", "abc", "abc"])
    replace = _to_sv(["x", "x", "x"])
    result = _sv_to_list(vector_replace(data, search, replace))
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
    data = _to_sv(["  hello  ", "xxE", None])

    assert _sv_to_list(vector_trim(data)) == ["hello", "xxE", None]
    assert _sv_to_list(vector_trim(data, "x")) == ["  hello  ", "E", None]

    assert _sv_to_list(vector_ltrim(data)) == ["hello  ", "xxE", None]
    assert _sv_to_list(vector_ltrim(data, " x")) == ["hello  ", "E", None]

    assert _sv_to_list(vector_rtrim(data)) == ["  hello", "xxE", None]
    assert _sv_to_list(vector_rtrim(data, " x")) == ["  hello", "xxE", None]


def test_compiled_trim_from_literal():
    import pyarrow as pa
    from draken.vectors.scalar_constructors import from_scalar
    from draken.vectors.string_vector import StringVector

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
    data = Vector.from_arrow(
        pyarrow.array([b"http://a.example", b"https://b.example"], type=pyarrow.binary())
    )
    pattern = b"^https?"
    replacement = b""

    result = vector_regex_replace(data, pattern, replacement).to_pylist()

    assert result == [b"://a.example", b"://b.example"]


def test_regex_replace_python_wrapper_returns_arrow():
    """Test that the Python wrapper returns Draken StringVector with bytes"""
    data = _to_sv(["Earth", "Europa"])
    pattern = numpy.array(["^E"], dtype=object)
    replacement = numpy.array(["G"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, StringVector)
    # Result is binary (bytes) because Draken works with bytes
    # But the scalar wrapper may return unicode; accept both formats.
    assert [x if isinstance(x, bytes) else x.encode("utf-8") for x in result.to_pylist()] == [
        b"Garth",
        b"Guropa",
    ]


def test_regex_replace_python_wrapper_dictionary_input():
    pa_dict_array = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1, 0, None], type=pyarrow.int8()),
        pyarrow.array(["http://a.example", "https://b.example"], type=pyarrow.string()),
    )
    data = vector_from_arrow(pa_dict_array)
    pattern = numpy.array([r"^https?".encode("utf8")], dtype=object)
    replacement = numpy.array([b""], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)

    assert isinstance(result, StringVector)
    assert result.to_pylist() == [b"://a.example", b"://b.example", b"://a.example", None]


def test_regex_replace_invalid_pattern_raises():
    from opteryx.exceptions import InvalidFunctionParameterError

    data = _to_sv(["test"])
    pattern = numpy.array(["("], dtype=object)
    replacement = numpy.array([""], dtype=object)

    try:
        string_functions.regex_replace(data, pattern, replacement)
    except InvalidFunctionParameterError:
        pass
    else:
        assert False, "Expected InvalidFunctionParameterError to be raised"


# ---------------------------------------------------------------------------
# _normalise_replacement unit tests
# ---------------------------------------------------------------------------


def test_normalise_replacement_double_backslash_digit_collapsed():
    """\\\\N (3 bytes) must be folded to \\N (2 bytes) for each digit 0-9."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement

    assert _normalise_replacement(b"\\\\1") == b"\\1"
    assert _normalise_replacement(b"\\\\0") == b"\\0"
    assert _normalise_replacement(b"\\\\9") == b"\\9"


def test_normalise_replacement_already_canonical_unchanged():
    """\\N (2 bytes, already canonical) must be returned as-is."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement

    assert _normalise_replacement(b"\\1") == b"\\1"
    assert _normalise_replacement(b"\\0") == b"\\0"


def test_normalise_replacement_non_digit_not_collapsed():
    """\\\\x where x is not a digit must NOT be collapsed."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement

    assert _normalise_replacement(b"\\\\x") == b"\\\\x"
    assert _normalise_replacement(b"\\\\n") == b"\\\\n"


def test_normalise_replacement_plain_literal_unchanged():
    """Plain literals with no backslash must pass through unmodified."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement

    assert _normalise_replacement(b"hello") == b"hello"
    assert _normalise_replacement(b"") == b""


def test_normalise_replacement_mixed():
    """Mixed content: only the \\\\N part is collapsed."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement

    assert _normalise_replacement(b"prefix\\\\1suffix") == b"prefix\\1suffix"


# ---------------------------------------------------------------------------
# Double-backslash SQL form: r'\\1' must produce correct domain extraction
# ---------------------------------------------------------------------------


def test_regex_replace_double_backslash_form_matches_single():
    """SQL r'\\\\1' (3-byte replacement) must produce the same output as r'\\1' (2-byte)."""
    data = _to_sv(
        [
            "https://www.example.com/path",
            "http://foo.bar/x",
            "https://sub.domain.net/path/to/page",
            None,
            "not-a-url",
        ]
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)

    repl_one_bs = numpy.array([b"\\1"], dtype=object)  # canonical 2-byte form
    repl_two_bs = numpy.array([b"\\\\1"], dtype=object)  # SQL r'\\1' 3-byte form

    result_one = string_functions.regex_replace(data, pattern, repl_one_bs)
    result_two = string_functions.regex_replace(data, pattern, repl_two_bs)

    assert result_one.to_pylist() == result_two.to_pylist(), (
        f"Single-backslash result {result_one.to_pylist()!r} "
        f"!= double-backslash result {result_two.to_pylist()!r}"
    )


def test_regex_replace_double_backslash_extracts_domains():
    """SQL r'\\\\1' (3-byte form) must extract domain names, not return '\\\\1' literally."""
    data = _to_sv(
        [
            "https://www.example.com/path",
            "http://foo.bar/x",
            "https://www.google.com/search?q=test",
        ]
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    repl_two_bs = numpy.array([b"\\\\1"], dtype=object)  # SQL r'\\1' 3-byte form

    result = string_functions.regex_replace(data, pattern, repl_two_bs)
    result_list = result.to_pylist()

    # Must NOT return the literal replacement string
    for val in result_list:
        if val is not None:
            decoded = val.decode("utf-8") if isinstance(val, bytes) else val
            assert decoded not in (r"\1", "\\1", r"\\1"), (
                f"Got literal replacement instead of domain name: {val!r}"
            )

    # Must produce actual domain names
    decoded_list = [v.decode("utf-8") if isinstance(v, bytes) else v for v in result_list]
    assert decoded_list == ["example.com", "foo.bar", "google.com"], (
        f"Unexpected domain extraction: {decoded_list!r}"
    )


compile_dfa_program = getattr(compiled_vector_ops, "compile_dfa_program")
vector_dfa_extract = getattr(compiled_vector_ops, "vector_dfa_extract")


def _const_sv(value: bytes) -> StringVector:
    return StringVector.from_constant(value, 1)


def test_dfa_extract_compiles_for_full_consume_pattern():
    program = compile_dfa_program(rb"^https?://(?:www\.)?([^/]+)/.*$", rb"\1")
    assert isinstance(program, bytes) and len(program) > 0


def test_dfa_extract_executes_full_consume_pattern():
    program = compile_dfa_program(rb"^https?://(?:www\.)?([^/]+)/.*$", rb"\1")
    data = _to_sv(["https://www.example.com/path", "http://foo.bar/x", "no-match-here"])
    result = vector_dfa_extract(data, _const_sv(program)).to_pylist()
    decoded = [v.decode("utf-8") if isinstance(v, bytes) else v for v in result]
    assert decoded == ["example.com", "foo.bar", "no-match-here"]


def test_dfa_extract_rejects_unanchored_pattern():
    # Without ^ the executor would silently drop the unmatched prefix.
    assert compile_dfa_program(rb"https?://([^/]+)/.*$", rb"\1") is None


def test_dfa_extract_rejects_non_consuming_pattern():
    # Anchored at start but not at end and without consume-to-end.
    assert compile_dfa_program(rb"^https?://([^/]+)", rb"\1") is None


def test_dfa_extract_non_match_returns_input():
    program = compile_dfa_program(rb"^https?://(?:www\.)?([^/]+)/.*$", rb"\1")
    data = _to_sv(["plain-string"])
    result = vector_dfa_extract(data, _const_sv(program)).to_pylist()
    decoded = [v.decode("utf-8") if isinstance(v, bytes) else v for v in result]
    assert decoded == ["plain-string"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
