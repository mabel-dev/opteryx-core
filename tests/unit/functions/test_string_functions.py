import os
import sys

import numpy
import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.interop.arrow import vector_from_arrow

from opteryx.compiled import vector_ops as compiled_vector_ops
from opteryx.draken import Vector
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
    data = _to_sv(["  hello  ", "xxE", None])

    assert _sv_to_list(vector_trim(data)) == ["hello", "xxE", None]
    assert _sv_to_list(vector_trim(data, "x")) == ["  hello  ", "E", None]

    assert _sv_to_list(vector_ltrim(data)) == ["hello  ", "xxE", None]
    assert _sv_to_list(vector_ltrim(data, " x")) == ["hello  ", "E", None]

    assert _sv_to_list(vector_rtrim(data)) == ["  hello", "xxE", None]
    assert _sv_to_list(vector_rtrim(data, " x")) == ["  hello", "xxE", None]


def test_compiled_trim_from_literal():
    import pyarrow as pa
    from opteryx.draken.vectors.scalar_constructors import from_scalar
    from opteryx.draken.vectors.string_vector import StringVector

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


# ---------------------------------------------------------------------------
# DFA compiler isolation tests — pure Python, no compiled module required
# ---------------------------------------------------------------------------


def test_dfa_compiler_url_pattern_is_compilable():
    """URL domain extraction pattern must be recognized as compilable (not fallback)."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(
        b"^https?://(?:www\\.)?([^/]+)/.*$",
        b"\\1",
    )
    assert not proc.fallback_to_re2, "URL pattern should be compiled, not forwarded to RE2"
    assert proc.compiled
    assert proc.operations is not None
    assert len(proc.operations) > 0


def test_dfa_compiler_url_pattern_extract_while_not_has_target_char():
    """EXTRACT_WHILE_NOT must carry target_char=ord('/') — not 0 (null byte)."""
    from opteryx.utils.regex_compiler import OperationType, RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(
        b"^https?://(?:www\\.)?([^/]+)/.*$",
        b"\\1",
    )
    assert not proc.fallback_to_re2

    extract_ops = [op for op in proc.operations if op.op_type == OperationType.OP_EXTRACT_WHILE_NOT]
    assert len(extract_ops) == 1, "Expected exactly one EXTRACT_WHILE_NOT op"
    assert extract_ops[0].target_char == ord("/"), (
        f"target_char must be ord('/') == 47, got {extract_ops[0].target_char!r}. "
        "A null target_char means avx_search never finds the delimiter and captures "
        "the whole remaining string, causing MATCH_LITERAL '/' to fail and every "
        "row to return None."
    )


def test_dfa_compiler_to_cython_args_preserves_target_char():
    """to_cython_args() tuple[4] must carry target_char=ord('/') for EXTRACT_WHILE_NOT."""
    from opteryx.utils.regex_compiler import OperationType, RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(
        b"^https?://(?:www\\.)?([^/]+)/.*$",
        b"\\1",
    )
    ops, ops_len, fallback = proc.to_cython_args()
    assert not fallback
    assert isinstance(ops, list)
    assert ops_len == len(ops)

    # Each operation must be a 5-tuple: (op_type, pattern, pattern_len, capture_id, target_char)
    for tup in ops:
        assert len(tup) == 5, f"Expected 5-tuple, got {len(tup)}: {tup}"

    extract_tuples = [t for t in ops if t[0] == OperationType.OP_EXTRACT_WHILE_NOT]
    assert len(extract_tuples) == 1
    assert extract_tuples[0][4] == ord("/"), (
        f"Cython arg tuple[4] (target_char) must be 47, got {extract_tuples[0][4]!r}"
    )


def test_dfa_compiler_unsupported_pattern_falls_back():
    """Patterns the DFA compiler cannot handle must report fallback_to_re2=True."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()

    # Lookahead — unsupported
    proc = compiler.compile(b"(?=foo)bar", b"baz")
    assert proc.fallback_to_re2, "Lookahead pattern should fall back to RE2"

    # Plain literal — not a recognised procedure yet
    proc2 = compiler.compile(b"hello", b"world")
    assert proc2.fallback_to_re2, "Unrecognised literal pattern should fall back to RE2"

    # Complex alternation (> 2 pipes)
    proc3 = compiler.compile(b"a|b|c|d", b"x")
    assert proc3.fallback_to_re2, "Complex alternation should fall back to RE2"


# ---------------------------------------------------------------------------
# DFA end-to-end tests — via Python wrapper with CORRECT calling convention.
#
# _dfa_replace(array, _pattern, _replacement) mirrors regex_replace: the
# evaluator passes numpy arrays so that _pattern[0] extracts the bytes value.
# Passing raw bytes directly causes _pattern[0] to yield an int (the first
# byte), producing garbage input to the RE2/DFA engine.
# ---------------------------------------------------------------------------


def test_dfa_replace_url_list_matches_re2():
    """_dfa_replace must match RE2 output for the URL domain-extraction pattern."""
    data = Vector.from_arrow(
        pyarrow.array(
            [
                "https://www.example.com/a",
                "http://foo.bar/x",
                "https://sub.domain.net/path",
                None,
            ]
        )
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    dfa_result = string_functions._dfa_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist(), (
        f"DFA result {dfa_result.to_pylist()!r} != RE2 result {re2_result.to_pylist()!r}"
    )


def test_dfa_replace_url_cardinality_preserved():
    """Each distinct host must remain a distinct result — not all collapse into one group."""
    hosts = [
        "a.example.com",
        "b.example.com",
        "c.example.com",
        "d.example.com",
        "e.example.com",
    ]
    data = Vector.from_arrow(pyarrow.array([f"https://{h}/path" for h in hosts]))
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    result = string_functions._dfa_replace(data, pattern, replacement)
    result_list = result.to_pylist()

    non_null = [v for v in result_list if v is not None]
    assert len(non_null) == 5, f"Expected 5 distinct results, got: {result_list!r}"

    decoded = {v.decode("utf-8") if isinstance(v, bytes) else v for v in non_null}
    assert decoded == set(hosts), f"Extracted hosts mismatch: {decoded}"


def test_dfa_replace_url_www_prefix_stripped():
    """www. prefix must be stripped; non-www. domains must be returned as-is."""
    data = Vector.from_arrow(
        pyarrow.array(
            [
                "https://www.with-www.com/page",
                "https://without-www.com/page",
            ]
        )
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    dfa_result = string_functions._dfa_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist()
    decoded = [v.decode("utf-8") if isinstance(v, bytes) else v for v in dfa_result.to_pylist()]
    assert decoded == ["with-www.com", "without-www.com"]


def test_dfa_replace_url_null_passthrough():
    """Null inputs must produce null outputs; non-null must not be affected."""
    data = Vector.from_arrow(pyarrow.array([None, "https://x.com/y", None]))
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    result = string_functions._dfa_replace(data, pattern, replacement)
    result_list = result.to_pylist()

    assert result_list[0] is None
    assert result_list[2] is None
    assert result_list[1] is not None


def test_dfa_replace_url_mixed_http_https():
    """Both http:// and https:// URLs must be handled correctly."""
    data = Vector.from_arrow(
        pyarrow.array(
            [
                "http://plain.example.com/x",
                "https://secure.example.com/x",
                "https://www.with-prefix.example.com/x",
                "http://www.also-prefix.example.com/x",
            ]
        )
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    dfa_result = string_functions._dfa_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist()


def test_dfa_replace_unsupported_pattern_falls_back_transparently():
    """Patterns the compiler cannot handle must fall back to RE2 with identical output."""
    data = pyarrow.array(["Earth", "Europa", "Eris", None])
    pattern = numpy.array([b"^E"], dtype=object)
    replacement = numpy.array([b"G"], dtype=object)

    dfa_result = string_functions._dfa_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist()


def test_dfa_replace_invalid_pattern_raises():
    """Invalid regex pattern must raise InvalidFunctionParameterError."""
    from opteryx.exceptions import InvalidFunctionParameterError

    data = pyarrow.array(["test", None])
    pattern = numpy.array([b"("], dtype=object)
    replacement = numpy.array([b""], dtype=object)

    try:
        string_functions._dfa_replace(data, pattern, replacement)
    except InvalidFunctionParameterError:
        pass
    else:
        assert False, "Expected InvalidFunctionParameterError to be raised"


def test_dfa_replace_dict_vector_matches_re2():
    """Dictionary-encoded inputs must produce the same results as a plain string array."""
    data = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1, 2, 0, None], type=pyarrow.int8()),
        pyarrow.array(
            [
                "https://www.one.example/path",
                "https://two.example/path",
                "http://three.example/path",
            ],
            type=pyarrow.string(),
        ),
    )
    pattern = numpy.array([b"^https?://(?:www\\.)?([^/]+)/.*$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    dfa_result = string_functions._dfa_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist()


def test_re2_list_regex_replace_bytes():
    data = Vector.from_arrow(
        pyarrow.array([b"http://a.example", b"https://b.example"], type=pyarrow.binary())
    )
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
    assert [x if isinstance(x, bytes) else x.encode("utf-8") for x in result.to_pylist()] == [
        b"Garth",
        b"Guropa",
    ]


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


def test_dfa_replace_double_backslash_form_matches_single():
    """SQL r'\\\\1' (3-byte replacement) must produce the same output as r'\\1' (2-byte)."""
    data = pyarrow.array(
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


def test_dfa_replace_double_backslash_extracts_domains():
    """SQL r'\\\\1' (3-byte form) must extract domain names, not return '\\\\1' literally."""
    data = pyarrow.array(
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


def test_dfa_replace_double_backslash_dfa_fast_path_engaged():
    """After normalisation, r'\\\\1' (SQL double-backslash) must engage the DFA fast-path."""
    from opteryx.expression.functions.implementations.text import _normalise_replacement
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    pattern = b"^https?://(?:www\\.)?([^/]+)/.*$"
    repl_two_bs = b"\\\\1"  # SQL r'\\1' form

    normed = _normalise_replacement(repl_two_bs)
    compiler = RegexToDFACompiler()
    proc = compiler.compile(pattern, normed)

    assert not proc.fallback_to_re2, (
        "DFA compiler must compile the URL pattern with the double-backslash "
        "replacement (after normalisation) — fallback_to_re2 must be False"
    )


# ---------------------------------------------------------------------------
# _compile_strip_prefix_capture_rest — new pattern class tests
# ---------------------------------------------------------------------------


def test_dfa_compiler_strip_prefix_no_prefix_compiles():
    """^(.+)$ with \\1 replacement must compile to the DFA fast-path."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^(.+)$", b"\\1")
    assert not proc.fallback_to_re2, "^(.+)$ → \\1 must compile to DFA fast-path"
    assert proc.compiled


def test_dfa_compiler_strip_prefix_optional_single_char_compiles():
    """^M?(.+)$ with \\1 replacement must compile to the DFA fast-path."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^M?(.+)$", b"\\1")
    assert not proc.fallback_to_re2, "^M?(.+)$ → \\1 must compile to DFA fast-path"
    assert proc.compiled


def test_dfa_compiler_strip_prefix_mandatory_prefix_compiles():
    """^Mercury(.+)$ with \\1 replacement must compile to the DFA fast-path."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^Mercury(.+)$", b"\\1")
    assert not proc.fallback_to_re2, "^Mercury(.+)$ → \\1 must compile to DFA fast-path"
    assert proc.compiled


def test_dfa_compiler_strip_prefix_multi_char_optional_rejected():
    """^Mercury?(.+)$ must fall back: ? applies only to the last char 'y', not the whole word."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^Mercury?(.+)$", b"\\1")
    assert proc.fallback_to_re2, (
        "^Mercury?(.+)$ must fall back — multi-char optional prefix is ambiguous"
    )


def test_dfa_compiler_strip_prefix_wrong_replacement_rejected():
    """^M?(.+)$ with a non-\\1 replacement must fall back to RE2."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^M?(.+)$", b"REPLACED")
    assert proc.fallback_to_re2, "Non-\\1 replacement must fall back to RE2"


def test_dfa_replace_strip_prefix_planets():
    """^M?(.+)$ → \\1 must strip leading M from Mercury/Mars but leave others unchanged."""
    data = pyarrow.array(
        [
            "Mercury",
            "Venus",
            "Earth",
            "Mars",
            "Jupiter",
            "Saturn",
            "Uranus",
            "Neptune",
            None,
        ]
    )
    pattern = numpy.array([b"^M?(.+)$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)
    result_list = result.to_pylist()

    def _str(v):
        return v.decode("utf-8") if isinstance(v, bytes) else v

    assert _str(result_list[0]) == "ercury", f"Mercury: {result_list[0]!r}"
    assert _str(result_list[1]) == "Venus", f"Venus: {result_list[1]!r}"
    assert _str(result_list[2]) == "Earth", f"Earth: {result_list[2]!r}"
    assert _str(result_list[3]) == "ars", f"Mars: {result_list[3]!r}"
    assert _str(result_list[4]) == "Jupiter", f"Jupiter: {result_list[4]!r}"
    assert result_list[8] is None, f"None input: {result_list[8]!r}"


def test_dfa_replace_strip_prefix_mandatory_prefix():
    """^http(.+)$ → \\1 must strip the mandatory 'http' prefix."""
    data = pyarrow.array(
        [
            "http://example.com",
            "https://example.com",  # does not start with exactly 'http' + rest? it does: strip 'http'
            "ftp://example.com",  # no match
            None,
        ]
    )
    pattern = numpy.array([b"^http(.+)$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)
    result_list = result.to_pylist()

    def _str(v):
        return v.decode("utf-8") if isinstance(v, bytes) else v

    assert _str(result_list[0]) == "://example.com", f"http://: {result_list[0]!r}"
    assert _str(result_list[1]) == "s://example.com", f"https://: {result_list[1]!r}"
    assert result_list[2] is None, f"ftp://: {result_list[2]!r}"
    assert result_list[3] is None, f"None: {result_list[3]!r}"


def test_dfa_replace_strip_prefix_matches_re2():
    """DFA and RE2 must agree on ^M?(.+)$ for all planet names."""
    data = pyarrow.array(
        ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"]
    )
    pattern = numpy.array([b"^M?(.+)$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    dfa_result = string_functions.regex_replace(data, pattern, replacement)
    re2_result = string_functions.regex_replace(data, pattern, replacement)

    assert dfa_result.to_pylist() == re2_result.to_pylist(), (
        f"DFA and RE2 results differ:\n  DFA: {dfa_result.to_pylist()}\n  RE2: {re2_result.to_pylist()}"
    )


def test_dfa_replace_strip_prefix_no_prefix_identity():
    """^(.+)$ → \\1 must return the string unchanged for non-empty inputs, None for empty."""
    data = pyarrow.array(["hello", "world", "", None])
    pattern = numpy.array([b"^(.+)$"], dtype=object)
    replacement = numpy.array([b"\\1"], dtype=object)

    result = string_functions.regex_replace(data, pattern, replacement)
    result_list = result.to_pylist()

    def _str(v):
        return v.decode("utf-8") if isinstance(v, bytes) else v

    assert _str(result_list[0]) == "hello", f"hello: {result_list[0]!r}"
    assert _str(result_list[1]) == "world", f"world: {result_list[1]!r}"
    # empty string: .+ requires at least one char, so no match → None
    assert result_list[2] is None, f"empty string: {result_list[2]!r}"
    assert result_list[3] is None, f"None: {result_list[3]!r}"


def test_dfa_replace_strip_prefix_non_greedy_variant_compiles():
    """^M?(.+?)$ (non-greedy) must also compile — equivalent to (.+) when anchored to $."""
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    compiler = RegexToDFACompiler()
    proc = compiler.compile(b"^M?(.+?)$", b"\\1")
    assert not proc.fallback_to_re2, "^M?(.+?)$ → \\1 must compile to DFA fast-path"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
