# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import functools
from typing import List

import numpy
import pyarrow
import pyarrow as pa
from pyarrow import compute

from opteryx.compiled.draken.vectors.string_vector import StringVector
from opteryx.compiled.draken.vectors.string_vector import lowercase as string_vector_lowercase
from opteryx.compiled.draken.vectors.string_vector import uppercase as string_vector_uppercase
from opteryx.compiled.vector_ops import (
    vector_initcap,
    vector_length,
    vector_ltrim,
    vector_match_against,
    vector_md5,
    vector_replace,
    vector_reverse,
    vector_rtrim,
    vector_sha1,
    vector_sha256,
    vector_sha512,
    vector_soundex,
    vector_string_length,
    vector_string_slice_left,
    vector_string_slice_right,
    vector_trim,
)
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.vectors.embeddings import get_embedding_provider

"""Text and encoding function kernels.

Includes:
- Case conversion: UPPER, LOWER, INITCAP
- String analysis: LENGTH, SOUNDEX
- String manipulation: LEFT, RIGHT, CONCAT, CONCAT_WS, SUBSTRING, POSITION, TRIM, LTRIM, RTRIM,
                       LPAD, RPAD, LEVENSHTEIN, SPLIT, REPLACE, REGEXP_REPLACE
- String matching: internal MATCH/AGAINST support
- Character conversion: CHAR, ASCII
- Hash/encoding: MD5, SHA1, SHA256, SHA512, SHA224, SHA384, BASE64, BASE85, HEX
"""


# ---------------------------------------------------------------------------
# SIMD / Draken-backed kernels (migrated from opteryx/functions/__init__.py)
# ---------------------------------------------------------------------------


def to_lower(arr):
    """Fast lowercase using buffer-level SIMD operations."""
    return string_vector_lowercase(_as_string_vector(arr))


def to_upper(arr):
    """Fast uppercase using buffer-level SIMD operations."""
    return string_vector_uppercase(_as_string_vector(arr))


def vector_lengther(arr):
    """Return string lengths using the Draken StringVector API.

    This implementation converts the input to a ``StringVector`` and then
    computes lengths by looking at the internal offsets buffer.  No Arrow
    compute kernels are used at all, and dictionary inputs are handled by the
    vector conversion step.

    NOTE: Registered as engine="arrow" intentionally. The compiled kernel
    `vector_string_length` does not propagate nulls — null positions return 0.
    The Arrow null-reapplication below (compute.if_else) is load-bearing
    for correctness. Fix: implement null-aware length in the Draken kernel,
    then switch registration to engine="draken" and remove the Arrow postpass.
    """

    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if arr.__class__.__name__ in ("ArrayVector", "VectorVector"):
        return vector_length(arr).to_arrow()

    arrow_arr = _as_arrow_string_array(arr)

    if pyarrow.types.is_list(arrow_arr.type) or pyarrow.types.is_large_list(arrow_arr.type):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        return vector_length(vector_from_arrow(arrow_arr)).to_arrow()

    sv: StringVector
    sv = _as_string_vector(arr)
    result = vector_string_length(sv).to_arrow()
    if arrow_arr.null_count:
        return compute.if_else(
            compute.is_null(arrow_arr), pyarrow.scalar(None, type=result.type), result
        )
    return result


def _initcap(arr):
    return vector_initcap(_as_string_vector(arr))


def _reverse(arr):
    return vector_reverse(_as_string_vector(arr))


def _soundex(arr):
    return vector_soundex(_as_string_vector(arr))


def _md5(arr):
    return vector_md5(_as_string_vector(arr))


def _sha1(arr):
    return vector_sha1(_as_string_vector(arr))


def _sha256(arr):
    return vector_sha256(_as_string_vector(arr))


def _sha512(arr):
    return vector_sha512(_as_string_vector(arr))


def _replace(data, search, replace_val):
    # scalar args arrive as numpy arrays (arrow engine) or Draken constant vectors (draken engine)
    if hasattr(search, "__len__") and not isinstance(search, (str, bytes)):
        search = search[0]
    if hasattr(replace_val, "__len__") and not isinstance(replace_val, (str, bytes)):
        replace_val = replace_val[0]
    if isinstance(search, str):
        search = search.encode("utf-8")
    if isinstance(replace_val, str):
        replace_val = replace_val.encode("utf-8")
    return vector_replace(_as_string_vector(data), search, replace_val)


def _string_slice_left(arr, length):
    # length arrives as numpy array or Draken constant vector
    if hasattr(length, "__len__") and not isinstance(length, (str, bytes)):
        length = int(length[0])
    return vector_string_slice_left(_as_string_vector(arr), length)


def _string_slice_right(arr, length):
    # length arrives as numpy array or Draken constant vector
    if hasattr(length, "__len__") and not isinstance(length, (str, bytes)):
        length = int(length[0])
    return vector_string_slice_right(_as_string_vector(arr), length)


# ---------------------------------------------------------------------------
# Pure-Python / PyArrow kernels (migrated from opteryx/functions/string_functions.py)
# ---------------------------------------------------------------------------

_MATCH_AGAINST_MIN_SCORE = 0.6


def _as_arrow_string_array(value):
    if isinstance(value, pyarrow.ChunkedArray):
        return value.combine_chunks()
    if isinstance(value, pyarrow.Array):
        return value
    if hasattr(value, "to_arrow"):
        return _as_arrow_string_array(value.to_arrow())
    if isinstance(value, numpy.ndarray):
        return pyarrow.array(value.tolist())
    return pyarrow.array(value)


def _as_string_vector(value) -> StringVector:
    if isinstance(value, StringVector):
        return value

    arrow_arr = _as_arrow_string_array(value)
    if pyarrow.types.is_dictionary(arrow_arr.type):
        return StringVector.from_arrow(arrow_arr.dictionary_decode())
    return StringVector.from_arrow(arrow_arr)


def _is_dictionary_encoded(value) -> bool:
    if isinstance(value, (pyarrow.Array, pyarrow.ChunkedArray)):
        return pyarrow.types.is_dictionary(value.type)

    to_arrow = getattr(value, "to_arrow", None)
    if to_arrow is None:
        return False

    try:
        arrow_arr = to_arrow()
    except Exception:
        return False

    return isinstance(
        arrow_arr, (pyarrow.Array, pyarrow.ChunkedArray)
    ) and pyarrow.types.is_dictionary(arrow_arr.type)


def _is_string_fastpath_vector(value) -> bool:
    return isinstance(value, StringVector) or _is_dictionary_encoded(value)


def _as_match_vector(arr):
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

    if _is_string_fastpath_vector(arr):
        return arr

    if hasattr(arr, "to_arrow"):
        arr = arr.to_arrow()
    elif isinstance(arr, (numpy.ndarray, list, tuple)):
        arr = pyarrow.array(arr)
    elif not isinstance(arr, pyarrow.Array):
        return None

    vec = vector_from_arrow(arr)
    if _is_string_fastpath_vector(vec):
        return vec
    return None


def split(arr, delimiter=",", limit=None):
    """
    Fast SIMD-based split for single-character delimiters.
    Falls back to PyArrow for multi-character patterns or limits.
    """
    if not isinstance(delimiter, str):
        delimiter = delimiter[0]

    if len(delimiter) == 1 and limit is None:
        from opteryx.compiled.vector_ops import vector_split

        return vector_split(_as_string_vector(arr), ord(delimiter))

    delimiter = delimiter[0] if isinstance(delimiter, list) else delimiter
    if limit is not None:
        limit = limit[0]
        if limit < 1:
            raise InvalidFunctionParameterError("SPLIT limit must be a greater than 0")
    return compute.split_pattern(arr, delimiter, max_splits=limit or None)


def get_sha224(item):
    """calculate SHA224 hash of a value"""
    import hashlib

    if item is None:
        return None
    return hashlib.sha224(str(item).encode()).hexdigest()


def get_sha384(item):
    """calculate SHA384 hash of a value"""
    import hashlib

    if item is None:
        return None
    return hashlib.sha384(str(item).encode()).hexdigest()


def base64_encode(arr):
    """calculate BASE64 encoding of a string"""
    from opteryx.third_party.alantsd.base64 import encode

    if isinstance(arr, numpy.ndarray):
        arr = arr.astype(object)
        arr = [item.encode("utf-8") if isinstance(item, str) else item for item in arr]
    return [encode(item) for item in arr]


def base64_decode(arr):
    """calculate BASE64 decoding of a string"""
    from opteryx.third_party.alantsd.base64 import decode

    if isinstance(arr, numpy.ndarray):
        arr = arr.astype(object)
        arr = [item.encode("utf-8") if isinstance(item, str) else item for item in arr]
    return [decode(item) for item in arr]


def get_base85_encode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if item is None:
        return None
    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85encode(item).decode("UTF8")


def get_base85_decode(item):
    """calculate BASE85 decoding of a string"""
    import base64

    if item is None:
        return None
    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85decode(item).decode("UTF8")


def get_hex_encode(item):
    """calculate HEX encoding of a string"""
    import base64

    if item is None:
        return None
    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16encode(item).decode("UTF8")


def get_hex_decode(item):
    """calculate HEX decoding of a string"""
    import base64

    if item is None:
        return None
    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16decode(item).decode("UTF8")


def substring(arr: List[str], from_pos, count=None) -> List[List[str]]:
    """
    Extracts substrings from each string in the 'arr' list.
    """
    import itertools

    if len(arr) == 0:
        return [[]]

    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(zero_copy_only=False)

    # Broadcast scalar from_pos / count to per-row iterables
    if not hasattr(from_pos, "__iter__") or isinstance(from_pos, (str, bytes)):
        from_pos = itertools.repeat(from_pos)
    if count is None:
        count = itertools.repeat(None)
    elif not hasattr(count, "__iter__") or isinstance(count, (str, bytes)):
        count = itertools.repeat(count)

    def _inner(val, _from, _for):
        if _from is None:
            _from = 0
        if _from > 0:
            _from -= 1
        _for = int(_for) if _for and _for == _for else None  # nosec
        if _for is None:
            return val[_from:]
        return val[_from : _for + _from]

    return [_inner(val, _from, _for) for val, _from, _for in zip(arr, from_pos, count)]


def position(string, sub):
    """
    Returns the starting position of the first instance of substring in string.
    Positions start with 1. If not found, 0 is returned.
    """
    if isinstance(string, bytes):
        string = string.decode("utf8", errors="ignore")
    if isinstance(sub, bytes):
        sub = sub.decode("utf8", errors="ignore")
    return string.find(sub) + 1


def levenshtein(a, b):
    from opteryx.compiled.vector_ops import vector_levenshtein

    return vector_levenshtein(_as_string_vector(a), _as_string_vector(b))


def to_char(arr) -> List[str]:
    return [chr(a) for a in arr]


def to_ascii(arr) -> List[int]:
    # Arrow engine passes pyarrow Arrays; coerce them to Python strings.
    if hasattr(arr, "to_pylist"):
        arr = arr.to_pylist()
    return [ord(a) for a in arr]


def left_pad(arr, width, fill):
    width = width[0]
    fill = fill[0]
    return [str(a).rjust(width, fill) for a in arr]


def right_pad(arr, width, fill):
    width = width[0]
    fill = fill[0]
    return [str(a).ljust(width, fill) for a in arr]


def match_against(arr, val):
    """
    Semantic text match using cosine similarity over embedded text.
    """
    if isinstance(val, (str, bytes)):
        literal = val
    else:
        if len(val) == 0:
            return []
        literal = val[0]

    if literal is None:
        return []
    if isinstance(literal, bytes):
        literal = literal.decode("utf8", errors="ignore")

    query_text = str(literal).strip()
    match_vector = _as_match_vector(arr)
    if match_vector is None:
        return []
    if not query_text:
        return pyarrow.array([False] * len(match_vector), type=pyarrow.bool_())

    provider = get_embedding_provider()
    if provider is None:
        return pyarrow.array([False] * len(match_vector), type=pyarrow.bool_())
    return vector_match_against(
        match_vector,
        provider,
        query_text,
        _MATCH_AGAINST_MIN_SCORE,
    )


def _normalise_replacement(repl: bytes) -> bytes:
    """
    Normalise regex replacement backreferences from double-backslash form to single.

    SQL raw-string literals written as r'\\1' produce 3 bytes (backslash, backslash,
    digit) because the `r` prefix suppresses escape processing but the two backslash
    characters are still present verbatim.  RE2 interprets ``\\1`` as a literal
    backslash followed by the digit 1, NOT as capture-group 1.  ClickHouse (and users
    following its conventions) write ``r'\\1'`` expecting capture-group substitution.

    This helper folds the double-backslash form into the canonical single-backslash
    form (``b'\\1'``) that both RE2 and the DFA compiler recognise as a backreference,
    so ``r'\\1'`` and ``r'\1'`` behave identically.

    Only backreference positions (backslash followed by a digit 0-9) are collapsed;
    other double-backslash sequences are left untouched.
    """
    import re as _re

    return _re.sub(rb"\\\\([0-9])", rb"\\\1", repl)


def regex_replace(array, _pattern, _replacement):
    """
    Regex replacement using the vendored RE2 engine exposed via vector_ops.
    """
    from opteryx.compiled import vector_ops as compiled_vector_ops

    vector_regex_replace = getattr(compiled_vector_ops, "vector_regex_replace")

    def _as_arrow(value, label):
        if isinstance(value, pyarrow.Array):
            return value
        if hasattr(value, "to_arrow"):
            return value.to_arrow()
        if isinstance(value, numpy.ndarray):
            if value.ndim != 1:
                raise InvalidFunctionParameterError(f"{label} must be one-dimensional.")
            return pyarrow.array(value)
        if isinstance(value, (list, tuple)):
            return pyarrow.array(value)
        return None

    def as_bytes(value):
        if hasattr(value, "item"):
            value = value.item()
        elif hasattr(value, "as_py"):
            value = value.as_py()
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return str(value).encode("utf-8")

    array_arrow = _as_arrow(array, "Input")
    data_vector = _as_string_vector(array_arrow)
    input_type = array_arrow.type if array_arrow is not None else None

    pattern = as_bytes(_pattern[0])
    # Normalise \\N → \N so that SQL r'\\1' (3 bytes) is treated as the RE2
    # backreference \1 (2 bytes), matching ClickHouse / standard conventions.
    replacement = _normalise_replacement(as_bytes(_replacement[0]))

    try:
        result = vector_regex_replace(data_vector, pattern, replacement).to_arrow()

        if input_type is not None and (
            pyarrow.types.is_string(input_type)
            or pyarrow.types.is_large_string(input_type)
            or pyarrow.types.is_binary(input_type)
            or pyarrow.types.is_large_binary(input_type)
        ):
            return pyarrow.compute.cast(result, pyarrow.string())
        return result
    except ValueError as exc:
        raise InvalidFunctionParameterError(str(exc)) from exc
