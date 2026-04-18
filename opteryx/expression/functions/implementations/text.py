# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List

from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.draken.vectors.string_vector import StringVector
from opteryx.compiled.vector_ops import vector_match_against
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
# Utility functions and fallbacks
# ---------------------------------------------------------------------------

_MATCH_AGAINST_MIN_SCORE = 0.6


def split(arr, delimiter=",", limit=None):
    """Fast SIMD-based split for single-character delimiters."""
    # Extract scalar from constant vector
    if not isinstance(delimiter, str):
        delimiter = delimiter[0]
    if limit is not None:
        limit = int(limit[0])
        if limit < 1:
            raise InvalidFunctionParameterError("SPLIT limit must be a greater than 0")

    if len(delimiter) == 1 and limit is None:
        from opteryx.compiled.vector_ops import vector_split

        return vector_split(arr, ord(delimiter))

    # Python fallback for multi-character split (Draken vector_split only supports single-char)
    result = []
    for s in arr:
        if s is None:
            result.append(None)
        else:
            parts = s.split(delimiter, maxsplit=limit or -1) if limit else s.split(delimiter)
            result.append(parts)
    return result


def get_sha224(item):
    """calculate SHA224 hash of a value"""
    import hashlib

    return hashlib.sha224(str(item).encode()).hexdigest()


def get_sha384(item):
    """calculate SHA384 hash of a value"""
    import hashlib

    return hashlib.sha384(str(item).encode()).hexdigest()


def get_base85_encode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85encode(item).decode("UTF8")


def get_base85_decode(item):
    """calculate BASE85 decoding of a string"""
    import base64

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85decode(item).decode("UTF8")


def substring(arr: List[str], from_pos, count=None) -> List[List[str]]:
    """Extracts substrings from each string in the 'arr' list."""
    import itertools

    # Broadcast scalars to iterables
    if not hasattr(from_pos, "__iter__") or isinstance(from_pos, (str, bytes)):
        from_pos = itertools.repeat(from_pos)
    if count is None:
        count = itertools.repeat(None)
    elif not hasattr(count, "__iter__") or isinstance(count, (str, bytes)):
        count = itertools.repeat(count)

    def _inner(val, _from, _for):
        _from = int(_from) if _from is not None else 0
        if _from > 0:
            _from -= 1
        _for = int(_for) if _for is not None else None
        if _for is None:
            return val[_from:]
        return val[_from : _for + _from]

    return [_inner(val, _from, _for) for val, _from, _for in zip(arr, from_pos, count)]


def to_char(arr) -> List[str]:
    return [chr(a) for a in arr]


def to_ascii(arr) -> List[int]:
    return [ord(a) for a in arr]


def left_pad(arr, width, fill):
    # Extract scalars from constant vectors
    width = width[0]
    fill = fill[0]
    return [str(a).rjust(width, fill) for a in arr]


def right_pad(arr, width, fill):
    # Extract scalars from constant vectors
    width = width[0]
    fill = fill[0]
    return [str(a).ljust(width, fill) for a in arr]


def match_against(arr, val):
    """
    Semantic text match using cosine similarity over embedded text.
    """
    # Extract scalar from constant vector
    if isinstance(val, (str, bytes)):
        literal = val
    else:
        literal = val[0]

    if isinstance(literal, bytes):
        literal = literal.decode("utf8")

    query_text = str(literal).strip()
    if not query_text:
        return vector_from_sequence([False] * len(arr))

    provider = get_embedding_provider()
    if provider is None:
        return vector_from_sequence([False] * len(arr))
    return vector_match_against(
        arr,
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


def regex_replace(array, pattern, replacement):
    """Regex replacement using the vendored RE2 engine."""
    from opteryx.compiled import vector_ops as compiled_vector_ops

    vector_regex_replace = getattr(compiled_vector_ops, "vector_regex_replace")

    pat = pattern[0]
    repl = replacement[0]

    # Convert to bytes if needed
    pattern_bytes = (
        pat
        if isinstance(pat, bytes)
        else pat.encode("utf-8")
        if isinstance(pat, str)
        else bytes(pat)
    )
    repl_bytes = (
        repl
        if isinstance(repl, bytes)
        else repl.encode("utf-8")
        if isinstance(repl, str)
        else bytes(repl)
    )

    replacement_bytes = _normalise_replacement(repl_bytes)

    try:
        result = vector_regex_replace(array, pattern_bytes, replacement_bytes)
        return result
    except ValueError as exc:
        raise InvalidFunctionParameterError(str(exc)) from exc
