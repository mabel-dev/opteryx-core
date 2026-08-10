"""
Value + shape parity for the native TRIM / LTRIM / RTRIM draken kernels
(draken/ops/kernels/string_trim.cpp).

These run TRIM/LTRIM/RTRIM over a column IN THE NATIVE ENGINE (the greenfield C++
path — there is no fallback) and check the result byte-for-byte against a Python
str.{strip,lstrip,rstrip} oracle. Two shapes are exercised:

  * DENSE     — user_name (mixed encoding, largely dense).
  * COMPRESSED — is_reply_to arrives dict/compressed from the scanner.

The kernels are SHAPE-PRESERVING: the compressed-input result must be byte-identical
to the per-row dense oracle (a dict-shaped input must not change the answer). We also
inject leading/trailing whitespace around a column value to prove the kernel actually
strips (the raw tweets columns have no boundary whitespace, so a no-op would pass a
naive raw==raw check).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

import opteryx


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(len(morsel)):
            out.append(morsel[i])
    return out


# ASCII-whitespace only — the kernel strips 0x09-0x0D + 0x20 (matches the catalog's
# whitespace TRIM). Python's str.strip() also strips these for the tweets data (no
# exotic Unicode whitespace at the boundaries), so it is a faithful oracle here.
_LIMIT = 3000


@pytest.mark.parametrize(
    "col",
    ["user_name", "is_reply_to"],  # dense, compressed
)
@pytest.mark.parametrize(
    "func,pyfn",
    [
        ("TRIM", str.strip),
        ("LTRIM", str.lstrip),
        ("RTRIM", str.rstrip),
    ],
)
def test_trim_value_parity(col, func, pyfn):
    src = _rows(f"SELECT {col} AS r FROM testdata.tweets LIMIT {_LIMIT}")
    got = _rows(f"SELECT {func}({col}) AS r FROM testdata.tweets LIMIT {_LIMIT}")
    assert len(src) == len(got)
    for (a,), (b,) in zip(src, got):
        expected = None if a is None else pyfn(a)
        assert b == expected, (a, expected, b)


@pytest.mark.parametrize(
    "func,pyfn",
    [
        ("TRIM", str.strip),
        ("LTRIM", str.lstrip),
        ("RTRIM", str.rstrip),
    ],
)
def test_trim_strips_injected_whitespace(func, pyfn):
    """Wrap a column value in whitespace, then trim — proves bytes are removed."""
    rows = _rows(
        f"SELECT {func}('  ' || user_name || '  ') AS t, user_name AS u "
        f"FROM testdata.tweets LIMIT {_LIMIT}"
    )
    for t, u in rows:
        assert t == pyfn("  " + u + "  ")


def test_trim_compressed_matches_dense():
    """SHAPE PARITY: TRIM over the compressed column is byte-identical to the
    per-row dense oracle — a dict-shaped input must not change the answer."""
    raw = _rows(f"SELECT is_reply_to AS r FROM testdata.tweets LIMIT {_LIMIT}")
    trimmed = _rows(f"SELECT TRIM(is_reply_to) AS r FROM testdata.tweets LIMIT {_LIMIT}")
    for (a,), (b,) in zip(raw, trimmed):
        assert b == (None if a is None else a.strip())


def test_trim_distinct_consumer():
    """Compression-aware DISTINCT consumer over shape-preserving TRIM output."""
    distinct_trim = {x[0] for x in _rows("SELECT DISTINCT TRIM(is_reply_to) AS r FROM testdata.tweets")}
    oracle = {None if v is None else v.strip() for (v,) in _rows("SELECT is_reply_to FROM testdata.tweets")}
    assert distinct_trim == oracle


def test_trim_scalar_edges():
    cases = [
        ("SELECT TRIM('   ') AS r", ""),
        ("SELECT TRIM('') AS r", ""),
        ("SELECT LTRIM('  ab  ') AS r", "ab  "),
        ("SELECT RTRIM('  ab  ') AS r", "  ab"),
        ("SELECT TRIM('  a b  ') AS r", "a b"),
    ]
    for sql, expected in cases:
        assert _rows(sql)[0][0] == expected


# ---------------------------------------------------------------------------
# SQL-92 `TRIM([BOTH|LEADING|TRAILING] <chars> FROM <str>)` — the two-argument arm.
# ---------------------------------------------------------------------------


def test_trim_characters_is_a_set_not_a_substring():
    """The argument is a SET of characters, matched in any order and repeated.

    This is the whole semantic and it is the one a substring implementation would
    silently get wrong: 'baXab' loses every leading b/a and every trailing a/b,
    leaving 'X'. A substring match on 'ab' would strip neither end.
    """
    cases = [
        ("SELECT TRIM(BOTH 'ab' FROM 'baXab') AS r", "X"),
        ("SELECT TRIM(LEADING 'ab' FROM 'baXab') AS r", "Xab"),
        ("SELECT TRIM(TRAILING 'ab' FROM 'baXab') AS r", "baX"),
        # Every character consumed.
        ("SELECT TRIM(BOTH 'ab' FROM 'abab') AS r", ""),
        # An EMPTY set strips nothing — it is not "fall back to whitespace".
        ("SELECT TRIM(BOTH '' FROM '  ab  ') AS r", "  ab  "),
        # No direction keyword means BOTH.
        ("SELECT TRIM('_' FROM '__init__') AS r", "init"),
        # The comma spelling of the same thing, for all three functions.
        ("SELECT TRIM('baXab', 'ab') AS r", "X"),
        ("SELECT LTRIM('baXab', 'ab') AS r", "Xab"),
        ("SELECT RTRIM('baXab', 'ab') AS r", "baX"),
    ]
    for sql, expected in cases:
        assert _rows(sql)[0][0] == expected, sql


def test_trim_comma_form_refuses_a_direction():
    """`TRIM(LEADING str, 'x')` must not parse.

    sqlparser's own comma branch drops the direction and returns `trim_where:
    None`, which downstream is indistinguishable from `TRIM(str, 'x')` — so
    accepting it would silently trim BOTH ends. src/opteryx_dialect.rs owns the
    TRIM production to refuse the mixture instead.
    """
    from opteryx.exceptions import QueryParseError

    for direction in ("BOTH", "LEADING", "TRAILING"):
        with pytest.raises(QueryParseError):
            _rows(f"SELECT TRIM({direction} name, 'M') AS r FROM $planets")


def test_trim_comma_form_takes_one_character_set():
    """A second characters argument is refused by arity, naming the function."""
    from opteryx.exceptions import IncompatibleTypesError

    with pytest.raises(IncompatibleTypesError):
        _rows("SELECT TRIM(name, 'a', 'b') AS r FROM $planets")


def test_trim_characters_over_nvarchar_scans_by_codepoint():
    """A non-ASCII set over NVARCHAR must not split a multibyte character.

    'é' is C3 A9 and '©' is C2 A9. A BYTE scan would see the trailing A9 of 'é' in
    the set and strip it, leaving a dangling C3 — a truncated UTF-8 sequence and a
    shorter string. The codepoint scan matches whole encoded characters, so the
    value is returned untouched, all five bytes of it.
    """
    assert _rows("SELECT TRIM(BOTH '©' FROM CAST('éXé' AS NVARCHAR)) AS r")[0][0] == "éXé"
    assert _rows("SELECT OCTET_LENGTH(TRIM(BOTH '©' FROM CAST('éXé' AS NVARCHAR))) AS r")[0][0] == 5
    # The set's own characters still strip, and a multibyte set member works.
    assert _rows("SELECT TRIM(BOTH 'é' FROM CAST('éXé' AS NVARCHAR)) AS r")[0][0] == "X"
    assert _rows("SELECT TRIM(BOTH '中x' FROM CAST('中x好x中' AS NVARCHAR)) AS r")[0][0] == "好"


def test_trim_characters_shape_is_preserved_over_a_compressed_column():
    """Dict-shaped input, character-set trim: the answer must equal the per-row oracle.

    The kernel computes the trimmed range once per PHYSICAL value and carries the
    input's selection onto the result; if that were wrong for this arm the dict
    column would disagree with the row-by-row answer.
    """
    raw = _rows(f"SELECT is_reply_to AS r FROM testdata.tweets LIMIT {_LIMIT}")
    got = _rows(f"SELECT TRIM(BOTH '0123456789' FROM is_reply_to) AS r FROM testdata.tweets LIMIT {_LIMIT}")
    assert len(raw) == len(got)
    for (a,), (b,) in zip(raw, got):
        assert b == (None if a is None else a.strip("0123456789"))


def test_trim_characters_must_be_constant():
    """A per-ROW character set is refused — it would break shape preservation."""
    from opteryx.exceptions import InvalidFunctionParameterError

    with pytest.raises(InvalidFunctionParameterError):
        _rows("SELECT TRIM(BOTH name FROM name) AS r FROM $planets")


def test_trim_null_character_set_is_null():
    """A NULL set is NULL for every row, not an error and not a no-op."""
    rows = _rows("SELECT TRIM(BOTH CAST(NULL AS VARCHAR) FROM name) AS r FROM $planets")
    assert len(rows) > 0
    assert all(value is None for (value,) in rows)


if __name__ == "__main__":
    import pytest as _pt

    _pt.main([__file__, "-v"])
