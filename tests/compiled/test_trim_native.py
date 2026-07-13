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


if __name__ == "__main__":
    import pytest as _pt

    _pt.main([__file__, "-v"])
