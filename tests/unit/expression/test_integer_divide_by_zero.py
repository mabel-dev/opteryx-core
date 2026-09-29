"""Integer `DIV` and `%` by zero RAISE on a live row (ruling 2026-09-29).

It used to return 0 — a silent wrong answer — because CASE/IIF evaluated every
branch, so a guarded `CASE WHEN d = 0 THEN 0 ELSE n DIV d END` would have crashed.
Lazy branch evaluation removed that reason. Scope: integer DIV and % (INT8..INT64,
UINT8..UINT64, and the DECIMAL128 promotion of UINT64 x INT64). NOT in scope:
`/` (true division, IEEE: x / 0 is +-inf or NaN) and DECIMAL true division (a NULL row).
A NULL row's divisor is never inspected.
"""

import os
import sys

import draken.draken_native as dn
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

T = "(SELECT id, id - 1 AS z FROM testdata.planets) AS t"  # z = 0 on exactly one row


def _rows(sql):
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.to_arrow().to_pylist())
    return out


@pytest.mark.parametrize(
    "sql,what",
    [
        ("SELECT 1 DIV 0 AS r", "division"),
        ("SELECT 1 % 0 AS r", "modulo"),
        (f"SELECT id DIV z AS r FROM {T}", "division"),
        (f"SELECT id % z AS r FROM {T}", "modulo"),
        (f"SELECT id DIV 0 AS r FROM {T}", "division"),
    ],
)
def test_division_and_modulo_by_zero_raise(sql, what):
    with pytest.raises(Exception, match=f"{what} by zero"):
        _rows(sql)


def test_guarded_division_does_not_raise_on_excluded_rows():
    got = [r["r"] for r in _rows(
        f"SELECT CASE WHEN z = 0 THEN 0 ELSE id DIV z END AS r FROM {T} ORDER BY id")]
    assert got == [0, 2, 1, 1, 1, 1, 1, 1, 1]
    got = [r["r"] for r in _rows(f"SELECT IIF(z = 0, -1, id % z) AS r FROM {T} ORDER BY id")]
    assert got == [-1, 0, 1, 1, 1, 1, 1, 1, 1]
    got = [r["id"] for r in _rows(f"SELECT id FROM {T} WHERE z > 0 AND id DIV z > 0 ORDER BY id")]
    assert got == list(range(2, 10))


def test_true_division_is_unchanged():
    got = [r["r"] for r in _rows(f"SELECT id / z AS r FROM {T} ORDER BY id LIMIT 2")]
    assert got[0] == float("inf") and got[1] == 2.0


def test_kernel_level_every_width():
    with pytest.raises(ValueError, match="INT64 division by zero"):
        dn.vector_int64_from_sequence([6, 7]).div(dn.vector_int64_from_sequence([2, 0]))
    with pytest.raises(ValueError, match="INT64 modulo by zero"):
        dn.vector_int64_from_sequence([6, 7]).mod(0)
    with pytest.raises(ValueError, match="INT16 division by zero"):
        dn.vector_int8_from_sequence([6, 7]).div(0)
    with pytest.raises(ValueError, match="INT16 division by zero"):
        dn.vector_int8_from_sequence([6, 7]).div(dn.vector_int8_from_sequence([1, 0]))
    with pytest.raises(ValueError, match="INT64 modulo by zero"):
        dn.vector_int32_from_sequence([6, 7]).mod(0)
    with pytest.raises(ValueError, match="UINT64 division by zero"):
        dn.vector_uint64_from_sequence([6, 7]).div(0)


def test_null_row_with_zero_divisor_never_raises_and_nonzero_still_works():
    a = dn.vector_int64_from_sequence([6, None])
    b = dn.vector_int64_from_sequence([2, 0])
    assert a.div(b).to_pylist() == [3, None]
    assert dn.vector_int64_from_sequence([6, 7, None]).div(2).to_pylist() == [3, 3, None]
    assert dn.vector_int64_from_sequence([6, 7, None]).mod(-1).to_pylist() == [0, 0, None]
    assert dn.vector_uint64_from_sequence([6, 7, None]).div(2).to_pylist() == [3, 3, None]
