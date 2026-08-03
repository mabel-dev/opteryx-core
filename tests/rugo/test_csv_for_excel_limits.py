# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
write_csv(for_excel=True) -- the Excel grid limits.

A CSV file has no limits of its own; these are the limits of the spreadsheet the
file is destined for. Excel enforces them by truncating the over-long cell and
dropping the off-sheet rows and columns without saying so, so for_excel raises
instead. Each limit is checked on both sides of its boundary: an off-by-one on
the permissive side is the failure mode that costs a user their data, and one on
the strict side rejects a file Excel would have opened perfectly.

The morsels here are built from constant vectors -- a 1,048,577-row morsel is
one physical value and a selection, so the row-count boundary costs nothing to
test.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.csv import write_csv

EXCEL_MAX_ROWS = 1048576
EXCEL_MAX_COLS = 16384
EXCEL_MAX_CELL_CHARS = 32767


def _morsel(names, nbs):
    return Morsel.from_vectors(list(names), [Vector(nb) for nb in nbs])


def _text(value, length=1):
    return dn.vector_varchar_from_constant(value.encode("utf-8"), length)


# ---------------- rows ----------------

def test_row_count_at_limit_is_written():
    m = _morsel(["a"], [dn.vector_int32_from_constant(7, EXCEL_MAX_ROWS)])
    assert write_csv(m, header=False, for_excel=True).count(b"\n") == EXCEL_MAX_ROWS


def test_row_count_over_limit_raises():
    m = _morsel(["a"], [dn.vector_int32_from_constant(7, EXCEL_MAX_ROWS + 1)])
    with pytest.raises(ValueError, match="1048577 lines"):
        write_csv(m, header=False, for_excel=True)


def test_header_row_counts_toward_the_row_limit():
    # Exactly at the limit as data, one line over it once the header is written.
    m = _morsel(["a"], [dn.vector_int32_from_constant(7, EXCEL_MAX_ROWS)])
    with pytest.raises(ValueError, match=r"1048577 lines \(1048576 rows \+ header\)"):
        write_csv(m, header=True, for_excel=True)


# ---------------- columns ----------------

def _wide(ncols):
    return _morsel(
        [f"c{i}" for i in range(ncols)],
        [dn.vector_int8_from_constant(1, 1) for _ in range(ncols)],
    )


def test_column_count_at_limit_is_written():
    out = write_csv(_wide(EXCEL_MAX_COLS), header=False, for_excel=True)
    assert out.count(b",") == EXCEL_MAX_COLS - 1


def test_column_count_over_limit_raises():
    with pytest.raises(ValueError, match="16385 columns"):
        write_csv(_wide(EXCEL_MAX_COLS + 1), header=False, for_excel=True)


# ---------------- cell width ----------------

def test_cell_at_limit_is_written():
    m = _morsel(["s"], [_text("x" * EXCEL_MAX_CELL_CHARS, 3)])
    assert len(write_csv(m, header=False, for_excel=True)) == 3 * (EXCEL_MAX_CELL_CHARS + 1)


def test_cell_over_limit_raises_naming_column_and_row():
    m = _morsel(["s"], [_text("x" * (EXCEL_MAX_CELL_CHARS + 1), 3)])
    with pytest.raises(ValueError, match=r"column 's' row 0 is 32768 characters"):
        write_csv(m, header=False, for_excel=True)


def test_cell_width_is_measured_in_characters_not_bytes():
    # 20,000 three-byte characters: 60,000 bytes, well over the limit, but only
    # 20,000 cells' worth of Excel characters -- Excel opens this fine.
    m = _morsel(["s"], [_text("€" * 20000)])
    assert len(write_csv(m, header=False, for_excel=True)) > 0


def test_astral_characters_cost_two_units_each():
    # Excel counts UTF-16 code units, so 20,000 astral characters are 40,000.
    m = _morsel(["s"], [_text("\U0001f600" * 20000)])
    with pytest.raises(ValueError, match="is 40000 characters"):
        write_csv(m, header=False, for_excel=True)


def test_over_wide_column_name_raises():
    m = _morsel(["n" * 40000], [dn.vector_int8_from_constant(1, 1)])
    with pytest.raises(ValueError, match="the name of column 0"):
        write_csv(m, for_excel=True)


def test_over_wide_column_name_is_not_checked_without_a_header():
    m = _morsel(["n" * 40000], [dn.vector_int8_from_constant(1, 1)])
    assert write_csv(m, header=False, for_excel=True) == b"1\n"


def test_over_wide_rendered_array_raises():
    # ARRAY renders as a JSON array; the check is on the rendered cell, which is
    # the only place its width exists.
    m = _morsel(["a"], [dn.vector_array_from_sequence([[123456789] * 4000])])
    with pytest.raises(ValueError, match="column 'a' row 0"):
        write_csv(m, header=False, for_excel=True)


def test_raising_mid_render_releases_the_cast_buffers():
    # The dictionary-encoded int column takes the batch cast path, so a throw
    # from the string column's emitter unwinds with a cast block still live.
    m = _morsel(
        ["i", "s"],
        [dn.vector_int32_from_dict([1, 2, 1, 2], [10, 20]), _text("x" * 40000, 4)],
    )
    for _ in range(50):
        with pytest.raises(ValueError):
            write_csv(m, header=False, for_excel=True)


# ---------------- default is off ----------------

def test_limits_are_not_enforced_by_default():
    m = _morsel(["s"], [_text("x" * 40000)])
    assert len(write_csv(m, header=False)) == 40001


def test_for_excel_does_not_change_conforming_output():
    m = _morsel(["s", "i"], [_text("a,b", 2), dn.vector_int32_from_constant(5, 2)])
    assert write_csv(m, for_excel=True) == write_csv(m)


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    _pytest.main([__file__, "-q"])
