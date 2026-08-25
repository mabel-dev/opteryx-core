"""
Boundary values through the dictionary decode-skip probe.

The probe compares a pushed needle against a row group's dictionary entries and,
on a miss, skips the row group's data pages. Parquet has no unsigned and no
narrow physical storage — INT8/UINT8/INT16/UINT16/UINT32 all travel as physical
INT32, and UINT64 as physical INT64 — so every one of those types reaches the
probe as a bit pattern that has to be re-read in the column's own signedness.

Reading it in the WRONG signedness is not a slow scan, it is a wrong answer, and
it has happened: see `tests/sql/test_unsigned_dict_scan_equality.py`, where the
probe sign-extended int32 dictionary entries and `= <value with bit 31 set>`
skipped every dictionary-encoded row group and returned ZERO rows, silently.

That test straddles the signed midpoint but never lands ON it. This file lands
on the exact boundaries, at both widths and in both signednesses:

    0, 1, INT32_MAX, INT32_MAX+1 (== 0x80000000), UINT32_MAX-1, UINT32_MAX
    0, 1, INT64_MAX, INT64_MAX+1 (== 0x8000000000000000), UINT64_MAX-1, UINT64_MAX
    INT32_MIN, -1  /  INT64_MIN, -1

`0x80000000` is the value a sign-extension bug flips first, and `UINT64_MAX` is
the value that does not fit the probe's signed int64 needle slot at all (it rides
as its two's-complement bit pattern, which is exactly what the dictionary entry
is). Both are one-value-wide cliffs; a test that brackets them without landing on
them can pass while either is broken.

UINT8 and UINT16 can never set bit 31 and so are safe from the int32 defect by
construction — but that is an argument, not a test, and they take the same
`is_unsigned` widening path, so they are asserted here rather than reasoned about.

INT32/INT64 columns are the control in the other direction: pyarrow writes them
with NO logical annotation, which `decode_column.cpp` treats as signed, so their
negative dictionary entries must NOT be zero-extended by a fix aimed at unsigned.

Every assertion is against a direct count over the values that were written, so
a probe that over-skips (drops rows) and one that under-matches both fail.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector

I32_MAX = (1 << 31) - 1
U32_MAX = (1 << 32) - 1
I64_MAX = (1 << 63) - 1
U64_MAX = (1 << 64) - 1

# Each value repeated so the column dictionary-encodes (a PLAIN chunk never
# reaches the probe at all, which would make every test below vacuous).
REPEATS = 40

CASES = {
    "u8":  ("u8",  pa.uint8(),  [0, 1, 0x7F, 0x80, 0xFF]),
    "u16": ("u16", pa.uint16(), [0, 1, 0x7FFF, 0x8000, 0xFFFF]),
    "u32": ("u32", pa.uint32(), [0, 1, I32_MAX, I32_MAX + 1, I32_MAX + 2, U32_MAX - 1, U32_MAX]),
    "u64": ("u64", pa.uint64(), [0, 1, I64_MAX, I64_MAX + 1, I64_MAX + 2, U64_MAX - 1, U64_MAX]),
    "i32": ("i32", pa.int32(),  [-(1 << 31), -1, 0, 1, I32_MAX]),
    "i64": ("i64", pa.int64(),  [-(1 << 63), -1, 0, 1, I64_MAX]),
}

_WS_COUNTER = [0]


@pytest.fixture(scope="module")
def dataset():
    """One file, one column per width/signedness, dictionary-encoded, split into
    several row groups.

    Small row groups matter: each row group gets its OWN dictionary covering only
    part of the value range, so most row groups are genuinely disjoint from any
    given needle and the decode-skip actually FIRES. With a single row group the
    probe would find every needle present and skip nothing, and these tests would
    pass without ever exercising the path they exist for.
    """
    _WS_COUNTER[0] += 1
    ws = f"ws_dictbound_{_WS_COUNTER[0]}"

    columns = {}
    expected = {}
    height = max(len(vals) for _, _, vals in CASES.values()) * REPEATS
    for key, (name, dtype, vals) in CASES.items():
        col = [v for v in vals for _ in range(REPEATS)]
        col += [vals[0]] * (height - len(col))  # pad to a common height
        columns[name] = pa.array(col, type=dtype)
        expected[key] = {v: col.count(v) for v in vals}

    tbl = pa.table(columns)
    tmp = tempfile.mkdtemp()
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    path = os.path.join(data_dir, "data.parquet")
    # A row-group size that is not a multiple of REPEATS, so a value's rows
    # straddle row-group boundaries rather than lining up with them.
    pq.write_table(tbl, path, use_dictionary=True, row_group_size=57, compression="none")

    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups > 1, md.num_row_groups
    for i in range(md.num_columns):
        chunk = md.row_group(0).column(i)
        assert chunk.has_dictionary_page, md.schema.column(i).name

    return tmp, ws, expected


def _count(dataset, where):
    tmp, ws, _ = dataset
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        opteryx.register_workspace(ws, DiskConnector)
        sql = f"SELECT COUNT(*) AS c FROM {ws}.t WHERE {where}"
        for morsel in opteryx.session().execute_to_morsels(sql):
            return morsel.column(b"c").to_pylist()[0]
        return None
    finally:
        os.chdir(cwd)


@pytest.mark.parametrize(
    ("key", "value"),
    [(key, value) for key, (_, _, vals) in CASES.items() for value in vals],
)
def test_equality_on_a_boundary_value(dataset, key, value):
    """`col = <boundary>` returns exactly the rows written with that value.

    Zero here is the sign-extension failure (the probe declared every row group
    disjoint and skipped it); a too-large count would be a probe that stopped
    discriminating.
    """
    name = CASES[key][0]
    _, _, expected = dataset
    assert _count(dataset, f"{name} = {value}") == expected[key][value]


def _bind_groups(vals):
    """Split values into groups that the logical planner will bind to ONE type.

    A literal above INT64_MAX binds differently from one below it, and an IN list
    mixing the two is refused before it ever reaches the scan — a pre-existing
    planner limitation, pinned by `test_in_list_mixing_widths_is_refused_by_the_planner`
    in `tests/sql/test_unsigned_dict_scan_equality.py`. Grouping here keeps this
    file testing the PROBE rather than re-testing that refusal.
    """
    low = [v for v in vals if v <= I64_MAX]
    high = [v for v in vals if v > I64_MAX]
    return [g for g in (low, high) if g]


@pytest.mark.parametrize("key", list(CASES))
def test_in_list_of_every_boundary_value(dataset, key):
    """An IN list is a disjunction, and a row group survives if ANY member is
    present. Listing the boundary values must therefore return every row they
    account for: a single member the probe mis-reads takes its rows with it.

    Across all groups, every boundary value in CASES is covered.
    """
    name, _, vals = CASES[key]
    _, _, expected = dataset
    total = 0
    for group in _bind_groups(vals):
        members = ", ".join(str(v) for v in group)
        total += _count(dataset, f"{name} IN ({members})")
    assert total == sum(expected[key].values())


@pytest.mark.parametrize("key", list(CASES))
def test_an_absent_value_is_still_zero(dataset, key):
    """The decode-skip must keep working. A value inside the column's range but
    never written still answers 0 — so none of the above can be satisfied by a
    probe that simply matches everything.
    """
    name, _, vals = CASES[key]
    absent = 3  # inside every column's range, in none of the CASES lists
    assert absent not in vals
    assert _count(dataset, f"{name} = {absent}") == 0


@pytest.mark.parametrize("key", ["u32", "u64"])
def test_the_unsigned_half_is_not_read_as_negative(dataset, key):
    """The specific shape of the original defect, stated as an ordering claim.

    Every value at or above the signed midpoint must compare GREATER than every
    value below it. Sign-extension inverts exactly this, and it does so
    consistently enough that equality counts alone can look plausible.
    """
    name, _, vals = CASES[key]
    midpoint = (1 << 31) if key == "u32" else (1 << 63)
    _, _, expected = dataset
    above = sum(c for v, c in expected[key].items() if v >= midpoint)
    assert _count(dataset, f"{name} >= {midpoint}") == above
    assert _count(dataset, f"{name} < {midpoint}") == sum(expected[key].values()) - above


@pytest.mark.parametrize("key", ["i32", "i64"])
def test_the_signed_negatives_are_not_zero_extended(dataset, key):
    """The control in the other direction: a fix for unsigned must not widen a
    SIGNED column's negative dictionary entries as if they were unsigned. These
    columns carry no logical annotation at all, which is precisely the case
    `decode_column.cpp` reads as signed.
    """
    name, _, vals = CASES[key]
    _, _, expected = dataset
    negative = sum(c for v, c in expected[key].items() if v < 0)
    assert negative > 0
    assert _count(dataset, f"{name} < 0") == negative
