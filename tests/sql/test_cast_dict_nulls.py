"""Regression: range-checked integer casts over a dictionary-encoded column with NULLs.

The range-checked cast kernels (draken/ops/kernels/cast_numeric.cpp: signed->int,
unsigned->int, signed->unsigned, unsigned->unsigned, and the two ->DATE32 casts)
convert data_length PHYSICAL values, but read `validity[j]` for physical slot j as
if it were logical row j. Validity is one bit per LOGICAL row (CLAUDE.md §11), so
on a dict-shaped vector a NULL in row r skipped DICTIONARY VALUE r: with a NULL
in row 1, CAST(x AS INT32) turned every row holding dictionary value 1 into 0.
Found 2026-09-27 via the widening an Iceberg-declared type needs (ClickBench's
uint16 EventDate). The kernels now skip a slot only when no valid row reads it.

pyarrow dictionary-encodes by default, so this file reaches the engine dict-shaped.
Every expected row is the input value itself - a cast between these widths is exact.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx

# NULL in rows 1 and 4; dictionary order is first appearance: 5, 7, 32000.
VALUES = [5, None, 7, 5, None, 32000, 7]


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    directory = tmp_path_factory.mktemp("cast_dict_nulls")
    pq.write_table(
        pa.table({
            "i16": pa.array(VALUES, pa.int16()),
            "u16": pa.array(VALUES, pa.uint16()),
            "i64": pa.array(VALUES, pa.int64()),
        }),
        os.path.join(directory, "part.parquet"),
        use_dictionary=True,
    )
    return str(directory)


def _column(dataset, expression):
    values = []
    for morsel in opteryx.session().execute_to_morsels(f"SELECT {expression} AS v FROM '{dataset}'"):
        values.extend(morsel.column("v").to_pylist())
    return values


@pytest.mark.parametrize(
    "expression",
    [
        "CAST(i16 AS INT32)",   # signed -> signed (range-checked narrowing kernel family)
        "CAST(i64 AS INT32)",
        "CAST(u16 AS INT32)",   # unsigned -> signed
        "CAST(i16 AS UINT32)",  # signed -> unsigned
        "CAST(u16 AS UINT32)",  # unsigned -> unsigned
    ],
)
def test_cast_keeps_every_value(dataset, expression):
    assert _column(dataset, expression) == VALUES


@pytest.mark.parametrize("column", ["i16", "u16"])
def test_date_cast_keeps_every_value(dataset, column):
    """The ->DATE32 casts share the kernel pattern: day n since the epoch."""
    import datetime

    epoch = datetime.date(1970, 1, 1)
    expected = [None if v is None else epoch + datetime.timedelta(days=v) for v in VALUES]
    assert _column(dataset, f"{column}::DATE") == expected
