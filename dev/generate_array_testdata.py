#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generate `testdata/flat/array_types` — the ARRAY (list) parity corpus.

Written for the R6 `non_admissible_kind:ARRAY` native-scan close-out. The
pre-existing ARRAY datasets in `testdata/` cover only a slice of the shape
space, and a dataset where every row looks the same makes a broken decoder look
correct:

  * `testdata/flat/struct_array`, `testdata.astronauts`,
    `testdata/flat/formats/parquet`, `testdata/nvd` — VARCHAR elements only.
  * `testdata/flat/null_lists` — INT64 elements, NULL and empty lists, but only
    14 rows and no NULL *elements*.
  * nothing carries NULL elements inside a present list, a nested
    list<list<...>>, BOOL/FLOAT/UINT elements, or a TIMESTAMP element (which
    the trampoline scan retags via `_sp_array_ts_unit_map`).

This file writes each of those shapes explicitly, over two row groups, with a
deliberately irregular value distribution. PyArrow is used only as the writer —
it is banned inside `opteryx/`, `draken/` and `rugo/`, but sanctioned in `dev/`
for test-data generation (CLAUDE.md §4), the same way `dev/generate_test_parquet.py`
already uses it.

Run from the repo root:  python3.14 dev/generate_array_testdata.py
"""

from __future__ import annotations

import datetime
import os

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = os.path.join("testdata", "flat", "array_types")
OUT_FILE = os.path.join(OUT_DIR, "data.parquet")

_UTC = datetime.timezone.utc


def _ts(*args) -> datetime.datetime:
    return datetime.datetime(*args, tzinfo=_UTC)


# One row per entry. Deliberately irregular: NULL lists, empty lists, lists
# holding NULL elements, single-element lists, long lists, and (for strings)
# both inline (<= 12 byte) and arena-resident (> 12 byte) values.
ROWS = [
    # id,  ints,                strs,                              floats,          bools,               stamps,                       nested,                smalls,       uints
    (1, [1, 2, 3], ["a", "bb", "ccc"], [1.5, -2.5], [True, False], [_ts(2020, 1, 1)], [[1, 2], [3]], [1, 2], [1, 2]),
    (2, None, None, None, None, None, None, None, None),
    (3, [], [], [], [], [], [], [], []),
    (4, [None], [None], [None], [None], [None], [None], [None], [None]),
    (5, [7, None, 9], ["x", None, "zzz"], [0.0, None, -0.0], [True, None, False], [_ts(1970, 1, 1), None, _ts(2038, 1, 19, 3, 14, 7)], [[7, None], None, []], [7, None, 9], [7, None, 9]),
    (6, [-9223372036854775808, 9223372036854775807], ["a string longer than twelve bytes", "short"], [1e308, -1e308], [False], [_ts(1900, 1, 1)], [[-1]], [-2147483648, 2147483647], [0, 18446744073709551615]),
    (7, [0], ["", "  "], [0.5], [True, True, True], [_ts(2026, 7, 31, 12, 30, 15)], [[]], [0], [0]),
    (8, None, ["only"], None, [False, True], None, None, None, [42]),
    (9, [5, 5, 5, 5, 5], None, [3.25, 3.25], None, [_ts(2000, 2, 29), _ts(2000, 3, 1)], [[5], [5, 5]], [5], None),
    (10, [11, 22], ["repeat", "repeat", "repeat"], [-1.0], [True], [_ts(2012, 12, 21)], [None], [11, 22], [11, 22]),
    (11, [], None, [], None, [], [], [], []),
    (12, [100], ["another value over twelve bytes long"], [2.718281828459045], [False, False], [_ts(2024, 2, 29, 23, 59, 59)], [[100, 200, 300]], [100], [100]),
]

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("ints", pa.list_(pa.int64())),
        pa.field("strs", pa.list_(pa.string())),
        pa.field("floats", pa.list_(pa.float64())),
        pa.field("bools", pa.list_(pa.bool_())),
        pa.field("stamps", pa.list_(pa.timestamp("us"))),
        pa.field("nested", pa.list_(pa.list_(pa.int64()))),
        pa.field("smalls", pa.list_(pa.int32())),
        pa.field("uints", pa.list_(pa.uint64())),
    ]
)


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    columns = list(zip(*ROWS))
    table = pa.Table.from_arrays(
        [pa.array(col, type=field.type) for col, field in zip(columns, SCHEMA)],
        schema=SCHEMA,
    )
    # Two row groups, so the native scan's per-row-group decode is exercised more
    # than once and a cross-row-group offset/validity bug cannot hide.
    pq.write_table(table, OUT_FILE, row_group_size=7, compression="snappy")
    print("wrote %s (%d rows, %d row groups)" % (OUT_FILE, table.num_rows,
                                                 pq.ParquetFile(OUT_FILE).num_row_groups))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
