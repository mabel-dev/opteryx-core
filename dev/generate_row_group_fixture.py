#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generate `testdata/flat/many_row_groups` — the pushed-LIMIT frontier fixture.

Exists for ONE test:
`tests/unit/operators/test_native_scan_residual_gate.py::test_pushed_limit_skips_uncontributing_row_groups`.

That test proves the submit frontier caps enqueues from the footer's per-row-group
row counts, so a small LIMIT decodes ONE row group rather than the whole prefetch
window. The assertion is only meaningful when the file holds MORE row groups than
that window (`in_flight_limit` == workers + 2), so the row-group count here is the
load-bearing property — not the row count, and not the column set.

It previously ran on `testdata/tpch_1/lineitem`, a 223MB SF1 dataset that lived
only on a dev machine: `**.parquet` in .gitignore meant it was never committed, so
CI failed with DatasetNotFoundError. This fixture reproduces the one property that
mattered — 64 row groups, the same count 16x4 SF1 lineitem happened to have — in
~1MB that a clean checkout actually has.

Deliberately NOT lineitem-shaped. Nothing about TPC-H was doing any work; naming it
after lineitem only invited someone to point benchmark work at it.

PyArrow is the writer only — banned inside `opteryx/`, `draken/` and `rugo/`,
sanctioned in `dev/` for test-data generation (CLAUDE.md §4).

Run from the repo root:  python3.14 dev/generate_row_group_fixture.py
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = os.path.join("testdata", "flat", "many_row_groups")
OUT_FILE = os.path.join(OUT_DIR, "data.parquet")

# 64 row groups of 1,000 rows. 64 is comfortably above `in_flight_limit` on every
# machine the suite runs on (4-core CI runner, 32-core prod box), which is what
# makes "decoded exactly one" a real assertion rather than a tautology.
ROW_GROUP_SIZE = 1_000
ROW_GROUPS = 64
ROWS = ROW_GROUP_SIZE * ROW_GROUPS

_LABELS = ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf")

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("value", pa.float64()),
        pa.field("label", pa.string()),
    ]
)


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    # Fully deterministic — no RNG, so a regenerated fixture is byte-comparable.
    ids = list(range(1, ROWS + 1))
    table = pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array([i * 0.25 for i in ids], type=pa.float64()),
            pa.array([_LABELS[i % len(_LABELS)] for i in ids], type=pa.string()),
        ],
        schema=SCHEMA,
    )
    pq.write_table(table, OUT_FILE, row_group_size=ROW_GROUP_SIZE, compression="snappy")

    written = pq.ParquetFile(OUT_FILE)
    # The row-group count IS the fixture. Fail loudly if the writer ever coalesces.
    if written.num_row_groups != ROW_GROUPS:
        raise SystemExit(
            "expected %d row groups, wrote %d" % (ROW_GROUPS, written.num_row_groups)
        )
    print(
        "wrote %s (%d rows, %d row groups, %d bytes)"
        % (OUT_FILE, table.num_rows, written.num_row_groups, os.path.getsize(OUT_FILE))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
