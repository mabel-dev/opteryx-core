#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generate `testdata/flat/join_consolidation` — the join build-side consolidation fixture.

Exists for ONE test file:
`tests/unit/operators/test_join_build_consolidation_telemetry.py`.

`Join2BuildSink` only considers consolidating its retained build payload when the
build side arrives as MORE THAN ONE morsel, and only consolidates when the payload
is wide enough per row and the estimated output dwarfs the build block
(`decide_consolidation` in src/cpp/engine/native_join2.hpp). Those tests used
`testdata/tpch_1`, a dataset that lived only on a dev machine (`**.parquet` in
.gitignore kept it out of the repo), and TPC-H SF0.01 cannot stand in for it: its
`customer` is one file with one row group, so the build side is a single morsel and
the gate returns `declined_single_morsel` before it measures anything.

The load-bearing properties, in the order the gate checks them:

  * `accounts` has 8 row groups, so the build side is 8 morsels, not 1.
  * `accounts` carries three STRING columns whose values are distinct per row. Text
    that repeats is dictionary-encoded and shrinks to ~50 bytes/row, which only just
    clears the gate's 4x-code-size threshold; distinct text measures ~250 bytes/row
    against 12 bytes/row of codes.
  * `events` holds 10 events per account, so the join's estimated output (32,000) is
    10x the build side and the amortisation test has headroom.

Deliberately NOT TPC-H-shaped. Nothing about TPC-H was doing any work; naming it
after customer/orders only invites someone to point benchmark work at it.

Fully deterministic — a linear congruential generator, no RNG — so a regenerated
fixture is byte-comparable.

PyArrow is the writer only — banned inside `opteryx/`, `draken/` and `rugo/`,
sanctioned in `dev/` for test-data generation (CLAUDE.md §4).

Run from the repo root:  python3.14 dev/generate_join_consolidation_fixture.py
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = os.path.join("testdata", "flat", "join_consolidation")

ACCOUNT_ROWS = 3_200
ACCOUNT_ROW_GROUP_SIZE = 400
ACCOUNT_ROW_GROUPS = ACCOUNT_ROWS // ACCOUNT_ROW_GROUP_SIZE  # 8
EVENTS_PER_ACCOUNT = 10
EVENT_ROWS = ACCOUNT_ROWS * EVENTS_PER_ACCOUNT
EVENT_ROW_GROUP_SIZE = 8_000
REGIONS = 5

_WORDS = (
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    "golf", "hotel", "india", "juliet", "kilo", "lima",
)  # fmt: skip


def _text(row: int, words: int) -> str:
    """`words` words for `row`, distinct per row (LCG-driven, deterministic)."""
    x = (row * 2654435761) & 0xFFFFFFFF
    out = []
    for _ in range(words):
        x = (x * 1664525 + 1013904223) & 0xFFFFFFFF
        out.append(_WORDS[(x >> 16) % len(_WORDS)] + str(x % 97))
    return " ".join(out)


def _write(name: str, table: pa.Table, row_group_size: int, expected_row_groups: int) -> None:
    directory = os.path.join(OUT_DIR, name)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "data.parquet")
    pq.write_table(table, path, row_group_size=row_group_size, compression="snappy")
    written = pq.ParquetFile(path)
    # The row-group count IS the fixture. Fail loudly if the writer ever coalesces.
    if written.num_row_groups != expected_row_groups:
        raise SystemExit(
            "%s: expected %d row groups, wrote %d" % (name, expected_row_groups, written.num_row_groups)
        )
    print(
        "wrote %s (%d rows, %d row groups, %d bytes)"
        % (path, table.num_rows, written.num_row_groups, os.path.getsize(path))
    )


def main() -> int:
    account_ids = list(range(1, ACCOUNT_ROWS + 1))
    _write(
        "accounts",
        pa.table(
            {
                "account_id": pa.array(account_ids, pa.int64()),
                "region_id": pa.array([i % REGIONS for i in account_ids], pa.int64()),
                "name": ["Account#%09d" % i for i in account_ids],
                "address": [_text(i, 6) for i in account_ids],
                "note": [_text(i, 16) for i in account_ids],
            }
        ),
        ACCOUNT_ROW_GROUP_SIZE,
        ACCOUNT_ROW_GROUPS,
    )

    event_ids = list(range(1, EVENT_ROWS + 1))
    _write(
        "events",
        pa.table(
            {
                "event_id": pa.array(event_ids, pa.int64()),
                "account_id": pa.array([(i % ACCOUNT_ROWS) + 1 for i in event_ids], pa.int64()),
            }
        ),
        EVENT_ROW_GROUP_SIZE,
        EVENT_ROWS // EVENT_ROW_GROUP_SIZE,
    )

    _write(
        "regions",
        pa.table(
            {
                "region_id": pa.array(range(REGIONS), pa.int64()),
                "name": ["region_%d" % i for i in range(REGIONS)],
            }
        ),
        REGIONS,
        1,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
