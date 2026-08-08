#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generate the single-table SELECT fuzzer's purpose-built corpus.

The fuzzer used to read `$planets` and nothing else: 9 rows, 20 columns, four
type categories (FLOAT/INTEGER/VARCHAR/DECIMAL), zero NULLs. That shape decides
what the fuzzer can possibly find:

  * BOOLEAN, DATE, TIMESTAMP, VARBINARY and ARRAY branches of the generator were
    unreachable — the generator picks predicates by column type, and no column
    had those types.
  * With no NULLs, three-valued logic is never exercised: `IS NULL` always
    matches nothing, `NOT p` and `p IS NOT TRUE` are indistinguishable, and the
    predicate-partition oracle can never see the third partition.
  * 9 rows never crosses a morsel boundary (65,536 rows), so the engine's
    multi-morsel paths — hash-aggregate growth, top-k across morsels,
    cross-morsel DISTINCT — were never executed by the fuzzer at all.

The four parquet relations already in `testdata/` (planets, satellites,
missions, astronauts) close some of that but none has a BOOLEAN column and none
is large enough to produce a second morsel. Two purpose-built relations close
the rest:

  `testdata.fuzzing.mixed`  small, every fuzzable type, NULL-heavy. The workhorse
                            — the oracles run several queries per case, so the
                            default relation has to stay cheap.
  `testdata.fuzzing.wide`   200,000 rows over four row groups, so every query
                            against it crosses morsel boundaries. Narrower, and
                            the fuzzer picks it a minority of the time.

DELIBERATE OMISSION: no NaN or ±Inf in the float columns. Draken's sort places
NaN divergently from other engines and that is a known-open question, not a
settled contract — seeding NaN here would make the ORDER BY oracles fail every
run on a bug that is already recorded, drowning out everything else. Adding NaN
coverage is a separate, deliberate piece of work.

PyArrow is the writer only. It is banned inside `opteryx/`, `draken/` and
`rugo/`, and sanctioned in `dev/` for test-data generation (CLAUDE.md §4), the
same way `dev/generate_array_testdata.py` already uses it.

Run from the repo root:  python3.14 dev/generate_fuzz_testdata.py
"""

from __future__ import annotations

import datetime
import decimal
import json
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

# One fixed seed. The corpus is data, not a fuzzing dimension: regenerating it
# must produce byte-identical files, or a pinned regression seed stops
# reproducing the failure it was pinned for.
SEED = 20260808

OUT_ROOT = os.path.join("testdata", "fuzzing")

_UTC = datetime.timezone.utc
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_TS = datetime.datetime(2000, 1, 1, tzinfo=_UTC)

# Low-cardinality string pool. Short enough to sit inline in draken's string
# layout, and small enough that a GROUP BY on it produces a handful of groups.
_CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"]

# Strings chosen for what they break, not for what they mean:
#   ''            empty is not NULL, and the two must not collapse
#   '  padded  '  TRIM/LTRIM/RTRIM have something to remove
#   '100%'/'a_b'  LIKE metacharacters as literal data
#   'ÅΩ漢字'      multi-byte UTF-8, so LENGTH != OCTET_LENGTH
#   long string   longer than the 12-byte inline threshold, so it lands in the arena
_AWKWARD_STRINGS = [
    "",
    "  padded  ",
    "100%",
    "a_b",
    "ÅΩ漢字",
    "a string comfortably longer than twelve bytes",
    "NULL",
    "0",
]


def _null_mask(rng: random.Random, count: int, rate: float) -> list:
    """Positions that should be NULL. Row 0 is always non-null.

    Keeping row 0 valid means `LIMIT 1` without an ORDER BY still returns a row
    with values in it, which keeps the cheapest generated queries meaningful.
    """
    return [False] + [rng.random() < rate for _ in range(count - 1)]


def _apply(values: list, mask: list) -> list:
    return [None if m else v for v, m in zip(values, mask)]


def build_mixed(rows: int = 2_000) -> pa.Table:
    """Every fuzzable type, in both a never-NULL and a sometimes-NULL flavour.

    The `_d` / `_n` suffix split is load-bearing. A generator that only ever sees
    nullable columns cannot tell "this predicate dropped the NULL rows correctly"
    from "this predicate dropped rows it should have kept": the `_d` column gives
    the oracles a partition where the third (`IS NULL`) bucket is provably empty,
    and the `_n` column gives them one where it provably is not.
    """
    rng = random.Random(SEED)

    row_id = list(range(rows))
    # Low cardinality: a GROUP BY key that produces ~16 groups on any subset.
    i_group = [rng.randrange(16) for _ in range(rows)]
    # Bounded so that `col * 2`, `col + col` and SUM() over the whole relation
    # cannot overflow INT64 — an overflow is a real question about the engine's
    # arithmetic, but it is not the question this fuzzer is asking, and it would
    # make the aggregate identity oracles fire on every run.
    i_value = [rng.randrange(-1_000_000, 1_000_001) for _ in range(rows)]
    f_value = [rng.uniform(-1e6, 1e6) for _ in range(rows)]
    # Exact quarters: representable in both DECIMAL(18,4) and FLOAT64, so a
    # DECIMAL/FLOAT comparison is not fighting binary rounding.
    d_value = [decimal.Decimal(rng.randrange(-4_000_000, 4_000_001)) / 4 for _ in range(rows)]
    b_value = [rng.random() < 0.5 for _ in range(rows)]
    s_low = [_CATEGORIES[rng.randrange(len(_CATEGORIES))] for _ in range(rows)]
    s_high = [f"row-{i:06d}-{rng.randrange(1 << 20):06x}" for i in range(rows)]
    s_awkward = [_AWKWARD_STRINGS[rng.randrange(len(_AWKWARD_STRINGS))] for _ in range(rows)]
    bin_value = [bytes(rng.randrange(256) for _ in range(rng.randrange(1, 9))) for _ in range(rows)]
    dt_value = [_EPOCH_DATE + datetime.timedelta(days=rng.randrange(0, 20_000)) for _ in range(rows)]
    ts_value = [
        _EPOCH_TS + datetime.timedelta(seconds=rng.randrange(-1_000_000_000, 1_000_000_000))
        for _ in range(rows)
    ]

    # Arrays: present-and-populated, empty, and NULL-element cases all appear, so
    # ARRAY_CONTAINS / subscript / UNNEST see more than the happy path.
    arr_int = []
    arr_str = []
    for _ in range(rows):
        roll = rng.random()
        if roll < 0.1:
            arr_int.append([])
            arr_str.append([])
        elif roll < 0.2:
            arr_int.append([rng.randrange(10), None])
            arr_str.append([None, _CATEGORIES[rng.randrange(len(_CATEGORIES))]])
        else:
            size = rng.randrange(1, 5)
            arr_int.append([rng.randrange(100) for _ in range(size)])
            arr_str.append([_CATEGORIES[rng.randrange(len(_CATEGORIES))] for _ in range(size)])

    # JSON documents as VARBINARY — the shape `testdata.astronauts.birth_place`
    # already uses, and the one the `->` / `->>` / `@?` operators are proven
    # against. Every document carries the same key set so a path accessor has a
    # defined answer on every row.
    json_doc = [
        json.dumps(
            {
                "name": s_low[i],
                "n": i_group[i],
                "nested": {"flag": b_value[i], "score": round(f_value[i], 3)},
                "tags": arr_str[i] if arr_str[i] and None not in arr_str[i] else [],
            }
        ).encode()
        for i in range(rows)
    ]

    n20 = _null_mask(rng, rows, 0.20)
    n50 = _null_mask(rng, rows, 0.50)
    n05 = _null_mask(rng, rows, 0.05)

    columns = {
        # Never NULL — the identity column. Unique, dense, ordered.
        "row_id": pa.array(row_id, pa.int64()),
        # Never NULL.
        "i_group": pa.array(i_group, pa.int64()),
        "i_value": pa.array(i_value, pa.int64()),
        "f_value": pa.array(f_value, pa.float64()),
        "d_value": pa.array(d_value, pa.decimal128(18, 4)),
        "b_value": pa.array(b_value, pa.bool_()),
        "s_low": pa.array(s_low, pa.string()),
        "s_high": pa.array(s_high, pa.string()),
        "s_awkward": pa.array(s_awkward, pa.string()),
        "bin_value": pa.array(bin_value, pa.binary()),
        "dt_value": pa.array(dt_value, pa.date32()),
        "ts_value": pa.array(ts_value, pa.timestamp("us", tz="UTC")),
        "json_doc": pa.array(json_doc, pa.binary()),
        # Sometimes NULL, same types. Three different rates so a predicate over
        # two nullable columns does not see the same null positions twice.
        "i_null": pa.array(_apply(i_value, n20), pa.int64()),
        "f_null": pa.array(_apply(f_value, n50), pa.float64()),
        "d_null": pa.array(_apply(d_value, n20), pa.decimal128(18, 4)),
        "b_null": pa.array(_apply(b_value, n50), pa.bool_()),
        "s_null": pa.array(_apply(s_awkward, n20), pa.string()),
        "bin_null": pa.array(_apply(bin_value, n05), pa.binary()),
        "dt_null": pa.array(_apply(dt_value, n20), pa.date32()),
        "ts_null": pa.array(_apply(ts_value, n20), pa.timestamp("us", tz="UTC")),
        # Arrays are nullable by nature; a NULL list and an empty list are
        # different things and both appear.
        "arr_int": pa.array(_apply(arr_int, n05), pa.list_(pa.int64())),
        "arr_str": pa.array(_apply(arr_str, n05), pa.list_(pa.string())),
    }
    return pa.table(columns)


def build_wide(rows: int = 200_000) -> pa.Table:
    """Large enough that every scan of it produces several morsels.

    A morsel is 65,536 rows, so 200,000 rows is four of them; written as four
    50,000-row row groups so the scan also has to stitch row-group boundaries
    that do not line up with morsel boundaries. `grp_wide` has ~50,000 distinct
    values specifically to make a hash aggregate grow past its initial table.
    """
    rng = random.Random(SEED + 1)

    return pa.table(
        {
            "row_id": pa.array(range(rows), pa.int64()),
            # ~50k distinct: forces hash-aggregate table growth.
            "grp_wide": pa.array([rng.randrange(50_000) for _ in range(rows)], pa.int64()),
            # 8 distinct: dictionary-encoded on write, and a cheap GROUP BY key.
            "cat": pa.array(
                [_CATEGORIES[rng.randrange(len(_CATEGORIES))] for _ in range(rows)], pa.string()
            ),
            "val": pa.array([rng.uniform(-1000, 1000) for _ in range(rows)], pa.float64()),
            "flag": pa.array([rng.random() < 0.5 for _ in range(rows)], pa.bool_()),
            "ts": pa.array(
                [_EPOCH_TS + datetime.timedelta(seconds=rng.randrange(0, 900_000_000)) for _ in range(rows)],
                pa.timestamp("us", tz="UTC"),
            ),
            "txt": pa.array(
                _apply(
                    [f"item-{rng.randrange(1 << 24):08x}" for _ in range(rows)],
                    _null_mask(rng, rows, 0.15),
                ),
                pa.string(),
            ),
        }
    )


def write(name: str, table: pa.Table, row_group_size: int) -> None:
    directory = os.path.join(OUT_ROOT, name)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "data.parquet")
    pq.write_table(table, path, row_group_size=row_group_size, compression="zstd")
    size = os.path.getsize(path)
    print(f"wrote {path}: {table.num_rows:,} rows x {table.num_columns} cols, {size:,} bytes")


def main() -> None:
    write("mixed", build_mixed(), row_group_size=500)
    write("wide", build_wide(), row_group_size=50_000)


if __name__ == "__main__":
    main()
