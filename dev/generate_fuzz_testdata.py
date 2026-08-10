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

FLOAT SPECIAL VALUES live in their own columns — `f_special` / `f_special_null`
here and `val_special` in `wide` — NOT scattered through `f_value` and `val`.
Two reasons, both deliberate:

  * Poisoning the ordinary float columns would make SUM/AVG/MAX over them NaN
    on EVERY case, since IEEE arithmetic propagates. The aggregate oracles would
    then compare NaN against NaN forever and the relation would stop testing
    ordinary float arithmetic at all. Specials belong where a query can choose
    to touch them.
  * The specials are built from the row index and consume NO rng draws, so every
    pre-existing column keeps its exact prior values. Adding this coverage moved
    no other byte.

This corpus carried no NaN or ±Inf until 2026-08-09, on the stated grounds that
NaN ordering was "a known-open question, not a settled contract". That was never
true: draken/ops/float_ops.h has carried an architect lock since 2026-05-22 —
total order, NaN ranks above every value and ±inf, NaN == NaN, -0.0 == 0.0
canonicalised at ingestion, NaN is a VALUE (validity bit set) and not a NULL.
The wrong answer that made NaN look unsettled (`NOT (density > ...)` losing a
row) was row-group pruning on Parquet bounds that legitimately exclude NaN, not
the comparison semantics.

The oracles are safe against this because `harness.result_multiset` compares
`repr(row)` strings, not floats: `repr(nan)` is `'nan'`, so two NaNs compare
equal where `nan == nan` in Python would not. An oracle that ever starts
comparing raw float values has to solve that problem before it can run here.

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
#   'MiXeD CaSe'  UPPER/LOWER/INITCAP/ILIKE have something to change
#   long string   longer than the 12-byte inline threshold, so it lands in the arena
#
# ASCII ONLY, and not for want of ambition. These land in `s_awkward`/`s_null`,
# parquet `string` columns, which the engine reads as VARCHAR — and VARCHAR is
# ASCII BYTES, so non-ASCII content in one is undefined behaviour
# (single_table_known_gaps/RATIFIED/
# varchar-is-ascii-bytes-and-non-ascii-content-is-undefined). 'ÅΩ漢字' used to sit
# in this list to make LENGTH != OCTET_LENGTH. What it actually did was seed every
# byte-wise string function in the corpus with input the engine makes no promise
# about: `SUBSTRING(s_awkward, 1) || RIGHT(s_null, 2)` sliced a codepoint in half
# and the fuzzer read back an undecodable VARCHAR — a finding the contract already
# disclaims. Unicode belongs in a corpus of NVARCHAR columns, where the promise
# exists.
#
# The list LENGTH is load-bearing: the draw below is `randrange(len(...))`, and
# `random.randrange` consumes a different number of underlying bits for a
# different range, so shortening this list re-rolls every column generated after
# it. Replacing an entry keeps the corpus byte-identical apart from that one
# string; removing one moved `ts_null`'s null count from 411 to 394 and tripped
# the corpus tripwire in test_is_null_over_temporal_and_empty_string_predicates_
# reports_unknown. Swap entries here, do not delete them.
_AWKWARD_STRINGS = [
    "",
    "  padded  ",
    "100%",
    "a_b",
    "MiXeD CaSe",
    "a string comfortably longer than twelve bytes",
    "NULL",
    "0",
]


# The float values that are IN a column but OUT of its Parquet min/max bounds,
# or that the ingestion canonicaliser is contracted to fold together. Cycled
# positionally rather than drawn randomly, so they consume no rng and land at
# known, evenly spread rows.
#
#   NaN         ranks ABOVE every value including +inf; `NaN = NaN` is TRUE and
#               `NaN IS NULL` is FALSE. Parquet omits it from min/max, which is
#               what made `WHERE f > <big>` lose rows until the bound-pruning fix.
#   ±inf        ordinary values at the ends of the order — they ARE in min/max,
#               so they separate "NaN is special" from "extreme is special".
#   -0.0 / 0.0  contracted to compare equal and to hash together; float_ops.h
#               canonicalises -0.0 to +0.0 at ingestion, so a query that ever
#               returns `-0.0` has found a gap in that canonicalisation.
_FLOAT_SPECIALS = [
    float("nan"),
    float("inf"),
    float("-inf"),
    -0.0,
    0.0,
]

# One special every Nth row. Coprime with the row counts and the row-group size
# so the specials do not align to a boundary, and frequent enough that a random
# predicate has a real chance of straddling one (2,000 rows -> ~118 specials,
# ~24 of them NaN).
_SPECIAL_STRIDE = 17


def _float_special_column(rows: int, offset: int = 0) -> list:
    """A float column carrying `_FLOAT_SPECIALS` at a fixed stride, ordinary
    finite values elsewhere. Deterministic in the row index — no rng, so adding
    or changing this column cannot shift any other column's values.

    The finite fill repeats (`i % 401`), which keeps the column groupable and
    gives min/max bounds a real interior to be an interval over.
    """
    out = []
    for i in range(rows):
        position = i + offset
        if position % _SPECIAL_STRIDE == 0:
            out.append(_FLOAT_SPECIALS[(position // _SPECIAL_STRIDE) % len(_FLOAT_SPECIALS)])
        else:
            out.append(float((position % 401) - 200) / 4.0)
    return out


def _wide_special_column(rows: int, row_group_size: int = 50_000) -> list:
    """`wide`'s float specials, deliberately NOT spread evenly: NaN appears only
    in row groups 1 and 3, ±inf only in row group 2, and row group 0 is entirely
    ordinary finite values.

    Even spreading would hide the bug this column exists to catch. Row-group
    pruning decides per row group, from that group's own min/max, so a corpus
    where every group looks alike can never show a prune that keeps one group and
    wrongly drops another. Here `WHERE val_special > 1e6` must return exactly the
    NaN rows of groups 1 and 3 — group 0 has no special at all and group 2's
    +inf is inside its own bounds, so all three outcomes are different and a
    pruner that treats them alike is visibly wrong.

    Group 0 being clean also leaves a float column whose bounds ARE a true
    bound, so the pruning that must still happen has somewhere to happen.
    """
    out = []
    for i in range(rows):
        group = i // row_group_size
        finite = float((i % 401) - 200) / 4.0
        if group in (1, 3) and i % _SPECIAL_STRIDE == 0:
            out.append(float("nan"))
        elif group == 2 and i % _SPECIAL_STRIDE == 0:
            out.append(float("inf") if (i // _SPECIAL_STRIDE) % 2 == 0 else float("-inf"))
        else:
            out.append(finite)
    return out


def _special_null_mask(values: list) -> list:
    """NULL positions for a specials column, chosen so a NULL NEVER lands on a
    special value.

    If NULL and NaN could share a row the column could not distinguish them: a
    query returning the wrong count would leave you unable to say whether the
    NULL handling or the NaN handling was at fault, which is the exact confusion
    this whole area suffers from. Row 0 stays non-null for the same reason
    `_null_mask` keeps it — `LIMIT 1` should return something.
    """
    return [
        index != 0 and index % 11 == 5 and value == value and value not in (float("inf"), float("-inf"))
        for index, value in enumerate(values)
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

    # Float specials. Built from the row index and placed AFTER every rng draw
    # above so that adding them left `f_value` and friends bit-identical.
    f_special = _float_special_column(rows)
    f_special_offset = _float_special_column(rows, offset=3)
    special_nulls = _special_null_mask(f_special_offset)

    columns = {
        # Never NULL — the identity column. Unique, dense, ordered.
        "row_id": pa.array(row_id, pa.int64()),
        # Never NULL.
        "i_group": pa.array(i_group, pa.int64()),
        "i_value": pa.array(i_value, pa.int64()),
        "f_value": pa.array(f_value, pa.float64()),
        # NaN / ±inf / ±0.0 with NO NULLs. This is the column that makes the
        # three predicate buckets (`p`, `NOT p`, `p IS NULL`) a real test of NaN
        # rather than of null handling: the third bucket here is provably empty,
        # so a NaN row falling out of the partition can only be a NaN bug.
        "f_special": pa.array(f_special, pa.float64()),
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
        # The same specials WITH NULLs, and the two never share a row (see
        # `_special_null_mask`). NULL and NaN are different things — one is an
        # absent value, the other a present one that ranks highest — and a column
        # where they overlapped could not tell the two apart when a query
        # returned the wrong count.
        "f_special_null": pa.array(_apply(f_special_offset, special_nulls), pa.float64()),
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
            # NaN in SOME row groups only — see `_wide_special_column`. This is
            # the one column in the corpus where min/max row-group pruning and
            # NaN meet at scale, which is precisely where the pruning bug lived.
            "val_special": pa.array(_wide_special_column(rows), pa.float64()),
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
