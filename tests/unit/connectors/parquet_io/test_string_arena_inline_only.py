# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A parquet string column whose every value fits inline keeps no arena.

A value of at most STR_INLINE_MAX (12) bytes lives whole in its 16-byte slot,
so the scan's string builders (rugo io_pipeline.hpp build_direct_string_plain
and build_direct_string_dict) drop the decoded page buffer when no value is
longer. Before, a short-string column carried that buffer - PLAIN length
prefixes plus bytes already inline - as a dead arena, doubling its footprint;
H2O j5's join build retained it (7.8 vs 5.2 GB against skene).

Values must round-trip exactly either side of the 12/13-byte boundary, with
nulls and empty strings, in both encodings; only an all-inline column may shed
its arena.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from rugo.parquet import write_parquet

ROWS = 3000
SLOT_BYTES = 16


def _strings_sql(width, distinct):
    """`ROWS` strings of exactly `width` bytes from `distinct` values, with
    every 7th row NULL and every 11th row empty."""
    return (
        "SELECT CASE WHEN g % 7 = 0 THEN NULL WHEN g % 11 = 0 THEN '' "
        f"ELSE LPAD(CAST(g % {distinct} AS VARCHAR), {width}, 'x') END AS s "
        f"FROM generate_series(1, {ROWS}) AS g"
    )


def _expected(width, distinct):
    out = []
    for g in range(1, ROWS + 1):
        if g % 7 == 0:
            out.append(None)
        elif g % 11 == 0:
            out.append("")
        else:
            # LPAD truncates a longer string to `width`, as SQL defines it
            out.append(str(g % distinct).rjust(width, "x")[:width])
    return out


def _scan(tmp_path, width, distinct, dictionary):
    source = [m for m in opteryx.session().execute_to_morsels(_strings_sql(width, distinct)) if m is not None]
    assert len(source) == 1
    path = tmp_path / f"s_{width}_{distinct}_{dictionary}.parquet"
    path.write_bytes(write_parquet(source[0], dictionary=dictionary))
    values, nbytes = [], 0
    for morsel in opteryx.session().execute_to_morsels(f"SELECT s FROM READ_PARQUET('{path}')"):
        if morsel is None or not morsel.num_rows:
            continue
        values.extend(morsel.column(b"s").to_pylist())
        nbytes += morsel.nbytes
    return values, nbytes


@pytest.mark.parametrize("dictionary,distinct", [(False, ROWS), (True, 40)])
@pytest.mark.parametrize("width", [1, 11, 12, 13, 40])
def test_values_round_trip_either_side_of_the_inline_limit(tmp_path, dictionary, distinct, width):
    values, _ = _scan(tmp_path, width, distinct, dictionary)
    assert values == _expected(width, distinct)


def test_all_inline_dictionary_column_keeps_no_arena(tmp_path):
    # 12-byte values: every one inline, so the column is its slots and its
    # validity, never a dictionary page buffer.
    #
    # Dictionary path only. The PLAIN path ADOPTS the decoder's page buffer
    # rather than copying it, and an adopted buffer is not counted in
    # `nbytes`, so this assertion passes with or without the fix there
    # (checked 2026-10-19) - it would guard nothing. The PLAIN fix is
    # measured on H2O j5 instead.
    _, short_bytes = _scan(tmp_path, 12, 40, True)
    assert short_bytes <= ROWS * SLOT_BYTES + 4096, short_bytes


@pytest.mark.parametrize("dictionary,distinct", [(False, ROWS), (True, 40)])
def test_one_long_value_keeps_the_arena(tmp_path, dictionary, distinct):
    # 13-byte values: none fits inline, so the arena is live and must stay.
    values, long_bytes = _scan(tmp_path, 13, distinct, dictionary)
    assert any(v and len(v) > 12 for v in values)
    assert long_bytes > ROWS * SLOT_BYTES, long_bytes


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
