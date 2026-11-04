# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""rugo.parquet.append_row_groups: a file grown by appending, without decoding it.

Row groups are copied byte for byte while the file is below its fragmentation
threshold, and runs of small row groups are coalesced once it is above it -
either way the rows read back are base's followed by the addition's, in order.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import append_row_groups
from rugo.parquet import read_parquet
from rugo.parquet import write_parquet
from rugo.rugo_native import read_rowgroup_stats


def _file(start, n, name_column="name"):
    m = Morsel()
    m.append_vector("id", vector_from_sequence(list(range(start, start + n)), dtype="INTEGER"))
    m.append_vector(name_column, vector_from_sequence([f"row-{i}" for i in range(start, start + n)], dtype="VARCHAR"))
    m.append_vector("tags", vector_from_sequence([[i, i + 1] for i in range(start, start + n)], dtype="ARRAY"))
    return write_parquet(m)


def _rows(data):
    out = {}
    with read_parquet(data) as reader:
        for morsel in reader:
            for name in morsel.column_names:
                out.setdefault(name, []).extend(morsel.column(name).to_pylist())
    return out


def _row_groups(data):
    return [rg["num_rows"] for rg in read_rowgroup_stats(data)]


def _assert_concatenation(result, *parts):
    expected = {}
    for part in parts:
        for name, values in _rows(part).items():
            expected.setdefault(name, []).extend(values)
    assert _rows(result) == expected


def test_append_copies_every_row_group_and_keeps_order():
    base, addition = _file(0, 100), _file(100, 7)
    result = append_row_groups(base, addition)
    assert _row_groups(result) == [100, 7]
    _assert_concatenation(result, base, addition)


def test_append_copies_chunk_bytes_verbatim():
    # A copy, not a re-encode: the addition's page bytes appear unchanged in the result.
    base, addition = _file(0, 50), _file(50, 50)
    result = append_row_groups(base, addition)
    pages = addition[4 : len(addition) - 8 - int.from_bytes(addition[-8:-4], "little")]
    assert pages in result


def test_appending_repeatedly_is_cumulative():
    data = _file(0, 10)
    parts = [data]
    for k in range(1, 4):
        part = _file(k * 10, 10)
        parts.append(part)
        data = append_row_groups(data, part)
    assert _row_groups(data) == [10, 10, 10, 10]
    _assert_concatenation(data, *parts)


def test_a_schema_mismatch_is_refused():
    with pytest.raises(RuntimeError, match="not declared the way"):
        append_row_groups(_file(0, 5), _file(5, 5, name_column="label"))


def test_fragments_under_the_threshold_are_left_alone():
    data = _file(0, 10)
    for k in range(1, 3):
        data = append_row_groups(data, _file(k * 10, 10), max_small_row_groups=3)
    assert _row_groups(data) == [10, 10, 10]


def test_fragments_over_the_threshold_are_coalesced_in_order():
    parts = [_file(0, 10)]
    data = parts[0]
    for k in range(1, 5):
        part = _file(k * 10, 10)
        parts.append(part)
        # Every row group is a fragment against a 1 GiB target, so the 5th exceeds 4.
        data = append_row_groups(
            data, part, target_row_group_bytes=1 << 30, max_small_row_groups=4
        )
    assert _row_groups(data) == [50]
    _assert_concatenation(data, *parts)


def test_coalescing_keeps_large_row_groups_verbatim():
    big = _file(0, 1000)
    big_size = read_rowgroup_stats(big)[0]["total_byte_size"]
    data = big
    parts = [big]
    for k in range(3):
        part = _file(1000 + k * 5, 5)
        parts.append(part)
        data = append_row_groups(
            data, part, target_row_group_bytes=big_size, max_small_row_groups=2
        )
    assert _row_groups(data) == [1000, 15]
    _assert_concatenation(data, *parts)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
