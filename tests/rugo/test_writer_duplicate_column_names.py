# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Every rugo writer refuses a morsel with two columns of the same name: readers
resolve columns by name and take the first match, so the second column would be
written and then be unreachable (JSONL would collapse the keys outright).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.draken_native import DrakenType  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo import avro, csv, jsonl, parquet  # noqa: E402
from rugo.rugo_native import open_parquet_writer  # noqa: E402


def _dup_morsel(names):
    return Morsel.from_vectors(
        names, [vector_from_sequence([1, 2], dtype=DrakenType.INT64) for _ in names]
    )


def _stream_parquet(morsel):
    with open_parquet_writer(lambda b: None) as w:
        w.write_row_group(morsel)


WRITERS = {
    "parquet": parquet.write_parquet,
    "parquet_stream": _stream_parquet,
    "csv": csv.write_csv,
    "csv_no_header": lambda m: csv.write_csv(m, header=False),
    "jsonl": jsonl.write_jsonl,
    "avro": avro.write_avro,
}


@pytest.mark.parametrize("fmt", WRITERS)
def test_duplicate_name_is_refused(fmt):
    with pytest.raises(ValueError, match="cannot write: column name 'id' appears more than once"):
        WRITERS[fmt](_dup_morsel(["id", "x", "id"]))


@pytest.mark.parametrize("fmt", WRITERS)
def test_every_duplicate_is_named(fmt):
    with pytest.raises(ValueError, match="column names 'a', 'b' appear more than once"):
        WRITERS[fmt](_dup_morsel(["a", "b", "a", "b", "a"]))


@pytest.mark.parametrize("fmt", WRITERS)
def test_unique_names_still_write(fmt):
    WRITERS[fmt](_dup_morsel(["a", "b"]))


# The streaming writer's footer schema is the FIRST row group's, so every later
# row group must match it — otherwise its columns land under the wrong names.


def _vec(values, dtype=DrakenType.INT64):
    return vector_from_sequence(values, dtype=dtype)


def _stream(*morsels):
    out = []
    with open_parquet_writer(out.append) as w:
        for m in morsels:
            w.write_row_group(m)
    return b"".join(out)


FIRST = Morsel.from_vectors([b"id", b"x"], [_vec([1, 2]), _vec([3, 4])])


@pytest.mark.parametrize(
    "later, message",
    [
        (Morsel.from_vectors([b"id", b"id"], [_vec([1]), _vec([2])]),
         "row group 1: column 1 is 'id', the first row group has 'x'"),
        (Morsel.from_vectors([b"a", b"b"], [_vec([1]), _vec([2])]),
         "row group 1: column 0 is 'a', the first row group has 'id'"),
        (Morsel.from_vectors([b"id"], [_vec([1])]),
         "row group 1: it has 1 columns, the first row group has 2"),
        (Morsel.from_vectors([b"id", b"x"], [_vec([1]), _vec(["s"], DrakenType.VARCHAR)]),
         "row group 1: column 'x' has a different type from the first row group's"),
    ],
    ids=["duplicate", "renamed", "fewer_columns", "retyped"],
)
def test_later_row_group_must_match_the_first(later, message):
    with pytest.raises(ValueError, match=message):
        _stream(FIRST, later)


def test_matching_row_groups_round_trip():
    # Nulls in one row group only, and string cardinality that differs per row
    # group, must not read as a schema difference.
    first = Morsel.from_vectors(
        [b"id", b"s"], [_vec([1, 2]), _vec(["a", "a"], DrakenType.VARCHAR)]
    )
    second = Morsel.from_vectors(
        [b"id", b"s"], [_vec([None, 4]), _vec(["b", None], DrakenType.VARCHAR)]
    )
    third = Morsel.from_vectors(
        [b"id", b"s"],
        [_vec(list(range(500))), _vec([f"v{i}" for i in range(500)], DrakenType.VARCHAR)],
    )
    groups = list(parquet.read_parquet(_stream(first, second, third)))
    assert [g.column_names for g in groups] == [[b"id", b"s"]] * 3
    assert groups[1].column("id").to_pylist() == [None, 4]
    assert groups[1].column("s").to_pylist() == ["b", None]
    assert groups[2].num_rows == 500
