"""
`Morsel.combine` keeps an EMPTY child at every level of a nested ARRAY.

When no part contributed a single element (every innermost list empty, as in the
catalog manifest's `element_min_k_hashes` column), `vector_concat` dropped the
child, so the ARRAY<ARRAY<UINT64>> middle level arrived with no child and
`write_parquet` raised "array_child: DRAKEN_ARRAY vector has no child".
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector


def _morsel(rows):
    vector = Vector(dn.vector_array_from_sequence(rows, int(dn.UINT64.value), 2))
    return Morsel.from_vectors(["a"], [vector])


def _round_trip(morsel):
    with rp.read_parquet(rp.write_parquet(morsel, compression="none")) as reader:
        return list(reader)[0]


def test_combine_all_empty_nested_arrays_keeps_child_and_round_trips():
    left = [[[] for _ in range(19)] for _ in range(3)]
    right = [[[], []], [[]]]

    combined = Morsel.combine([_round_trip(_morsel(left)), _round_trip(_morsel(right))])

    middle = combined.column("a")._nb.array_child
    assert middle.array_child_type == dn.UINT64
    assert middle.array_child.length == 0
    assert _round_trip(combined).column("a").to_pylist() == left + right


def _morsel_depth(rows, depth):
    vector = Vector(dn.vector_array_from_sequence(rows, int(dn.INT64.value), depth))
    return Morsel.from_vectors(["a"], [vector])


def test_zero_row_list_column_reads_back_as_array():
    # A zero-row chunk has no repetition levels; the reader must route on the
    # schema (max_rep_level), not on whether levels happened to be decoded.
    for depth in (1, 2):
        morsel = _round_trip(_morsel_depth([], depth))
        assert morsel.num_rows == 0
        assert morsel.column_types == [dn.ARRAY], depth


def test_zero_row_row_group_then_filled_row_group_combine():
    # The catalog manifest shape: a delete-all leaves a zero-row row group, the
    # next insert appends a filled one, and the reader combines them.
    data = rp.append_row_groups(
        rp.write_parquet(_morsel_depth([], 1), compression="none"),
        rp.write_parquet(_morsel_depth([[1, 2], []], 1), compression="none"),
        compression="none",
    )
    with rp.read_parquet(data) as reader:
        parts = list(reader)
    assert [p.column_types for p in parts] == [[dn.ARRAY], [dn.ARRAY]]
    assert Morsel.combine(parts).column("a").to_pylist() == [[1, 2], []]
