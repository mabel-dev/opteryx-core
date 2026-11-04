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
