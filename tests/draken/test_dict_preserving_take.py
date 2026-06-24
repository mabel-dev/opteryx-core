# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compression-preserving take/mask for numeric dict vectors.

When a dict/constant vector is filtered (mask) or gathered (take) and the value
array is no larger than the output (data_length <= n), the kernels keep the
compressed shape (copy the K values, gather the per-row codes) instead of
materialising N values. Correctness (values) must be identical to the dense path,
and the result must remain dict-shaped.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
from draken.vectors.vector import Vector
from draken.morsels.morsel import Morsel


def _expected(values, codes, keep):
    return [values[codes[i]] for i in range(len(codes)) if keep[i]]


def test_int64_dict_mask_preserves_shape_and_values():
    values = [10, 20, 30]
    codes = [0, 1, 2, 0, 1, 2, 2, 1, 0, 0] * 4  # 40 rows, 3 distinct
    keep = [(i % 3) != 0 for i in range(len(codes))]
    vec = Vector(dn.vector_from_dict(values, codes))
    mask = Vector(dn.vector_from_bool_sequence(keep))
    out = Morsel.from_vectors([b"x"], [vec]).filter_mask(mask)
    col = out.column(b"x")
    assert col._nb.is_dict, "filtered numeric dict must stay dict-shaped"
    assert col._nb.data_length == 3
    assert col.to_pylist() == _expected(values, codes, keep)


def test_int64_dict_with_nulls_mask():
    values = [7, 8, 9]
    codes = [0, 1, 2] * 10
    nullable = [(i % 5) != 0 for i in range(30)]  # every 5th row null
    keep = [i % 2 == 0 for i in range(30)]
    vec = Vector(dn.vector_from_dict(values, codes, nullable))
    mask = Vector(dn.vector_from_bool_sequence(keep))
    out = Morsel.from_vectors([b"x"], [vec]).filter_mask(mask)
    col = out.column(b"x")
    expected = [
        (values[codes[i]] if nullable[i] else None)
        for i in range(30) if keep[i]
    ]
    assert col.to_pylist() == expected


def test_take_compacts_dead_entries():
    # 32-entry dict; survivors reference only codes 0..3 -> dict compacts to 4.
    values = list(range(32))
    codes = [i % 32 for i in range(320)]  # all 32 referenced before filtering
    keep = [codes[i] < 4 for i in range(320)]
    vec = Vector(dn.vector_from_dict(values, codes))
    out = Morsel.from_vectors([b"x"], [vec]).filter_mask(Vector(dn.vector_from_bool_sequence(keep)))
    col = out.column(b"x")
    assert col._nb.is_dict, "compacted result should still be dict-shaped"
    assert col._nb.data_length == 4, f"expected compaction 32->4, got {col._nb.data_length}"
    assert col.to_pylist() == [values[codes[i]] for i in range(320) if keep[i]]


def test_min_max_ends_read_sorted_codes_dense():
    # A sorted dict, filtered so the take compacts -> codes-dense + sorted: MIN/MAX
    # read the ends. Result must match a value scan.
    values = list(range(0, 64))  # ascending
    codes = [i % 64 for i in range(640)]
    keep = [(i % 4) != 0 for i in range(640)]
    surv = [values[codes[i]] for i in range(640) if keep[i]]
    v = dn.vector_from_dict(values, codes)
    dn._test_mark_dict_keys_sorted(v)  # the writer/scan would set this
    out = Morsel.from_vectors([b"x"], [Vector(v)]).filter_mask(Vector(dn.vector_from_bool_sequence(keep)))
    col = out.column(b"x")._nb
    assert col.dict_keys_sorted, "compacting take must preserve the sorted hint"
    assert col.min() == min(surv)
    assert col.max() == max(surv)


if __name__ == "__main__":
    test_int64_dict_mask_preserves_shape_and_values()
    test_int64_dict_with_nulls_mask()
    test_take_compacts_dead_entries()
    test_min_max_ends_read_sorted_codes_dense()
    print("✅ okay")
