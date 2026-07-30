"""
Correctness for ``char_class_field_totals`` (opteryx/compiled/nanobind/
vector_sketch_reduce.cpp), the manifest-side reduction that sums a column's
8-class byte counts across every (or, with ``rows``, only the surviving)
file in a manifest's ``char_class_counts`` nested vector. Backs
``Manifest.get_char_class_stats`` (opteryx/models/manifest.py).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from draken import draken_native as _dn
from opteryx.compiled.nanobind.vectors import char_class_field_totals


def _build_char_class_vector(per_file_per_column):
    """per_file_per_column: list[file] of list[column] of list[8] (or [] for
    'no stats for this column in this file')."""
    return _dn.vector_array_from_sequence(
        per_file_per_column, element_type=_dn.DrakenType.INT64.value, nesting_depth=2
    )


def test_sums_across_all_files():
    per_file = [
        [[1, 2, 3, 4, 5, 6, 7, 8], []],  # file 0: column 0 has stats, column 1 doesn't
        [[10, 20, 30, 40, 50, 60, 70, 80], []],
    ]
    vec = _build_char_class_vector(per_file)
    totals = char_class_field_totals(vec, 0)
    assert totals == [11, 22, 33, 44, 55, 66, 77, 88]
    assert char_class_field_totals(vec, 1) is None  # never any stats for column 1


def test_rows_parameter_restricts_to_live_files():
    per_file = [
        [[1, 1, 1, 1, 1, 1, 1, 1]],
        [[100, 100, 100, 100, 100, 100, 100, 100]],  # excluded via rows
        [[2, 2, 2, 2, 2, 2, 2, 2]],
    ]
    vec = _build_char_class_vector(per_file)
    totals_all = char_class_field_totals(vec, 0)
    assert totals_all == [103] * 8
    totals_live = char_class_field_totals(vec, 0, [0, 2])
    assert totals_live == [3] * 8


def test_negative_field_id_returns_none():
    vec = _build_char_class_vector([[[1, 2, 3, 4, 5, 6, 7, 8]]])
    assert char_class_field_totals(vec, -1) is None


def test_no_files_returns_none():
    vec = _build_char_class_vector([])
    assert char_class_field_totals(vec, 0) is None


def test_empty_rows_selection_returns_none():
    vec = _build_char_class_vector([[[1, 2, 3, 4, 5, 6, 7, 8]]])
    assert char_class_field_totals(vec, 0, []) is None


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
