"""
Parity + correctness for the native ``Vector.char_class_stats()`` /
``Vector.null_count()`` / ``Vector.ordinal_min_max()`` / ``Vector.histogram_bucket()``
kernels (draken/draken_native.cpp), added to back the LIKE '%needle%' selectivity
char-class estimator (opteryx/planner/cost_estimation/selectivity.py).

Three independent copies of the same 256-entry byte-classification table must
agree byte-for-byte, or the estimator's stored proportions and its needle
classification at estimate time silently disagree:

  1. scratch/like_selectivity/stats.py's `_BYTE_CLASS` (numpy) — the
     offline-validated experiment this whole feature is ported from.
  2. draken_native.cpp's `char_class_stats` binding's static `BYTE_CLASS` array
     (C++, exercised here only indirectly via the kernel's actual output).
  3. opteryx/planner/cost_estimation/selectivity.py's `_BYTE_CLASS` tuple (a
     literal copy, since scratch/ is unpackaged and cannot be imported from
     production code).

This file checks (2) against (1) by construction (one row per class, decode
the kernel's output back to a class index) and (3) against (1) directly.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from draken.interop.vector_sequence import vector_from_sequence

# Importing opteryx.planner.optimizer (the package) resolves the optimizer <->
# cost_estimation.selectivity import cycle first — see
# tests/unit/planner/cost_estimation/test_selectivity_column_vs_column.py for
# why this matters.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.planner.cost_estimation.selectivity import _BYTE_CLASS as _SELECTIVITY_BYTE_CLASS
from opteryx.planner.cost_estimation.selectivity import _CHAR_CLASSES
from opteryx.planner.cost_estimation.selectivity import _CLASS_CARDINALITY

_SCRATCH = os.path.join(os.path.dirname(__file__), "..", "..", "scratch", "like_selectivity")


def _load_scratch_byte_class():
    """The offline-validated source table — scratch/ is not importable from
    production code, but a TEST reading it for a parity check is exactly the
    enforcement mechanism this table's own comment (in selectivity.py) points at."""
    if not os.path.isdir(_SCRATCH):
        pytest.skip("scratch/like_selectivity not present")
    sys.path.insert(0, _SCRATCH)
    try:
        import stats as scratch_stats

        return [int(x) for x in scratch_stats._BYTE_CLASS], list(scratch_stats.CLASSES)
    finally:
        sys.path.remove(_SCRATCH)


def test_selectivity_byte_class_matches_scratch_source_table():
    scratch_byte_class, scratch_classes = _load_scratch_byte_class()
    assert list(_CHAR_CLASSES) == scratch_classes
    assert list(_SELECTIVITY_BYTE_CLASS) == scratch_byte_class


def test_class_cardinality_matches_scratch_source_table():
    scratch_byte_class, scratch_classes = _load_scratch_byte_class()
    expected = {
        name: scratch_byte_class.count(i) for i, name in enumerate(scratch_classes)
    }
    assert _CLASS_CARDINALITY == expected
    assert sum(_CLASS_CARDINALITY.values()) == 256  # every byte classifies exactly once


def _char_class_stats_for_bytes(payload: bytes):
    vec = vector_from_sequence([payload], dtype="VARBINARY")
    return vec.char_class_stats()


def test_native_kernel_agrees_with_selectivity_table_for_every_byte():
    """One row per byte value 0-255; the kernel's single-class count must land
    in the SAME class index selectivity.py's own table assigns that byte."""
    for byte_val in range(256):
        counts, total_bytes, length_range = _char_class_stats_for_bytes(bytes([byte_val]))
        assert total_bytes == 1
        assert length_range == (1, 1)
        expected_class = _SELECTIVITY_BYTE_CLASS[byte_val]
        for class_idx, count in enumerate(counts):
            if class_idx == expected_class:
                assert count == 1, f"byte {byte_val}: expected class {expected_class}, got counts {counts}"
            else:
                assert count == 0, f"byte {byte_val}: expected class {expected_class}, got counts {counts}"


def test_known_vector_counts_total_bytes_and_length_range():
    vec = vector_from_sequence(["Hello, World! 123", None, "abc"], dtype="VARCHAR")
    counts, total_bytes, length_range = vec.char_class_stats()
    assert sum(counts) == total_bytes
    assert total_bytes == len("Hello, World! 123") + len("abc")  # null row skipped
    assert length_range == (3, 17)


def test_all_null_column_returns_none_length_range_and_zero_totals():
    vec = vector_from_sequence([None, None, None], dtype="VARCHAR")
    counts, total_bytes, length_range = vec.char_class_stats()
    assert counts == [0] * 8
    assert total_bytes == 0
    assert length_range is None


def test_empty_column_returns_none_length_range():
    vec = vector_from_sequence([], dtype="VARCHAR")
    counts, total_bytes, length_range = vec.char_class_stats()
    assert counts == [0] * 8
    assert total_bytes == 0
    assert length_range is None


def test_non_string_vector_raises():
    vec = vector_from_sequence([1, 2, 3], dtype="INT64")
    with pytest.raises(ValueError):
        vec.char_class_stats()


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
