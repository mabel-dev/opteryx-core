from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

import rugo.rugo_native as parquet

DATASET = Path("testdata/parquet_tests/data_index_bloom_encoding_stats.parquet")


def _bloom_info():
    with open(DATASET, "rb") as f:
        data = f.read()
    row_groups = parquet.read_rowgroup_stats(data)
    column = row_groups[0]["columns"][0]
    return column["bloom_offset"], column["bloom_length"]


def test_bloom_filter_detects_present_value():
    offset, length = _bloom_info()
    assert offset is not None

    assert parquet.bloom_filter_maybe_contains(DATASET, offset, length, b"Hello")
    assert parquet.bloom_filter_maybe_contains(DATASET, offset, length, b"This is")
    assert parquet.bloom_filter_maybe_contains(DATASET, offset, None, b"a")


def test_bloom_filter_rejects_absent_value():
    offset, length = _bloom_info()
    assert not parquet.bloom_filter_maybe_contains(DATASET, offset, length, b"missing item")
    assert not parquet.bloom_filter_maybe_contains(DATASET, offset, length, b"totally unknown")


def test_bloom_filter_validates_offset():
    offset, length = _bloom_info()
    with pytest.raises(ValueError):
        parquet.bloom_filter_maybe_contains(DATASET, -1, length, b"Hello")
    with pytest.raises(ValueError):
        parquet.bloom_filter_maybe_contains(DATASET, None, length, b"Hello")


if __name__ == "__main__":
    pytest.main([__file__])
