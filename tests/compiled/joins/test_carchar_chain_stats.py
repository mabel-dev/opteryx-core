"""
Tests for adaptive join statistics — Phase 1 (per docs/adaptive_join_statistics.md).

Verifies that the build-side hash join exposes accurate chain-length statistics:
unique key count, total row count, and average chain length.
"""

import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

from draken.morsels.morsel import Morsel
from draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.joins import (
    build_side_carchar_morsel_map,
    get_last_draken_inner_join_metrics,
)


def _morsel_from_int_column(name: str, values: list[int]) -> Morsel:
    arr = array("q", values)
    vec = vector_from_sequence(arr)
    return Morsel.from_vectors([name], [vec])


def _read_chain_stats() -> tuple[int, int, float]:
    """(unique_keys, total_rows, avg_chain_length) from the last build."""
    metrics = get_last_draken_inner_join_metrics()
    return metrics[10], metrics[11], metrics[12]


def test_unique_keys_no_duplicates():
    """A 1:1 build side has unique_keys == total_rows and avg chain length 1.0."""
    m = _morsel_from_int_column("id", [10, 20, 30, 40, 50])
    build_side_carchar_morsel_map(m, [b"id"])
    unique, total, avg = _read_chain_stats()
    assert unique == 5
    assert total == 5
    assert avg == pytest.approx(1.0)


def test_avg_chain_length_with_duplicates():
    """5 unique keys, each appearing 4 times => avg chain length 4.0."""
    values = [k for k in range(5) for _ in range(4)]  # [0,0,0,0, 1,1,1,1, ...]
    m = _morsel_from_int_column("k", values)
    build_side_carchar_morsel_map(m, [b"k"])
    unique, total, avg = _read_chain_stats()
    assert unique == 5
    assert total == 20
    assert avg == pytest.approx(4.0)


def test_avg_chain_length_skewed():
    """Skewed distribution: one hot key, rest unique."""
    values = [99] * 100 + list(range(1, 11))  # 1 hot + 10 unique = 11 keys, 110 rows
    m = _morsel_from_int_column("k", values)
    build_side_carchar_morsel_map(m, [b"k"])
    unique, total, avg = _read_chain_stats()
    assert unique == 11
    assert total == 110
    assert avg == pytest.approx(110 / 11)


def test_empty_morsel_zero_stats():
    """Empty build side reports zero across the board (no division by zero)."""
    m = _morsel_from_int_column("k", [0])
    # Slice out everything by passing an empty morsel — easier to construct via empty list:
    m_empty = _morsel_from_int_column("k", [])  # may raise; if so use a different approach
    build_side_carchar_morsel_map(m_empty, [b"k"])
    unique, total, avg = _read_chain_stats()
    assert unique == 0
    assert total == 0
    assert avg == 0.0
