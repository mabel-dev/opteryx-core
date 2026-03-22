"""
Tests for opteryx.compiled.morsel_ops.distinct
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pyarrow as pa

import opteryx.draken as draken
from opteryx.compiled.morsel_ops.distinct import CarcharSetWrapper, distinct


def _make(data: dict) -> draken.Morsel:
    return draken.Morsel.from_arrow(pa.table(data))


def test_distinct_basic():
    """All-unique rows: morsel is unchanged."""
    morsel = _make({"a": [1, 2, 3, 4, 5]})
    distinct(morsel, CarcharSetWrapper())
    assert len(morsel) == 5


def test_distinct_duplicates():
    """Duplicate rows: only first occurrence survives."""
    morsel = _make({"a": [1, 2, 1, 3, 2]})
    distinct(morsel, CarcharSetWrapper())
    assert len(morsel) == 3


def test_distinct_multi_column():
    """Two-column key: all four combinations are distinct."""
    morsel = _make({"a": [1, 1, 2, 2], "b": [10, 20, 10, 20]})
    distinct(morsel, CarcharSetWrapper(), columns=[b"a", b"b"])
    assert len(morsel) == 4


def test_distinct_column_subset():
    """Distinct on column 'a' only; 3 unique values → 3 rows."""
    morsel = _make({"a": [1, 2, 1, 3, 2], "b": [10, 20, 30, 40, 50]})
    distinct(morsel, CarcharSetWrapper(), columns=[b"a"])
    assert len(morsel) == 3


def test_distinct_all_duplicates():
    """All identical rows: only the first survives."""
    morsel = _make({"a": [7, 7, 7, 7]})
    distinct(morsel, CarcharSetWrapper())
    assert len(morsel) == 1


def test_distinct_all_duplicates_empties_morsel():
    """When every row is a duplicate of a prior morsel, the morsel is emptied."""
    seen = CarcharSetWrapper()
    morsel1 = _make({"a": [1, 2, 3]})
    morsel2 = _make({"a": [1, 2, 3]})

    distinct(morsel1, seen)
    distinct(morsel2, seen)

    assert len(morsel2) == 0


def test_distinct_empty_morsel():
    """Empty morsel stays empty."""
    morsel = _make({"a": pa.array([], type=pa.int64())})
    distinct(morsel, CarcharSetWrapper())
    assert len(morsel) == 0


def test_distinct_streaming():
    """Seen hashes accumulate across morsels via mutation."""
    seen = CarcharSetWrapper()

    m1 = _make({"a": [1, 2, 3]})
    distinct(m1, seen)
    assert len(m1) == 3   # all new

    m2 = _make({"a": [2, 3, 4]})
    distinct(m2, seen)
    assert len(m2) == 1   # only 4 is new

    m3 = _make({"a": [4, 5, 6]})
    distinct(m3, seen)
    assert len(m3) == 2   # 5 and 6 are new


def test_distinct_set_grows_across_calls():
    """CarcharSetWrapper accumulates unique keys across morsels."""
    seen = CarcharSetWrapper()
    assert len(seen) == 0

    distinct(_make({"a": [1, 2]}), seen)
    assert len(seen) == 2

    distinct(_make({"a": [3, 4]}), seen)
    assert len(seen) == 4


def test_distinct_mixed_types():
    """Mixed-type columns all contribute to the row hash."""
    morsel = _make({
        "i": [1, 2, 1, 3],
        "s": ["a", "b", "a", "c"],
        "f": [1.1, 2.2, 1.1, 3.3],
    })
    distinct(morsel, CarcharSetWrapper())
    # Rows 0 and 2 are identical.
    assert len(morsel) == 3


def test_distinct_large_dataset():
    """50 % duplicates: half the rows survive."""
    n = 10_000
    data = list(range(n // 2)) * 2
    morsel = _make({"a": data})
    distinct(morsel, CarcharSetWrapper())
    assert len(morsel) == n // 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    run_tests()
