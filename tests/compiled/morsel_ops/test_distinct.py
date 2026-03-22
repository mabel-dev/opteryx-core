"""
Tests for opteryx.compiled.morsel_ops.distinct
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pyarrow as pa

import opteryx.draken as draken
from opteryx.compiled.morsel_ops.distinct import CarcharSetWrapper, distinct


def test_distinct_basic():
    """All-unique rows returns every row index."""
    table = pa.table({"a": [1, 2, 3, 4, 5]})
    morsel = draken.Morsel.from_arrow(table)
    seen = CarcharSetWrapper()

    indices = distinct(morsel, seen)

    assert len(indices) == 5
    assert list(indices) == [0, 1, 2, 3, 4]


def test_distinct_duplicates():
    """Duplicate rows: only first occurrence is kept."""
    table = pa.table({"a": [1, 2, 1, 3, 2]})
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper())

    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_multi_column():
    """Two-column key: (1,10), (1,20), (2,10), (2,20) are all distinct."""
    table = pa.table({"a": [1, 1, 2, 2], "b": [10, 20, 10, 20]})
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper(), columns=[b"a", b"b"])

    assert len(indices) == 4
    assert list(indices) == [0, 1, 2, 3]


def test_distinct_column_subset():
    """Distinct on column 'a' only, ignoring 'b'."""
    table = pa.table({"a": [1, 2, 1, 3, 2], "b": [10, 20, 30, 40, 50]})
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper(), columns=[b"a"])

    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_all_duplicates():
    """All identical rows: only the first is kept."""
    table = pa.table({"a": [7, 7, 7, 7]})
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper())

    assert len(indices) == 1
    assert list(indices) == [0]


def test_distinct_empty_morsel():
    """Empty morsel returns empty index array."""
    table = pa.table({"a": pa.array([], type=pa.int64())})
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper())

    assert len(indices) == 0


def test_distinct_streaming():
    """Seen hashes persist across morsel boundaries via mutation."""
    morsel1 = draken.Morsel.from_arrow(pa.table({"a": [1, 2, 3]}))
    morsel2 = draken.Morsel.from_arrow(pa.table({"a": [2, 3, 4]}))
    morsel3 = draken.Morsel.from_arrow(pa.table({"a": [4, 5, 6]}))

    seen = CarcharSetWrapper()

    indices1 = distinct(morsel1, seen)
    assert len(indices1) == 3
    assert list(indices1) == [0, 1, 2]

    # 2 and 3 already seen; only 4 (row 2) is new.
    indices2 = distinct(morsel2, seen)
    assert len(indices2) == 1
    assert list(indices2) == [2]

    # 4 already seen; 5 (row 1) and 6 (row 2) are new.
    indices3 = distinct(morsel3, seen)
    assert len(indices3) == 2
    assert list(indices3) == [1, 2]


def test_distinct_set_grows_across_calls():
    """The same CarcharSetWrapper accumulates entries across calls."""
    morsel1 = draken.Morsel.from_arrow(pa.table({"a": [1, 2]}))
    morsel2 = draken.Morsel.from_arrow(pa.table({"a": [3, 4]}))

    seen = CarcharSetWrapper()
    assert len(seen) == 0

    distinct(morsel1, seen)
    assert len(seen) == 2

    distinct(morsel2, seen)
    assert len(seen) == 4


def test_distinct_mixed_types():
    """Mixed-type columns all contribute to the row hash."""
    table = pa.table(
        {
            "i": [1, 2, 1, 3],
            "s": ["a", "b", "a", "c"],
            "f": [1.1, 2.2, 1.1, 3.3],
        }
    )
    morsel = draken.Morsel.from_arrow(table)

    indices = distinct(morsel, CarcharSetWrapper())

    # Rows 0 and 2 are identical.
    assert len(indices) == 3
    assert list(indices) == [0, 1, 3]


def test_distinct_large_dataset():
    """50 % duplicates: half the rows are kept."""
    n = 10_000
    data = list(range(n // 2)) * 2
    morsel = draken.Morsel.from_arrow(pa.table({"a": data}))

    indices = distinct(morsel, CarcharSetWrapper())

    assert len(indices) == n // 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
