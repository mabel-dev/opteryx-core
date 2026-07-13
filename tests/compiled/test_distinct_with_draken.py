"""
Tests for the low-level distinct() morsel op (opteryx.compiled.morsel_ops.distinct).

distinct() filters a draken Morsel to distinct rows IN PLACE, accumulating row
hashes into a caller-supplied seen-set (Carchar/Parvi) across calls, and returns an
overflow bool. Assertions therefore check the surviving rows of the mutated morsel.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.compiled.morsel_ops.distinct import distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper

_DT = dn.DrakenType


def _from_arrow(table):
    """Build a draken Morsel from a pyarrow.Table via the pyarrow-free vector path
    (draken.Morsel.from_arrow was removed with draken's pyarrow purge — §4). pyarrow
    is a test-only dependency here, used solely to read the fixture data + its type."""
    names, vecs = [], []
    for name in table.column_names:
        col = table.column(name)
        vals = col.to_pylist()
        pat = col.type
        if pa.types.is_boolean(pat):
            dt = _DT.BOOL
        elif pa.types.is_floating(pat):
            dt = _DT.FLOAT64
        elif pa.types.is_integer(pat):
            dt = _DT.INT64
        elif pa.types.is_string(pat) or pa.types.is_large_string(pat):
            dt = _DT.VARCHAR
            vals = [v.encode("utf-8") if isinstance(v, str) else v for v in vals]
        elif pa.types.is_binary(pat) or pa.types.is_large_binary(pat):
            dt = _DT.VARCHAR
        else:
            raise TypeError(f"_from_arrow: unsupported fixture type {pat}")
        names.append(name.encode("utf-8") if isinstance(name, str) else name)
        vecs.append(vector_from_sequence(vals, dtype=dt))
    return Morsel.from_vectors(names, vecs)


def test_distinct_with_draken_basic():
    """Distinct over all columns keeps the first occurrence of each unique row."""
    morsel = _from_arrow(pa.table({"a": [1, 2, 1, 3, 2], "b": [10, 20, 10, 30, 20]}))

    distinct(morsel, CarcharSetWrapper())

    # First occurrences are rows 0, 1, 3 → (1,10), (2,20), (3,30).
    assert len(morsel) == 3
    assert morsel.column(b"a").to_pylist() == [1, 2, 3]
    assert morsel.column(b"b").to_pylist() == [10, 20, 30]


def test_distinct_with_draken_single_column_bytes():
    """Distinct on a single column specified as bytes."""
    morsel = _from_arrow(pa.table({"a": [1, 2, 1, 3, 2], "b": [10, 20, 30, 40, 50]}))

    distinct(morsel, CarcharSetWrapper(), columns=[b"a"])

    # First occurrence of 1, 2, 3 → rows 0, 1, 3.
    assert len(morsel) == 3
    assert morsel.column(b"a").to_pylist() == [1, 2, 3]
    assert morsel.column(b"b").to_pylist() == [10, 20, 40]


def test_distinct_with_draken_multiple_columns_bytes():
    """Distinct on multiple columns specified as bytes; all combinations unique."""
    morsel = _from_arrow(pa.table({
        "a": [1, 1, 2, 2],
        "b": [10, 20, 10, 20],
        "c": [100, 200, 300, 400],
    }))

    distinct(morsel, CarcharSetWrapper(), columns=[b"a", b"b"])

    assert len(morsel) == 4


def test_distinct_with_draken_streaming():
    """Seen hashes accumulate across morsels via the shared set (in-place)."""
    seen = CarcharSetWrapper()

    m1 = _from_arrow(pa.table({"a": [1, 2, 3]}))
    distinct(m1, seen)
    assert len(m1) == 3  # all new
    assert m1.column(b"a").to_pylist() == [1, 2, 3]

    m2 = _from_arrow(pa.table({"a": [2, 3, 4]}))
    distinct(m2, seen)
    assert len(m2) == 1  # only 4 is new
    assert m2.column(b"a").to_pylist() == [4]

    m3 = _from_arrow(pa.table({"a": [4, 5, 6]}))
    distinct(m3, seen)
    assert len(m3) == 2  # 5 and 6 are new
    assert m3.column(b"a").to_pylist() == [5, 6]


def test_distinct_with_draken_all_duplicates():
    """All rows identical: only the first survives."""
    morsel = _from_arrow(pa.table({"a": [1, 1, 1, 1]}))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == 1
    assert morsel.column(b"a").to_pylist() == [1]


def test_distinct_with_draken_all_unique():
    """All rows unique: morsel unchanged."""
    morsel = _from_arrow(pa.table({"a": [1, 2, 3, 4, 5]}))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == 5
    assert morsel.column(b"a").to_pylist() == [1, 2, 3, 4, 5]


def test_distinct_with_draken_empty_morsel():
    """Empty morsel stays empty."""
    morsel = _from_arrow(pa.table({"a": pa.array([], type=pa.int64())}))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == 0


def test_distinct_with_draken_with_nulls():
    """NULL participates in the row key: (1,10), (None,20), (2,30) are distinct."""
    morsel = _from_arrow(pa.table({
        "a": [1, None, 1, None, 2],
        "b": [10, 20, 10, 20, 30],
    }))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == 3
    assert morsel.column(b"a").to_pylist() == [1, None, 2]
    assert morsel.column(b"b").to_pylist() == [10, 20, 30]


def test_distinct_with_draken_column_names_as_strings():
    """Column names as str also work (distinct accepts str or bytes)."""
    morsel = _from_arrow(pa.table({"a": [1, 2, 1, 3, 2], "b": [10, 20, 30, 40, 50]}))

    distinct(morsel, CarcharSetWrapper(), columns=["a"])

    assert len(morsel) == 3
    assert morsel.column(b"a").to_pylist() == [1, 2, 3]


def test_distinct_with_draken_mixed_types():
    """Mixed-type columns all contribute to the row hash; rows 0 and 2 are identical."""
    morsel = _from_arrow(pa.table({
        "int_col": [1, 2, 1, 3],
        "str_col": ["a", "b", "a", "c"],
        "float_col": [1.1, 2.2, 1.1, 3.3],
        "bool_col": [True, False, True, False],
    }))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == 3
    assert morsel.column(b"int_col").to_pylist() == [1, 2, 3]


def test_distinct_with_draken_large_dataset():
    """50% duplicates: half the rows survive."""
    n = 10000
    data = list(range(n // 2)) * 2
    morsel = _from_arrow(pa.table({"a": data}))

    distinct(morsel, CarcharSetWrapper())

    assert len(morsel) == n // 2


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
