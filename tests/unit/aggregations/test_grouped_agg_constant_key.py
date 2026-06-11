"""Regression test for GROUP BY on constant-literal keys.

A `GROUP BY 1` (or `GROUP BY <any constant literal>`) collapses every input row
into a single group. The binder strips literal group keys, leaving the
AggregateAndGroup node with zero group columns. Pre-fix, the hashed engine's
KeyStore had no path for zero key columns: store_new_rows and
reconstruct_vectors both fell through to `raise RuntimeError("legacy key codec
path removed")`. The fix no-ops the key store for zero columns — there is no key
material to store, and the single group's identity is implicit. The constant
itself (`1 AS k`) is re-added by the Project inserted above the aggregate.

ClickBench q35 (`GROUP BY 1, URL`) already worked because the real `URL` key
kept the column count at one; the all-constant case was the gap.

$planets has 9 rows; numberOfMoons sums to 210.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import opteryx


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        cols = {
            (n.decode() if isinstance(n, bytes) else n): m.column(n).to_pylist()
            for n in m.column_names
        }
        out = [dict(zip(cols.keys(), vals)) for vals in zip(*cols.values())]
    s.close()
    return out


def test_group_by_single_constant_literal():
    rows = _rows("SELECT 1 AS k, SUM(numberOfMoons) AS s FROM $planets GROUP BY 1")
    assert rows == [{"k": 1, "s": 210}], rows


def test_group_by_constant_matches_ungrouped_aggregate():
    grouped = _rows("SELECT SUM(numberOfMoons) AS s FROM $planets GROUP BY 1")
    ungrouped = _rows("SELECT SUM(numberOfMoons) AS s FROM $planets")
    assert grouped == ungrouped == [{"s": 210}], (grouped, ungrouped)


def test_group_by_multiple_constant_literals():
    rows = _rows("SELECT 1 AS a, 2 AS b, COUNT(*) AS c FROM $planets GROUP BY 1, 2")
    assert rows == [{"a": 1, "b": 2, "c": 9}], rows


def test_group_by_constant_expression():
    # A constant arithmetic expression has no bare identifier — still one group.
    rows = _rows("SELECT COUNT(*) AS c FROM $planets GROUP BY 1 + 1")
    assert rows == [{"c": 9}], rows


def test_constant_key_alongside_real_column_still_works():
    # q35 shape: the real `gravity` key keeps the column count non-zero.
    rows = _rows(
        "SELECT 1 AS k, gravity, COUNT(*) AS c FROM $planets GROUP BY 1, gravity"
    )
    assert all(r["k"] == 1 for r in rows), rows
    assert sum(r["c"] for r in rows) == 9, rows


if __name__ == "__main__":
    for fn in [
        test_group_by_single_constant_literal,
        test_group_by_constant_matches_ungrouped_aggregate,
        test_group_by_multiple_constant_literals,
        test_group_by_constant_expression,
        test_constant_key_alongside_real_column_still_works,
    ]:
        fn()
        print("PASS", fn.__name__)
