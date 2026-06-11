"""Regression test for grouped aggregation over narrow-integer (INT8/16/32) inputs.

Pre-fix, the grouped-aggregate factory routed INT8/16/32 SUM/AVG to a FLOAT64
collector and MIN/MAX to an object collector, all of which read the narrow
source at the wrong (8-byte) stride and produced garbage tagged FLOAT64. The
fix routes narrow ints to the int64-output collectors with width-aware reads,
matching the scalar aggregate path.

$planets stores `id` and `numberOfMoons` as physical INT8.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import opteryx


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        cols = {(n.decode() if isinstance(n, bytes) else n): m.column(n).to_pylist()
                for n in m.column_names}
        out = list(zip(*cols.values()))
    s.close()
    return out


def test_grouped_sum_narrow_int_matches_value_per_group():
    raw = {i: v for i, v in _rows("SELECT id, numberOfMoons FROM $planets")}
    got = {k: s for k, s in _rows("SELECT id, SUM(numberOfMoons) AS s FROM $planets GROUP BY id")}
    assert got == raw, got


def test_grouped_minmax_narrow_int():
    raw = {i: v for i, v in _rows("SELECT id, numberOfMoons FROM $planets")}
    gmin = {k: s for k, s in _rows("SELECT id, MIN(numberOfMoons) AS s FROM $planets GROUP BY id")}
    gmax = {k: s for k, s in _rows("SELECT id, MAX(numberOfMoons) AS s FROM $planets GROUP BY id")}
    assert gmin == raw, gmin
    assert gmax == raw, gmax


def test_grouped_avg_narrow_int_is_float_value():
    raw = {i: v for i, v in _rows("SELECT id, numberOfMoons FROM $planets")}
    got = {k: s for k, s in _rows("SELECT id, AVG(numberOfMoons) AS s FROM $planets GROUP BY id")}
    assert got == {k: float(v) for k, v in raw.items()}, got


def test_grouped_sum_narrow_matches_widened():
    """Narrow-int SUM must equal the INT32-widened SUM (the wide path is the oracle)."""
    narrow = sorted(_rows("SELECT id, SUM(numberOfMoons) AS s FROM $planets GROUP BY id"))
    wide = sorted(_rows("SELECT id, SUM(CAST(numberOfMoons AS INTEGER)) AS s FROM $planets GROUP BY id"))
    assert narrow == wide, (narrow, wide)


if __name__ == "__main__":
    for fn in [test_grouped_sum_narrow_int_matches_value_per_group,
               test_grouped_minmax_narrow_int,
               test_grouped_avg_narrow_int_is_float_value,
               test_grouped_sum_narrow_matches_widened]:
        fn(); print("PASS", fn.__name__)
