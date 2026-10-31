"""GROUP BY with few groups: pass C sums into interleaved local copies per group.

When a worker holds at most kGBFewGroups groups, GroupBySink's pass C
(pass_c_few in native_group_sinks.hpp) adds each row into one of several local
copies of its group's cells and folds them into the lanes once per morsel. These
tests check each group's SUM/AVG/COUNT against the same aggregate computed per
group by the UNGROUPED path (a different sink), over a multi-file table so many
morsels contribute and some groups are absent from some morsels. Integer and
decimal sums must match exactly; float sums to rounding.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import opteryx

T = "testdata.tpch_1_rugo.lineitem"


def _rows(sql):
    out = []
    for m in opteryx.session().execute_to_morsels(sql):
        cols = [m.column(n).to_pylist() for n in m.column_names]
        out += list(zip(*cols))
    return out


def test_few_groups_match_ungrouped_per_group():
    grouped = _rows(
        f"SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_linenumber), "
        f"AVG(l_extendedprice), COUNT(l_tax), COUNT(*), SUM(l_extendedprice * (1 - l_discount)) "
        f"FROM {T} GROUP BY l_returnflag, l_linestatus"
    )
    assert 3 <= len(grouped) <= 6
    for flag, status, sq, sl, avgp, ct, cs, rev in grouped:
        (ref,) = _rows(
            f"SELECT SUM(l_quantity), SUM(l_linenumber), AVG(l_extendedprice), COUNT(l_tax), "
            f"COUNT(*), SUM(l_extendedprice * (1 - l_discount)) FROM {T} "
            f"WHERE l_returnflag = '{flag}' AND l_linestatus = '{status}'"
        )
        assert (sq, sl, ct, cs, rev) == (ref[0], ref[1], ref[3], ref[4], ref[5]), (flag, status)
        assert math.isclose(float(avgp), float(ref[2]), rel_tol=1e-12), (flag, status)


def test_few_groups_with_filter_and_nulls():
    # The filter makes rows sparse per morsel; NULLIF puts NULL operands in.
    grouped = _rows(
        f"SELECT l_shipmode, SUM(NULLIF(l_linenumber, 3)), COUNT(NULLIF(l_linenumber, 3)), COUNT(*) "
        f"FROM {T} WHERE l_quantity < 5 GROUP BY l_shipmode"
    )
    assert len(grouped) == 7
    for mode, s, c, n in grouped:
        (ref,) = _rows(
            f"SELECT SUM(NULLIF(l_linenumber, 3)), COUNT(NULLIF(l_linenumber, 3)), COUNT(*) "
            f"FROM {T} WHERE l_quantity < 5 AND l_shipmode = '{mode}'"
        )
        assert (s, c, n) == ref, mode


if __name__ == "__main__":
    test_few_groups_match_ungrouped_per_group()
    test_few_groups_with_filter_and_nulls()
    print("ok")
