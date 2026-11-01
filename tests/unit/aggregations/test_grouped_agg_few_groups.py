"""GROUP BY with few groups: pass C sums into interleaved local copies per group.

When a worker holds at most kGBFewGroups groups, GroupBySink's pass C
(pass_c_few in native_group_sinks.hpp) adds each row into one of several local
copies of its group's cells and folds them into the lanes once per morsel. These
tests check each group's SUM/AVG/COUNT against the same aggregate computed per
group by the UNGROUPED path (a different sink), over a multi-file table so many
morsels contribute and some groups are absent from some morsels. Integer and
decimal sums must match exactly; float sums to rounding.
"""
import decimal
import math
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.connectors import DiskConnector

T = "gbfew.lineitem"
FLAG_STATUS = [("A", "F"), ("N", "F"), ("N", "O"), ("R", "F")]
SHIPMODES = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"]
FILES = 6
ROWS_PER_FILE = 6000
RG = 1000

SCHEMA = pa.schema(
    [
        ("l_returnflag", pa.string()),
        ("l_linestatus", pa.string()),
        ("l_quantity", pa.decimal128(15, 2)),
        ("l_linenumber", pa.int32()),
        ("l_extendedprice", pa.decimal128(15, 2)),
        ("l_discount", pa.decimal128(15, 2)),
        ("l_tax", pa.decimal128(15, 2)),
        ("l_shipmode", pa.string()),
    ]
)


@pytest.fixture(scope="module", autouse=True)
def lineitem(tmp_path_factory):
    # lineitem-shaped multi-file table; file k lacks group k % 4, so groups are absent from some morsels
    root = tmp_path_factory.mktemp("gb_few_groups")
    path = root / "gbfew" / "lineitem"
    path.mkdir(parents=True)
    rng = random.Random(42)
    cents = lambda lo, hi: decimal.Decimal(rng.randint(lo, hi)) / 100
    for k in range(FILES):
        groups = [g for i, g in enumerate(FLAG_STATUS) if i != k % len(FLAG_STATUS)]
        rows = [
            (*rng.choice(groups), cents(100, 5000), rng.randint(1, 7), cents(90000, 10494950),
             cents(0, 10), cents(0, 8), rng.choice(SHIPMODES))
            for _ in range(ROWS_PER_FILE)
        ]
        table = pa.table({name: pa.array([r[i] for r in rows], SCHEMA.field(name).type) for i, name in enumerate(SCHEMA.names)})
        pq.write_table(table, str(path / f"part-{k:03d}.parquet"), row_group_size=RG)
    cwd = os.getcwd()
    os.chdir(root)
    opteryx.register_workspace("gbfew", DiskConnector)
    yield
    os.chdir(cwd)


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
    assert len(grouped) == len(FLAG_STATUS)
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
    assert len(grouped) == len(SHIPMODES)
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
