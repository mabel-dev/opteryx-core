"""A pushed predicate over a column some files store NARROWER than the relation's type.

The scan's worker prefilter evaluates the pushed predicate on the decode workers, over
views built at the width the column DECODED (INT16 for the second file here). The
predicate was compiled against the PLAN's type (INT32, from the first file), which is
also what an Iceberg `int` column over a parquet INT(16) column gives. The eval entry
used to stamp the plan's tag on the INT16 view without converting it, so the kernel
read 4-byte values from a 2-byte buffer, and past its end: wrong answers that also
changed from run to run (AIStor ClickBench Q01 counted ~70M rows for 630,500).

The pushed form must agree with a form the planner cannot push (`v + 0`), which the
consumer evaluates over the Source's correctly widened column.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../.."))

import pytest

import opteryx
from rugo import parquet as rugo_parquet

ROWS = 60_000       # one morsel, so the file holds every row
EVERY = 37          # one row in 37 is non-zero
NONZERO = ROWS // EVERY


def _write(path, sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) == 1
    with open(path, "wb") as handle:
        handle.write(rugo_parquet.write_parquet(morsels[0]))


def _count(sql):
    s = opteryx.session()
    n = [v for m in s.execute_to_morsels(sql) for v in m.column("n").to_pylist()][0]
    s.close()
    return n


@pytest.fixture(scope="module")
def relation(tmp_path_factory):
    d = tmp_path_factory.mktemp("widened")
    # Read first, so the relation's type for `v` is INT32 and the INT16 file widens.
    _write(str(d / "a_int32.parquet"), "SELECT CAST(0 AS INT32) AS v FROM generate_series(1, 10) AS g")
    _write(str(d / "b_int16.parquet"),
           f"SELECT CAST(CASE WHEN g % {EVERY} = 0 THEN 7 ELSE 0 END AS INT16) AS v "
           f"FROM generate_series(1, {ROWS}) AS g")
    return f"'{d}'"


@pytest.mark.parametrize("op, expected", [("<> 0", NONZERO), ("= 0", ROWS + 10 - NONZERO),
                                          ("> 0", NONZERO), ("= 7", NONZERO)])
def test_pushed_predicate_on_a_widened_column_matches_the_unpushed_form(relation, op, expected):
    assert _count(f"SELECT COUNT(*) AS n FROM {relation} WHERE v + 0 {op}") == expected
    for _ in range(3):   # the defect read past the buffer, so it also varied run to run
        assert _count(f"SELECT COUNT(*) AS n FROM {relation} WHERE v {op}") == expected


def test_the_column_reads_back_widened(relation):
    s = opteryx.session()
    rows = [r for m in s.execute_to_morsels(f"SELECT v, COUNT(*) AS n FROM {relation} GROUP BY v ORDER BY v")
            for r in zip(m.column("v").to_pylist(), m.column("n").to_pylist())]
    s.close()
    assert rows == [(0, ROWS + 10 - NONZERO), (7, NONZERO)]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
