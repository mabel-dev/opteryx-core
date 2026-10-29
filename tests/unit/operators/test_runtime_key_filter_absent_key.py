"""Runtime join KEY filter over a schema-evolved probe folder.

The key filter (src/cpp/engine/runtime_key_filter.hpp, docs/VELOX_BORROW_PLAN.md
item 1) tests a probe-side join key in the parquet scan's decode workers. Under a
pushed predicate the key column decodes WITH the predicate columns and is gathered by
the survivor mask like one of them. A file written before the key column existed does
not hold it: that file must read the key as NULL (absent_fill), match nothing, and
never be treated as holding a predicate column — the gather over a column the file
lacks is a hard error in the Source.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx

NEW_ROWS = 3000
OLD_ROWS = 5000


@pytest.fixture(scope="module")
def folders(tmp_path_factory):
    fact = tmp_path_factory.mktemp("kf_fact")
    dim = tmp_path_factory.mktemp("kf_dim")
    # `a_new` holds the join key `k`; `b_old` predates it.
    pq.write_table(pa.table({
        "id": pa.array(range(NEW_ROWS), pa.int64()),
        "k": pa.array([i % 100 for i in range(NEW_ROWS)], pa.int64()),
        "v": pa.array([i % 7 for i in range(NEW_ROWS)], pa.int64()),
    }), fact / "a_new.parquet", row_group_size=1000)
    pq.write_table(pa.table({
        "id": pa.array(range(NEW_ROWS, NEW_ROWS + OLD_ROWS), pa.int64()),
        "v": pa.array([i % 7 for i in range(OLD_ROWS)], pa.int64()),
    }), fact / "b_old.parquet", row_group_size=1000)
    pq.write_table(pa.table({
        "k": pa.array(range(100), pa.int64()),
        "tag": pa.array(["a" if i % 10 == 0 else "b" for i in range(100)]),
    }), dim / "dim.parquet")
    return str(fact), str(dim)


def _run(sql, enabled):
    session = opteryx.session()
    if not enabled:
        for _ in session.execute_to_morsels("SET disable_runtime_minmax_join_filter = true"):
            pass
    rows = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    scans = session.telemetry.get("io_scan_diagnostics") or []
    return rows, sum(s.get("key_filter_rows_in", 0) for s in scans)


def test_old_file_without_the_key_matches_nothing_and_does_not_fail(folders):
    fact, dim = folders
    # `f.v > 2` is a pushed predicate the query also READS (not filter-only, so no
    # range settlement): the scan runs the prefilter path the key column joins.
    sql = (f"SELECT f.id, f.v FROM '{fact}' AS f INNER JOIN '{dim}' AS d ON f.k = d.k "
           f"WHERE d.tag = 'a' AND f.v > 2")
    expected = sorted((i, i % 7) for i in range(NEW_ROWS) if (i % 100) % 10 == 0 and i % 7 > 2)
    on_rows, tested = _run(sql, True)
    off_rows, off_tested = _run(sql, False)
    assert sorted(on_rows) == expected
    assert sorted(off_rows) == expected
    assert off_tested == 0
    assert tested > 0, "the key filter did not fire on the file that holds the key"
