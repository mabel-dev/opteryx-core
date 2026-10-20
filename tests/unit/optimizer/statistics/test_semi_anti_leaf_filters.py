# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A WHERE with EXISTS / NOT EXISTS must not hide the query's other filters from join planning.

The statistics refresh claims a single-relation filter for its scan by walking up from the
scan through transparent steps. It used to stop at a decorrelated semi / anti join, so in
a query like

    SELECT ... FROM fact f, dim d
    WHERE f.f_dim = d.d_id AND d.d_flag = 'x' AND EXISTS (SELECT 1 FROM other o WHERE o.o_f = f.f_id)

the semi join sat between the cross-join chain and `d.d_flag = 'x'`, the filter was never
claimed, and join planning (DPccp) costed `dim` at its full unfiltered size. TPC-H Q21 lost
`nation` 25 -> 1 and `orders` 15M -> 5M that way; with accurate key NDVs its two best join
orders tied within 0.1% and the worse one won (+37% at SF10).

A semi / anti join emits only rows of its LEFT input, each kept or dropped on its own, so a
filter on left-only columns above it commutes with it exactly; claiming it at the left leg's
scan changes no result. The right leg is not transparent: a conjunct above the join cannot
name its columns.

The leaf sizes are read off the join graph the enumerator is handed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

import draken.draken_native as dn
import opteryx
import opteryx.planner.optimizer.strategies.join_planning as join_planning
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.parquet import write_parquet

DIM_ROWS = 1000
FACT_ROWS = 20000
# `other` holds every 300th fact id: of the fact rows that join a flagged dim row, some are
# in it and some are not, so EXISTS and NOT EXISTS keep different rows.
OTHER_IDS = list(range(0, FACT_ROWS, 300))


def _write(directory, name, columns):
    os.makedirs(directory / name)
    morsel = Morsel.from_vectors(list(columns), list(columns.values()))
    (directory / name / "part-000.parquet").write_bytes(write_parquet(morsel, max_rows_per_row_group=1024))


@pytest.fixture
def tables(tmp_path):
    _write(
        tmp_path,
        "dim",
        {
            "d_id": Vector(dn.vector_int64_from_sequence(list(range(DIM_ROWS)))),
            "d_flag": Vector(dn.vector_from_string_sequence([b"x" if i % 100 == 0 else b"y" for i in range(DIM_ROWS)])),
        },
    )
    _write(
        tmp_path,
        "fact",
        {
            "f_id": Vector(dn.vector_int64_from_sequence(list(range(FACT_ROWS)))),
            "f_dim": Vector(dn.vector_int64_from_sequence([i % DIM_ROWS for i in range(FACT_ROWS)])),
        },
    )
    _write(tmp_path, "other", {"o_f": Vector(dn.vector_int64_from_sequence(OTHER_IDS))})
    return tmp_path


def _leaves_seen_by_the_enumerator(monkeypatch, sql):
    """{relation tail: (row_count, base_row_count)} for the graph DPccp is handed."""
    seen = {}
    original = join_planning.enumerate_join_tree

    def spy(graph, *args, **kwargs):
        for vertex in graph.vertices:
            seen[vertex.name.rstrip("'").split("/")[-1]] = (vertex.row_count, vertex.base_row_count)
        return original(graph, *args, **kwargs)

    monkeypatch.setattr(join_planning, "enumerate_join_tree", spy)
    session = opteryx.session()
    for _ in session.execute_to_morsels("EXPLAIN " + sql):
        pass
    session.close()
    return seen


def _query(tables, tail):
    return (
        f"SELECT COUNT(*) FROM '{tables}/fact' AS f, '{tables}/dim' AS d "
        f"WHERE f.f_dim = d.d_id AND d.d_flag = 'x' {tail}"
    )


def test_the_filter_is_visible_with_no_subquery(tables, monkeypatch):
    """The control: this always worked."""
    seen = _leaves_seen_by_the_enumerator(monkeypatch, _query(tables, ""))
    rows, base = seen["d"]
    assert base == DIM_ROWS and rows < base, seen


@pytest.mark.parametrize("kind", ["EXISTS", "NOT EXISTS"])
def test_the_filter_stays_visible_above_a_decorrelated_semi_or_anti_join(tables, monkeypatch, kind):
    sql = _query(tables, f"AND {kind} (SELECT 1 FROM '{tables}/other' AS o WHERE o.o_f = f.f_id)")
    seen = _leaves_seen_by_the_enumerator(monkeypatch, sql)
    rows, base = seen["d"]
    assert base == DIM_ROWS and rows < base, f"dim reached DPccp unfiltered ({rows} of {base}): {seen}"


def test_in_subquery_is_a_semi_join_too(tables, monkeypatch):
    sql = _query(tables, f"AND f.f_id IN (SELECT o_f FROM '{tables}/other')")
    seen = _leaves_seen_by_the_enumerator(monkeypatch, sql)
    rows, base = seen["d"]
    assert rows < base, seen


def test_results_are_unchanged(tables):
    """Claiming the filter earlier is an estimate-side change only."""
    flagged = {i for i in range(DIM_ROWS) if i % 100 == 0}
    joined = [f for f in range(FACT_ROWS) if f % DIM_ROWS in flagged]
    in_other = [f for f in joined if f in set(OTHER_IDS)]
    expected = {
        "": len(joined),
        f"AND EXISTS (SELECT 1 FROM '{tables}/other' AS o WHERE o.o_f = f.f_id)": len(in_other),
        f"AND NOT EXISTS (SELECT 1 FROM '{tables}/other' AS o WHERE o.o_f = f.f_id)": len(joined) - len(in_other),
    }
    assert 0 < len(in_other) < len(joined), "the fixture must make EXISTS and NOT EXISTS differ"
    for tail, count in expected.items():
        session = opteryx.session()
        got = [m.column(m.column_names[0]).to_pylist()[0] for m in session.execute_to_morsels(_query(tables, tail))]
        session.close()
        assert got == [count], (tail, got)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
