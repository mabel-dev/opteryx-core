"""`ORDER BY COSINE_DISTANCE(col, 'query') LIMIT k` through a vector index, end to end
through SQL — local disk.

D-4 ruled the spelling COSINE_DISTANCE (2026-10-03: the index answers the plain function);
D-9 the knob (`SET nprobe`; unset = exact). The scan of an indexed search decodes only the
rows the index finds — plus every row of files the index does not cover yet, searched
exactly — and the distance reported is always the COSINE_DISTANCE kernel's. The REFERENCE
in every test is computed without the index: every row's distance, ordered here. What
each test protects:
  * the default search is exact: the index never changes an answer;
  * under `SET nprobe`, every distance returned is still the row's exact one, in order;
  * files the index does not cover are searched exactly, and EXPLAIN says how many;
  * deleted rows are never returned, nor rows with no embedding (NULL text) - with or
    without an index, with or without a LIMIT, even when that leaves fewer than LIMIT;
  * a WHERE is applied BEFORE the search and every survivor is scored, `nprobe` or not;
  * every other shape runs as written, without the index;
  * the index is used per file only where it is estimated cheaper than an exact search
    (ruled 2026-10-04), and EXPLAIN states both estimates.

So that the index path is what they exercise whatever the measured constants become,
every test but the cost-model ones prices embedding at a second a row
(`_index_is_cheaper`), which makes the index the cheaper path.
"""

import math
import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))

import opteryx  # noqa: E402

from test_optimize_local import pytestmark  # noqa: E402,F401
from test_vector_index_compaction_local import TABLE  # noqa: E402
from test_vector_index_compaction_local import _entries  # noqa: E402
from test_vector_index_compaction_local import env  # noqa: E402,F401

QUERY = "storm moon ring"


@pytest.fixture(autouse=True)
def _index_is_cheaper(request, monkeypatch):
    """Embedding at 1 s/row: the cost model then sends every covered file through the
    index. Tests marked `measured_costs` keep the measured constants."""
    if request.node.get_closest_marker("measured_costs") is None:
        from opteryx import config

        monkeypatch.setitem(config.VECTOR_COST_EMBED_SECONDS_PER_ROW, "static-hash", 1.0)


def _rows(sql, session=None):
    session = session or opteryx.session(user="tester")
    out = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = morsel.column_names
        out.extend(zip(*[morsel.column(c).to_pylist() for c in names]))
    return out


def _search(k, nprobe=None, extra=""):
    session = opteryx.session(user="tester")
    if nprobe is not None:
        _rows(f"SET nprobe = {nprobe}", session)
    return _rows(
        f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} {extra} ORDER BY d LIMIT {k}",
        session,
    )


def _reference(k=None, where=None, descending=False):
    """Without the index: every row's distance (no ORDER BY, so no index), ordered here.
    Rows with no distance are never an answer (ruled 2026-10-03)."""
    clause = f"WHERE {where}" if where else ""
    scored = [
        (i, d) for i, d in _rows(f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') FROM {TABLE} {clause}")
        if d is not None and not math.isnan(d)
    ]
    scored.sort(key=lambda r: ((-r[1] if descending else r[1]), r[0]))
    return scored if k is None else scored[:k]


def _same(found, expected):
    """Equal answers: the same rows at the same distances, in distance order (rows with
    equal distances may come in either order)."""
    keys = [d for _, d in found]
    assert keys == sorted(keys) or keys == sorted(keys, reverse=True)
    assert sorted(found, key=lambda r: (r[1], r[0])) == sorted(expected, key=lambda r: (r[1], r[0]))


def _explain(sql):
    return {row[0].decode() if type(row[0]) is bytes else row[0]: row[1] for row in _rows(f"EXPLAIN {sql}")}


def _decision(sql):
    return [v for k, v in _explain(sql).items() if "vector search" in k]


def _create(build="sync"):
    opteryx.session(user="tester")
    _rows(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = '{build}')")


def test_the_default_search_is_exact(env):
    """No `SET nprobe`: every stored vector is scored, so the answer is the exact one."""
    _create()
    (decision,) = _decision(f"SELECT id FROM {TABLE} ORDER BY COSINE_DISTANCE(body, '{QUERY}') LIMIT 5")
    assert "index body_idx" in decision and "3 of 3 file(s) via the index" in decision
    _same(_search(10), _reference(10))
    _same(_search(10, nprobe=0), _reference(10))
    _same(_search(10, nprobe=100000), _reference(10))     # probing every cluster, likewise


def test_every_distance_is_exact_whatever_is_probed(env):
    _create()
    exact = dict(_reference())
    for nprobe in (1, 2, None):
        found = _search(10, nprobe=nprobe)
        assert len(found) == 10
        assert [d for _, d in found] == sorted(d for _, d in found)
        assert all(d == exact[i] for i, d in found)


def test_uncovered_files_are_searched_exactly_and_explain_says_so(env):
    _create(build="async")                                   # nothing built yet
    _same(_search(10), _reference(10))
    (decision,) = _decision(f"SELECT id FROM {TABLE} ORDER BY COSINE_DISTANCE(body, '{QUERY}') LIMIT 5")
    assert "not used" in decision and "3 not covered by the index" in decision


@pytest.mark.measured_costs
def test_with_the_measured_costs_local_files_go_through_the_index(env):
    """Locally a read costs next to nothing, so embedding every row is what an exact search
    pays and the index is cheaper. EXPLAIN states both estimates."""
    _create()
    (decision,) = _decision(f"SELECT id FROM {TABLE} ORDER BY COSINE_DISTANCE(body, '{QUERY}') LIMIT 5")
    assert "3 of 3 file(s) via the index (0 uncosted)" in decision
    assert "est index" in decision and "over 3 costed file(s)" in decision
    # The catalog records each file's row-group count, so none is estimated.
    assert "row groups estimated for 0" in decision
    _same(_search(10), _reference(10))


@pytest.mark.measured_costs
def test_when_embedding_is_free_every_file_is_searched_exactly(env, monkeypatch):
    """With nothing to save on embedding, the index's extra reads lose: no file uses it,
    the scan is not routed through it, and the answer is the exact one."""
    from opteryx import config

    _create()
    monkeypatch.setitem(config.VECTOR_COST_EMBED_SECONDS_PER_ROW, "static-hash", 0.0)
    (decision,) = _decision(f"SELECT id FROM {TABLE} ORDER BY COSINE_DISTANCE(body, '{QUERY}') LIMIT 5")
    assert "not used: every file is cheaper searched exactly" in decision
    _same(_search(10), _reference(10))


@pytest.mark.measured_costs
def test_an_embedder_with_no_measured_cost_is_refused(env, monkeypatch):
    from opteryx import config
    from opteryx.exceptions import InvalidConfigurationError

    _create()
    monkeypatch.delitem(config.VECTOR_COST_EMBED_SECONDS_PER_ROW, "static-hash")
    with pytest.raises(InvalidConfigurationError, match="static-hash"):
        _search(10)


def test_deleted_rows_are_never_returned(env):
    _create()
    (best_id, _), *_ = _reference(1)
    from test_vector_index_compaction_local import ROWS

    seed = sorted(e["file_path"] for e in _entries(env.dataset))[best_id // ROWS]
    env.dataset.delete_rows({seed: [best_id % ROWS]}, author="tester")
    found = _search(10)
    assert best_id not in {i for i, _ in found}
    _same(found, _reference(10))


@pytest.mark.parametrize("build", ["sync", "async"])
def test_rows_with_no_embedding_are_never_returned(env, build):
    _create(build=build)
    total = len(_rows(f"SELECT id FROM {TABLE} WHERE body IS NOT NULL"))
    found = _search(899)                                      # 900 rows, some NULL text
    assert total < 899 and len(found) == total                # fewer than LIMIT: no padding
    assert all(d is not None for _, d in found)


@pytest.mark.parametrize("limit", ["LIMIT 899", ""])
def test_rows_with_no_embedding_are_dropped_without_an_index_too(env, limit):
    """The same SQL gives the same answer whether or not an index exists."""
    total = len(_rows(f"SELECT id FROM {TABLE} WHERE body IS NOT NULL"))
    sql = f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} ORDER BY d {limit}"
    assert _decision(sql) == []
    found = _rows(sql)
    assert len(found) == total
    _same(found, _reference())


@pytest.mark.parametrize("build", ["sync", "async"])
def test_a_where_is_applied_before_the_search(env, build):
    _create(build=build)
    where = "id >= 150 AND id < 750"
    _same(_search(10, extra=f"WHERE {where}"), _reference(10, where))


def test_a_where_scores_every_survivor_even_with_nprobe_set(env):
    """Five qualifying rows, scattered across clusters: a probe picks clusters by
    proximity and could miss them, so under a WHERE `nprobe` is not applied - every
    survivor is scored from its stored vector."""
    _create()
    where = "id IN (3, 170, 333, 512, 871)"
    found = _search(10, nprobe=1, extra=f"WHERE {where}")
    _same(found, _reference(10, where))
    assert len(found) == len(_rows(f"SELECT id FROM {TABLE} WHERE {where} AND body IS NOT NULL"))


@pytest.mark.parametrize(
    "order, descending",
    [
        ("ORDER BY d DESC LIMIT 3", True),                   # the farthest: not what the index finds
        ("ORDER BY d", False),                               # no LIMIT
        ("ORDER BY d, id LIMIT 3", False),                   # a second key
    ],
)
def test_other_shapes_run_as_written_without_the_index(env, order, descending):
    _create()
    sql = f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} {order}"
    assert _decision(sql) == []
    found = _rows(sql)
    expected = _reference(None, descending=descending)
    if "LIMIT 3" in order:
        assert [d for _, d in found] == [d for _, d in expected[:3]]
    else:
        _same(found, expected)


def test_a_query_that_is_not_a_literal_runs_without_the_index(env):
    _create()
    sql = f"SELECT id, COSINE_DISTANCE(body, body) AS d FROM {TABLE} ORDER BY d LIMIT 3"
    assert _decision(sql) == []
    found = _rows(sql)
    assert len(found) == 3 and all(d is not None for _, d in found)


def test_a_table_outside_the_catalog_runs_without_an_index(env):
    found = _rows(f"SELECT name, COSINE_DISTANCE(name, '{QUERY}') AS d FROM $planets ORDER BY d LIMIT 3")
    assert len(found) == 3 and all(d is not None for _, d in found)
