"""`ORDER BY APPROX_COSINE_DISTANCE(col, 'query') LIMIT k`, end to end through SQL — local disk.

D-4 ruled the spelling, D-9 the recall knob (`nprobe`: the index's own, or `SET nprobe`).
The scan of an approximate search decodes only the index's candidates — plus every row
of files the index does not cover yet, searched exactly (ruled 2026-10-03) — and the
distance reported is always the EXACT one. What each test protects:
  * probing every cluster gives exactly the exact search's answer (ids and distances);
  * whatever is probed, every distance returned is the row's exact COSINE_DISTANCE, in order;
  * files the index does not cover are searched exactly, and EXPLAIN says how many;
  * deleted rows are never returned, nor rows with no embedding (NULL text) - from indexed
    and uncovered files alike, even when that leaves fewer than LIMIT rows;
  * a WHERE is applied BEFORE the search (only its survivors are candidates): broad or
    selective, indexed or not, the answer holds LIMIT rows whenever that many qualify;
  * every other shape is refused, never silently run exactly.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))

import opteryx  # noqa: E402

from test_optimize_local import pytestmark  # noqa: E402,F401
from test_vector_index_compaction_local import TABLE  # noqa: E402
from test_vector_index_compaction_local import _entries  # noqa: E402
from test_vector_index_compaction_local import _texts  # noqa: E402
from test_vector_index_compaction_local import env  # noqa: E402,F401

QUERY = "storm moon ring"


def _rows(sql, session=None):
    session = session or opteryx.session(user="tester")
    out = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = morsel.column_names
        out.extend(zip(*[morsel.column(c).to_pylist() for c in names]))
    return out


def _approx(k, nprobe=None, extra=""):
    session = opteryx.session(user="tester")
    if nprobe is not None:
        _rows(f"SET nprobe = {nprobe}", session)
    return _rows(
        f"SELECT id, APPROX_COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} {extra} ORDER BY d LIMIT {k}",
        session,
    )


def _exact_where(k, where):
    return _rows(
        f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} "
        f"WHERE body IS NOT NULL AND ({where}) ORDER BY d LIMIT {k}"
    )


def _exact(k):
    # Rows with no embedding (NULL text) are never an approximate search's answer (ruled
    # 2026-10-03); the exact reference is the exact search over the rows that have one.
    return _rows(
        f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') AS d FROM {TABLE} "
        f"WHERE body IS NOT NULL ORDER BY d LIMIT {k}"
    )


def _explain(sql):
    return {row[0].decode() if type(row[0]) is bytes else row[0]: row[1] for row in _rows(f"EXPLAIN {sql}")}


def _create(build="sync"):
    opteryx.session(user="tester")
    _rows(f"CREATE INDEX body_idx ON {TABLE} USING IVF (body) WITH (build = '{build}')")


def test_probing_every_cluster_is_the_exact_answer(env):
    _create()
    assert _approx(10, nprobe=100000) == _exact(10)


def test_every_distance_is_exact_whatever_is_probed(env):
    _create()
    exact = dict(_rows(f"SELECT id, COSINE_DISTANCE(body, '{QUERY}') FROM {TABLE}"))
    for nprobe in (1, 2, None):
        found = _approx(10, nprobe=nprobe)
        assert len(found) == 10
        assert [d for _, d in found] == sorted(d for _, d in found)
        assert all(d == exact[i] for i, d in found)


def test_uncovered_files_are_searched_exactly_and_explain_says_so(env):
    _create(build="async")                                   # nothing built yet
    assert _approx(10) == _exact(10)
    plan = _explain(f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, '{QUERY}') LIMIT 5")
    (decision,) = [v for k, v in plan.items() if "vector search" in k]
    assert "0 of 3 file(s) indexed, 3 searched exactly" in decision


def test_deleted_rows_are_never_returned(env):
    _create()
    (best_id, _), *_ = _exact(1)
    by_id = {}
    for entry in _entries(env.dataset):
        texts = _texts(entry["file_path"])
        for ordinal, _text in enumerate(texts):
            by_id[entry["file_path"], ordinal] = None
    # Find the file and ordinal holding best_id: ids are f * ROWS + i in seed file f.
    from test_vector_index_compaction_local import ROWS

    seed = sorted(e["file_path"] for e in _entries(env.dataset))[best_id // ROWS]
    env.dataset.delete_rows({seed: [best_id % ROWS]}, author="tester")
    found = _approx(10, nprobe=100000)
    assert best_id not in {i for i, _ in found}
    assert found == _exact(10)


@pytest.mark.parametrize("build", ["sync", "async"])
def test_rows_with_no_embedding_are_never_returned(env, build):
    _create(build=build)
    total = len(_rows(f"SELECT id FROM {TABLE} WHERE body IS NOT NULL"))
    found = _approx(899, nprobe=100000)                       # 900 rows, some NULL text
    assert total < 899 and len(found) == total                # fewer than LIMIT: no padding
    assert all(d is not None for _, d in found)


@pytest.mark.parametrize("build", ["sync", "async"])
def test_a_where_is_applied_before_the_search(env, build):
    _create(build=build)
    where = "id >= 150 AND id < 750"
    assert _approx(10, nprobe=100000, extra=f"WHERE {where}") == _exact_where(10, where)


def test_a_selective_where_still_finds_every_qualifying_row(env):
    """Five qualifying rows, scattered: the probe cannot be relied on to reach them, so
    a file whose probe falls short of k is searched exactly over its survivors."""
    _create()
    where = "id IN (3, 170, 333, 512, 871)"
    found = _approx(10, extra=f"WHERE {where}")                # the DEFAULT nprobe
    assert found == _exact_where(10, where)
    assert len(found) == len(_rows(f"SELECT id FROM {TABLE} WHERE {where} AND body IS NOT NULL"))


@pytest.mark.parametrize(
    "sql, message",
    [
        (f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, 'x')", "LIMIT"),
        (f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, 'x') DESC LIMIT 3", "NEAREST"),
        (f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, 'x'), id LIMIT 3", "one key"),
        (f"SELECT APPROX_COSINE_DISTANCE(body, 'x') FROM {TABLE}", "only valid"),
        (f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, body) LIMIT 3", "text literal"),
        ("SELECT name FROM $planets ORDER BY APPROX_COSINE_DISTANCE(name, 'x') LIMIT 3", "no vector index|not a catalog table"),
    ],
)
def test_every_other_shape_is_refused(env, sql, message):
    _create()
    with pytest.raises(Exception, match=message):
        _rows(sql)


def test_a_column_without_an_index_is_refused(env):
    with pytest.raises(Exception, match="has no vector index"):
        _rows(f"SELECT id FROM {TABLE} ORDER BY APPROX_COSINE_DISTANCE(body, 'x') LIMIT 3")
