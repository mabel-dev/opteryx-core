# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
GroupLimitKeyBoundStrategy: `GROUP BY k LIMIT n` (no ORDER BY) gets `k <= T` from
manifest min/max, so files that cannot hold a chosen group are never read.

Two layers:
- the bound derivation (`_integer_bound` / `_string_bound`) on hand-built bounds,
  including the truncated-string and prefix cases a real writer only produces on
  long strings;
- end to end on real Parquet files: the rule fires, prunes files, returns exactly
  LIMIT groups, and EVERY returned group's count equals its count from the same
  query with the rule force-disabled (a chosen group must never be a partial one).
  Shapes that remove rows or groups (WHERE, HAVING, ORDER BY) must not fire it.
"""

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx import config
from opteryx.planner.optimizer.strategies.group_limit_key_bound import _increment
from opteryx.planner.optimizer.strategies.group_limit_key_bound import _integer_bound
from opteryx.planner.optimizer.strategies.group_limit_key_bound import _string_bound
from rugo.parquet import write_parquet

FILES = 12
ROWS_PER_KEY = 3
KEYS_PER_FILE = 100


def _int_bounds(*pairs):
    return [(("int", lo), ("int", hi)) for lo, hi in pairs]


def _str_bounds(*pairs, tag="text"):
    return [((tag, lo), (tag, hi)) for lo, hi in pairs]


# --------------------------------------------------------------------------
# Bound derivation
# --------------------------------------------------------------------------


def test_increment():
    assert _increment(b"abc") == b"abd"
    assert _increment(b"ab\xff") == b"ac"
    assert _increment(b"\xff\xff") is None
    assert _increment(b"") is None


def test_integer_bound_uses_mins_and_maxes_as_witnesses():
    bounds = _int_bounds((0, 99), (1000, 1099), (2000, 2099), (3000, 3099))
    # 4th smallest of {0,99,1000,1099,2000,2099,3000,3099} is 1099: two files qualify
    assert _integer_bound(bounds, 4) == (1099, 2)
    assert _integer_bound(bounds, 1) == (0, 1)


def test_integer_bound_needs_k_distinct_witnesses():
    # one file, min == max: a single provable distinct value
    assert _integer_bound(_int_bounds((5, 5), (5, 5)), 2) is None
    assert _integer_bound(_int_bounds((1, 2), (3, 4)), 5) is None


def test_integer_bound_rejects_non_integer_bounds():
    assert _integer_bound([(("text", b"a"), ("text", b"b"))], 1) is None


def test_string_bound_bumps_the_truncated_prefix():
    bounds = _str_bounds((b"a", b"z"), (b"b", b"z"), (b"c", b"z"), (b"d", b"z"))
    # K=2: witnesses a, b -> T = inc("b") = "c"; files with min <= "c": a, b, c
    assert _string_bound(bounds, 2) == (b"c", 3)


def test_string_bound_skips_a_witness_that_is_a_prefix_of_another():
    # "ab" could be the truncation of "abc..." - or equal to it - so they are NOT
    # two provable distinct values; "ab" and "abc" count once.
    bounds = _str_bounds((b"ab", b"z"), (b"abc", b"z"), (b"b", b"z"), (b"c", b"z"), (b"e", b"z"))
    # witnesses "ab", "b" -> T = inc("b") = "c": a min equal to T is kept, "e" is pruned
    assert _string_bound(bounds, 2) == (b"c", 4)
    # witnesses "ab", "b", "c" -> T = inc("c") = "d"
    assert _string_bound(bounds, 3) == (b"d", 4)


def test_string_bound_accepts_binary_bounds_too():
    assert _string_bound(_str_bounds((b"a", b"z"), (b"b", b"z"), (b"c", b"z"), tag="bytes"), 1) == (b"b", 2)


def test_string_bound_declines_empty_and_unbumpable():
    # all mins are the empty string: one witness, cannot reach K=2
    assert _string_bound(_str_bounds((b"", b"x"), (b"", b"y")), 2) is None
    # last witness all 0xFF: no greater string to bound with
    assert _string_bound(_str_bounds((b"a", b"b"), (b"\xff", b"\xff")), 2) is None


# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dataset():
    ds_dir = Path("testdata") / "group_limit_key_bound"
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    session = opteryx.session()
    for i in range(FILES):
        sql = (
            f"SELECT ({i} * 1000) + (g % {KEYS_PER_FILE}) AS id, "
            f"CONCAT('n{i:02d}_', CAST((g % {KEYS_PER_FILE}) AS VARCHAR)) AS name, "
            f"g AS payload FROM GENERATE_SERIES(0, {KEYS_PER_FILE * ROWS_PER_KEY - 1}) AS g"
        )
        morsel = list(session.execute_to_morsels(sql))[0]
        with open(ds_dir / f"part-{i}.parquet", "wb") as f:
            f.write(write_parquet(morsel))
    name = "testdata.group_limit_key_bound"
    list(session.execute_to_morsels(f"ANALYZE TABLE {name}"))
    yield name
    shutil.rmtree(ds_dir, ignore_errors=True)


def _rows(session, sql):
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(morsel.to_arrow().to_pylist())
    return rows


def _fired(sql):
    session = opteryx.session()
    text = "".join(str(m) for m in session.execute_to_morsels("EXPLAIN " + sql))
    return "group-limit key bound" in text


def _full_counts(session, dataset, key):
    original = config.features.disable_group_limit_key_bound
    try:
        config.features.disable_group_limit_key_bound = True
        return {r[key]: r["c"] for r in _rows(session, f"SELECT {key}, COUNT(*) AS c FROM {dataset} GROUP BY {key}")}
    finally:
        config.features.disable_group_limit_key_bound = original


@pytest.mark.parametrize("key", ["id", "name"])
@pytest.mark.parametrize("limit, offset", [(10, 0), (1, 0), (10, 5), (250, 0)])
def test_chosen_groups_are_complete(dataset, key, limit, offset):
    session = opteryx.session()
    sql = f"SELECT {key}, COUNT(*) AS c FROM {dataset} GROUP BY {key} LIMIT {limit}"
    if offset:
        sql += f" OFFSET {offset}"
    got = _rows(session, sql)
    assert len(got) == limit
    truth = _full_counts(session, dataset, key)
    for row in got:
        assert row["c"] == truth[row[key]] == ROWS_PER_KEY, f"partial or wrong group {row}"
    assert len({r[key] for r in got}) == limit


def test_fires_and_names_the_pruning(dataset):
    assert _fired(f"SELECT id, COUNT(*) FROM {dataset} GROUP BY id LIMIT 10")
    assert _fired(f"SELECT name, COUNT(*) FROM {dataset} GROUP BY name LIMIT 10")


def test_does_not_fire_when_no_file_could_be_dropped(dataset):
    # 12 files x 100 keys: K above what a strict subset of files provides
    assert not _fired(f"SELECT id, COUNT(*) FROM {dataset} GROUP BY id LIMIT {FILES * KEYS_PER_FILE}")


@pytest.mark.parametrize(
    "shape",
    [
        "SELECT id, COUNT(*) FROM {d} WHERE payload > 1 GROUP BY id LIMIT 10",
        "SELECT id, COUNT(*) AS c FROM {d} GROUP BY id HAVING COUNT(*) > 1 LIMIT 10",
        "SELECT id, COUNT(*) AS c FROM {d} GROUP BY id ORDER BY c DESC LIMIT 10",
        "SELECT id, COUNT(*) FROM {d} GROUP BY id",
        "SELECT DISTINCT id FROM {d} LIMIT 10",
    ],
)
def test_abandons_shapes_that_remove_rows_or_groups(dataset, shape):
    assert not _fired(shape.format(d=dataset))
