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
from draken.morsels.morsel import Morsel
from opteryx import config
from opteryx.planner.optimizer.strategies.group_limit_key_bound import _increment
from opteryx.planner.optimizer.strategies.group_limit_key_bound import _integer_window
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


def _units(*triples):
    """(lo, hi, proven) per file, numbered in order."""
    return [(lo, hi, proven, n) for n, (lo, hi, proven) in enumerate(triples)]


def test_integer_window_uses_mins_and_maxes_as_witnesses():
    units = _units((0, 99, 0), (1000, 1099, 0), (2000, 2099, 0), (3000, 3099, 0))
    # 4 consecutive witnesses fit [0, 1099] (2 files) - or [99, 2000]/[1000, 2099]
    # (3 files each, touching a file on both sides); the prefix touches fewest files
    assert _integer_window(units, 4) == (0, 1099, 2, 2)
    assert _integer_window(units, 1) == (0, 0, 1, 1)


def test_integer_window_cuts_from_the_top_when_that_touches_fewer_files():
    # files in DESCENDING key order: the K smallest keys are in the LAST files, and a
    # window anchored at the low end finds them without reading the early files
    units = _units((9000, 9099, 0), (5000, 5099, 0), (1000, 1099, 0), (0, 99, 0))
    assert _integer_window(units, 4) == (0, 1099, 2, 2)
    # a sorted-descending layout where only a MIDDLE window is cheap
    units = _units((0, 9, 0), (100, 109, 0), (101, 108, 0), (900, 909, 0))
    lower, upper, kept_units, kept_files = _integer_window(units, 4)
    assert (lower, upper) == (100, 109) and kept_files == 2


def test_integer_window_proven_floor_beats_witnesses_on_a_clustered_layout():
    # K=10. Witnesses alone: 10th smallest of 2 per file = file 5's max -> 5 files.
    units = _units(*[(i * 1000, i * 1000 + 99, 0) for i in range(8)])
    assert _integer_window(units, 10) == (0, 4099, 5, 5)
    # file 0 PROVES 10 distinct values inside [0, 99]: one file
    floors = [10] + [0] * 7
    proven = _units(*[(i * 1000, i * 1000 + 99, floors[i]) for i in range(8)])
    assert _integer_window(proven, 10) == (0, 99, 1, 1)
    # a floor below K proves nothing
    weak = _units(*[(i * 1000, i * 1000 + 99, 9 if i == 0 else 0) for i in range(8)])
    assert _integer_window(weak, 10) == (0, 4099, 5, 5)


def test_integer_window_floor_window_has_a_lower_edge_when_it_skips_units():
    # the file proving K=5 distinct values is in the MIDDLE of the key space: its own
    # [lo, hi] excludes the files wholly below and above it
    units = _units((0, 9, 0), (100, 199, 5), (500, 599, 0))
    assert _integer_window(units, 5) == (100, 199, 1, 1)


def test_integer_window_unclustered_floor_never_loosens_the_witness_window():
    # every file spans the domain: a floor's [lo, hi] touches ALL files, the witness
    # window (mins clustered low) touches fewer
    units = _units(*[(i, 1000 + i, 50) for i in range(8)])
    lower, upper, kept_units, kept_files = _integer_window(units, 4)
    assert (lower, upper) == (0, 3) and kept_files == 4


def test_integer_window_needs_k_distinct_witnesses():
    # one value in two files: a single provable distinct value
    assert _integer_window(_units((5, 5, 0), (5, 5, 0)), 2) is None
    assert _integer_window(_units((1, 2, 0), (3, 4, 0)), 5) is None


def test_integer_window_within_confines_the_search():
    units = _units((0, 9, 0), (100, 109, 0), (200, 209, 0))
    assert _integer_window(units, 2, within=(100, 209))[:2] == (100, 109)


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
        morsel = Morsel.combine(list(session.execute_to_morsels(sql)))
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


# --------------------------------------------------------------------------
# Skene: a proven per-file distinct count tightens the bound
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def skene_dataset():
    import skene
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    ds_dir = Path("testdata") / "group_limit_key_bound_skene"
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    for i in range(6):
        ids = [i * 1000 + (g % KEYS_PER_FILE) for g in range(KEYS_PER_FILE * ROWS_PER_KEY)]
        morsel = Morsel.from_vectors(
            ["id", "payload"],
            [
                vector_from_sequence(ids, DrakenType.INT64),
                vector_from_sequence(list(range(len(ids))), DrakenType.INT64),
            ],
        )
        (ds_dir / f"part-{i}.skene").write_bytes(skene.write_morsel(morsel, read_acceleration=True))
    yield "testdata.group_limit_key_bound_skene"
    shutil.rmtree(ds_dir, ignore_errors=True)


def test_skene_proven_distinct_count_keeps_one_file(skene_dataset):
    sql = f"SELECT id, COUNT(*) AS c FROM {skene_dataset} GROUP BY id LIMIT 10"
    session = opteryx.session()
    got = _rows(session, sql)
    # witnesses alone would keep 5 of 6 files (prune 1); the file's PROVEN 100
    # distinct keys keep exactly 1 (prune 5)
    assert session.telemetry["files_pruned"] == 5
    assert len(got) == 10
    assert len({r["id"] for r in got}) == 10
    assert all(r["c"] == ROWS_PER_KEY for r in got)


# --------------------------------------------------------------------------
# Row-group level: files that overlap completely, so no FILE bound can prune
# --------------------------------------------------------------------------

RG_FILES = 4
RG_PER_FILE = 8
RG_ROWS = 50
# Row groups 0-3 and 4-7 of a file hold the SAME 25 keys (two rows each per row
# group), so every key's rows are spread over four row groups: a key kept at all
# must be kept in every one of them, or its count is partial.
RG_KEY_ROWS = 8


def _rg_keys(file_no, rg_no):
    return [(rg_no // 4) * 1000 + file_no * 25 + (i % 25) for i in range(RG_ROWS)]


def _write_rg_dataset(kind, name):
    import skene
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    ds_dir = Path("testdata") / name
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    for f in range(RG_FILES):
        keys = [k for rg in range(RG_PER_FILE) for k in _rg_keys(f, rg)]
        if kind == "parquet":
            morsel = Morsel.from_vectors(
                ["id", "payload"],
                [
                    vector_from_sequence(keys, DrakenType.INT64),
                    vector_from_sequence(list(range(len(keys))), DrakenType.INT64),
                ],
            )
            (ds_dir / f"part-{f}.parquet").write_bytes(
                write_parquet(morsel, max_rows_per_row_group=RG_ROWS)
            )
        else:
            writer = skene.SkeneWriter(read_acceleration=True)
            for rg in range(RG_PER_FILE):
                ids = _rg_keys(f, rg)
                writer.add_row_group(
                    Morsel.from_vectors(
                        ["id", "payload"],
                        [
                            vector_from_sequence(ids, DrakenType.INT64),
                            vector_from_sequence(list(range(len(ids))), DrakenType.INT64),
                        ],
                    )
                )
            writer.write_to(str(ds_dir / f"part-{f}.skene"))
    return ds_dir, f"testdata.{name}"


@pytest.fixture(scope="module", params=["parquet", "skene"])
def row_group_dataset(request):
    ds_dir, name = _write_rg_dataset(request.param, f"group_limit_key_bound_rg_{request.param}")
    yield name
    shutil.rmtree(ds_dir, ignore_errors=True)


def test_row_group_bound_fires_where_no_file_bound_can(row_group_dataset):
    # file witnesses {0,25,50,75,1024,1049,1074,1099}: the 5th is 1024, which keeps
    # every file. Row-group witnesses give T=50 and only the row groups that can hold
    # a key <= 50.
    sql = f"SELECT id, COUNT(*) AS c FROM {row_group_dataset} GROUP BY id LIMIT 5"
    assert _fired(sql)
    session = opteryx.session()
    got = _rows(session, sql)
    assert len(got) == 5
    assert len({r["id"] for r in got}) == 5
    truth = _full_counts(session, row_group_dataset, "id")
    for row in got:
        assert row["c"] == truth[row["id"]] == RG_KEY_ROWS, f"partial or wrong group {row}"
    # RG0-3 of the three files whose first key is <= 50 survive; the rest are pruned
    # by the bound's zone-map term
    assert all(row["id"] <= 50 for row in got)


def test_row_group_bound_never_loosens_the_file_bound(row_group_dataset):
    # K large enough that the row-group witnesses cannot beat the file-level answer
    # or cannot prune: whatever fires, every returned group is complete
    session = opteryx.session()
    got = _rows(session, f"SELECT id, COUNT(*) AS c FROM {row_group_dataset} GROUP BY id LIMIT 60")
    assert len(got) == 60
    assert all(r["c"] == RG_KEY_ROWS for r in got)


def test_window_with_a_lower_edge_reads_fewer_files_and_stays_complete():
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    ds_dir = Path("testdata") / "group_limit_key_bound_window"
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    try:
        # witnesses 0,9,100,101,108,109,900,909: the four to take are 100,101,108,109,
        # so [100, 109] touches 2 files where a plain `<= 101` would touch 3
        for n, (lo, hi) in enumerate([(0, 9), (100, 109), (101, 108), (900, 909)]):
            ids = [lo + (g % (hi - lo + 1)) for g in range((hi - lo + 1) * 3)]
            morsel = Morsel.from_vectors(["id"], [vector_from_sequence(ids, DrakenType.INT64)])
            (ds_dir / f"part-{n}.parquet").write_bytes(write_parquet(morsel))
        name = "testdata.group_limit_key_bound_window"
        sql = f"SELECT id, COUNT(*) AS c FROM {name} GROUP BY id LIMIT 4"
        session = opteryx.session()
        got = _rows(session, sql)
        # one row group per file: the window [100, 109] leaves 2 of 4 to read where
        # `<= 101` would leave 3
        assert session.telemetry["operations"][1]["row_groups_read"] == 2
        assert len(got) == 4
        truth = _full_counts(session, name, "id")
        for row in got:
            assert 100 <= row["id"] <= 109
            assert row["c"] == truth[row["id"]], f"partial group {row}"
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)
