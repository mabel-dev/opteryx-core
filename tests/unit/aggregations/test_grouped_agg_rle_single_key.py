"""
Verify the RLE-encoded single-key fast path in GroupHashEngine produces
results matching the materialized (dense) path for the same logical input.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

from draken.morsels.morsel import Morsel
from draken.vectors.string_vector import StringVector, _test_make_rle_string
from opteryx.operators._operators import (
    AggregationSpec,
    GroupHashEngine,
    create_collectors,
)


def _rle_morsel(values, run_lengths, name=b"col"):
    v = _test_make_rle_string(values, run_lengths)
    return Morsel.from_vectors([name], [v])


def _dense_morsel(values, run_lengths, name=b"col"):
    expanded = []
    for val, rl in zip(values, run_lengths):
        expanded.extend([val] * rl)
    unique = list({v: None for v in expanded}.keys())
    code_of = {v: i for i, v in enumerate(unique)}
    codes = [code_of[v] for v in expanded]
    v = StringVector.from_dict(codes, unique).materialize()
    return Morsel.from_vectors([name], [v])


def _run(group_columns, agg_specs, morsels):
    collectors, _ = create_collectors(agg_specs, group_columns)
    engine = GroupHashEngine(group_columns, collectors, use_parvi=False)
    for m in morsels:
        engine.ingest(m)
    return list(engine.finalize_morsels(65536))


def _to_dict(out, key=b"col", count=b"cnt"):
    r = {}
    for m in out:
        keys = m.column(key).to_pylist()
        cnts = m.column(count).to_pylist()
        for k, c in zip(keys, cnts):
            r[k] = r.get(k, 0) + c
    return r


@pytest.fixture
def count_spec():
    return [AggregationSpec(alias="cnt", function="count", column=None)]


class TestSingleMorsel:
    def test_basic(self, count_spec):
        rle_out = _run([b"col"], count_spec, [_rle_morsel(["a", "b", "c"], [3, 2, 4])])
        dense_out = _run(
            [b"col"], count_spec, [_dense_morsel(["a", "b", "c"], [3, 2, 4])]
        )
        assert _to_dict(rle_out) == _to_dict(dense_out) == {b"a": 3, b"b": 2, b"c": 4}

    def test_repeated_value_in_separate_runs(self, count_spec):
        # Two non-adjacent runs of 'a' must collapse to one group.
        rle_out = _run(
            [b"col"], count_spec, [_rle_morsel(["a", "b", "a", "c"], [2, 1, 3, 4])]
        )
        assert _to_dict(rle_out) == {b"a": 5, b"b": 1, b"c": 4}

    def test_zero_length_runs_excluded(self, count_spec):
        # An empty run must not introduce a group.
        rle_out = _run(
            [b"col"], count_spec, [_rle_morsel(["x", "y", "z"], [0, 4, 3])]
        )
        assert _to_dict(rle_out) == {b"y": 4, b"z": 3}

    def test_single_run(self, count_spec):
        rle_out = _run([b"col"], count_spec, [_rle_morsel(["solo"], [7])])
        assert _to_dict(rle_out) == {b"solo": 7}


class TestMultiMorsel:
    def test_disjoint_runs(self, count_spec):
        m1 = _rle_morsel(["alpha"], [3])
        m2 = _rle_morsel(["beta", "gamma"], [2, 4])
        out = _run([b"col"], count_spec, [m1, m2])
        assert _to_dict(out) == {b"alpha": 3, b"beta": 2, b"gamma": 4}

    def test_overlapping_runs_across_morsels(self, count_spec):
        m1 = _rle_morsel(["shared", "left"], [2, 3])
        m2 = _rle_morsel(["right", "shared"], [4, 1])
        out = _run([b"col"], count_spec, [m1, m2])
        assert _to_dict(out) == {b"shared": 3, b"left": 3, b"right": 4}

    def test_rle_and_dense_match(self, count_spec):
        # Encoding-mix sanity check: hash of RLE run value must equal
        # hash of the same dense value.
        m_rle = _rle_morsel(["shared", "alpha"], [2, 3])
        m_dense = _dense_morsel(["shared", "beta"], [2, 3])
        out = _run([b"col"], count_spec, [m_rle, m_dense])
        assert _to_dict(out) == {b"shared": 4, b"alpha": 3, b"beta": 3}


class TestMultipleAggregates:
    def test_count_plus_count_distinct(self):
        m = _rle_morsel(["a", "b", "a"], [3, 2, 1])
        specs = [
            AggregationSpec(alias="cnt", function="count", column=None),
            AggregationSpec(alias="cd", function="count_distinct", column=b"col"),
        ]
        out = _run([b"col"], specs, [m])

        result = {}
        for ms in out:
            keys = ms.column(b"col").to_pylist()
            cnt = ms.column(b"cnt").to_pylist()
            cd = ms.column(b"cd").to_pylist()
            for k, n, d in zip(keys, cnt, cd):
                result[k] = (n, d)
        # 'a' has 4 rows total, all the same value → 1 distinct.
        assert result == {b"a": (4, 1), b"b": (2, 1)}
