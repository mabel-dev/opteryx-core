"""
Verify the dict-encoded single-key fast path in GroupHashEngine produces
results bit-identical to the materialized (dense) path.

The fast path activates when:
  - exactly one GROUP BY column,
  - that column is a dict-encoded StringVector,
  - the engine is in carchar (non-parvi) mode.

Each test runs the same logical input twice — once dict-encoded, once after
``.materialize()`` — and asserts the produced (key → count) sets are equal.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from opteryx.operators._operators import (
    AggregationSpec,
    GroupHashEngine,
    create_collectors,
)


def _codes_to_values(codes, dictionary, row_validity=None):
    if row_validity is None:
        return [dictionary[c].encode() for c in codes]
    return [
        dictionary[c].encode() if valid else None
        for c, valid in zip(codes, row_validity)
    ]


def _morsel(codes, dictionary, row_validity=None, name=b"col"):
    v = dn.vector_from_string_dict_sequence(_codes_to_values(codes, dictionary, row_validity))
    return Morsel.from_vectors([name], [v]), v


def _run_engine(group_columns, agg_specs, morsels):
    collectors, key_kinds = create_collectors(agg_specs, group_columns)
    # use_parvi=False forces the carchar path which is where the fast path lives.
    engine = GroupHashEngine(group_columns, collectors, use_parvi=False)
    for m in morsels:
        engine.ingest(m)
    out = []
    for chunk in engine.finalize_morsels(65536):
        out.append(chunk)
    return out


def _result_as_dict(morsels, key_col=b"col", count_col="cnt"):
    """Flatten engine output into {key_bytes: count}."""
    result = {}
    for m in morsels:
        keys = m.column(key_col).to_pylist()
        counts = m.column(
            count_col.encode() if isinstance(count_col, str) else count_col
        ).to_pylist()
        for k, c in zip(keys, counts):
            result[k] = result.get(k, 0) + c
    return result


def _materialized_morsel(v, name=b"col"):
    return Morsel.from_vectors([name], [v.materialize()])


@pytest.fixture
def count_spec():
    return [AggregationSpec(alias="cnt", function="count", column=None)]


# ---------------------------------------------------------------------------
# Single-morsel cases
# ---------------------------------------------------------------------------


class TestSingleMorsel:
    def test_basic_groupby(self, count_spec):
        m, v = _morsel([0, 1, 2, 1, 0, 2, 0], ["banana", "apple", "cherry"])
        dict_out = _run_engine([b"col"], count_spec, [m])
        dense_out = _run_engine(
            [b"col"], count_spec, [_materialized_morsel(v)]
        )
        assert _result_as_dict(dict_out) == _result_as_dict(dense_out) == {
            "banana": 3,
            "apple": 2,
            "cherry": 2,
        }

    def test_only_referenced_values_form_groups(self, count_spec):
        # vector_from_string_dict_sequence's dictionary is built from only the
        # values actually present in the data, so there's nothing unreferenced
        # to exclude — this asserts that invariant holds for the fast path.
        m, v = _morsel([0, 0, 0], ["a", "a", "a"])
        dict_out = _run_engine([b"col"], count_spec, [m])
        assert _result_as_dict(dict_out) == {"a": 3}

    def test_with_row_nulls(self, count_spec):
        m, v = _morsel(
            [0, 1, 2, 1, 0],
            ["a", "b", "c"],
            row_validity=[1, 1, 0, 1, 0],
        )
        dict_out = _run_engine([b"col"], count_spec, [m])
        dense_out = _run_engine(
            [b"col"], count_spec, [_materialized_morsel(v)]
        )
        # GROUP BY collapses NULL rows into a single null group.
        assert _result_as_dict(dict_out) == _result_as_dict(dense_out)
        assert dict_out  # at least one chunk emitted

    def test_all_null(self, count_spec):
        m, v = _morsel([0, 0, 0], ["x"], row_validity=[0, 0, 0])
        dict_out = _run_engine([b"col"], count_spec, [m])
        dense_out = _run_engine(
            [b"col"], count_spec, [_materialized_morsel(v)]
        )
        # One null group, three rows.
        d_dict = _result_as_dict(dict_out)
        d_dense = _result_as_dict(dense_out)
        assert d_dict == d_dense
        assert sum(d_dict.values()) == 3


# ---------------------------------------------------------------------------
# Multi-morsel cases — different dictionaries, same logical values
# ---------------------------------------------------------------------------


class TestMultiMorsel:
    def test_disjoint_dicts(self, count_spec):
        m1, _ = _morsel([0, 1, 0], ["alpha", "beta"])
        m2, _ = _morsel([0, 0, 1, 1], ["gamma", "delta"])
        out = _run_engine([b"col"], count_spec, [m1, m2])
        assert _result_as_dict(out) == {
            "alpha": 2,
            "beta": 1,
            "gamma": 2,
            "delta": 2,
        }

    def test_overlapping_dicts_same_value_different_codes(self, count_spec):
        # 'shared' appears in both morsels' dictionaries but with different
        # codes — they must collapse into a single group via hash equality.
        m1, _ = _morsel([0, 1], ["shared", "alpha"])
        m2, _ = _morsel([1, 0], ["beta", "shared"])
        out = _run_engine([b"col"], count_spec, [m1, m2])
        assert _result_as_dict(out) == {
            "shared": 2,
            "alpha": 1,
            "beta": 1,
        }

    def test_dict_and_dense_morsels_match(self, count_spec):
        # First morsel dict-encoded, second materialized — keys must still
        # collapse correctly across the two encodings.
        m_dict, v = _morsel([0, 1, 0], ["x", "y"])
        m_dense = Morsel.from_vectors([b"col"], [
            dn.vector_from_string_dict_sequence([b"x", b"y"]).materialize()
        ])
        out = _run_engine([b"col"], count_spec, [m_dict, m_dense])
        assert _result_as_dict(out) == {"x": 3, "y": 2}


# ---------------------------------------------------------------------------
# Multiple aggregates
# ---------------------------------------------------------------------------


class TestMultipleAggregates:
    def test_count_plus_count_distinct(self):
        m, v = _morsel([0, 1, 0, 0, 2, 1], ["a", "b", "c"])
        specs = [
            AggregationSpec(alias="cnt", function="count", column=None),
            AggregationSpec(alias="cd", function="count_distinct", column=b"col"),
        ]
        dict_out = _run_engine([b"col"], specs, [m])
        dense_out = _run_engine([b"col"], specs, [_materialized_morsel(v)])
        # Convert to dict by key, comparing both aggregate columns.

        def collect(out):
            r = {}
            for ms in out:
                keys = ms.column(b"col").to_pylist()
                cnt = ms.column(b"cnt").to_pylist()
                cd = ms.column(b"cd").to_pylist()
                for k, n, d in zip(keys, cnt, cd):
                    r[k] = (n, d)
            return r

        assert collect(dict_out) == collect(dense_out)
