import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.aggregations.group_by_engine import CarcharGroupStateEngine
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.int64_vector import Int64Vector


def _rows_by_key(rows, key_name):
    out = {}
    for row in rows:
        key = row[key_name]
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        out[key] = row
    return out


def test_carchar_engine_accepts_typed_constant_key_for_count_star():
    engine = CarcharGroupStateEngine([b"k"], [("cnt", "count", None)])
    morsel = Morsel.from_vectors(["k"], [Int64Vector.from_constant(7, 4)])

    engine.ingest(morsel)
    rows = _rows_by_key(engine.finalize().to_arrow().to_pylist(), "k")

    assert rows[7]["cnt"] == 4
    assert engine.readings["feature_groupby_engine_constant"] == 1
    assert engine.readings["feature_groupby_engine_carchar"] == 0


def test_carchar_engine_uses_typed_constant_value_for_sum_in_constant_mode():
    engine = CarcharGroupStateEngine([b"k"], [("sum_v", "sum", b"v")])
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            Int64Vector.from_constant(7, 4),
            Int64Vector.from_constant(3, 4),
        ],
    )

    engine.ingest(morsel)
    rows = _rows_by_key(engine.finalize().to_arrow().to_pylist(), "k")

    assert rows[7]["sum_v"] == 12
    assert engine.readings["feature_groupby_engine_constant"] == 1


def test_carchar_engine_handles_typed_all_null_constant_value_for_any_value():
    engine = CarcharGroupStateEngine([b"k"], [("h", "any_value", b"v")])
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            Int64Vector.from_constant(7, 3),
            Int64Vector.from_constant(None, 3, is_null=True),
        ],
    )

    engine.ingest(morsel)
    rows = _rows_by_key(engine.finalize().to_arrow().to_pylist(), "k")

    assert rows[7]["h"] is None
    assert engine.readings["feature_groupby_engine_constant"] == 1


def test_carchar_engine_handles_typed_all_null_constant_value_for_count_value():
    engine = CarcharGroupStateEngine([b"k"], [("cnt_v", "count", b"v")])
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            Int64Vector.from_constant(7, 5),
            Int64Vector.from_constant(None, 5, is_null=True),
        ],
    )

    engine.ingest(morsel)
    rows = _rows_by_key(engine.finalize().to_arrow().to_pylist(), "k")

    assert rows[7]["cnt_v"] == 0
    assert engine.readings["feature_groupby_engine_constant"] == 1
