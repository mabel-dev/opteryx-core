import os
import sys

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.scalar_constructors import from_scalar as constant_from_scalar
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.vectors.string_vector import StringVector
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
from opteryx.operators.shuffle import AggregationSpec


def _rows_by_key(rows, key_name):
    out = {}
    for row in rows:
        key = row[key_name]
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        out[key] = row
    return out


def test_constant_groupby_telemetry_hit_for_single_group_key_output():
    morsel = Morsel.from_vectors(["k"], [constant_from_scalar("a", 3, dtype=pa.string())])
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )

    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["cnt"] == 3
    assert op.readings["draken_constant_groupby_fastpath_hits"] == 1
    assert op.readings["draken_constant_groupby_fastpath_fallbacks"] == 0
    assert op.readings["draken_constant_groupby_output_vector_hits"] == 1
    assert op.readings["draken_constant_groupby_output_vector_fallbacks"] == 0


def test_constant_groupby_telemetry_non_constant_key_does_not_count_runtime_fastpath():
    morsel = Morsel.from_arrow(pa.table({"k": pa.array(["a", "b", "a"], type=pa.string())}))
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )

    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["cnt"] == 2
    assert rows["b"]["cnt"] == 1
    assert op.readings["draken_constant_groupby_fastpath_hits"] == 0
    assert op.readings["draken_constant_groupby_fastpath_fallbacks"] == 0
    assert op.readings["draken_constant_groupby_output_vector_hits"] == 0
    assert op.readings["draken_constant_groupby_output_vector_fallbacks"] == 1


def test_constant_groupby_telemetry_runtime_hit_for_sum():
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            constant_from_scalar("a", 3, dtype=pa.string()),
            vector_from_sequence([1.0, 2.0, 3.0]),
        ],
    )
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )

    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["sum_v"] == 6.0
    assert op.readings["draken_constant_groupby_fastpath_hits"] == 1
    assert op.readings["draken_constant_groupby_fastpath_fallbacks"] == 0
    assert op.readings["draken_constant_groupby_output_vector_hits"] == 1


def test_constant_groupby_telemetry_runtime_fallback_for_unsupported_agg():
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            constant_from_scalar("a", 3, dtype=pa.string()),
            vector_from_sequence([1.0, 2.0, 3.0]),
        ],
    )
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="h", function="hash_one", column="v")],
    )

    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["h"] is not None
    assert op.readings["draken_constant_groupby_fastpath_hits"] == 1
    assert op.readings["draken_constant_groupby_fastpath_fallbacks"] == 0


def test_constant_groupby_output_vector_telemetry_accepts_typed_constant_encoding():
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )

    op._record_constant_groupby_vector(StringVector.from_constant("a", 1))

    assert op.readings["draken_constant_groupby_output_vector_hits"] == 1
    assert op.readings["draken_constant_groupby_output_vector_fallbacks"] == 0


def test_constant_groupby_output_vector_telemetry_keeps_legacy_constant_support():
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
    )

    op._record_constant_groupby_vector(constant_from_scalar("a", 1, dtype=pa.string()))

    assert op.readings["draken_constant_groupby_output_vector_hits"] == 1
    assert op.readings["draken_constant_groupby_output_vector_fallbacks"] == 0


def test_typed_constant_key_uses_compiled_constant_mode_for_sum():
    morsel = Morsel.from_vectors(
        ["k", "v"],
        [
            StringVector.from_constant("a", 3),
            vector_from_sequence([1.0, 2.0, 3.0]),
        ],
    )
    op = ShuffleGroupByOperationV2(
        group_by_columns=["k"],
        aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
    )

    op.ingest(morsel)
    rows = _rows_by_key(op.finalize().to_arrow().to_pylist(), "k")

    assert rows["a"]["sum_v"] == 6.0
    assert op.readings["feature_groupby_engine_constant"] == 1
    assert op.readings["draken_constant_groupby_fastpath_hits"] == 1
    assert op.readings["draken_constant_groupby_fastpath_fallbacks"] == 0
