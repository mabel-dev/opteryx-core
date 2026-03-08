import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.expression.ops import _inner_filter_operations
from opteryx.expression.ops import get_dict_expr_telemetry
from opteryx.expression.ops import reset_dict_expr_telemetry


def _as_list(result):
    to_pylist = getattr(result, "to_pylist", None)
    if to_pylist is not None:
        return to_pylist()
    tolist = getattr(result, "tolist", None)
    if tolist is not None:
        return tolist()
    return list(result)


def _make_dictionary_array(include_null=False):
    dictionary = pa.array(["a", "b", "c"], type=pa.string())
    if include_null:
        indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    else:
        indices = pa.array([0, 1, 2, 1], type=pa.int8())
    return pa.DictionaryArray.from_arrays(indices, dictionary)


def _make_numeric_dictionary_array(include_null=False):
    dictionary = pa.array([10, 20, 30], type=pa.int32())
    if include_null:
        indices = pa.array([0, 1, None, 2, 1], type=pa.int8())
    else:
        indices = pa.array([0, 1, 2, 1], type=pa.int8())
    return pa.DictionaryArray.from_arrays(indices, dictionary)


def _make_cardinality_dictionary_array(cardinality):
    dictionary = pa.array([f"v{i:06d}" for i in range(cardinality)], type=pa.string())
    index_type = pa.int32() if cardinality > 32767 else pa.int16()
    indices = pa.array(list(range(cardinality)), type=index_type)
    return pa.DictionaryArray.from_arrays(indices, dictionary)


def test_dictionary_fastpath_eq_noteq():
    reset_dict_expr_telemetry()
    arr = _make_dictionary_array(include_null=False)
    eq = _inner_filter_operations(arr, "Eq", "b")
    neq = _inner_filter_operations(arr, "NotEq", "b")
    assert eq.__class__.__name__ == "BoolVector"
    assert neq.__class__.__name__ == "BoolVector"
    assert _as_list(eq) == [False, True, False, True]
    assert _as_list(neq) == [True, False, True, False]
    tel = get_dict_expr_telemetry()
    assert tel["draken_dict_expr_fastpath_hits"] == 2
    assert tel["draken_dict_expr_fastpath_fallbacks"] == 0


def test_dictionary_fastpath_matches_materialized_results():
    reset_dict_expr_telemetry()
    arr = _make_dictionary_array(include_null=True)
    materialized = arr.dictionary_decode()

    assert _as_list(_inner_filter_operations(arr, "Eq", "b")) == _as_list(
        _inner_filter_operations(materialized, "Eq", "b")
    )
    assert _as_list(_inner_filter_operations(arr, "InList", ["a", "c", None])) == _as_list(
        _inner_filter_operations(materialized, "InList", ["a", "c", None])
    )
    assert _as_list(_inner_filter_operations(arr, "Like", "a%")) == _as_list(
        _inner_filter_operations(materialized, "Like", "a%")
    )
    assert _as_list(_inner_filter_operations(arr, "ILike", "B%")) == _as_list(
        _inner_filter_operations(materialized, "ILike", "B%")
    )
    assert _as_list(_inner_filter_operations(arr, "RLike", "^c")) == _as_list(
        _inner_filter_operations(materialized, "RLike", "^c")
    )


def test_dictionary_fastpath_numeric_range_ops():
    reset_dict_expr_telemetry()
    arr = _make_numeric_dictionary_array(include_null=True)
    lt_res = _inner_filter_operations(arr, "Lt", 25)
    gt_res = _inner_filter_operations(arr, "Gt", 15)
    lte_res = _inner_filter_operations(arr, "LtEq", 20)
    gte_res = _inner_filter_operations(arr, "GtEq", 20)
    assert _as_list(lt_res) == [True, True, False, False, True]
    assert _as_list(gt_res) == [False, True, False, True, True]
    assert _as_list(lte_res) == [True, True, False, False, True]
    assert _as_list(gte_res) == [False, True, False, True, True]
    tel = get_dict_expr_telemetry()
    assert tel["draken_dict_expr_fastpath_hits"] == 4
    assert tel["draken_dict_expr_fastpath_fallbacks"] == 0


def test_dictionary_unsupported_operator_raises():
    reset_dict_expr_telemetry()
    arr = _make_dictionary_array(include_null=True)
    with pytest.raises(NotImplementedError, match="does not support operator"):
        _inner_filter_operations(arr, "Lt", "b")
    tel = get_dict_expr_telemetry()
    assert tel["draken_dict_expr_fastpath_hits"] == 0
    assert tel["draken_dict_expr_fastpath_fallbacks"] == 0


def test_dictionary_multichunk_raises():
    reset_dict_expr_telemetry()
    dictionary = pa.array(["a", "b"], type=pa.string())
    chunk1 = pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()), dictionary)
    chunk2 = pa.DictionaryArray.from_arrays(pa.array([1, 0], type=pa.int8()), dictionary)
    arr = pa.chunked_array([chunk1, chunk2])
    with pytest.raises(NotImplementedError, match="multi-chunk dictionary arrays"):
        _inner_filter_operations(arr, "Eq", "a")


@pytest.mark.parametrize("cardinality", [64, 1024, 100000])
def test_dictionary_fastpath_string_multi_cardinality_parity(cardinality):
    reset_dict_expr_telemetry()
    arr = _make_cardinality_dictionary_array(cardinality)
    materialized = arr.dictionary_decode()
    probe = f"v{(cardinality // 2):06d}"
    in_list = ["v000000", probe, f"v{(cardinality - 1):06d}"]

    assert _as_list(_inner_filter_operations(arr, "Eq", probe)) == _as_list(
        _inner_filter_operations(materialized, "Eq", probe)
    )
    assert _as_list(_inner_filter_operations(arr, "InList", in_list)) == _as_list(
        _inner_filter_operations(materialized, "InList", in_list)
    )

    telemetry = get_dict_expr_telemetry()
    assert telemetry["draken_dict_expr_fastpath_hits"] == 2
    assert telemetry["draken_dict_expr_fastpath_fallbacks"] == 0
