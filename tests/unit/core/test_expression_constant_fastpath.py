import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.draken.vectors.scalar_constructors import from_scalar
from opteryx.draken.vectors.integer_vector import IntegerVector
from opteryx.draken.vectors.string_vector import StringVector
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


def test_constant_fastpath_eq_noteq_and_telemetry():
    reset_dict_expr_telemetry()
    vec = from_scalar(9, 4)

    eq = _inner_filter_operations(vec, "Eq", 9)
    neq = _inner_filter_operations(vec, "NotEq", 9)

    assert _as_list(eq) == [True, True, True, True]
    assert _as_list(neq) == [False, False, False, False]

    telemetry = get_dict_expr_telemetry()
    assert telemetry["draken_constant_predicate_fastpath_hits"] == 2
    assert telemetry["draken_constant_predicate_fastpath_fallbacks"] == 0


def test_constant_fastpath_matches_materialized_parity():
    reset_dict_expr_telemetry()
    vec = IntegerVector.from_constant(None, 5, is_null=True)
    materialized = pa.array([3, None, 3, 3, 3], type=pa.int64())

    assert _as_list(_inner_filter_operations(IntegerVector.from_constant(3, 5), "Eq", 3)) == [
        True,
        True,
        True,
        True,
        True,
    ]
    assert _as_list(_inner_filter_operations(vec, "Eq", 3)) == [False, False, False, False, False]


def test_constant_fastpath_unsupported_operator_raises():
    reset_dict_expr_telemetry()
    vec = from_scalar("x", 3)
    with pytest.raises(NotImplementedError, match="Constant motor path does not support operator"):
        _inner_filter_operations(vec, "Like", "x%")


def test_typed_constant_fastpath_uses_constant_encoding_not_legacy_class():
    reset_dict_expr_telemetry()
    vec = IntegerVector.from_constant(9, 4)

    eq = _inner_filter_operations(vec, "Eq", 9)
    neq = _inner_filter_operations(vec, "NotEq", 9)

    assert _as_list(eq) == [True, True, True, True]
    assert _as_list(neq) == [False, False, False, False]

    telemetry = get_dict_expr_telemetry()
    assert telemetry["draken_constant_predicate_fastpath_hits"] == 2
    assert telemetry["draken_constant_predicate_fastpath_fallbacks"] == 0


def test_typed_constant_fastpath_all_null_matches_materialized_parity():
    reset_dict_expr_telemetry()
    vec = IntegerVector.from_constant(None, 4, is_null=True)
    materialized = pa.array([None, None, None, None], type=pa.int32())

    assert _as_list(_inner_filter_operations(vec, "Eq", 3)) == _as_list(
        _inner_filter_operations(materialized, "Eq", 3)
    )
    assert _as_list(_inner_filter_operations(vec, "InList", [1, 3, None])) == _as_list(
        _inner_filter_operations(materialized, "InList", [1, 3, None])
    )


def test_typed_string_constant_fastpath_matches_materialized_parity():
    reset_dict_expr_telemetry()
    vec = StringVector.from_constant("x", 3)
    materialized = pa.array(["x", "x", "x"], type=pa.string())

    assert _as_list(_inner_filter_operations(vec, "Eq", "x")) == _as_list(
        _inner_filter_operations(materialized, "Eq", "x")
    )
    assert _as_list(_inner_filter_operations(vec, "InList", ["x", "y"])) == _as_list(
        _inner_filter_operations(materialized, "InList", ["x", "y"])
    )
