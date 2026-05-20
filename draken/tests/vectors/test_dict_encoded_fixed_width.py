"""Regression net for dict-encoded fixed-width vectors through C1-C5 consumers.

These tests prove the unified-format contract holds for dict-encoded INT64 and
FLOAT64 inputs. They exist because dict-encoded fixed-width vectors are not
exercised by any other test today — a kernel could silently regress to a
dense-only fast path and `make q` would still pass. This file is that gap.

BoolVector and Integer8/16/32 are not covered because they lack public
`from_dict` constructors. If those constructors are added later, extend this
file to mirror the INT64/FLOAT64 cases.

C1 (_helper_select._sel_fixed_family) is not directly callable from Python.
Coverage is indirect: every test below that passes a dict-encoded fixed-width
vector through COALESCE or IIF exercises the helper's dispatch path.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest
import pyarrow as pa

from draken import Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.integer64_vector import Integer64Vector
from draken.morsels.morsel import Morsel
from opteryx.compiled.vector_ops import vector_coalesce, vector_iif


def _to_pylist(vec):
    return vec.to_pylist()


def assert_vectors_equal(a, b):
    assert len(a) == len(b), f"length mismatch: {len(a)} vs {len(b)}"
    assert _to_pylist(a) == _to_pylist(b), (
        f"value mismatch:\n  a={_to_pylist(a)}\n  b={_to_pylist(b)}"
    )


# ---------------------------------------------------------------------------
# C2 — vector_coalesce
# ---------------------------------------------------------------------------


def test_coalesce_dict_int64_with_nulls_and_dense_fallback():
    dict_vec = Integer64Vector.from_dict(
        [0, 1, 2, 1, 0], [10, 20, 30], row_validity=[1, 0, 1, 1, 0]
    )
    fallback = Vector.from_arrow(pa.array([99, 99, 99, 99, 99], type=pa.int64()))

    result = vector_coalesce(dict_vec, fallback)

    dense_vec = Vector.from_arrow(pa.array([10, None, 30, 20, None], type=pa.int64()))
    expected = vector_coalesce(dense_vec, fallback)

    assert_vectors_equal(result, expected)


def test_coalesce_dict_float64_with_nulls_and_dense_fallback():
    dict_vec = Float64Vector.from_dict(
        [0, 1, 2, 1, 0], [1.5, 2.5, 3.5], row_validity=[1, 0, 1, 1, 0]
    )
    fallback = Vector.from_arrow(pa.array([9.9, 9.9, 9.9, 9.9, 9.9], type=pa.float64()))

    result = vector_coalesce(dict_vec, fallback)

    dense_vec = Vector.from_arrow(pa.array([1.5, None, 3.5, 2.5, None], type=pa.float64()))
    expected = vector_coalesce(dense_vec, fallback)

    assert_vectors_equal(result, expected)


def test_coalesce_three_args_mixed_layouts_int64():
    # Three-arg coalesce: dict-encoded, dict-encoded, dense — all same type.
    dict_a = Integer64Vector.from_dict(
        [0, 1, 2, 1, 0], [10, 20, 30], row_validity=[1, 0, 0, 1, 0]
    )
    dict_b = Integer64Vector.from_dict(
        [0, 0, 1, 0, 1], [50, 60], row_validity=[0, 1, 0, 0, 1]
    )
    dense_c = Vector.from_arrow(pa.array([99, 99, 99, 99, 99], type=pa.int64()))

    result = vector_coalesce(dict_a, dict_b, dense_c)

    dense_a = Vector.from_arrow(pa.array([10, None, None, 20, None], type=pa.int64()))
    dense_b = Vector.from_arrow(pa.array([None, 50, None, None, 60], type=pa.int64()))
    expected = vector_coalesce(dense_a, dense_b, dense_c)

    assert_vectors_equal(result, expected)


# ---------------------------------------------------------------------------
# C3 — vector_iif (fixed-width kernel)
# ---------------------------------------------------------------------------


def test_iif_dict_int64_true_branch_dense_false_branch():
    condition = Vector.from_arrow(pa.array([True, False, True, False, True], type=pa.bool_()))
    true_dict = Integer64Vector.from_dict([0, 1, 2, 1, 0], [10, 20, 30])
    false_dense = Vector.from_arrow(pa.array([100, 200, 300, 400, 500], type=pa.int64()))

    result = vector_iif(condition, true_dict, false_dense)

    true_dense = Vector.from_arrow(pa.array([10, 20, 30, 20, 10], type=pa.int64()))
    expected = vector_iif(condition, true_dense, false_dense)

    assert_vectors_equal(result, expected)


def test_iif_dict_int64_both_branches_dict():
    condition = Vector.from_arrow(pa.array([True, False, True, False, True], type=pa.bool_()))
    true_dict = Integer64Vector.from_dict([0, 1, 2, 1, 0], [10, 20, 30])
    false_dict = Integer64Vector.from_dict([0, 1, 2, 1, 0], [100, 200, 300])

    result = vector_iif(condition, true_dict, false_dict)

    true_dense = Vector.from_arrow(pa.array([10, 20, 30, 20, 10], type=pa.int64()))
    false_dense = Vector.from_arrow(pa.array([100, 200, 300, 200, 100], type=pa.int64()))
    expected = vector_iif(condition, true_dense, false_dense)

    assert_vectors_equal(result, expected)


def test_iif_dict_float64_both_branches_dict():
    condition = Vector.from_arrow(pa.array([True, False, True, False, True], type=pa.bool_()))
    true_dict = Float64Vector.from_dict([0, 1, 2, 1, 0], [1.5, 2.5, 3.5])
    false_dict = Float64Vector.from_dict([0, 1, 2, 1, 0], [10.0, 20.0, 30.0])

    result = vector_iif(condition, true_dict, false_dict)

    true_dense = Vector.from_arrow(pa.array([1.5, 2.5, 3.5, 2.5, 1.5], type=pa.float64()))
    false_dense = Vector.from_arrow(pa.array([10.0, 20.0, 30.0, 20.0, 10.0], type=pa.float64()))
    expected = vector_iif(condition, true_dense, false_dense)

    assert_vectors_equal(result, expected)


# ---------------------------------------------------------------------------
# C4 — CSV and JSON row writers
# ---------------------------------------------------------------------------

cio = pytest.importorskip("opteryx.compiled.io")

if not hasattr(cio, "morsel_to_csv_strings"):
    pytest.skip("morsel_to_csv_strings not available", allow_module_level=True)

if not hasattr(cio, "morsel_to_json_strings"):
    pytest.skip("morsel_to_json_strings not available", allow_module_level=True)

from opteryx.compiled.io import morsel_to_csv_strings, morsel_to_json_strings


def test_csv_writer_dict_int64_matches_dense():
    dict_vec = Integer64Vector.from_dict([0, 1, 2, 1, 0], [10, 20, 30])
    dense_vec = Vector.from_arrow(pa.array([10, 20, 30, 20, 10], type=pa.int64()))

    rows_dict = morsel_to_csv_strings(Morsel.from_vectors(["x"], [dict_vec]))
    rows_dense = morsel_to_csv_strings(Morsel.from_vectors(["x"], [dense_vec]))

    assert rows_dict == rows_dense


def test_csv_writer_dict_float64_matches_dense():
    dict_vec = Float64Vector.from_dict([0, 1, 2, 1, 0], [1.5, 2.5, 3.5])
    dense_vec = Vector.from_arrow(pa.array([1.5, 2.5, 3.5, 2.5, 1.5], type=pa.float64()))

    rows_dict = morsel_to_csv_strings(Morsel.from_vectors(["x"], [dict_vec]))
    rows_dense = morsel_to_csv_strings(Morsel.from_vectors(["x"], [dense_vec]))

    assert rows_dict == rows_dense


def test_json_writer_dict_int64_matches_dense():
    dict_vec = Integer64Vector.from_dict([0, 1, 2, 1, 0], [10, 20, 30])
    dense_vec = Vector.from_arrow(pa.array([10, 20, 30, 20, 10], type=pa.int64()))

    rows_dict = morsel_to_json_strings(Morsel.from_vectors(["x"], [dict_vec]))
    rows_dense = morsel_to_json_strings(Morsel.from_vectors(["x"], [dense_vec]))

    assert rows_dict == rows_dense


def test_json_writer_dict_float64_matches_dense():
    dict_vec = Float64Vector.from_dict([0, 1, 2, 1, 0], [1.5, 2.5, 3.5])
    dense_vec = Vector.from_arrow(pa.array([1.5, 2.5, 3.5, 2.5, 1.5], type=pa.float64()))

    rows_dict = morsel_to_json_strings(Morsel.from_vectors(["x"], [dict_vec]))
    rows_dense = morsel_to_json_strings(Morsel.from_vectors(["x"], [dense_vec]))

    assert rows_dict == rows_dense


# ---------------------------------------------------------------------------
# Null handling — one representative test per kernel
# ---------------------------------------------------------------------------


def test_null_handling_coalesce_dict_int64_fills_nulls():
    # row_validity=[1,1,0,1,0] → rows 2 and 4 are null; fallback supplies 99.
    dict_vec = Integer64Vector.from_dict(
        [0, 1, 2, 1, 0], [10, 20, 30], row_validity=[1, 1, 0, 1, 0]
    )
    fallback = Vector.from_arrow(pa.array([99, 99, 99, 99, 99], type=pa.int64()))

    result = vector_coalesce(dict_vec, fallback)

    assert result.to_pylist() == [10, 20, 99, 20, 99]


def test_null_handling_iif_dict_int64_passes_nulls_through():
    # IIF selects the true branch for rows where condition is True;
    # nulls in the selected branch propagate to the output.
    condition = Vector.from_arrow(pa.array([True, True, False, False, True], type=pa.bool_()))
    true_dict = Integer64Vector.from_dict(
        [0, 1, 2, 1, 0], [10, 20, 30], row_validity=[1, 0, 1, 1, 0]
    )
    false_branch = Vector.from_arrow(pa.array([100, 200, 300, 400, 500], type=pa.int64()))

    result = vector_iif(condition, true_dict, false_branch)

    dense_true = Vector.from_arrow(pa.array([10, None, 30, 20, None], type=pa.int64()))
    expected = vector_iif(condition, dense_true, false_branch)

    assert_vectors_equal(result, expected)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
