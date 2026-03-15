import os
import sys
from types import SimpleNamespace

import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.vector_ops import vector_iif
from opteryx.expression.evaluator import apply_bounded_function
from opteryx.draken.vectors.float64_vector import Float64Vector
from opteryx.draken.vectors.string_vector import StringVector
from opteryx.draken.vectors.vector import Vector


def test_vector_iif_treats_null_condition_as_false():
    result = vector_iif(
        pyarrow.array([True, False, None], type=pyarrow.bool_()),
        pyarrow.array([1, 1, 1], type=pyarrow.int64()),
        pyarrow.array([2, 2, 2], type=pyarrow.int64()),
    )

    assert result.to_pylist() == [1, 2, 2]


def test_vector_iif_broadcasts_scalar_branch_values():
    result = vector_iif(
        pyarrow.array([True, False, True], type=pyarrow.bool_()),
        "yes",
        "no",
    )

    assert result.to_pylist() == [b"yes", b"no", b"yes"]


def test_vector_iif_returns_draken_vector_for_fixed_width_family():
    result = vector_iif(
        Vector.from_arrow(pyarrow.array([True, False, None], type=pyarrow.bool_())),
        Vector.from_arrow(pyarrow.array([1.5, 2.5, 3.5], type=pyarrow.float64())),
        9.0,
    )

    assert isinstance(result, Float64Vector)
    assert result.to_pylist() == [1.5, 9.0, 9.0]


def test_vector_iif_returns_draken_vector_for_string_family():
    result = vector_iif(
        Vector.from_arrow(pyarrow.array([True, False, True], type=pyarrow.bool_())),
        b"left",
        b"right",
    )

    assert isinstance(result, StringVector)
    assert result.to_pylist() == [b"left", b"right", b"left"]


def test_vector_iif_rejects_mixed_branch_types():
    with pytest.raises(TypeError, match="vector_iif only supports Draken"):
        vector_iif(
            pyarrow.array([True, False, None], type=pyarrow.bool_()),
            pyarrow.array([1, 2, 3], type=pyarrow.int64()),
            "other",
        )


def test_apply_bounded_function_keeps_iif_on_draken_path():
    node = SimpleNamespace(
        value="IIF",
        function_ref=SimpleNamespace(
            selected_overload=SimpleNamespace(
                kernel=SimpleNamespace(
                    callable_ref=vector_iif,
                    engine="draken",
                    null_policy="bypass",
                )
            )
        ),
    )

    result = apply_bounded_function(
        node,
        Vector.from_arrow(pyarrow.array([True, False, None], type=pyarrow.bool_())),
        Vector.from_arrow(pyarrow.array([1.5, 2.5, 3.5], type=pyarrow.float64())),
        9.0,
    )

    assert isinstance(result, Float64Vector)
    assert result.to_pylist() == [1.5, 9.0, 9.0]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
