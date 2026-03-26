import pyarrow as pa
import numpy as np
from types import SimpleNamespace

from opteryx.compiled.draken.vectors.vector import Vector
from opteryx.expression.evaluator import apply_bounded_function


def _make_node(kernel):
    return SimpleNamespace(
        value="TEST",
        function_ref=SimpleNamespace(
            selected_overload=SimpleNamespace(kernel=kernel),
        ),
    )


def test_engine_arrow_coerces_draken_to_arrow():
    seen = {}

    def kernel(arg):
        # Expect that the Draken Vector was converted to a PyArrow Array.
        assert isinstance(arg, pa.Array)
        seen['arg'] = arg
        return 1

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="arrow", null_policy="compress"))

    result = apply_bounded_function(node, Vector.from_arrow(pa.array([1, 2, 3], type=pa.int64())))
    assert result == 1
    assert isinstance(seen['arg'], pa.Array)


def test_engine_draken_preserves_draken_vector():
    seen = {}

    def kernel(arg):
        # This kernel expects a native Draken Vector.
        assert isinstance(arg, Vector)
        seen['arg'] = arg
        return 2

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="draken", null_policy="passthru"))

    result = apply_bounded_function(node, Vector.from_arrow(pa.array([1, 2, 3], type=pa.int64())))
    assert result == 2
    assert isinstance(seen['arg'], Vector)


def test_engine_draken_preserves_draken_vector_from_arrow():
    seen = {}

    def kernel(arg):
        # Draken engine should preserve the Draken vector instead of coercing.
        assert isinstance(arg, Vector)
        seen['arg'] = arg
        return 3

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="draken", null_policy="passthru"))

    result = apply_bounded_function(node, pa.array([1, 2, 3], type=pa.int64()))
    assert result == 3
    assert isinstance(seen['arg'], Vector)
