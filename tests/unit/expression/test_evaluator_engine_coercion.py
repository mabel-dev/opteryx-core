from types import SimpleNamespace

import pytest

from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.draken.vectors.vector import Vector
from opteryx.expression.evaluator import apply_bounded_function


def _make_node(kernel):
    return SimpleNamespace(
        value="TEST",
        function_ref=SimpleNamespace(
            selected_overload=SimpleNamespace(kernel=kernel),
        ),
    )


def test_engine_draken_preserves_draken_vector():
    seen = {}

    def kernel(arg):
        assert isinstance(arg, Vector)
        seen["arg"] = arg
        return 2

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="draken", null_policy="passthru"))
    arg = vector_from_sequence([1, 2, 3])

    result = apply_bounded_function(node, arg)
    assert result == 2
    assert isinstance(seen["arg"], Vector)


def test_engine_arrow_is_rejected():
    def kernel(arg):
        return arg

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="arrow", null_policy="compress"))
    arg = vector_from_sequence([1, 2, 3])

    with pytest.raises(Exception, match="Expected: 'draken'"):
        apply_bounded_function(node, arg)


def test_engine_draken_rejects_non_draken_column_data():
    def kernel(arg):
        return arg

    node = _make_node(SimpleNamespace(callable_ref=kernel, engine="draken", null_policy="passthru"))

    with pytest.raises(Exception, match="non-Draken column data"):
        apply_bounded_function(node, [1, 2, 3])
