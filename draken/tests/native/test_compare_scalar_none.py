"""compare_scalar rejects a None scalar, for every column type, with a message
that says what is wrong.

None is not a comparison operand: SQL's `x = NULL` is UNKNOWN for every row, so
there is no mask to return. Before this check the literal fell through to the
integer path's nb::cast<int64_t> at the bottom of the binding, and nanobind
reported the cast failure as an ARITY error —

    compare_scalar(): incompatible function arguments. The following argument
    types are supported:
        1. compare_scalar(self, scalar: object, op: int) -> Vector

— a signature that advertises `object` and then refuses NoneType, naming
neither NULL nor the thing to use instead.

The exception stays a TypeError (nb::type_error): it is one, and
test_float/test_fp16/test_bool_compare_scalar already assert that type.
"""

import pytest
from decimal import Decimal

import draken.draken_native as dn

ALL_OPS = [0, 1, 2, 3, 4, 5]


def _vectors():
    """One vector per branch of the compare_scalar dispatch."""
    return {
        "int64": dn.vector_int64_from_sequence([1, 2, 3]),
        "float64": dn.vector_float64_from_sequence([1.0, 2.0]),
        "float32": dn.vector_float32_from_sequence([1.0, 2.0]),
        "string": dn.vector_from_string_sequence([b"a", b"b"]),
        "decimal": dn.vector_decimal_from_sequence(
            [Decimal("1.00"), Decimal("2.00")], precision=10, scale=2),
    }


@pytest.mark.parametrize("name", sorted(_vectors()))
@pytest.mark.parametrize("op", ALL_OPS)
def test_none_scalar_raises_a_named_type_error(name, op):
    vec = _vectors()[name]
    with pytest.raises(TypeError) as exc:
        vec.compare_scalar(None, op)
    message = str(exc.value)
    assert "None" in message
    assert "NULL" in message
    # the old failure was nanobind's arity report, which named no cause
    assert "incompatible function arguments" not in message


def test_message_points_at_the_null_test():
    """The error has to say what to use instead, or it just relocates the
    dead end — these are the two accessors that read the validity bitmap."""
    with pytest.raises(TypeError) as exc:
        dn.vector_int64_from_sequence([1]).compare_scalar(None, 0)
    assert "is_null_mask" in str(exc.value)
    assert "is_not_null_mask" in str(exc.value)


def test_null_typed_column_also_rejects_none():
    """A DRAKEN_NULL column used to answer an all-null mask for ANY scalar,
    None included. The scalar is still meaningless, and the column's type is
    not a reason to accept it — the check runs before the type dispatch."""
    null_vec = dn.vector_null_from_length(3)
    with pytest.raises(TypeError):
        null_vec.compare_scalar(None, 0)
    # a real scalar still gets the 3VL answer: all rows null
    null_vec.compare_scalar(42, 0)
