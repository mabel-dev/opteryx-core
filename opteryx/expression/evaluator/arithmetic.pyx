"""Binary arithmetic operations."""

import datetime

from libc.stdint cimport int16_t

import draken.draken_native as _draken_native
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.utils.vector_types import VectorType, get_vector_type
from opteryx.compiled.expression.compiled_expression cimport (
    BCBinaryOpCode,
    BC_TYPE_NONE,
    BC_TYPE_DATE,
    BC_TYPE_TIMESTAMP,
    BOP_PLUS,
    BOP_MINUS,
    BOP_MULTIPLY,
    BOP_DIVIDE,
    BOP_MODULO,
    BOP_INT_DIVIDE,
    BOP_STRING_CONCAT,
    BOP_BITWISE_OR,
    BOP_BITWISE_AND,
    BOP_BITWISE_XOR,
    BOP_SHIFT_LEFT,
    BOP_SHIFT_RIGHT,
)


cdef _unwrap_nb(v):
    """Extract nanobind Vector from Cython shim or return as-is if already nanobind."""
    nb = getattr(v, "_nb", None)
    return nb if nb is not None else v


cdef object _build_arithmetic_closure(int op_code):
    """Build a closure for numeric arithmetic ops.

    Type promotion (matching the binder's declared result types and SQL semantics):
      * Divide ("/") is TRUE division — integer operands are promoted to FLOAT64
        so int / int yields a float (e.g. 7 / 2 = 3.5), not truncated.
      * MyIntegerDivide ("DIV") is the integer variant — operands are left as-is so
        int DIV int truncates toward zero (the old "/" behaviour, preserved).
      * Mixed int × float (any op) promotes the integer side to FLOAT64 so cross-type
        arithmetic (7.0 / 2, 7 + 2.0, …) works instead of erroring.
    DECIMAL operands are left untouched — the native kernel intercepts and handles
    decimal arithmetic with proper scale before this promotion would apply.
    """
    cdef str method_name
    cdef bint is_true_divide = False
    if op_code == BOP_PLUS:
        method_name = "add"
    elif op_code == BOP_MINUS:
        method_name = "sub"
    elif op_code == BOP_MULTIPLY:
        method_name = "mul"
    elif op_code == BOP_DIVIDE:
        method_name = "div"
        is_true_divide = True
    elif op_code == BOP_MODULO:
        method_name = "mod"
    elif op_code == BOP_INT_DIVIDE:
        # MyIntegerDivide / "DIV" — integer (truncating) division: native `div`
        # on integer operands already truncates toward zero; no float promotion.
        method_name = "div"
    else:
        raise ValueError(f"_build_arithmetic_closure: unexpected op_code {op_code}")

    _int_types = (
        _draken_native.INT8, _draken_native.INT16, _draken_native.INT32, _draken_native.INT64,
    )
    _float_types = (_draken_native.FLOAT32, _draken_native.FLOAT64)

    def kernel(left, right):
        left_nb = _unwrap_nb(left)
        right_nb = _unwrap_nb(right)
        lt = getattr(left_nb, "type", None)
        rt = getattr(right_nb, "type", None)

        # DECIMAL/DECIMAL128 has its own kernel dispatch (dec_mul / decimal_*).
        # NOTE: _draken_native.DECIMAL is LogicalKind.DECIMAL (overwritten by
        # export_values); use DrakenType.DECIMAL explicitly to match vec.type.
        _has_decimal = (lt in (_draken_native.DrakenType.DECIMAL, _draken_native.DrakenType.DECIMAL128)
                        or rt in (_draken_native.DrakenType.DECIMAL, _draken_native.DrakenType.DECIMAL128))

        # DECIMAL × FLOAT: the decimal kernel rejects float operands. The operator
        # map says the result is FLOAT64, so promote the decimal side to float64
        # and let standard float arithmetic handle it.
        if _has_decimal:
            if lt in _float_types:
                _cast = getattr(right_nb, "to_float64", None)
                if _cast is not None:
                    right_nb = _cast()
                    rt = _draken_native.FLOAT64
                    _has_decimal = False
            elif rt in _float_types:
                _cast = getattr(left_nb, "to_float64", None)
                if _cast is not None:
                    left_nb = _cast()
                    lt = _draken_native.FLOAT64
                    _has_decimal = False

        # TRUE division: promote integer operands to FLOAT64 (int / int -> float).
        if is_true_divide and not _has_decimal:
            if lt in _int_types:
                _cast = getattr(left_nb, "to_float64", None)
                if _cast is not None:
                    left_nb = _cast()
                    lt = _draken_native.FLOAT64
            if rt in _int_types:
                _cast = getattr(right_nb, "to_float64", None)
                if _cast is not None:
                    right_nb = _cast()
                    rt = _draken_native.FLOAT64

        # Cross-type int × float: promote the integer side to FLOAT64.
        if not _has_decimal and lt in _int_types and rt in _float_types:
            _cast = getattr(left_nb, "to_float64", None)
            if _cast is not None:
                left_nb = _cast()
        elif rt in _int_types and lt in _float_types:
            _cast = getattr(right_nb, "to_float64", None)
            if _cast is not None:
                right_nb = _cast()

        method = getattr(left_nb, method_name, None)
        if method is not None:
            return method(right_nb)
        method = getattr(right_nb, method_name, None)
        if method is not None:
            return method(left_nb)
        return None
    return kernel


cdef object _build_string_concat_refusal():
    """StringConcat has NO Python kernel. This returns one that refuses.

    The Python concat closure that used to live here was a SECOND, COERCING
    implementation of `||`: it stringified whatever it was handed, so it accepted
    operand pairs the native kernel refuses and answered differently from it. That
    divergence was the root of two defects — with a VARBINARY operand it had no
    arm at all and fell through to `str(v)`, putting a Vector's Python repr, heap
    address and all, into user-visible data. Deleted 2026-08-09; see
    RATIFIED/string-concatenation-requires-homogeneous-string-types.

    It is unreachable by construction now: every StringConcat that survives
    binding is homogeneous or NULL-paired, and `_c_native_binop` admits exactly
    those, so the executor always takes the C-native branch. Measured, not
    assumed — wrapping the kernel this function returns recorded ZERO invocations
    across `make q`, tests/sql and 639 fuzz cases.

    A refusing kernel rather than nothing, because callers still ASK for one:
    compiled_expression.pyx resolves a kernel for every binop before it knows
    whether the slot is C-native, and the executor's C-native branch is guarded on
    both operands being real DrakenVector pointers — a NULL there falls through to
    `callable_ref`. Returning None would put a NULL in that slot and segfault the
    fallthrough. So the slot holds something that fails loudly instead: if that
    path is ever reached, it is a bug in the plan, and the one thing it must not
    do is quietly compute a different answer in Python.
    """
    def kernel(left, right):
        raise NotImplementedError(
            "StringConcat has no Python kernel: `||` is C-native for every operand "
            "pair the binder admits (same-type VARCHAR, NVARCHAR or VARBINARY, or a "
            "NULL operand). Reaching this means a StringConcat slot bypassed the "
            "C-native path — a plan bug, not an operand the engine should coerce."
        )
    return kernel


cdef object _build_bitwise_closure(int op_code):
    """Build closure for bitwise ops on INTEGER operands.

    The bare nanobind kernels (vector_bitwise_*) require nanobind Vectors —
    they call draken_vector_unwrap which rejects Cython shims. The executor
    passes whatever was in anchor[sp] (typically a Cython Vector), so we
    must unwrap here, not return the bare kernel.
    """
    from opteryx.compiled.nanobind.vectors import (
        vector_bitwise_or as _vector_bitwise_or,
        vector_bitwise_and as _vector_bitwise_and,
        vector_bitwise_xor as _vector_bitwise_xor,
        vector_bitwise_shift_left as _vector_bitwise_shift_left,
        vector_bitwise_shift_right as _vector_bitwise_shift_right,
    )

    cdef object _native_kernel
    if op_code == BOP_BITWISE_OR:
        _native_kernel = _vector_bitwise_or
    elif op_code == BOP_BITWISE_AND:
        _native_kernel = _vector_bitwise_and
    elif op_code == BOP_BITWISE_XOR:
        _native_kernel = _vector_bitwise_xor
    elif op_code == BOP_SHIFT_LEFT:
        _native_kernel = _vector_bitwise_shift_left
    elif op_code == BOP_SHIFT_RIGHT:
        _native_kernel = _vector_bitwise_shift_right
    else:
        raise ValueError(f"_build_bitwise_closure: unexpected op_code {op_code}")

    def kernel(left, right, _k=_native_kernel):
        return _k(_unwrap_nb(left), _unwrap_nb(right))
    return kernel


def resolve_binary_op(int op_code, left_sql, right_sql):
    """Bind-time resolver: return a callable for binary_op(left_vector, right_vector).

    Returns a callable with signature: (left_vector, right_vector) → Draken Vector.
    Raises NotImplementedError if (op_code, left_sql, right_sql) is unsupported.
    """
    from opteryx.types.logical_type import LogicalCategory
    from opteryx.utils.vector_types import VectorType

    # left_sql/right_sql are ColumnType objects; dispatch keys off the operator
    # category (LogicalCategory). ColumnType != LogicalCategory directly, so the
    # dispatch category MUST be taken via `.category` — comparing a ColumnType to
    # a LogicalCategory member is always False.
    left_cat = left_sql.category if left_sql is not None else None
    right_cat = right_sql.category if right_sql is not None else None

    # Temporal arithmetic (date/timestamp ± interval, date − date, interval ±
    # interval) MUST be tested before the generic numeric arithmetic branch —
    # BOP_PLUS/BOP_MINUS are in that tuple, so a date/interval operation would
    # otherwise be mis-routed to numeric arithmetic (std::bad_cast at runtime).
    if op_code in (BOP_PLUS, BOP_MINUS):
        from opteryx.expression.evaluator.temporal_ops import (
            _date_interval_op_draken,
            _date_minus_date_draken,
            _interval_interval_op_draken,
        )
        left_is_date = left_cat in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP)
        right_is_interval = right_cat == LogicalCategory.INTERVAL
        left_is_interval = left_cat == LogicalCategory.INTERVAL
        right_is_date = right_cat in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP)

        # date/timestamp ± interval (operands may arrive in either order) → TIMESTAMP
        if (left_is_date and right_is_interval) or (left_is_interval and right_is_date):
            def _date_interval_kernel(left, right, op_code=op_code):
                return _date_interval_op_draken(left, right, "Plus" if op_code == BOP_PLUS else "Minus")
            return _date_interval_kernel

        # date/timestamp − date/timestamp (Minus only) → INTERVAL
        if op_code == BOP_MINUS and left_is_date and right_is_date:
            return _date_minus_date_draken

        # interval ± interval → INTERVAL
        if left_is_interval and right_is_interval:
            op_str = "Plus" if op_code == BOP_PLUS else "Minus"
            def _interval_interval_kernel(left, right, op_str=op_str):
                return _interval_interval_op_draken(left, right, op_str)
            return _interval_interval_kernel

    # Arithmetic ops (Plus, Minus, Multiply, Divide, Modulo, IntegerDivide)
    if op_code in (BOP_PLUS, BOP_MINUS, BOP_MULTIPLY, BOP_DIVIDE, BOP_MODULO, BOP_INT_DIVIDE):
        # All numeric type combinations supported — deferred type checking to kernel
        return _build_arithmetic_closure(op_code)

    # String concatenation — C-native only; this kernel exists to refuse.
    if op_code == BOP_STRING_CONCAT:
        return _build_string_concat_refusal()

    # Bitwise ops on INTEGER
    if op_code in (BOP_BITWISE_OR, BOP_BITWISE_AND, BOP_BITWISE_XOR, BOP_SHIFT_LEFT, BOP_SHIFT_RIGHT):
        return _build_bitwise_closure(op_code)

    raise NotImplementedError(
        f"resolve_binary_op: no kernel for op_code={op_code}, left_sql={left_sql}, right_sql={right_sql}"
    )

