"""Draken comparison operations.

Explicit comparison dispatch for all native Draken vector types.

Dispatch strategy: by underlying data type (STRING, INT64, FLOAT64, TIMESTAMP, etc.).
Encoding (constant, dictionary, dense) is handled inside each type's compare kernels
via the unified DrakenVector view.
"""

import datetime

from draken.vectors.bool_vector import BoolVector
from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
)
from opteryx.compiled.nanobind.vectors import (
    vector_allop_eq,
    vector_allop_neq,
    vector_anyop_eq,
    vector_anyop_neq,
    vector_anyop_gt,
    vector_anyop_lt,
    vector_anyop_gte,
    vector_anyop_lte,
)
from opteryx.compiled.nanobind.vectors import vector_contains
from libc.stdint cimport int16_t

from opteryx.compiled.nanobind.vectors import vector_in_list
from opteryx.compiled.nanobind.vectors import vector_ipv4_in_cidr
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar
# Note: _json_at_arrow, _json_array_contains_all, _json_at_question,
# _coerce_date32, _coerce_float, _coerce_int64, _coerce_timestamp are textually
# included via __init__.pyx (json_ops.pyx, type_coercion.pyx) before this file.


cdef inline object _nb_vec_unwrap(object v):
    """Unwrap Cython Vector shim → raw nanobind Vector, or return as-is.

    Note: This function is kept for tree-walker and other non-executor code paths.
    Bytecode executor BC_COMPARE paths use inlined typed casts instead (Phase 1).
    """
    nb = getattr(v, "_nb", None)
    return nb if nb is not None else v


cdef inline object _wrap_nb_bool_result(object result):
    """Wrap a nanobind BOOL Vector in BoolVector; pass through BoolVector unchanged.

    Note: This function is kept for tree-walker and other non-executor code paths.
    Bytecode executor BC_COMPARE paths wrap unconditionally (Phase 1).
    """
    if isinstance(result, BoolVector):
        return result
    return BoolVector(result)


# ---------------------------------------------------------------------------
# DecimalVector op translation.
# DecimalVector uses its own op numbering (EQ=0,NEQ=1,LT=2,LTE=3,GT=4,GTE=5)
# which differs from Draken standard (Eq=0,Ne=1,Gt=2,Ge=3,Lt=4,Le=5).
# Index by our OP_* code (0–6); only 1–6 are valid inputs.
# ---------------------------------------------------------------------------
cdef int _DECIMAL_CMP_OP[7]
_DECIMAL_CMP_OP[0] = -1  # OP_UNKNOWN
_DECIMAL_CMP_OP[1] =  0  # OP_EQ    → Decimal EQ
_DECIMAL_CMP_OP[2] =  1  # OP_NOT_EQ→ Decimal NEQ
_DECIMAL_CMP_OP[3] =  2  # OP_LT    → Decimal LT
_DECIMAL_CMP_OP[4] =  4  # OP_GT    → Decimal GT
_DECIMAL_CMP_OP[5] =  3  # OP_LT_EQ → Decimal LTE
_DECIMAL_CMP_OP[6] =  5  # OP_GT_EQ → Decimal GTE


# ---------------------------------------------------------------------------
# Scalar-typed compare helpers
# ---------------------------------------------------------------------------

cdef _int64_compare(int op_code, vec, right):
    """Int64-vs-anything compare. Translates our op_code to Draken's
    internal op numbering once, then calls the integer-dispatched kernel
    directly via the (now cpdef) `_compare_scalar` / `_compare_vector` /
    `_compare_float64_vector` entries on Integer64Vector. No per-op
    named-method round-trip.
    """
    cdef int draken_op
    cdef object vec_nb

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        if op_code == OP_IN_LIST:
            # Unwrap Cython Vector shim to nanobind Vector if needed.
            if isinstance(vec, Vector):
                vec_nb = (<Vector>vec)._nb
            else:
                vec_nb = vec  # scalar
            return BoolVector(vector_in_list(vec_nb, right))
        raise NotImplementedError(f"Integer64Vector: set op (code {op_code}) not supported")

    draken_op = _DRAKEN_CMP_OP[op_code]
    if draken_op < 0:
        raise NotImplementedError(
            f"Integer64Vector: op_code {op_code} has no Draken compare kernel"
        )

    right_type = get_vector_type(right)
    if right_type in (VectorType.INT64, VectorType.INTEGER):
        return vec._compare_vector(right, draken_op)

    if right_type == VectorType.FLOAT64:
        return vec._compare_float64_vector(right, draken_op)

    value = _coerce_int64(right)
    return vec._compare_scalar(value, draken_op)


cdef _float64_compare(int op_code, vec, right):
    """Float64-vs-anything compare. Translates our op_code to Draken's
    internal op numbering once and calls the integer-dispatched kernel
    directly via the (now cpdef) `_compare_scalar` / `_compare_vector`
    entries on Float64Vector. Float64-vs-Int64 dispatches on the Int64
    side with a flipped op so the operand order matches the kernel.
    """
    cdef int draken_op
    cdef int draken_op_flipped
    cdef object vec_nb

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        if op_code == OP_IN_LIST:
            # Unwrap Cython Vector shim to nanobind Vector if needed.
            if isinstance(vec, Vector):
                vec_nb = (<Vector>vec)._nb
            else:
                vec_nb = vec  # scalar
            return BoolVector(vector_in_list(vec_nb, right))
        raise NotImplementedError(f"Float64Vector: set op (code {op_code}) not supported")

    draken_op = _DRAKEN_CMP_OP[op_code]
    if draken_op < 0:
        raise NotImplementedError(
            f"Float64Vector: op_code {op_code} has no Draken compare kernel"
        )

    right_type = get_vector_type(right)

    if right_type in (VectorType.INT64,):
        # Float64 OP Int64  ≡  Int64 (flipped-OP) Float64.
        draken_op_flipped = _DRAKEN_CMP_OP_FLIPPED[op_code]
        return right._compare_float64_vector(vec, draken_op_flipped)
    if right_type == VectorType.FLOAT64:
        return vec._compare_vector(right, draken_op)

    value = _coerce_float(right)
    return vec._compare_scalar(value, draken_op)


cdef _decimal_compare(int op_code, vec, right):
    if right is None:
        return BoolVector(len(vec))
    if op_code == OP_IN_LIST:
        if isinstance(right, (list, tuple, set, frozenset)):
            return vec.in_list(right)
        raise NotImplementedError(f"DecimalVector InList: expected a set/list, got {type(right)!r}")

    # Standard Draken op convention (Eq0 Ne1 Gt2 Ge3 Lt4 Le5) — matches the
    # scale-aware dec_compare_* kernels. (_DECIMAL_CMP_OP is the legacy kernel's
    # convention and is not used here.)
    cdef int decimal_op = _DRAKEN_CMP_OP[op_code]

    cdef object right_type = get_vector_type(right)

    # DECIMAL vs DECIMAL or DECIMAL vs INT64 vector: scale-aware native compare
    # (the native kernel aligns scales and treats INT64 as a scale-0 decimal).
    if right_type == VectorType.DECIMAL or right_type == VectorType.INT64:
        return vec._compare_vector(right, decimal_op)

    # DECIMAL vs FLOAT64 vector: the literal is a double, so compare in the
    # float64 domain (convert the decimal column to float64 first).
    if right_type == VectorType.FLOAT64:
        vec_float = vec.to_float64_vector()
        return _float64_compare(op_code, vec_float, right)

    # DECIMAL vs Python scalar: native compare_scalar is scale-aware and
    # converts the literal at its own scale.
    if is_scalar(right):
        return vec._compare_scalar(right, decimal_op)

    raise NotImplementedError(
        f"DecimalVector comparison for op (code {op_code}) with right={type(right)!r} not implemented"
    )


cdef _bool_compare(int op_code, left, right):
    cdef object left_nb
    if op_code == OP_IN_LIST:
        # Unwrap Cython Vector shim to nanobind Vector if needed.
        if isinstance(left, Vector):
            left_nb = (<Vector>left)._nb
        else:
            left_nb = left  # scalar
        return BoolVector(vector_in_list(left_nb, right))

    # BOOL has NO native compare kernel — the ops table registers only the BOOL
    # keying hash (ops/hash.h), so draken_compare_scalar / draken_compare_vector
    # throw "unsupported type" for a DRAKEN_BOOL operand. A boolean comparison is
    # computed here from the bit-packed mask, Kleene-correct (null in → null out).
    #
    # `left` is the BOOL column; `right` is the other operand. Bind-time scalar
    # literals are materialised as CONSTANT Vectors (BC_LOAD_LIT_CONST), so a
    # `bool_col = TRUE` predicate (e.g. NULLIF(bool_col, true) → IIF) arrives with
    # `right` a constant BOOL Vector, NOT a Python bool — `bool(right)` on a Vector
    # would read its truthiness (length), which is why the old path was wrong even
    # before the missing kernel. Reduce a scalar/constant right to the bool value;
    # a genuine BOOL column takes the vector path.
    cdef object lnb = (<Vector>left)._nb if isinstance(left, Vector) else left
    cdef BoolVector lcol = BoolVector(lnb)
    cdef BoolVector ncol = lcol.not_vector()

    cdef bint right_is_scalar = False
    cdef object rscalar = None
    if is_scalar(right):
        rscalar = right
        right_is_scalar = True
    elif isinstance(right, Vector):
        if (<Vector>right)._nb.data_length == 1:   # constant shape → a literal
            rscalar = right[0]
            right_is_scalar = True

    if right_is_scalar:
        if rscalar is None:
            # A NULL bool literal: NULL OP anything = NULL (3VL) on every row.
            return BoolVector.from_constant(False, lcol.length, is_null=True)
        s = bool(rscalar)
        # All-true / all-false masks that PRESERVE the column's validity (null rows
        # stay null): (col OR NOT col) is true on every non-null row; (col AND NOT
        # col) is false on every non-null row. (Kleene OR/AND/NOT propagate NULL.)
        if op_code == OP_EQ:
            return lcol if s else ncol
        if op_code == OP_LT:        # col <  s
            return ncol if s else lcol.and_vector(ncol)
        if op_code == OP_LT_EQ:     # col <= s
            return lcol.or_vector(ncol) if s else ncol
        if op_code == OP_GT:        # col >  s
            return lcol.and_vector(ncol) if s else lcol
        if op_code == OP_GT_EQ:     # col >= s
            return lcol if s else lcol.or_vector(ncol)
        raise NotImplementedError(f"_bool_compare: unsupported op code {op_code}")

    # Vector path: BOOL column vs BOOL column. EQ = NOT(a XOR b); the XOR (built
    # from Kleene OR/AND/NOT) propagates NULL so validity = va AND vb.
    cdef BoolVector rcol = <BoolVector>(right if isinstance(right, BoolVector) else BoolVector((<Vector>right)._nb))
    cdef BoolVector xor = lcol.xor_vector(rcol)
    if op_code == OP_EQ:
        return xor.not_vector()
    # Ordering of two boolean columns (false < true): col_a OP col_b expressed via
    # the bit algebra. a<b ⇔ (NOT a) AND b; a>b ⇔ a AND (NOT b).
    if op_code == OP_LT:
        return ncol.and_vector(rcol)
    if op_code == OP_GT:
        return lcol.and_vector(rcol.not_vector())
    if op_code == OP_LT_EQ:        # a <= b ⇔ NOT(a > b)
        return lcol.and_vector(rcol.not_vector()).not_vector()
    if op_code == OP_GT_EQ:        # a >= b ⇔ NOT(a < b)
        return ncol.and_vector(rcol).not_vector()
    raise NotImplementedError(f"_bool_compare: unsupported op code {op_code}")


# ---------------------------------------------------------------------------
# Main dispatch — int-op variant (used by bytecode executor)
# ---------------------------------------------------------------------------

cdef object _anyop_literal_scalar(object left):
    """`literal = ANY(array)` tests a SINGLE literal against each array row; the
    array-reduce kernel takes the raw Python scalar (int/str/bytes/None), not a
    Vector. The bytecode materializes scalar literals as constant Vectors, so
    unwrap element 0. A bare scalar passes through unchanged."""
    if isinstance(left, Vector):
        return (<Vector>left)._nb[0]
    if is_draken_vector(left):
        return left[0]
    return left


cpdef draken_compare_int(int op_code, left, right, int16_t left_schema_type=0, int16_t right_schema_type=0):
    """Same as draken_compare but takes pre-computed integer op_code and BCTypeCode type codes.

    Called by execute_bytecode() for BC_COMPARE instructions where
    slot.op_code != OP_UNKNOWN.  Skips the string→int translation and the
    AnyOp/AllOp/JSON string-dispatch chain that draken_compare() must walk.
    left_schema_type / right_schema_type are BCTypeCode values (BC_TYPE_NONE=0,
    BC_TYPE_DATE=1, BC_TYPE_TIMESTAMP=2) — never Python objects.
    """
    cdef object left_nb, right_nb

    # Direct dispatch for array/set operations before standard ops.
    # These do not use the negation/type-dispatch machinery below.
    if op_code == OP_ANYOP_EQ:
        left_nb = _anyop_literal_scalar(left)
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_eq(literal=left_nb, column=right_nb))
    if op_code == OP_ANYOP_NOT_EQ:
        left_nb = _anyop_literal_scalar(left)
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_neq(literal=left_nb, column=right_nb))
    if op_code == OP_ANYOP_GT:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_gt(left_nb, right_nb))
    if op_code == OP_ANYOP_LT:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_lt(left_nb, right_nb))
    if op_code == OP_ANYOP_GT_EQ:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_gte(left_nb, right_nb))
    if op_code == OP_ANYOP_LT_EQ:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_anyop_lte(left_nb, right_nb))
    if op_code == OP_ALLOP_EQ:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_allop_eq(left_nb, right_nb))
    if op_code == OP_ALLOP_NOT_EQ:
        left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
        right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
        return BoolVector(vector_allop_neq(left_nb, right_nb))
    if op_code == OP_AT_ARROW:
        return _json_at_arrow(left, right)
    if op_code == OP_ARRAY_CONTAINS_ALL:
        return _json_array_contains_all(left, right)
    if op_code == OP_AT_QUESTION:
        return _json_at_question(left, right)
    # IPv4 CIDR containment. `>>=` is the same predicate with the operands the
    # other way round (network on the left), so it reuses the one kernel rather
    # than duplicating the scan — there is no separate "contains" kernel to keep
    # in step.
    if op_code == OP_IP_CONTAINED_BY or op_code == OP_IP_CONTAINS:
        address = left if op_code == OP_IP_CONTAINED_BY else right
        network = right if op_code == OP_IP_CONTAINED_BY else left
        address_nb = (<Vector>address)._nb if isinstance(address, Vector) else address
        network_nb = (<Vector>network)._nb if isinstance(network, Vector) else network
        return BoolVector(vector_ipv4_in_cidr(address_nb, network_nb))
    # LIKE ANY / ILIKE ANY: the constant-pattern case is compiled to the native
    # draken_like_any kernel in compiled_expression.pyx (both scalar and
    # ARRAY<string> subjects), so it never reaches this Python evaluator. The
    # scalar fallback below (native glob, RE2-free) covers a scalar subject that
    # fell out of the c-native path; an ARRAY subject here is unported — fail
    # loud rather than reintroduce a Python/RE2 matcher on the execution path.
    if op_code in (OP_ANYOP_LIKE, OP_ANYOP_NOT_LIKE, OP_ANYOP_ILIKE, OP_ANYOP_NOT_ILIKE):
        if get_vector_type(left) != VectorType.STRING:
            raise NotImplementedError(
                "LIKE ANY over an ARRAY subject must be compiled to draken_like_any "
                "(native); it is not supported on the Python evaluator fallback."
            )
        ignore_case = op_code in (OP_ANYOP_ILIKE, OP_ANYOP_NOT_ILIKE)
        result = _string_anyop_like(left, right, ignore_case=ignore_case)
        if op_code in (OP_ANYOP_NOT_LIKE, OP_ANYOP_NOT_ILIKE):
            return result.not_vector()
        return result

    # Map negated op codes to their positive counterpart and set negate flag.
    # All "Not" variants are: NotEq=2, NotInList=8, NotLike=10, NotILike=12,
    # NotRLike=14, NotInStr=16, NotIInStr=18.
    cdef bint negate = False
    if op_code == OP_NOT_EQ:
        op_code = OP_EQ; negate = True
    elif op_code == OP_NOT_IN_LIST:
        op_code = OP_IN_LIST; negate = True
    elif op_code == OP_NOT_LIKE:
        op_code = OP_LIKE; negate = True
    elif op_code == OP_NOT_ILIKE:
        op_code = OP_ILIKE; negate = True
    elif op_code == OP_NOT_RLIKE:
        op_code = OP_RLIKE; negate = True
    elif op_code == OP_NOT_IN_STR:
        op_code = OP_IN_STR; negate = True
    elif op_code == OP_NOT_I_IN_STR:
        op_code = OP_I_IN_STR; negate = True

    if op_code == OP_IN_LIST:
        if isinstance(right, (list, tuple, set, frozenset)):
            # Unwrap Cython Vector shim to nanobind Vector if needed.
            if isinstance(left, Vector):
                left_nb = (<Vector>left)._nb
            else:
                left_nb = left  # scalar
            return BoolVector(vector_in_list(left_nb, right, negate))

    if is_scalar(left) and is_draken_vector(right):
        flip_ops = {OP_GT: OP_LT, OP_LT: OP_GT, OP_GT_EQ: OP_LT_EQ, OP_LT_EQ: OP_GT_EQ}
        op_code = flip_ops.get(op_code, op_code)
        left, right = right, left

    if right is None and not isinstance(left, (str, int, float, bytes, bool, type(None))):
        return BoolVector(len(left))

    vec_type = get_vector_type(left)

    if vec_type == VectorType.STRING:
        result = _string_compare(op_code, left, right)
    elif vec_type in (VectorType.INT64, VectorType.INTEGER):
        if left_schema_type == BC_TYPE_DATE or left_schema_type == BC_TYPE_TIMESTAMP:
            result = _int64_temporal_compare(op_code, left, right, left_schema_type)
        else:
            result = _int64_compare(op_code, left, right)
    elif vec_type == VectorType.FLOAT64:
        result = _float64_compare(op_code, left, right)
    elif vec_type == VectorType.TIMESTAMP:
        result = _timestamp_compare(op_code, left, right)
    elif vec_type == VectorType.DATE32:
        result = _date32_compare(op_code, left, right)
    elif vec_type == VectorType.INTERVAL:
        result = _interval_compare(op_code, left, right)
    elif vec_type == VectorType.BOOL:
        result = _bool_compare(op_code, left, right)
    elif vec_type == VectorType.DECIMAL:
        result = _decimal_compare(op_code, left, right)
    else:
        raise NotImplementedError(f"draken_compare_int: unsupported vector type {vec_type!r}")

    return result.not_vector() if negate else result
