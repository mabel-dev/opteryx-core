"""Main expression evaluation engine (Cython orchestration layer).

Layering (CLAUDE.md):
- Python   : user-facing API + planner/binder only.
- Cython   : execution orchestration (this file: tree walk + dispatch).
- C++      : execution kernels (Draken vector ops, called from here).

NodeType integer constants are inlined as compile-time DEFs to turn the
dispatch chain into a series of C-level integer compares. They MUST match
the values declared on the NodeType IntEnum in opteryx/expression/__init__.py;
a runtime check in opteryx/expression/evaluator/__init__.py verifies this.
"""

import sys as _sys

from opteryx.compiled.expression.compiled_expression import (
    BC_RESULT_NEEDS_NB_WRAP,
    BC_RESULT_WRAP_AS_BOOL,
    BC_RESULT_NO_DV,
)
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper as _CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet as _PerfectHashSet
from opteryx.compiled.nanobind.vectors import vector_bitwise_not as _vector_bitwise_not
from opteryx.compiled.nanobind.vectors import (
    vector_string_is_empty as _vector_string_is_empty,
    vector_string_is_not_empty as _vector_string_is_not_empty,
)
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError, IncompatibleTypesError
from opteryx.types.logical_type import LogicalCategory as _LogicalCategory, DATE as _CT_DATE, TIMESTAMP as _TIMESTAMP_factory
from opteryx.types.timestamps._datetime_conversion import timestamp_to_int64_us as _ts_to_us
from opteryx.utils.vector_types import VectorType, get_vector_type, is_draken_vector, is_scalar


# Imports from draken are safe at module level — draken does not import opteryx.expression.
from draken.vectors.bool_vector import BoolVector as _BoolVector
from draken.morsels.morsel import Morsel as _Morsel
import draken.draken_native as _draken_native
from opteryx.compiled.nanobind.vectors import vector_uint64_eq_scalar as _vector_uint64_eq_scalar
from draken.draken_native import vector_array_map_access as _vector_array_map_access

# ---------------------------------------------------------------------------
# C-level imports needed by the native bitmap helpers and unary operators.
# Must appear before any cdef/cpdef that uses these types.
# The execute_bytecode section at the bottom of this file repeats some of
# these; duplicates are harmless (Cython deduplicates internally).
# ---------------------------------------------------------------------------
from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libc.stddef cimport size_t
from draken.core.buffers cimport DrakenVector, DRAKEN_SEL_IDENTITY, DRAKEN_NULL
from draken.vectors.bool_vector cimport (
    BoolVector,
    c_and_bitmap,
    c_bitmap_and_inplace,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport simd_popcount
from cpython.object cimport PyObject

# bool_vector_from_bits returns a NEW reference (PyObject*, never `object`, in the
# extern — CLAUDE.md §3); `<object>raw` takes its own reference, this drops the
# bridge's. Cython 3's cpython.ref.Py_DECREF takes `object`, hence the C shim.
cdef extern from *:
    """static inline void _eval_decref(PyObject* op) { Py_DECREF(op); }"""
    void _eval_decref(PyObject* op)

# NodeType integer values — keep in sync with NodeType in opteryx/expression/__init__.py.
DEF NT_UNKNOWN = 0
DEF NT_AND = 17
DEF NT_OR = 18
DEF NT_XOR = 19
DEF NT_NOT = 20
DEF NT_DNF = 21
DEF NT_CNF = 22
DEF NT_CASE = 32
DEF NT_WILDCARD = 33
DEF NT_COMPARISON_OPERATOR = 34
DEF NT_BINARY_OPERATOR = 35
DEF NT_UNARY_OPERATOR = 36
DEF NT_FUNCTION = 37
DEF NT_IDENTIFIER = 38
DEF NT_SUBQUERY = 39
DEF NT_NESTED = 40
DEF NT_AGGREGATOR = 41
DEF NT_LITERAL = 42
DEF NT_EXPRESSION_LIST = 43
DEF NT_EVALUATED = 44
DEF NT_CAST = 45
DEF NT_EXTRACTION_OPERATOR = 46
DEF NT_BETWEEN = 47

# Truth-test op codes for _bv_truth_test_native.
DEF _BV_IS_TRUE = 0
DEF _BV_IS_FALSE = 1
DEF _BV_IS_NOT_TRUE = 2
DEF _BV_IS_NOT_FALSE = 3

# ColumnType instances for temporal coercion — passed to _coerce_temporal_scalar_for_arrow
# to disambiguate DATE vs TIMESTAMP from the BC_TYPE_* int codes on the AnyOp paths.
_CT_TIMESTAMP = _TIMESTAMP_factory()

def _is_scalar_value(obj):
    """Deprecated: use is_scalar() from opteryx.utils.vector_types instead."""
    return is_scalar(obj)


cdef _unary_op_kernel(int op_code, vec):
    """Apply a unary op to a pre-evaluated vector (bytecode executor path).

    op_code is a BCUnaryOpCode integer — no Python string comparison.
    """
    cdef DrakenVector* vec_dv
    cdef BoolVector _tt_bv
    cdef DrakenVector* _tt_dv
    cdef uint32_t _tt_rows
    cdef Py_ssize_t _tt_nbytes
    if op_code == UOP_IS_NULL:
        vec_dv = <DrakenVector*>(<Vector>vec)._dv
        if vec_dv == NULL:
            raise TypeError(f"_unary_op_kernel: IS NULL requires a Vector with valid _dv; got {type(vec).__name__!r}")
        return _is_null_from_dv(vec_dv, 0)
    if op_code == UOP_IS_NOT_NULL:
        vec_dv = <DrakenVector*>(<Vector>vec)._dv
        if vec_dv == NULL:
            raise TypeError(f"_unary_op_kernel: IS NOT NULL requires a Vector with valid _dv; got {type(vec).__name__!r}")
        return _is_null_from_dv(vec_dv, 1)
    if op_code == UOP_IS_EMPTY:
        return _BoolVector(_vector_string_is_empty(_nb_vec_unwrap(vec)))
    if op_code == UOP_IS_NOT_EMPTY:
        return _BoolVector(_vector_string_is_not_empty(_nb_vec_unwrap(vec)))
    if op_code == UOP_BITWISE_NOT:
        return Vector(_vector_bitwise_not(_nb_vec_unwrap(vec)))
    if op_code == UOP_IS_TRUE or op_code == UOP_IS_NOT_FALSE or op_code == UOP_IS_FALSE or op_code == UOP_IS_NOT_TRUE:
        if get_vector_type(vec) != VectorType.BOOL:
            raise TypeError(
                f"IS TRUE/IS FALSE requires a boolean expression; got {vec.__class__.__name__!r}"
            )
        _tt_bv = <BoolVector>vec
        _tt_dv = _tt_bv.unified()
        _tt_rows = _tt_dv.length
        _tt_nbytes = (<Py_ssize_t>_tt_rows + 7) >> 3
        if op_code == UOP_IS_TRUE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_TRUE, _tt_nbytes, _tt_rows)
        if op_code == UOP_IS_NOT_FALSE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_NOT_FALSE, _tt_nbytes, _tt_rows)
        if op_code == UOP_IS_FALSE:
            return _bv_truth_test_native(_tt_bv, _BV_IS_FALSE, _tt_nbytes, _tt_rows)
        return _bv_truth_test_native(_tt_bv, _BV_IS_NOT_TRUE, _tt_nbytes, _tt_rows)
    raise NotImplementedError(f"_unary_op_kernel: unsupported unary op code {op_code!r}")


cdef bint _is_temporal_type(column_type):
    """Check if a ColumnType is DATE or TIMESTAMP."""
    if column_type is None:
        return False
    cdef object cat = column_type.category
    return cat == _LogicalCategory.DATE or cat == _LogicalCategory.TIMESTAMP


cdef _validate_temporal_comparison(left_node, right_node, op):
    """
    Validate that temporal comparisons have literals explicitly cast.

    When comparing temporal and non-temporal operands, literals must be explicitly cast.
    Temporal columns do not require casting. Both operands must have temporal types.
    """
    left_sc = left_node.schema_column
    right_sc = right_node.schema_column
    left_type = left_sc.column_type if left_sc is not None else None
    right_type = right_sc.column_type if right_sc is not None else None

    cdef bint left_is_temporal = _is_temporal_type(left_type)
    cdef bint right_is_temporal = _is_temporal_type(right_type)

    if not (left_is_temporal or right_is_temporal):
        return
    if left_is_temporal and right_is_temporal:
        return

    non_temporal_node = right_node if left_is_temporal else left_node
    non_temporal_side = "right" if left_is_temporal else "left"

    if <int>non_temporal_node.node_type != NT_IDENTIFIER:
        raise IncompatibleTypesError(
            message=f"Temporal comparison requires literals to be explicitly cast to temporal types.\n"
            f"The {non_temporal_side} side is missing an explicit CAST or :: operator.\n\n"
            f"Examples of valid syntax:\n"
            f"  - col {op} literal::DATE\n"
            f"  - col {op} literal::TIMESTAMP[ms]\n"
            f"  - col::TIMESTAMP[ms] {op} literal::DATE\n\n"
            f"Supported temporal types: DATE, TIMESTAMP[ms], TIMESTAMP[us], TIMESTAMP[s], TIMESTAMP[ns], TIMESTAMP[d]"
        )


DEF _HASH_DISPATCH_MIN_ROWS = 1024

_TARGET_HASH_CACHE = {}
_TARGET_HASH_CACHE_MAX = 128




# ---------------------------------------------------------------------------
# Native bitmap helpers — replace Python BoolVector method dispatch
# ---------------------------------------------------------------------------

cdef inline const uint8_t* _bv_bitmap_ptr(
    BoolVector bv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    uint8_t** scratch_out,
) except NULL:
    """Return a dense uint8_t* bitmap for `bv`.

    Dense-identity vectors: returns dv.data directly; *scratch_out = NULL.
    Constant-shape (data_length == 1): expands into a malloc'd buffer; *scratch_out = that buffer.

    Caller must free(*scratch_out) if it is non-NULL.
    Raises NotImplementedError for unexpected encoding shapes (§1: no silent fallback).
    """
    cdef DrakenVector* dv = bv.unified()
    cdef uint8_t fill
    cdef uint8_t* out
    scratch_out[0] = NULL
    # Returning dv.data directly is valid ONLY when selection is identity.
    # data_length == length also admits a PERMUTATION, whose bits sit in physical
    # (not logical) order — returning dv.data would silently reorder them.
    if dv.data_length == dv.length and (dv.flags & DRAKEN_SEL_IDENTITY):
        return <const uint8_t*>dv.data
    if dv.data_length == 1:
        out = <uint8_t*>malloc(<size_t>nbytes)
        if out == NULL:
            raise MemoryError("_bv_bitmap_ptr: malloc failed")
        fill = 0xFF if ((<const uint8_t*>dv.data)[0] & 1u) else 0x00
        memset(out, fill, <size_t>nbytes)
        if num_rows & 7u:
            out[nbytes - 1] = fill & <uint8_t>((1u << (num_rows & 7u)) - 1u)
        scratch_out[0] = out
        return out
    raise NotImplementedError(
        f"_bv_bitmap_ptr: unexpected BoolVector encoding "
        f"data_length={dv.data_length} length={dv.length} (CLAUDE.md §1: no silent fallback)"
    )


cdef BoolVector _bv_op2_native(
    BoolVector lbv,
    BoolVector rbv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    int op,
):
    """Apply a binary boolean bitmap operation with no Python method dispatch.

    op: 0 = AND, 1 = OR, 2 = XOR
    Returns a new dense BoolVector owning its own draken_malloc'd bitmap.
    """
    cdef const uint8_t* l_data
    cdef const uint8_t* r_data
    cdef uint8_t* l_scratch = NULL
    cdef uint8_t* r_scratch = NULL
    cdef uint8_t* out_data
    cdef uint8_t* out_null
    cdef DrakenVector* lv = lbv.unified()
    cdef DrakenVector* rv = rbv.unified()
    cdef bint had_null
    cdef object result_obj

    l_data = _bv_bitmap_ptr(lbv, nbytes, num_rows, &l_scratch)
    r_data = _bv_bitmap_ptr(rbv, nbytes, num_rows, &r_scratch)

    out_data = <uint8_t*>malloc(<size_t>nbytes)
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_data == NULL or out_null == NULL:
        if l_scratch != NULL: free(l_scratch)
        if r_scratch != NULL: free(r_scratch)
        free(out_data)
        free(out_null)
        raise MemoryError("_bv_op2_native: malloc failed")

    if op == 0:
        had_null = c_and_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)
    elif op == 1:
        had_null = c_or_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)
    else:
        had_null = c_xor_bitmap(out_data, out_null, l_data, lv.validity, r_data, rv.validity, <size_t>nbytes, num_rows)

    cdef PyObject* raw
    try:
        raw = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if l_scratch != NULL: free(l_scratch)
        if r_scratch != NULL: free(r_scratch)

    result_obj = <object>raw
    _eval_decref(raw)
    # bool_vector_from_bits returns a nanobind Vector (not a cdef BoolVector);
    # wrap in _BoolVector so callers get a proper typed BoolVector instance.
    return _BoolVector(result_obj)


cdef BoolVector _bv_not_native(
    BoolVector bv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
):
    """Apply NOT to a BoolVector with no Python method dispatch."""
    cdef const uint8_t* src_data
    cdef uint8_t* src_scratch = NULL
    cdef uint8_t* out_data
    cdef uint8_t* out_null
    cdef DrakenVector* dv
    cdef bint had_null
    cdef object result_obj

    dv = bv.unified()
    src_data = _bv_bitmap_ptr(bv, nbytes, num_rows, &src_scratch)

    out_data = <uint8_t*>malloc(<size_t>nbytes)
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_data == NULL or out_null == NULL:
        if src_scratch != NULL: free(src_scratch)
        free(out_data)
        free(out_null)
        raise MemoryError("_bv_not_native: malloc failed")

    had_null = c_not_bitmap(out_data, out_null, src_data, dv.validity, <size_t>nbytes, num_rows)

    cdef PyObject* raw
    try:
        raw = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if src_scratch != NULL: free(src_scratch)

    result_obj = <object>raw
    _eval_decref(raw)
    # bool_vector_from_bits returns a nanobind Vector (not a cdef BoolVector);
    # wrap in _BoolVector so callers get a proper typed BoolVector instance.
    return _BoolVector(result_obj)


cdef inline bint _bv_any_native(BoolVector bv, Py_ssize_t nbytes) except -1:
    """Return True if the BoolVector has at least one True bit (ignoring nulls)."""
    cdef DrakenVector* dv = bv.unified()
    if dv.data_length == 1:
        return bool((<const uint8_t*>dv.data)[0] & 1u)
    return simd_popcount(<uint8_t*>dv.data, <size_t>nbytes) > 0


cdef inline bint _bv_all_native(
    BoolVector bv, Py_ssize_t nbytes, uint32_t num_rows,
) except -1:
    """Return True if all bits are True and there are no nulls."""
    cdef DrakenVector* dv = bv.unified()
    if dv.validity != NULL:
        return False  # has nulls — not all-true
    if dv.data_length == 1:
        return bool((<const uint8_t*>dv.data)[0] & 1u)
    return <uint32_t>simd_popcount(<uint8_t*>dv.data, <size_t>nbytes) == num_rows


cdef inline void _fill_is_null_bits(
    const DrakenVector* dv, bint is_null, uint8_t* out, Py_ssize_t nbytes, uint32_t num_rows,
) noexcept nogil:
    """Write IS NULL (is_null=1) / IS NOT NULL (is_null=0) for `dv` into `out`
    (`nbytes` bytes, bit-packed, tail bits beyond `num_rows` cleared).

    The validity mask is indexed by LOGICAL row, so no selection hop: IS NOT NULL
    is the validity bitmap, IS NULL its complement — a byte-wide copy/invert.

    DRAKEN_NULL is checked FIRST: that type tag is self-describing (every row is
    null, with no data and no validity buffer allocated), so its absent validity
    means all-null — the opposite of the all-valid reading an absent validity
    buffer carries for every other type. An untyped `NULL` literal materialises
    exactly this vector, and reading its (absent) validity as all-valid made
    `NULL IS NULL` answer False."""
    cdef const uint8_t* validity = dv.validity
    cdef Py_ssize_t k
    if dv.type == DRAKEN_NULL:
        memset(out, 0xFF if is_null else 0x00, <size_t>nbytes)
    elif validity == NULL:
        memset(out, 0x00 if is_null else 0xFF, <size_t>nbytes)
    elif is_null:
        for k in range(nbytes):
            out[k] = <uint8_t>~validity[k]
    else:
        memcpy(out, validity, <size_t>nbytes)
    if num_rows & 7u:
        out[nbytes - 1] &= <uint8_t>((1u << (num_rows & 7u)) - 1u)


cdef BoolVector _is_null_from_dv(DrakenVector* dv, bint negate) noexcept:
    """Produce a BoolVector of IS NULL (negate=0) / IS NOT NULL (negate=1) from a
    DrakenVector's validity bitmap — see _fill_is_null_bits."""
    cdef uint32_t num_rows = dv.length
    cdef Py_ssize_t nbytes = (<Py_ssize_t>num_rows + 7) >> 3
    cdef uint8_t* out_data = <uint8_t*>malloc(<size_t>nbytes)
    cdef object result_obj

    if out_data == NULL:
        raise MemoryError("_is_null_from_dv: malloc failed")

    cdef PyObject* raw
    try:
        _fill_is_null_bits(dv, not negate, out_data, nbytes, num_rows)
        # Result has no nulls — IS NULL/NOT NULL always yields a definite answer
        raw = bool_vector_from_bits(out_data, NULL, num_rows)
    finally:
        free(out_data)

    result_obj = <object>raw
    _eval_decref(raw)
    return _BoolVector(result_obj)


cdef BoolVector _bv_truth_test_native(
    BoolVector bv, int op, Py_ssize_t nbytes, uint32_t num_rows,
):
    """Apply IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE with no Python dispatch.

    SQL three-value logic (validity bitmap: 1=valid, 0=null):
      IS TRUE      : data & validity
      IS FALSE     : ~data & validity
      IS NOT TRUE  : ~data | ~validity
      IS NOT FALSE : data | ~validity
    Result is always null-free (IS TRUE/FALSE always yield a definite boolean).
    """
    cdef DrakenVector* dv = bv.unified()
    cdef const uint8_t* data
    cdef uint8_t* scratch = NULL
    cdef const uint8_t* validity = dv.validity
    cdef uint8_t* out_data = <uint8_t*>malloc(<size_t>nbytes)
    cdef object result_obj
    cdef PyObject* raw
    cdef Py_ssize_t k
    cdef uint8_t tail_mask

    if out_data == NULL:
        raise MemoryError("_bv_truth_test_native: malloc failed")

    data = _bv_bitmap_ptr(bv, nbytes, num_rows, &scratch)

    try:
        if validity == NULL:
            # No nulls: IS TRUE == IS NOT FALSE == data;
            #            IS FALSE == IS NOT TRUE == ~data
            if op == _BV_IS_TRUE or op == _BV_IS_NOT_FALSE:
                for k in range(nbytes):
                    out_data[k] = data[k]
            else:
                for k in range(nbytes):
                    out_data[k] = ~data[k]
        else:
            if op == _BV_IS_TRUE:
                for k in range(nbytes):
                    out_data[k] = data[k] & validity[k]
            elif op == _BV_IS_FALSE:
                for k in range(nbytes):
                    out_data[k] = (~data[k]) & validity[k]
            elif op == _BV_IS_NOT_TRUE:
                for k in range(nbytes):
                    out_data[k] = (~data[k]) | (~validity[k])
            else:  # _BV_IS_NOT_FALSE
                for k in range(nbytes):
                    out_data[k] = data[k] | (~validity[k])

        # Mask tail bits beyond num_rows
        if num_rows & 7u:
            tail_mask = <uint8_t>((1u << (num_rows & 7u)) - 1u)
            out_data[nbytes - 1] &= tail_mask

        # Result has no nulls — IS TRUE/FALSE always yields a definite answer
        raw = bool_vector_from_bits(out_data, NULL, num_rows)
    finally:
        free(out_data)
        if scratch != NULL:
            free(scratch)

    result_obj = <object>raw
    _eval_decref(raw)
    return _BoolVector(result_obj)


cpdef execute_and_append(list compiled_evals, morsel):
    """Execute pre-compiled (identity, CompiledBytecode) pairs and append results.

    Successor to the tree-walker evaluate_and_append_draken.  Filtering
    (should_evaluate) and ordering (prioritize_evaluation) must
    have been applied at bind time by compile_eval_nodes().

    The identity-already-present check is still performed at runtime because
    upstream operators may have materialised the column before this call.
    """
    cdef set existing = None
    cdef list col_names = None
    cdef list col_vecs = None
    cdef bint appended = False

    if not compiled_evals:
        return morsel

    for entry in compiled_evals:
        identity = entry[0]

        if existing is None:
            existing = set()
            for _n in morsel.column_names:
                if isinstance(_n, bytes):
                    existing.add(_n.decode())
                else:
                    existing.add(_n)

        if identity in existing:
            continue

        if col_names is None:
            col_names = list(morsel.column_names)
            col_vecs = []
            for _n in col_names:
                if isinstance(_n, bytes):
                    col_vecs.append(morsel._cxx_column(_n))
                else:
                    col_vecs.append(morsel._cxx_column(_n.encode()))

        result = execute_bytecode(entry[1], morsel)
        col_names.append(identity)
        col_vecs.append(result)
        existing.add(identity)
        appended = True

    if not appended:
        return morsel

    # Preserve the input's representation: a Cxx-backed input stays on the
    # substrate (cursor is the sole shim); a PyObject input stays PyObject.
    if morsel._cxx is not None:
        return _Morsel.from_cxx_vectors(col_names, col_vecs)
    return _Morsel.from_vectors(col_names, col_vecs)


# ---------------------------------------------------------------------------
# Bytecode VM executor
#
# execute_bytecode() consumes the flat postfix instruction list produced by
# build_bytecode() at bind time.  It maintains a small operand stack of
# Draken vectors and dispatches on CompiledInstruction.node_type using a
# chain of C-level integer compares (Cython optimize.use_switch folds these
# into a switch statement in the generated C).
#
# Each node pops `arity` vectors and pushes one result.
# ---------------------------------------------------------------------------

from opteryx.compiled.expression.compiled_expression cimport (
    BC_AND,
    BC_BINARY_OP,
    BC_CASE,
    BC_CAST,
    BC_CMP_LEFT_TEMPORAL,
    BC_CMP_RIGHT_TEMPORAL,
    BC_CMP_INLIST_INLINE,
    BC_CNF,
    BC_COMPARE,
    BC_DNF,
    BC_EXTRACTION,
    BC_FUNCTION,
    BC_LAZY,
    BC_C_NATIVE_CHILD,
    BC_INSTR_C_NATIVE,
    BC_LOAD_COL,
    BC_LOAD_LIT_BOOL,
    BC_LOAD_LIT_CONST,
    BC_LOAD_LIT_SCALAR,
    BC_LOAD_LIT_SET,
    BC_NOT,
    BC_OR,
    BC_UNARY_OP,
    BC_XOR,
    BytecodeInstr,
    CompiledBytecode,
    # Type codes
    BC_TYPE_NONE, BC_TYPE_DATE, BC_TYPE_TIMESTAMP,
    # Binary op codes
    BOP_UNKNOWN, BOP_PLUS, BOP_MINUS, BOP_MULTIPLY, BOP_DIVIDE,
    BOP_MODULO, BOP_INT_DIVIDE, BOP_STRING_CONCAT,
    BOP_BITWISE_OR, BOP_BITWISE_AND, BOP_BITWISE_XOR,
    BOP_SHIFT_LEFT, BOP_SHIFT_RIGHT,
    # Unary op codes
    UOP_UNKNOWN, UOP_IS_NULL, UOP_IS_NOT_NULL, UOP_IS_EMPTY,
    UOP_IS_NOT_EMPTY, UOP_BITWISE_NOT,
    UOP_IS_TRUE, UOP_IS_NOT_FALSE, UOP_IS_FALSE, UOP_IS_NOT_TRUE,
    # Extraction op codes — only arr[i] is discriminated here; every other
    # sub-op runs its bind-time-resolved kernel without the VM naming it.
    BC_EXTR_MAP_ARRAY,
)
from libc.stdint cimport uint8_t, int8_t, int16_t, int32_t, int64_t, uintptr_t, uint32_t

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_BOOL, DRAKEN_NULL, DRAKEN_ARRAY, DRAKEN_VECTOR_FP16, draken_vector_from_dense
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_UINT8, DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY, DRAKEN_VARIANT
from draken.core.buffers cimport DRAKEN_DECIMAL, DRAKEN_DECIMAL128, DRAKEN_TIMESTAMP64
from draken.core.buffers cimport DRAKEN_INTERVAL
from draken.core.buffers cimport DRAKEN_FLOAT32, DRAKEN_FLOAT64, DRAKEN_DATE32
from draken.core.buffers cimport DRAKEN_TIME32, DRAKEN_TIME64, DRAKEN_SEL_PERMUTATION
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot
from draken.core.buffers cimport str_length, str_is_inline, str_data, str_clone_with_offset
from draken.core.buffers cimport draken_zero_sel, draken_zero_validity, draken_identity_sel
from draken.core.buffers cimport DRAKEN_DICT_CODES_DENSE
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

from draken.morsels.morsel cimport Morsel, cxx_to_morsel
from draken.morsels.cxx_morsel cimport CxxMorsel, cxx_mask_c, cxx_mask_with_consts_c, cxx_column_child_vec
from libcpp.memory cimport shared_ptr
from draken.vectors.bool_vector cimport (
    BoolVector,
    from_decoded,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport Vector, simd_popcount, from_decoded as vec_from_decoded
from draken.vectors.vector cimport from_decoded_with_arena as vec_from_decoded_with_arena
from draken.core.frame_arena cimport (
    DrakenFrameArena,
    draken_frame_arena_create,
    draken_frame_arena_destroy,
    draken_frame_arena_alloc,
    draken_frame_arena_release,
    draken_frame_arena_adopt,
    draken_frame_arena_contains,
)
from draken.ops.compare_dv cimport draken_compare_dv
from draken.ops.arithmetic_dv cimport draken_arithmetic_dv

# Phase 9c: C kernel ABI — function-pointer signatures for binary ops, casts, extractions
# VecResult itself is declared in _impl.pxd (this file's own .pxd, implicitly
# visible here) — NOT redeclared in this .pyx. A second `cdef extern from
# "ops/vec_result.h"` block here previously conflicted with the .pxd's and
# silently degraded every `VecResult` use in this file to an untyped Python
# object (Cython keeps whichever cdef-extern-type declaration it sees first).

# Kleene boolean ops for the VM — value-aware 3VL via draken::ops::bool_*, exposed
# as C-ABI shims in bitmap_ops.cpp (linked into draken_native.so, RTLD_GLOBAL). These
# replace the value-blind c_*_bitmap null merge and safely accept a DRAKEN_NULL operand.
cdef extern from "core/bitmap_ops.h" nogil:
    VecResult draken_vm_bool_binop(int op, const DrakenVector* a, const DrakenVector* b,
                                   uint32_t num_rows) nogil
    VecResult draken_vm_bool_not(const DrakenVector* a, uint32_t num_rows) nogil
    VecResult draken_vm_bool_truth_test(int op, const DrakenVector* a, uint32_t num_rows) nogil

# LAZY branch evaluation (see draken/core/lazy_region.h): which rows a guard admits,
# narrowing a loaded column to them, and scattering the branch result back to full
# length (excluded rows NULL). All return owned VecResults for the VM to adopt.
cdef extern from "core/lazy_region.h" nogil:
    uint32_t draken_lz_rows(int kind, const DrakenVector* const* guards,
                            uint32_t nguards, uint32_t n, uint32_t* out_rows)
    VecResult draken_lz_narrow(const DrakenVector* v, const uint32_t* rows, uint32_t k)
    VecResult draken_lz_scatter(const DrakenVector* compact, const uint32_t* rows,
                                uint32_t k, uint32_t n)

# Function-pointer typedefs per Decision 3 (Phase 9 design, §Post-design)
# `noexcept` is load-bearing: these point at C kernels that cannot raise, and
# without it Cython wraps every call in __Pyx_ErrOccurredWithGIL() — a GIL take
# per kernel call inside the nogil VM, which deadlocks the in-worker scan
# prefilter against a scan close that holds the GIL.
ctypedef VecResult (*binop_fn_t)(void* ctx, const DrakenVector* left, const DrakenVector* right) noexcept nogil
ctypedef VecResult (*cast_fn_t)(void* ctx, const DrakenVector* v) noexcept nogil
# ARRAY->VARCHAR (BC_C_NATIVE_CHILD): parent + owner-held child element vector.
ctypedef VecResult (*cast_child_fn_t)(void* ctx, const DrakenVector* parent,
                                      const DrakenVector* child) noexcept nogil
ctypedef VecResult (*extr_fn_t)(void* ctx, const DrakenVector* v, const DrakenVector* key) noexcept nogil
ctypedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs) noexcept nogil
ctypedef VecResult (*case_fn_t)(void* ctx, void* morsel) noexcept nogil

# VecResult → Python Vector (VectorOwner) trampoline. Declared returning `object`
# so Cython manages the new reference; honors validity_embedded + ts_unit, which a
# bare arena DV* cannot carry (string consolidated block / timestamp unit descriptor).
cdef extern from "vectors/_vector_bridge.h":
    object draken_vecresult_own_c(VecResult res)

cdef extern from "core/draken_capi.h":
    void draken_vecresult_discard_c(VecResult* res) noexcept nogil


# ---------------------------------------------------------------------------
# C-callable interface — worker item and global function pointer.
# Declared extern here; the global and setter are defined in bytecode_worker.cpp.
# ---------------------------------------------------------------------------

cdef extern from "bytecode_worker.h" nogil:
    ctypedef struct BytecodeWorkerItem:
        const void*  instrs
        size_t       n_instrs
        const void*  col_cache
        uint8_t**    bitmaps
        uint8_t**    null_bitmaps
        int8_t*      slot_has_null
        size_t       n_slots
        size_t       nbytes
        size_t       n_rows
        int          error_code

    ctypedef int (*opteryx_worker_fn_t)(BytecodeWorkerItem*)
    opteryx_worker_fn_t opteryx_worker_fn
    void opteryx_set_worker_fn(opteryx_worker_fn_t fn)


# ---------------------------------------------------------------------------
# Bitmap VM — three-phase GIL-free predicate evaluation
#
# Phase 1 (_execute_bytecode_prepass): GIL held.
#   Resolves BC_LOAD_COL columns; mallocs scratch bitmap buffers.
# Phase 2 (c_execute_bytecode_inner): noexcept nogil.
#   Operates entirely on uint8_t* scratch bitmaps; no Python objects.
# Phase 3 (_execute_bytecode_postpass): GIL held.
#   Wraps the result bitmap into a BoolVector for Python callers.
#
# Only runs when bc.is_pure_bitmap is True — bytecodes containing only
# BC_LOAD_LIT_BOOL, BC_LOAD_COL (BoolVector columns), and boolean
# combinators (AND/OR/XOR/NOT/DNF/CNF).
# ---------------------------------------------------------------------------

ctypedef struct ColCache:
    uint8_t*        data       # ptr to BoolVector bitmap data (unified view)
    uint8_t*        null_bm    # ptr to validity bitmap (NULL = no nulls)
    const uint32_t* sel        # per-logical-row selection into `data`
    bint            is_bool    # True if the column resolved to a BoolVector


cdef int _execute_bytecode_prepass(
    CompiledBytecode bc,
    Morsel morsel,
    Py_ssize_t num_rows,
    ColCache* col_cache,
    uint8_t** bitmaps,
    uint8_t** null_bitmaps,
    int8_t* slot_has_null,
    Py_ssize_t n_slots,
    Py_ssize_t nbytes,
    list anchors,
) except? -2:
    """GIL-held pre-pass: resolve columns and malloc scratch bitmap buffers.

    Returns -1 when a BC_LOAD_COL column is not a BoolVector (caller must fall
    back to execute_bytecode); returns 0 on success.  -1 is a *valid* return
    value, NOT an error sentinel — the declared error sentinel is -2 with the
    `except?` form, so Cython disambiguates a real exception (MemoryError) from
    the -1 fall-back signal by checking PyErr_Occurred().  Using `except -1`
    here is a bug: it makes Cython treat the legitimate -1 fall-back return as
    a raised exception and propagate a non-existent one (SIGSEGV).
    """
    cdef Py_ssize_t j, k
    cdef BytecodeInstr* slot
    cdef Vector v
    cdef BoolVector bv
    cdef uint8_t* p
    cdef DrakenVector* uv

    # Allocate n_slots + 2 bitmap buffers:
    #   [0 .. n_slots-1] = stack slots
    #   [n_slots]        = primary scratch for binary ops
    #   [n_slots+1]      = secondary scratch for DNF/CNF fold
    #
    # Slot 0 is the result slot: allocated with draken_malloc so ownership can
    # be transferred to draken_vector_own_raw (via from_decoded) in the postpass.
    # All other slots are scratch and stay on libc malloc.
    for j in range(n_slots + 2):
        if j == 0:
            p = <uint8_t*>draken_malloc(nbytes)
        else:
            p = <uint8_t*>malloc(nbytes)
        if p == NULL:
            raise MemoryError("evaluate_bitmap: failed to allocate bitmap buffer")
        memset(p, 0, nbytes)
        bitmaps[j] = p

        if j == 0:
            p = <uint8_t*>draken_malloc(nbytes)
        else:
            p = <uint8_t*>malloc(nbytes)
        if p == NULL:
            raise MemoryError("evaluate_bitmap: failed to allocate null bitmap buffer")
        memset(p, 0, nbytes)
        null_bitmaps[j] = p

        slot_has_null[j] = 0

    # Resolve BC_LOAD_COL instructions
    for k in range(bc.count):
        slot = &bc.instrs[k]
        if slot.opcode != BC_LOAD_COL:
            col_cache[k].is_bool = False
            continue

        v = morsel._cxx_column(<bytes>slot.column_identity, <bytes>slot.column_name)
        if not isinstance(v, BoolVector):
            return -1  # not a BoolVector — caller must fall back

        bv = <BoolVector>v
        anchors.append(bv)  # keep alive during inner loop
        uv = bv.unified()
        col_cache[k].is_bool = True
        col_cache[k].data = <uint8_t*>uv.data
        col_cache[k].null_bm = uv.validity
        col_cache[k].sel = uv.selection

    return 0


cdef int c_execute_bytecode_inner(
    BytecodeInstr* instrs,
    Py_ssize_t n_instrs,
    ColCache* col_cache,
    uint8_t** bitmaps,
    uint8_t** null_bitmaps,
    int8_t* slot_has_null,
    Py_ssize_t n_slots,
    Py_ssize_t nbytes,
    Py_ssize_t num_rows,
) noexcept nogil:
    """Nogil VM inner loop for pure-bitmap bytecodes.

    Operates entirely on pre-allocated uint8_t* scratch buffers — no Python
    objects, no GIL. Stack slots are indices into the bitmaps/null_bitmaps
    arrays. Binary ops write to bitmaps[n_slots] (scratch) then swap pointers.

    Returns 0 on success, 1 if an unexpected opcode is encountered.
    """
    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i, j, base, arity
    cdef int opcode
    cdef BytecodeInstr* slot
    cdef uint8_t* tmp_ptr
    cdef bint had_null
    cdef Py_ssize_t scratch0 = n_slots
    cdef Py_ssize_t scratch1 = n_slots + 1
    cdef Py_ssize_t popcount_val

    for i in range(n_instrs):
        slot = &instrs[i]
        opcode = slot.opcode

        # ------------------------------------------------------------------
        # BC_LOAD_LIT_BOOL — fill bitmap slot with constant pattern
        # ------------------------------------------------------------------
        if opcode == BC_LOAD_LIT_BOOL:
            if slot.bool_value != 0:
                memset(bitmaps[sp], 0xFF, nbytes)
                if (num_rows & 7) != 0:
                    bitmaps[sp][nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
            else:
                memset(bitmaps[sp], 0x00, nbytes)
            slot_has_null[sp] = 0
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_LOAD_COL — copy pre-resolved BoolVector bitmap into stack slot
        # ------------------------------------------------------------------
        if opcode == BC_LOAD_COL:
            if not col_cache[i].is_bool:
                return 1  # unexpected non-bool column
            memset(bitmaps[sp], 0, nbytes)
            for j in range(num_rows):
                base = col_cache[i].sel[j]
                if (col_cache[i].data[base >> 3] >> (base & 7)) & 1:
                    bitmaps[sp][j >> 3] |= <uint8_t>(1 << (j & 7))
            if col_cache[i].null_bm != NULL:
                memcpy(null_bitmaps[sp], col_cache[i].null_bm, nbytes)
                slot_has_null[sp] = 1
            else:
                slot_has_null[sp] = 0
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_AND — binary AND with pointer-swap to avoid aliasing
        # ------------------------------------------------------------------
        if opcode == BC_AND:
            sp -= 2
            had_null = c_and_bitmap(
                bitmaps[scratch0],
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_OR — binary OR with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_OR:
            sp -= 2
            had_null = c_or_bitmap(
                bitmaps[scratch0],
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_XOR — binary XOR with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_XOR:
            sp -= 2
            had_null = c_xor_bitmap(
                bitmaps[scratch0],
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                bitmaps[sp + 1],
                null_bitmaps[sp + 1] if slot_has_null[sp + 1] else NULL,
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_NOT — unary NOT with pointer-swap
        # ------------------------------------------------------------------
        if opcode == BC_NOT:
            sp -= 1
            had_null = c_not_bitmap(
                bitmaps[scratch0],
                null_bitmaps[scratch0],
                bitmaps[sp],
                null_bitmaps[sp] if slot_has_null[sp] else NULL,
                nbytes, num_rows,
            )
            tmp_ptr = bitmaps[sp]
            bitmaps[sp] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[sp]
            null_bitmaps[sp] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[sp] = had_null
            sp += 1
            continue

        # ------------------------------------------------------------------
        # BC_DNF — variadic AND fold (uses scratch0 as accumulator, scratch1
        # as output; alternates to avoid aliasing)
        # ------------------------------------------------------------------
        if opcode == BC_DNF:
            arity = slot.arity
            base = sp - arity
            # initialise accumulator from bitmaps[base]
            memcpy(bitmaps[scratch0], bitmaps[base], nbytes)
            memcpy(null_bitmaps[scratch0], null_bitmaps[base], nbytes)
            slot_has_null[scratch0] = slot_has_null[base]
            for j in range(1, arity):
                # short-circuit: if accumulator is all-false, skip the rest
                popcount_val = <Py_ssize_t>simd_popcount(bitmaps[scratch0], <size_t>nbytes)
                if popcount_val == 0 and not slot_has_null[scratch0]:
                    break
                had_null = c_and_bitmap(
                    bitmaps[scratch1],
                    null_bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
                    nbytes, num_rows,
                )
                # swap scratch0 <-> scratch1 (accumulate into scratch0)
                tmp_ptr = bitmaps[scratch0]
                bitmaps[scratch0] = bitmaps[scratch1]
                bitmaps[scratch1] = tmp_ptr
                tmp_ptr = null_bitmaps[scratch0]
                null_bitmaps[scratch0] = null_bitmaps[scratch1]
                null_bitmaps[scratch1] = tmp_ptr
                slot_has_null[scratch0] = had_null
            # swap accumulator into bitmaps[base]
            tmp_ptr = bitmaps[base]
            bitmaps[base] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[base]
            null_bitmaps[base] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[base] = slot_has_null[scratch0]
            sp = base + 1
            continue

        # ------------------------------------------------------------------
        # BC_CNF — variadic OR fold
        # ------------------------------------------------------------------
        if opcode == BC_CNF:
            arity = slot.arity
            base = sp - arity
            memcpy(bitmaps[scratch0], bitmaps[base], nbytes)
            memcpy(null_bitmaps[scratch0], null_bitmaps[base], nbytes)
            slot_has_null[scratch0] = slot_has_null[base]
            for j in range(1, arity):
                # short-circuit: if accumulator is all-true, skip the rest
                popcount_val = <Py_ssize_t>simd_popcount(bitmaps[scratch0], <size_t>nbytes)
                if popcount_val == num_rows and not slot_has_null[scratch0]:
                    break
                had_null = c_or_bitmap(
                    bitmaps[scratch1],
                    null_bitmaps[scratch1],
                    bitmaps[scratch0],
                    null_bitmaps[scratch0] if slot_has_null[scratch0] else NULL,
                    bitmaps[base + j],
                    null_bitmaps[base + j] if slot_has_null[base + j] else NULL,
                    nbytes, num_rows,
                )
                tmp_ptr = bitmaps[scratch0]
                bitmaps[scratch0] = bitmaps[scratch1]
                bitmaps[scratch1] = tmp_ptr
                tmp_ptr = null_bitmaps[scratch0]
                null_bitmaps[scratch0] = null_bitmaps[scratch1]
                null_bitmaps[scratch1] = tmp_ptr
                slot_has_null[scratch0] = had_null
            tmp_ptr = bitmaps[base]
            bitmaps[base] = bitmaps[scratch0]
            bitmaps[scratch0] = tmp_ptr
            tmp_ptr = null_bitmaps[base]
            null_bitmaps[base] = null_bitmaps[scratch0]
            null_bitmaps[scratch0] = tmp_ptr
            slot_has_null[base] = slot_has_null[scratch0]
            sp = base + 1
            continue

        return 1  # unexpected opcode

    return 0


cdef int _c_bytecode_worker_trampoline(BytecodeWorkerItem* item) noexcept nogil:
    """C-callable trampoline for moodycamel worker threads.

    Calls c_execute_bytecode_inner with no GIL held. On return, item.error_code
    is 0 (success, result at item.bitmaps[0]) or 1 (unexpected opcode; caller
    must re-run via execute_bytecode from a GIL-held thread).
    """
    cdef int rc = c_execute_bytecode_inner(
        <BytecodeInstr*>item.instrs,
        <Py_ssize_t>item.n_instrs,
        <ColCache*>item.col_cache,
        item.bitmaps,
        item.null_bitmaps,
        item.slot_has_null,
        <Py_ssize_t>item.n_slots,
        <Py_ssize_t>item.nbytes,
        <Py_ssize_t>item.n_rows,
    )
    item.error_code = rc
    return rc


def get_bytecode_worker_fn_ptr():
    """Return the trampoline function pointer as a Python int.

    Allows C++ code loaded via ctypes to retrieve the opteryx_worker_fn
    address without a Python callback round-trip. Value is stable for the
    lifetime of the process.
    """
    return <uintptr_t>opteryx_worker_fn


cdef BoolVector _execute_bytecode_postpass(
    uint8_t* result_bitmap,
    uint8_t* result_null,
    bint has_null,
    Py_ssize_t num_rows,
):
    """Wrap a draken_malloc'd result bitmap into a BoolVector.

    Ownership of result_bitmap and (if has_null) result_null is transferred
    to the returned BoolVector via from_decoded → draken_vector_own_raw.
    The caller must null out those pointers after this call so the finally
    block does not double-free them.
    """
    return from_decoded(
        <void*>result_bitmap,
        result_null if has_null else NULL,
        <size_t>num_rows,
    )


cpdef object evaluate_bitmap(CompiledBytecode bc, Morsel morsel):
    """GIL-free predicate evaluation path for pure-bitmap bytecodes.

    Allocates scratch buffers (GIL held), runs the nogil bitmap VM, then
    wraps the result bitmap into a BoolVector. Falls back to execute_bytecode
    if any BC_LOAD_COL column is not a BoolVector at runtime.

    Returns a BoolVector on the bitmap path; the fall-back path may return a
    non-bool Vector (e.g. a bare LOAD_COL of an INT column used as a CASE
    result), so the declared return type is the general `object`.
    """
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef Py_ssize_t n_slots = bc.max_stack_depth
    if n_slots < 1:
        n_slots = 1

    # Allocate ColCache (one entry per instruction) on the C heap
    cdef ColCache* col_cache = <ColCache*>malloc(bc.count * sizeof(ColCache))
    if col_cache == NULL:
        raise MemoryError("evaluate_bitmap: failed to allocate ColCache")

    # Allocate bitmap pointer arrays (n_slots + 2 slots: stack + 2 scratch)
    cdef uint8_t** bitmaps = <uint8_t**>malloc((n_slots + 2) * sizeof(uint8_t*))
    cdef uint8_t** null_bitmaps = <uint8_t**>malloc((n_slots + 2) * sizeof(uint8_t*))
    cdef int8_t* slot_has_null = <int8_t*>malloc((n_slots + 2) * sizeof(int8_t))
    if bitmaps == NULL or null_bitmaps == NULL or slot_has_null == NULL:
        free(col_cache); free(bitmaps); free(null_bitmaps); free(slot_has_null)
        raise MemoryError("evaluate_bitmap: failed to allocate stack arrays")

    cdef list anchors = []  # keeps BoolVector Python objects alive during inner loop
    cdef int rc
    cdef Py_ssize_t j

    try:
        rc = _execute_bytecode_prepass(
            bc, morsel, num_rows,
            col_cache, bitmaps, null_bitmaps, slot_has_null,
            n_slots, nbytes, anchors,
        )
        if rc == -1:
            # A BC_LOAD_COL column is not a BoolVector — fall back to the DV
            # operand-stack path.  Column types are schema-bound and stable for
            # the lifetime of this CompiledBytecode, so permanently clear the
            # is_pure_bitmap flag: this both avoids infinite recursion (the
            # fallback re-enters execute_bytecode, which would otherwise
            # re-dispatch straight back here) and skips the now-known-futile
            # bitmap prepass on every subsequent morsel.
            bc.is_pure_bitmap = False
            return execute_bytecode(bc, morsel)

        with nogil:
            rc = c_execute_bytecode_inner(
                bc.instrs, bc.count,
                col_cache, bitmaps, null_bitmaps, slot_has_null,
                n_slots, nbytes, num_rows,
            )

        if rc != 0:
            # Unexpected opcode — fall back (shouldn't happen if is_pure_bitmap is correct)
            return execute_bytecode(bc, morsel)

        result = _execute_bytecode_postpass(
            bitmaps[0],
            null_bitmaps[0],
            slot_has_null[0] != 0,
            num_rows,
        )
        # Postpass transferred ownership of slot-0 buffers to the BoolVector.
        # Null them out so the finally block does not double-free.
        bitmaps[0] = NULL
        if slot_has_null[0]:
            null_bitmaps[0] = NULL
        return result
    finally:
        # Slot 0 was draken_malloc'd; use draken_free (NULL-safe if transferred).
        # Slots 1..n_slots+1 are libc malloc'd.
        if bitmaps[0] != NULL:
            draken_free(bitmaps[0])
        if null_bitmaps[0] != NULL:
            draken_free(null_bitmaps[0])
        for j in range(1, n_slots + 2):
            free(bitmaps[j])
            free(null_bitmaps[j])
        free(col_cache)
        free(bitmaps)
        free(null_bitmaps)
        free(slot_has_null)


cdef inline uint8_t* _ensure_dense_bitmap_c(
    DrakenVector* dv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    DrakenFrameArena* arena,
) noexcept nogil:
    """Nogil core of _ensure_dense_bitmap. Returns NULL on arena-alloc failure.

    Dense (data_length == length): returns dv->data directly — no copy.
    Constant-shape (data_length == 1): expands to a dense arena allocation.
    Dict-compressed (1 < data_length < length): scatters per-code bits dense.

    Shared by the GIL VM (via the raising wrapper below) and the nogil DV* inner
    (S2) — one source of expansion logic, no duplication.
    """
    cdef uint8_t fill
    cdef uint8_t* out
    cdef const uint32_t* sel
    cdef const uint8_t* src
    cdef uint32_t code
    cdef uint32_t r
    if dv.data_length == dv.length:
        return <uint8_t*>dv.data
    if dv.data_length == 1:
        fill = 0xFF if ((<uint8_t*>dv.data)[0] & 1u) else 0x00
        out = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
        if out == NULL:
            return NULL
        memset(out, fill, <size_t>nbytes)
        if num_rows & 7:
            out[nbytes - 1] = fill & <uint8_t>((1u << (num_rows & 7u)) - 1u)
        return out
    # Dict-compressed (1 < data_length < length): scatter the per-code data bits
    # into a dense per-logical-row bitmap via the uniform data[selection[i]] path
    # (same expansion as the pure-bitmap BC_LOAD_COL). Per-row validity is read
    # separately by the combinator from dv.validity, so only data is expanded.
    sel = dv.selection
    src = <const uint8_t*>dv.data
    out = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    if out == NULL:
        return NULL
    memset(out, 0, <size_t>nbytes)
    for r in range(num_rows):
        code = sel[r]
        if (src[code >> 3] >> (code & 7u)) & 1u:
            out[r >> 3] |= <uint8_t>(1u << (r & 7u))
    return out


cdef inline uint8_t* _ensure_dense_bitmap(
    DrakenVector* dv,
    Py_ssize_t nbytes,
    uint32_t num_rows,
    DrakenFrameArena* arena,
) except NULL:
    """GIL-path raising wrapper over _ensure_dense_bitmap_c (NULL → MemoryError)."""
    cdef uint8_t* out = _ensure_dense_bitmap_c(dv, nbytes, num_rows, arena)
    if out == NULL:
        raise MemoryError("_ensure_dense_bitmap: arena alloc failed")
    return out


# ---------------------------------------------------------------------------
# S2 — shared nogil VM op helpers.
#
# Each operates purely on the DV* operand stack (dv_stack), the inline result
# store (dv_store), the frame arena, and sp (in/out) — NO PyObject, NO anchor.
# Called from BOTH the GIL VM (execute_bytecode) and the nogil DV* inner (S2.2),
# so the C-native op logic lives ONCE (architect structure decision b).
# Return code: 0 = ok, 1 = NULL operand (→ TypeError at the GIL edge),
# 2 = arena-alloc failure (→ MemoryError). The GIL caller sets anchor[result]
# to None after a 0 return (the result is an arena DV*, never a Python object).
# ---------------------------------------------------------------------------
cdef inline int _dv_bool_binop_c(
    int op,                       # 0 = AND, 1 = OR, 2 = XOR
    DrakenVector** dv_stack,
    DrakenVector* dv_store,
    Py_ssize_t* sp_io,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes,
    uint32_t num_rows,
) noexcept nogil:
    cdef Py_ssize_t sp = sp_io[0]
    cdef DrakenVector* dv_right_ptr
    cdef DrakenVector* dv_left_ptr
    cdef VecResult vr
    sp -= 1
    dv_right_ptr = dv_stack[sp]
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    if dv_left_ptr == NULL or dv_right_ptr == NULL:
        return 1
    # Value-aware Kleene (draken::ops::bool_*), not the value-blind c_*_bitmap merge.
    # The shim normalizes a DRAKEN_NULL operand to an all-null BOOL, so no crash.
    vr = draken_vm_bool_binop(op, dv_left_ptr, dv_right_ptr, num_rows)
    if vr.data == NULL:
        return 2
    sp_io[0] = sp + 1
    return _dv_vecresult_adopt_c(&vr, dv_store, dv_stack, sp, arena)


cdef inline int _dv_not_c(
    DrakenVector** dv_stack,
    DrakenVector* dv_store,
    Py_ssize_t* sp_io,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes,
    uint32_t num_rows,
) noexcept nogil:
    cdef Py_ssize_t sp = sp_io[0]
    cdef DrakenVector* dv_left_ptr
    cdef VecResult vr
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    if dv_left_ptr == NULL:
        return 1
    # Value-aware Kleene NOT (¬N = N); shim normalizes a DRAKEN_NULL operand.
    vr = draken_vm_bool_not(dv_left_ptr, num_rows)
    if vr.data == NULL:
        return 2
    sp_io[0] = sp + 1
    return _dv_vecresult_adopt_c(&vr, dv_store, dv_stack, sp, arena)


cdef inline int _dv_unary_bool_test_c(
    int uop,                      # UOP_IS_TRUE / UOP_IS_FALSE / UOP_IS_NOT_TRUE / UOP_IS_NOT_FALSE
    DrakenVector** dv_stack,
    DrakenVector* dv_store,
    Py_ssize_t* sp_io,
    DrakenFrameArena* arena,
    uint32_t num_rows,
) noexcept nogil:
    """IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE over a BOOL input: a
    never-null truth test via the uniform data[selection[i]] kernel
    (draken_vm_bool_truth_test — bool_logical.h's bool_truth_test). Mirrors
    _dv_not_c's arena/adopt idiom; op-code mapping matches _bv_truth_test_native's
    _BV_IS_TRUE/_BV_IS_FALSE/_BV_IS_NOT_TRUE/_BV_IS_NOT_FALSE convention."""
    cdef Py_ssize_t sp = sp_io[0]
    cdef DrakenVector* dv_left_ptr
    cdef VecResult vr
    cdef int op
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    if dv_left_ptr == NULL:
        return 1
    if uop == UOP_IS_TRUE:
        op = _BV_IS_TRUE
    elif uop == UOP_IS_FALSE:
        op = _BV_IS_FALSE
    elif uop == UOP_IS_NOT_TRUE:
        op = _BV_IS_NOT_TRUE
    else:
        op = _BV_IS_NOT_FALSE
    vr = draken_vm_bool_truth_test(op, dv_left_ptr, num_rows)
    if vr.data == NULL:
        return 2
    sp_io[0] = sp + 1
    return _dv_vecresult_adopt_c(&vr, dv_store, dv_stack, sp, arena)


cdef inline int _dv_unary_null_c(
    int uop,                      # UOP_IS_NULL or UOP_IS_NOT_NULL
    DrakenVector** dv_stack,
    DrakenVector* dv_store,
    Py_ssize_t* sp_io,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes,
    uint32_t num_rows,
) noexcept nogil:
    """IS [NOT] NULL over ANY input type: a pure validity-bitmap copy/invert
    (see _fill_is_null_bits), producing a never-null bit-packed BOOL result.
    Mirrors _dv_not_c's arena/result idiom."""
    cdef Py_ssize_t sp = sp_io[0]
    cdef DrakenVector* dv
    cdef uint8_t* result
    sp -= 1
    dv = dv_stack[sp]
    if dv == NULL:
        return 1
    result = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    if result == NULL:
        return 2
    _fill_is_null_bits(dv, uop == UOP_IS_NULL, result, nbytes, num_rows)
    dv_store[sp] = draken_vector_from_dense(result, num_rows, DRAKEN_BOOL, NULL)
    dv_stack[sp] = &dv_store[sp]
    sp += 1
    sp_io[0] = sp
    return 0


cdef inline int _dv_variadic_bool_c(
    int op,                       # 0 = DNF (AND-fold of terms), 1 = CNF (OR-fold)
    int arity,
    DrakenVector** dv_stack,
    DrakenVector* dv_store,
    Py_ssize_t* sp_io,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes,
    uint32_t num_rows,
) noexcept nogil:
    # DNF (op 0) folds terms with AND; CNF (op 1) with OR — the same op code the
    # binary Kleene shim takes. Fold pairwise left-to-right, adopting each
    # intermediate into the result slot and reusing it as the next left operand.
    cdef Py_ssize_t sp = sp_io[0]
    cdef Py_ssize_t base = sp - arity
    cdef Py_ssize_t j
    cdef VecResult vr
    cdef int rc
    if dv_stack[base] == NULL:
        return 1
    if arity == 1:
        # Degenerate single-term DNF/CNF: the term is the result; leave it in place.
        return 0
    # left operand is always the base slot: term0 on the first pass, then the
    # accumulator that each adopt writes back into dv_store[base]/dv_stack[base].
    for j in range(1, arity):
        if dv_stack[base + j] == NULL:
            return 1
        vr = draken_vm_bool_binop(op, dv_stack[base], dv_stack[base + j], num_rows)
        if vr.data == NULL:
            return 2
        rc = _dv_vecresult_adopt_c(&vr, dv_store, dv_stack, base, arena)
        if rc != 0:
            return rc
    sp_io[0] = base + 1
    return 0


cdef inline int _dv_compare_c(
    int dv_op,                    # pre-resolved draken_compare_dv op (< 0 = N/A)
    DrakenVector** dv_stack,
    Py_ssize_t* sp_io,
    int16_t left_type_code,
    int16_t right_type_code,
    uint32_t num_rows,
    DrakenFrameArena* arena,
) noexcept nogil:
    """Normal-case BC_COMPARE fast path (draken_compare_dv). Pops two operands.

    rc 0 = result pushed (sp advanced past it). rc 3 = fast path not applicable
    (unsupported op / NULL operand / kernel declined): sp is left DECREMENTED by
    two, so the GIL caller's Python fallback re-reads operands at dv_stack[sp] /
    dv_stack[sp+1]. The result DV* is borrowed from the frame arena (no PyObject).
    """
    cdef Py_ssize_t sp = sp_io[0]
    cdef DrakenVector* dv_right_ptr
    cdef DrakenVector* dv_left_ptr
    cdef DrakenVector* dv_result_ptr
    sp -= 1
    dv_right_ptr = dv_stack[sp]
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    sp_io[0] = sp                 # leave decremented (fallback re-reads from here)
    if dv_op >= 0 and dv_left_ptr != NULL and dv_right_ptr != NULL:
        dv_result_ptr = draken_compare_dv(
            dv_op, dv_left_ptr, dv_right_ptr,
            left_type_code, right_type_code, num_rows, arena)
        if dv_result_ptr != NULL:
            dv_stack[sp] = dv_result_ptr
            sp_io[0] = sp + 1
            return 0
    return 3


cdef inline int _dv_vecresult_adopt_c(
    VecResult* vr,
    DrakenVector* dv_store, DrakenVector** dv_stack, Py_ssize_t slot_idx,
    DrakenFrameArena* arena,
) noexcept nogil:
    """Adopt an owned VecResult of ANY shape — dense, dict (owned codes), or a
    canonical-block string (validity EMBEDDED in the block: one adoption frees
    everything) — into the frame arena and push it. DECIMAL/DECIMAL128/TIMESTAMP64
    results fold as RAW values (the VM's whole decimal/temporal model is
    raw-domain — int64 or int128 — with bind-time scale/unit ctx; the descriptor
    is re-attached at the plan-known boundary — ExprProjectOperator). rc 0."""
    draken_frame_arena_adopt(arena, vr.data)
    if vr.owns_selection:
        draken_frame_arena_adopt(arena, <void*><uint32_t*>vr.selection)
    if vr.validity != NULL and not vr.validity_embedded:
        draken_frame_arena_adopt(arena, vr.validity)
    # A separately-allocated string arena is a SECOND owned buffer behind the same
    # slot. The arena's tracking is what `_slot_to_pyobj` later reads to decide
    # whether a pointer is independently owned (the same question it already asks
    # of `validity`), so adopting it here is both the free and the record of it.
    if vr.arena != NULL:
        draken_frame_arena_adopt(arena, vr.arena)
    dv_store[slot_idx].data = vr.data
    dv_store[slot_idx].selection = vr.selection
    dv_store[slot_idx].data_length = vr.data_length
    dv_store[slot_idx].length = vr.length
    dv_store[slot_idx].validity = vr.validity
    dv_store[slot_idx].type = vr.type
    # Shape hints are trusted for genuinely dense results only: some kernels
    # reshape a dense block into a dict WITHOUT clearing the IDENTITY hint, and a
    # wrongly-set hint (unlike a missed one) can change answers in shape-
    # specialized consumers. 0 = "don't know" = uniform path, always correct.
    dv_store[slot_idx].flags = vr.flags if vr.data_length == vr.length else 0
    dv_stack[slot_idx] = &dv_store[slot_idx]
    return 0


cdef inline int _dv_vecresult_string_fold_c(
    VecResult* vr,
    DrakenVector* dv_store, DrakenVector** dv_stack, Py_ssize_t slot_idx,
    DrakenFrameArena* arena,
) noexcept nogil:
    """rc-5 handler for BINARY_OP/CAST inside the nogil VM: fold string-family,
    DECIMAL and TIMESTAMP64 results natively (raw domain; descriptors re-attach at
    the plan-known ExprProject boundary). DECIMAL128 stays rc 5 — and stays
    unreachable: nogil admission never admits it."""
    return _dv_vecresult_adopt_c(vr, dv_store, dv_stack, slot_idx, arena)


cdef inline int _dv_function_kernel_c(
    void* kernel_fn,
    void* ctx_ptr,
    const DrakenVector* const* fargs,
    uint32_t arity,
    DrakenVector* dv_store, DrakenVector** dv_stack, Py_ssize_t slot_idx,
    DrakenFrameArena* arena, VecResult* out_vr, bint own_array_result,
) noexcept nogil:
    """Phase 9a-fn: C-ABI scalar function dispatch (func_fn_t) — the kernel is
    called DIRECTLY from the nogil VM, no Python/nanobind between them. rc 0 =
    fixed-width or canonical-string result folded and pushed; rc 4 = kernel error
    sentinel; rc 5 = descriptor-carrying result the arena DV cannot hold; rc 6 =
    ARRAY result the CALLER must own (see own_array_result).

    ``own_array_result`` is set by the GIL VM only. An ARRAY result carries its
    elements on VecResult.child, and the frame arena has nowhere to put a child —
    _dv_vecresult_adopt_c would fold the offsets and silently drop the elements,
    leaving an ARRAY pointing at nothing (which `arr[i]` then reports as "DRAKEN_ARRAY
    vector has no child"). The engine VM solves this with its out_child
    out-parameter; the GIL VM has none, so it instead owns the whole result as a
    Vector via draken_vecresult_own_c, whose vecresult_to_owner adopts the child
    recursively. Returning BEFORE the adopt is the point: once the arena has
    registered the buffers, owning them too would double-free."""
    cdef VecResult vr = (<func_fn_t>kernel_fn)(ctx_ptr, fargs, arity)
    out_vr[0] = vr
    if vr.data == NULL:
        return 4
    if own_array_result and vr.child != NULL:
        return 6
    return _dv_vecresult_adopt_c(out_vr, dv_store, dv_stack, slot_idx, arena)


cdef inline int _dv_extraction_kernel_c(
    void* kernel_fn,
    void* ctx_ptr,
    const DrakenVector* operand,
    const DrakenVector* child,
    DrakenVector* dv_store, DrakenVector** dv_stack, Py_ssize_t slot_idx,
    DrakenFrameArena* arena, VecResult* out_vr,
) noexcept nogil:
    """C-ABI BC_EXTRACTION dispatch (`->`, `->>`, str[i], arr[i]) — called DIRECTLY
    from the nogil VM. The path/index is bound into extraction_ctx, so no key operand
    is popped and exactly one vector is consumed off the stack.

    `child` rides in the ABI's free second slot: arr[i] needs the ARRAY's element
    vector, which hangs off the column owner and is unreachable from the parent
    DrakenVector (BC_C_NATIVE_CHILD — the caller resolves it from dv_cache). NULL for
    every other sub-op, which ignores the slot.

    _dv_vecresult_adopt_c takes the result of ANY shape, so arr[i]'s element-typed
    (possibly fixed-width) result folds the same way the string sub-ops' canonical
    blocks do. rc 0 = pushed; rc 4 = kernel error sentinel (invalid JSON, bad operand
    type, unsupported element type, OOM)."""
    cdef VecResult vr = (<extr_fn_t>kernel_fn)(ctx_ptr, operand, child)
    out_vr[0] = vr
    if vr.data == NULL:
        return 4
    return _dv_vecresult_adopt_c(out_vr, dv_store, dv_stack, slot_idx, arena)


cdef inline int _dv_binop_kernel_c(
    void* kernel_fn,
    void* ctx_ptr,
    DrakenVector* dv_left_ptr,
    DrakenVector* dv_right_ptr,
    DrakenVector* dv_store,
    DrakenVector** dv_stack,
    Py_ssize_t slot_idx,          # result slot (== sp after the two pops)
    DrakenFrameArena* arena,
    VecResult* out_vr,
) noexcept nogil:
    """C-native BC_BINARY_OP kernel dispatch. Caller guarantees BC_INSTR_C_NATIVE
    and non-NULL operands, and has already popped both (slot_idx = result slot).

    rc 0 = FIXED-WIDTH result folded into the arena and pushed (fully nogil).
    rc 4 = kernel error sentinel (out_vr.data == NULL) — GIL caller raises.
    rc 5 = STRING result in out_vr — GIL caller wraps it as a Vector (can't go
    nogil; is_all_c_native excludes string-producing binops so the nogil inner
    never sees rc 5).
    """
    cdef VecResult vr = (<binop_fn_t>kernel_fn)(ctx_ptr, dv_left_ptr, dv_right_ptr)
    out_vr[0] = vr
    if vr.data == NULL:
        return 4
    if (vr.type == DRAKEN_VARCHAR or vr.type == DRAKEN_NVARCHAR
            or vr.type == DRAKEN_VARBINARY):
        return 5
    # Parameterized fixed-width results (DECIMAL/DECIMAL128/TIMESTAMP64) carry a
    # LogicalType descriptor (precision/scale or unit) that the arena DV* cannot
    # hold — own them as a Vector via the descriptor-attaching wrap (rc 5), like
    # strings. is_all_c_native excludes these (no BC_C_NATIVE_FIXED), so the nogil
    # whole-expression fast path never reaches this rc 5 either.
    if (vr.type == DRAKEN_DECIMAL or vr.type == DRAKEN_DECIMAL128
            or vr.type == DRAKEN_TIMESTAMP64):
        return 5
    # Adopt at the result's OWN shape. A shape-preserving kernel (result_helpers.h's
    # kernel_preserve_shape) returns data holding only data_length physical values
    # with the input's selection carried through; re-declaring that dense at
    # vr.length reads data[i] past a K-element buffer — wrong rows, then SIGBUS.
    return _dv_vecresult_adopt_c(out_vr, dv_store, dv_stack, slot_idx, arena)


cdef inline int _dv_cast_kernel_c(
    void* kernel_fn,
    void* ctx_ptr,
    DrakenVector* dv_left_ptr,
    DrakenVector* dv_store,
    DrakenVector** dv_stack,
    Py_ssize_t slot_idx,          # result slot (== sp after the one pop)
    DrakenFrameArena* arena,
    VecResult* out_vr,
    bint own_array_result,
) noexcept nogil:
    """C-native BC_CAST kernel dispatch (unary; mirrors _dv_binop_kernel_c).
    rc 0 = fixed-width result folded into the arena and pushed; rc 4 = kernel
    error; rc 5 = string result in out_vr (GIL caller wraps as a Vector); rc 6 =
    ARRAY result the caller must own — see _dv_function_kernel_c for why
    (CAST(json AS ARRAY<T>) is the one cast that returns a child)."""
    cdef VecResult vr = (<cast_fn_t>kernel_fn)(ctx_ptr, dv_left_ptr)
    out_vr[0] = vr
    if vr.data == NULL:
        return 4
    if own_array_result and vr.child != NULL:
        return 6
    if (vr.type == DRAKEN_VARCHAR or vr.type == DRAKEN_NVARCHAR
            or vr.type == DRAKEN_VARBINARY):
        return 5
    # Adopt at the result's OWN shape — see the note in _dv_binop_kernel_c. Every
    # cast kernel in cast_numeric.cpp / cast_temporal.cpp is compression-aware
    # (casts data_length values, carries the input's selection), so forcing dense
    # here mis-indexed every dict- or constant-shaped input.
    return _dv_vecresult_adopt_c(out_vr, dv_store, dv_stack, slot_idx, arena)


cdef object _slot_to_pyobj(DrakenVector* dv, object anc, DrakenFrameArena* arena):
    """Recover a Python Vector from a DV* stack slot.

    Hot path (borrowed slot): anc is the Python Vector whose .unified() the DV*
    was taken from — return it directly, zero allocation.

    Cold path (arena slot): anc is None — the DV* is arena-owned.  Release the
    data/validity buffers from the arena (transferring ownership to the Python
    object we're about to create), then wrap via from_decoded / vec_from_decoded.
    Called only from Python-fallback paths (LIKE/RLIKE, string concat, etc.);
    never on the ordinal-compare hot path.
    """
    cdef Vector av
    if anc is not None:
        # A bind-time scalar literal is anchored as a constant-shape Vector cached
        # at length 1; the hot path re-stamps only the DV. When a Python-fallback
        # kernel needs the Vector object at the morsel length, hand back a
        # zero-copy length-adjusted view (the cached value is reused, not
        # re-encoded). Non-constant anchors (length already matches) pass through.
        if isinstance(anc, Vector):
            av = <Vector>anc
            if av._dv != NULL and av._dv.length != dv.length:
                if av._dv.type == DRAKEN_NULL:
                    return Vector(_draken_native.vector_null_from_length(dv.length))
                if av._dv.data_length == 1:
                    return Vector(_draken_native.vector_constant_view(av._nb, dv.length))
        return anc
    cdef void*    dp = dv.data
    cdef uint8_t* vp = dv.validity
    cdef size_t   vbytes
    cdef uint8_t* vcopy
    cdef uint8_t* sa_arena
    # from_decoded / vec_from_decoded hand BOTH buffers to draken_vector_own_raw,
    # which takes ownership of each as an INDEPENDENT draken_malloc'd allocation
    # and frees them when the Vector dies. That holds for a dense kernel result
    # (data + separately-malloc'd validity), but NOT for a canonical-block string
    # result (binop_string_concat and the *_to_string casts, via
    # vecresult_from_string_buffers): those embed the null bitmap INSIDE the data
    # block, so `vp` is an interior pointer. Freeing it aborts the process with
    # POINTER_BEING_FREED_WAS_NOT_ALLOCATED once the Vector is collected.
    #
    # The VecResult's validity_embedded flag is gone by here (a DrakenVector has
    # no such field), but the arena still knows: _dv_vecresult_adopt_c adopts
    # validity ONLY when it is independently allocated. So "tracked by the arena"
    # is exactly "safe to own"; anything else is embedded and must be COPIED out
    # rather than adopted — passing NULL instead would drop the nulls and silently
    # return wrong answers.
    if vp != NULL and not draken_frame_arena_contains(arena, vp):
        vbytes = <size_t>((((dv.length + 7u) >> 3) + 7u) & ~<size_t>7u)
        if vbytes == 0:
            vbytes = 8
        vcopy = <uint8_t*>draken_malloc(vbytes)
        if vcopy == NULL:
            raise MemoryError("_slot_to_pyobj: validity copy alloc failed")
        memcpy(vcopy, vp, vbytes)
        vp = vcopy
    else:
        draken_frame_arena_release(arena, vp)
    draken_frame_arena_release(arena, dp)
    if dv.type == DRAKEN_BOOL:
        return from_decoded(dp, vp, <size_t>dv.length)
    # A string block whose byte arena is a SEPARATE allocation has TWO owned
    # pointers behind this one slot, and releasing only `dp` would leave the
    # arena to be freed by the dying frame arena while the Vector's slots still
    # resolve against it — a use-after-free, not a leak. Which case this is, is
    # asked of the arena's own tracking, exactly as the validity branch above
    # asks it: adopted means independently owned, so release it too and hand it
    # over. An arena embedded in the block is not tracked, so it is not touched
    # and the block continues to own its own bytes.
    # VARIANT shares the VARCHAR family's slot/arena layout (see the copy helpers).
    if (dv.type == DRAKEN_VARCHAR or dv.type == DRAKEN_NVARCHAR
            or dv.type == DRAKEN_VARBINARY or dv.type == DRAKEN_VARIANT):
        sa_arena = (<DrakenStringArena*>dp).arena
        if sa_arena != NULL and draken_frame_arena_contains(arena, sa_arena):
            draken_frame_arena_release(arena, sa_arena)
            return vec_from_decoded_with_arena(dp, sa_arena, vp, dv.length, dv.type)
    return vec_from_decoded(dp, vp, dv.length, dv.type)


cdef int _dv_native_prepass(
    CompiledBytecode bc, Morsel morsel, Py_ssize_t num_rows,
    DrakenVector** dv_cache,
) except -1:
    """GIL prepass for evaluate_c_native: resolve every BC_LOAD_COL /
    BC_LOAD_LIT_CONST source DV* into dv_cache. No anchoring needed — the column
    owners are kept alive by morsel._cxx (shared_ptr) and the literal Vectors by
    the bytecode (slot.literal_obj is _hold'd), both of which outlive this call.
    Other ops: NULL.
    """
    cdef Py_ssize_t k
    cdef BytecodeInstr* slot
    cdef Vector v
    cdef object scalar_obj
    for k in range(bc.count):
        slot = &bc.instrs[k]
        dv_cache[k] = NULL
        if slot.opcode == BC_LOAD_COL:
            v = morsel._cxx_column(<bytes>slot.column_identity, <bytes>slot.column_name)
            if v is None:
                raise ColumnReferencedBeforeEvaluationError(
                    column=(<bytes>slot.column_name).decode())
            dv_cache[k] = <DrakenVector*>(<Vector>v)._dv
        elif slot.opcode == BC_LOAD_LIT_CONST:
            scalar_obj = <object>slot.literal_obj
            dv_cache[k] = (<Vector>scalar_obj).unified()
    return 0


cdef int _dv_lazy_region_c(
    BytecodeInstr* instrs, Py_ssize_t i,
    DrakenVector** dv_cache, DrakenVector** dv_stack, DrakenVector* dv_store,
    Py_ssize_t* sp_io, DrakenFrameArena* arena,
    Py_ssize_t nbytes, uint32_t num_rows,
    int* err_op, const char** err_msg, VecResult** out_child,
) noexcept nogil:
    """Execute ONE lazy branch region (BC_LAZY at instrs[i]) and push its result.

    Layout: [BC_LAZY][typed-NULL LOAD_LIT_CONST][branch instrs ...]; slot.arity is
    the number of instructions FOLLOWING the BC_LAZY, slot.op_code the row-selection
    kind (DRAKEN_LZ_*), slot.bool_value how far below the stack top the first guard
    sits, slot.flags the guard count. The branch runs ONLY on the rows the guard
    admits, so a data error (checked-arithmetic overflow, a cast failure) on a row the
    guard excludes never surfaces. Excluded rows come back NULL, so the blend / Kleene
    op that follows combines the result unchanged.

      every row admitted -> the branch runs as it always did (no narrowing);
      no row admitted    -> the branch is skipped, a NULL literal is pushed;
      otherwise          -> every column the branch loads is narrowed to the
                            admitted rows, the branch runs over k rows (recursively,
                            on the stack above the guards), and the k-row result is
                            scattered back to full length.

    The recursion reuses the caller's stack (dv_stack/dv_store + sp), so the bind-time
    max_stack_depth already bounds it. Returns 0 or the failing rc (err_op/err_msg set
    by whoever failed)."""
    cdef BytecodeInstr* slot = &instrs[i]
    cdef Py_ssize_t sp = sp_io[0]
    cdef const DrakenVector* guards[16]
    cdef uint32_t ng = <uint32_t>slot.flags
    cdef Py_ssize_t g0 = sp - slot.bool_value
    cdef Py_ssize_t region = i + 2
    cdef Py_ssize_t region_n = slot.arity - 1
    cdef Py_ssize_t t, nload, nl
    cdef uint32_t j, k
    cdef uint32_t* rows
    cdef DrakenVector** ncache
    cdef DrakenVector** nptrs
    cdef DrakenVector* nstore
    cdef DrakenVector compact
    cdef VecResult vr
    cdef int rc
    cdef uint32_t* nsel
    cdef uint8_t* nval
    if ng < 1 or ng > 16 or g0 < 0 or region_n < 1:
        err_op[0] = BC_LAZY
        return 99
    for j in range(ng):
        if dv_stack[g0 + j] == NULL:
            err_op[0] = BC_LAZY
            return 1
        guards[j] = dv_stack[g0 + j]
    rows = <uint32_t*>draken_frame_arena_alloc(
        arena, <size_t>(num_rows if num_rows > 0 else 1) * sizeof(uint32_t))
    if rows == NULL:
        err_op[0] = BC_LAZY
        return 2
    k = draken_lz_rows(slot.op_code, guards, ng, num_rows, rows)

    if k == num_rows:
        rc = c_execute_dv_inner(instrs + region, region_n, dv_cache + region,
                                dv_stack + sp, dv_store + sp, arena, nbytes, num_rows,
                                err_op, err_msg, out_child)
        if rc != 0:
            return rc
        sp_io[0] = sp + 1
        return 0
    if k == 0:
        rc = c_execute_dv_inner(instrs + i + 1, 1, dv_cache + i + 1,
                                dv_stack + sp, dv_store + sp, arena, nbytes, num_rows,
                                err_op, err_msg, out_child)
        if rc != 0:
            return rc
        sp_io[0] = sp + 1
        return 0

    nload = 0
    for t in range(region_n):
        if instrs[region + t].opcode == BC_LOAD_COL:
            nload += 1
    ncache = <DrakenVector**>draken_frame_arena_alloc(
        arena, <size_t>region_n * sizeof(DrakenVector*))
    nptrs = <DrakenVector**>draken_frame_arena_alloc(
        arena, <size_t>(nload if nload > 0 else 1) * sizeof(DrakenVector*))
    nstore = <DrakenVector*>draken_frame_arena_alloc(
        arena, <size_t>(nload if nload > 0 else 1) * sizeof(DrakenVector))
    if ncache == NULL or nptrs == NULL or nstore == NULL:
        err_op[0] = BC_LAZY
        return 2
    nl = 0
    for t in range(region_n):
        if instrs[region + t].opcode == BC_LOAD_COL and dv_cache[region + t].type == DRAKEN_ARRAY:
            # An ARRAY parent is a VIEW, not a copy: its offsets stay put (the elements
            # hang off the column owner and are indexed through them) and only the
            # selection and validity are narrowed. Every array kernel reads the parent
            # through data[selection[i]] (array_subscript.h, array_membership.h,
            # array_reductions.h), so the narrowed view is read correctly. `flags` 0 =
            # "don't know": the selection is no longer the identity.
            nstore[nl] = dv_cache[region + t][0]
            nsel = <uint32_t*>draken_frame_arena_alloc(
                arena, <size_t>k * sizeof(uint32_t))
            if nsel == NULL:
                err_op[0] = BC_LAZY
                return 2
            for j in range(k):
                nsel[j] = nstore[nl].selection[rows[j]]
            nstore[nl].selection = nsel
            nstore[nl].length = k
            nstore[nl].flags = 0
            if nstore[nl].validity != NULL:
                nval = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>((k + 7) >> 3))
                if nval == NULL:
                    err_op[0] = BC_LAZY
                    return 2
                memset(nval, 0, <size_t>((k + 7) >> 3))
                for j in range(k):
                    if (dv_cache[region + t].validity[rows[j] >> 3] >> (rows[j] & 7u)) & 1u:
                        nval[j >> 3] |= <uint8_t>(1u << (j & 7u))
                nstore[nl].validity = nval
            ncache[t] = &nstore[nl]
            nl += 1
        elif instrs[region + t].opcode == BC_LOAD_COL:
            vr = draken_lz_narrow(dv_cache[region + t], rows, k)
            if vr.data == NULL:
                err_op[0] = BC_LAZY
                err_msg[0] = vr.error_msg
                return 4
            _dv_vecresult_adopt_c(&vr, nstore, nptrs, nl, arena)
            ncache[t] = &nstore[nl]
            nl += 1
        else:
            ncache[t] = dv_cache[region + t]
    rc = c_execute_dv_inner(instrs + region, region_n, ncache,
                            dv_stack + sp, dv_store + sp, arena,
                            <Py_ssize_t>((k + 7) >> 3), k,
                            err_op, err_msg, out_child)
    if rc != 0:
        return rc
    if out_child[0] != NULL:
        # An ARRAY result cannot be scattered: its elements ride out on VecResult.child.
        draken_vecresult_discard_c(out_child[0])
        out_child[0] = NULL
        err_op[0] = BC_LAZY
        err_msg[0] = "lazy branch evaluation does not support an ARRAY-typed branch result"
        return 4
    compact = dv_stack[sp][0]
    if compact.type == DRAKEN_NULL:
        # An all-NULL branch (`THEN NULL`): a constant NULL of full length, built the
        # way BC_LOAD_LIT_CONST builds one.
        dv_store[sp] = compact
        dv_store[sp].length = num_rows
        dv_store[sp].selection = draken_zero_sel(num_rows)
        if dv_store[sp].validity != NULL:
            dv_store[sp].validity = <uint8_t*>draken_zero_validity(num_rows)
        dv_stack[sp] = &dv_store[sp]
        sp_io[0] = sp + 1
        return 0
    vr = draken_lz_scatter(&compact, rows, k, num_rows)
    if vr.data == NULL:
        err_op[0] = BC_LAZY
        err_msg[0] = vr.error_msg
        return 4
    _dv_vecresult_adopt_c(&vr, dv_store, dv_stack, sp, arena)
    sp_io[0] = sp + 1
    return 0


cdef int c_execute_dv_inner(
    BytecodeInstr* instrs, Py_ssize_t n_instrs,
    DrakenVector** dv_cache,
    DrakenVector** dv_stack, DrakenVector* dv_store,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes, uint32_t num_rows,
    int* err_op,
    const char** err_msg,
    VecResult** out_child,
) noexcept nogil:
    """Nogil DV* VM inner loop for is_all_c_native bytecodes.

    Loads read pre-resolved DV* from dv_cache; compute ops call the shared
    _dv_* helpers. Returns 0 on success (result at dv_stack[0]); otherwise the
    helper rc (1 NULL-operand, 2 alloc, 3 compare-N/A, 4 kernel-error, 5 string,
    96 kernel DATA error) with *err_op set to the failing opcode. On rc 4 and rc
    96, *err_msg is the failing kernel's VecResult.error_msg (see vec_result.h —
    a pointer into THIS thread's error_handling.cpp buffer, valid until the next
    kernel call on this thread; NULL otherwise). No PyObject, no anchor — the GIL
    caller materializes dv_stack[0] (an arena result) and maps any error.

    rc 96 is rc 4 whose message is about the DATA, not the engine: a string that
    is not a number, a value outside the target's range. The kernel classified it
    (VecResult.data_error) and the message is complete user-facing text, so every
    consumer must raise it VERBATIM — no operator name, no opcode — and none may
    treat it as a decline and fall back to the GIL VM, which would re-run the same
    bad values through a different implementation. It is deliberately NOT in the
    0-5 band: 5 and 6 are already taken inside this rc space, and rc 5's
    fall-back-to-the-VM arm is exactly the treatment a data error must not get.

    *out_child is set to NULL, then to a BC_FUNCTION kernel's VecResult.child
    (see vec_result.h) whenever one is produced — currently only possible for
    an ARRAY-returning kernel (JSONB_OBJECT_KEYS). Ownership is NOT arena-
    managed (the kernel draken_malloc'd it standalone, matching the
    VectorOwner-adoption contract) — it is caller's to adopt or, if the
    caller cannot use an ARRAY result, to fail loud over. No kernel today
    reads an ARRAY operand (draken_length et al. reject it explicitly), so an
    ARRAY value can only ever be the PROGRAM's final result — never consumed
    by a later instruction — meaning *out_child, if set, always corresponds
    to dv_stack[0] on a rc==0 return. This is a standing invariant, not
    something this function re-verifies per call.
    """
    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i
    cdef int opcode, rc, dv_op
    cdef int arity, j
    cdef const DrakenVector* fargs[16]
    # Row-count carrier for arity-0 C-native functions (RANDOM/NORMAL): the func_fn_t
    # ABI passes only operand vectors, so a nullary kernel cannot learn the morsel row
    # count. We synthesize a length-only operand whose `length` IS num_rows; the kernel
    # reads ONLY .length (data/selection/validity are NULL — safe because this is
    # confined to the arity-0 path, and RANDOM/NORMAL are the only nullary C-natives).
    cdef DrakenVector _zeroarg_rowcount
    cdef uint32_t _eff_nargs
    cdef BytecodeInstr* slot
    cdef DrakenVector* dv_left_ptr
    cdef DrakenVector* dv_right_ptr
    cdef void* result_data_ptr
    cdef VecResult vr
    err_msg[0] = NULL
    out_child[0] = NULL
    cdef Py_ssize_t skip_to = 0     # a BC_LAZY region is consumed whole by its handler
    for i in range(n_instrs):
        if i < skip_to:
            continue
        slot = &instrs[i]
        opcode = slot.opcode

        if opcode == BC_LOAD_COL:
            dv_stack[sp] = dv_cache[i]
            sp += 1
            continue

        if opcode == BC_LAZY:
            rc = _dv_lazy_region_c(instrs, i, dv_cache, dv_stack, dv_store, &sp,
                                   arena, nbytes, num_rows, err_op, err_msg, out_child)
            if rc != 0:
                return rc           # err_op / err_msg set by whoever failed
            skip_to = i + 1 + slot.arity
            continue

        if opcode == BC_LOAD_LIT_CONST:
            dv_store[sp] = dv_cache[i][0]               # copy the cached const DV
            dv_store[sp].length = num_rows
            dv_store[sp].selection = draken_zero_sel(num_rows)
            if dv_store[sp].validity != NULL:
                dv_store[sp].validity = <uint8_t*>draken_zero_validity(num_rows)
            dv_stack[sp] = &dv_store[sp]
            sp += 1
            continue

        if opcode == BC_LOAD_LIT_BOOL:
            result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
            if result_data_ptr == NULL:
                err_op[0] = opcode
                return 2
            if slot.bool_value != 0:
                memset(<uint8_t*>result_data_ptr, 0xFF, <size_t>nbytes)
                if num_rows & 7:
                    (<uint8_t*>result_data_ptr)[nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
            else:
                memset(<uint8_t*>result_data_ptr, 0x00, <size_t>nbytes)
            dv_store[sp] = draken_vector_from_dense(result_data_ptr, num_rows, DRAKEN_BOOL, NULL)
            dv_stack[sp] = &dv_store[sp]
            sp += 1
            continue

        if opcode == BC_AND:
            rc = _dv_bool_binop_c(0, dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_OR:
            rc = _dv_bool_binop_c(1, dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_XOR:
            rc = _dv_bool_binop_c(2, dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_NOT:
            rc = _dv_not_c(dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_DNF:
            rc = _dv_variadic_bool_c(0, slot.arity, dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_CNF:
            rc = _dv_variadic_bool_c(1, slot.arity, dv_stack, dv_store, &sp, arena, nbytes, num_rows)
        elif opcode == BC_COMPARE:
            dv_op = -1
            if 0 < slot.op_code < 19:
                dv_op = _DRAKEN_CMP_OP[slot.op_code]
            rc = _dv_compare_c(dv_op, dv_stack, &sp,
                               slot.left_type_code, slot.right_type_code, num_rows, arena)
        elif opcode == BC_UNARY_OP and (slot.op_code == UOP_IS_NULL
                                        or slot.op_code == UOP_IS_NOT_NULL):
            rc = _dv_unary_null_c(slot.op_code, dv_stack, dv_store, &sp, arena,
                                  nbytes, num_rows)
        elif opcode == BC_UNARY_OP and (slot.op_code == UOP_IS_TRUE
                                        or slot.op_code == UOP_IS_FALSE
                                        or slot.op_code == UOP_IS_NOT_TRUE
                                        or slot.op_code == UOP_IS_NOT_FALSE):
            rc = _dv_unary_bool_test_c(slot.op_code, dv_stack, dv_store, &sp,
                                       arena, num_rows)
        elif opcode == BC_FUNCTION and (slot.flags & BC_INSTR_C_NATIVE) != 0:
            # Phase 9a-fn: C-ABI function kernel, dispatched with NO Python between
            # the VM and the kernel (the whole point — see engine_cutover memory).
            arity = slot.arity
            if arity < 0 or arity > 16:
                err_op[0] = opcode
                return 99
            sp -= arity
            for j in range(arity):
                if dv_stack[sp + j] == NULL:
                    err_op[0] = opcode
                    return 1
                fargs[j] = dv_stack[sp + j]
            _eff_nargs = <uint32_t>arity
            if arity == 0:
                # Nullary C-native (RANDOM/NORMAL): hand the kernel a synthetic
                # length-only operand carrying num_rows so it can size its output.
                _zeroarg_rowcount.data = NULL
                _zeroarg_rowcount.selection = NULL
                _zeroarg_rowcount.validity = NULL
                _zeroarg_rowcount.data_length = num_rows
                _zeroarg_rowcount.length = num_rows
                _zeroarg_rowcount.type = DRAKEN_FLOAT64
                fargs[0] = &_zeroarg_rowcount
                _eff_nargs = 1
            elif (slot.flags & BC_C_NATIVE_CHILD) != 0:
                # SORT(arr): the element vector hangs off the column owner, not
                # reachable from the parent DrakenVector* (same wall CAST's
                # ARRAY->VARCHAR hits). dv_cache[i] holds it — resolved the SAME
                # way BC_CAST's BC_C_NATIVE_CHILD case is (_dv_fill_cache_cxx),
                # via this instruction's own column_identity (set at bind time
                # only when the arg is a direct column load — see
                # compiled_expression.pyx). Append as a synthetic extra arg
                # rather than a new kernel signature — draken_sort keeps the
                # plain func_fn_t(ctx, args[], nargs) shape.
                if dv_cache[i] == NULL:
                    err_op[0] = opcode
                    return 4
                fargs[arity] = dv_cache[i]
                _eff_nargs = <uint32_t>(arity + 1)
            # own_array_result=0: the engine VM captures an ARRAY's child in
            # out_child below, so the arena fold is correct here.
            rc = _dv_function_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr, fargs,
                                       _eff_nargs, dv_store, dv_stack, sp,
                                       arena, &vr, 0)
            if rc == 0:
                sp += 1
                if vr.child != NULL:
                    out_child[0] = <VecResult*>vr.child
        elif opcode == BC_EXTRACTION and (slot.flags & BC_INSTR_C_NATIVE) != 0:
            # `->`, `->>`, str[i], arr[i] — one operand in. The path/index rides in
            # extraction_ctx (bound once), so no key is popped.
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            if dv_left_ptr == NULL:
                err_op[0] = opcode
                return 1
            if (slot.flags & BC_C_NATIVE_CHILD) != 0:
                # arr[i]: dv_cache[i] holds the owner-resolved child element vector
                # (same resolution BC_CAST's ARRAY->VARCHAR arm uses). NULL means the
                # column carries no child — fail loud rather than answer without one.
                if dv_cache[i] == NULL:
                    err_op[0] = opcode
                    return 4
                rc = _dv_extraction_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr,
                                             dv_left_ptr, dv_cache[i],
                                             dv_store, dv_stack, sp, arena, &vr)
            else:
                rc = _dv_extraction_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr,
                                             dv_left_ptr, NULL,
                                             dv_store, dv_stack, sp, arena, &vr)
            if rc == 0:
                sp += 1
        elif opcode == BC_BINARY_OP:
            sp -= 1
            dv_right_ptr = dv_stack[sp]
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            if dv_left_ptr == NULL or dv_right_ptr == NULL:
                err_op[0] = opcode
                return 1
            rc = _dv_binop_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr,
                                    dv_left_ptr, dv_right_ptr, dv_store, dv_stack, sp, arena, &vr)
            if rc == 5:
                # canonical-block string result (STRING_CONCAT) — fold nogil
                rc = _dv_vecresult_string_fold_c(&vr, dv_store, dv_stack, sp, arena)
            if rc == 0:
                sp += 1
        elif opcode == BC_CAST:
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            if dv_left_ptr == NULL:
                err_op[0] = opcode
                return 1
            if (slot.flags & BC_C_NATIVE_CHILD) != 0:
                # ARRAY->VARCHAR: dv_cache[i] holds the owner-resolved child
                # element vector (NULL when the column carries no child).
                if dv_cache[i] == NULL:
                    err_op[0] = opcode
                    return 4
                vr = (<cast_child_fn_t>slot.kernel_fn)(<void*>slot.ctx_ptr,
                                                       dv_left_ptr, dv_cache[i])
                rc = 4 if vr.data == NULL else 5
            else:
                rc = _dv_cast_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr,
                                       dv_left_ptr, dv_store, dv_stack, sp, arena, &vr, 0)
            if rc == 5:
                # canonical-block string result (`*_to_string` casts) — fold nogil
                rc = _dv_vecresult_string_fold_c(&vr, dv_store, dv_stack, sp, arena)
            if rc == 0:
                sp += 1
                # CAST(json AS ARRAY<T>) returns its elements on VecResult.child,
                # exactly as the BC_FUNCTION ARRAY producers do. Until that cast
                # existed no CAST ever returned a child (ARRAY->VARCHAR goes the
                # other way), so this capture was absent — and without it the child
                # is dropped and leaked, leaving an ARRAY with offsets but no
                # elements. Same one-child-terminal-result invariant as
                # BC_FUNCTION: no kernel READS an ARRAY operand, so an ARRAY result
                # can only ever be the program's terminal value, never an
                # intermediate — one pointer, captured once.
                if vr.child != NULL:
                    out_child[0] = <VecResult*>vr.child
        else:
            err_op[0] = opcode
            return 99
        if rc != 0:
            err_op[0] = opcode
            if rc == 4:
                err_msg[0] = vr.error_msg
                # The kernel's own classification of what it just wrote. Read here,
                # at the ONE site that turns a sentinel VecResult into an rc for the
                # caller, so no consumer has to know about VecResult at all.
                if vr.data_error != 0:
                    return 96
            elif rc == 3:
                # rc 3 is _dv_compare_c's exclusive return (see its docstring) —
                # the fast BC_COMPARE path declined (unsupported operand type or
                # a cross-type/cross-scale pair). Without this, the raised error
                # named the opcode but carried no message at all (see
                # native_expression.hpp's format_kernel_error) — err_op=11 with
                # nothing after the colon, on a path with no Python fallback to
                # otherwise explain the failure.
                err_msg[0] = "BC_COMPARE fast path declined (unsupported operand type or cross-type/cross-scale comparison)"
            return rc
    return 0


cdef object _vecresult_error_exc(VecResult* vr, str fallback):
    """The exception for an error sentinel the GIL Morsel VM holds in hand.

    Same classification as _kernel_error_exc, read straight off the sentinel
    (VecResult.data_error) because this path never converts it to an rc: a kernel
    that says the DATA failed gets DataError with its message verbatim, anything
    else stays a ValueError."""
    cdef object msg = (
        vr.error_msg.decode("utf-8", "replace")
        if vr.error_msg != NULL else fallback
    )
    if vr.data_error != 0:
        from opteryx.exceptions import DataError

        return DataError(msg)
    return ValueError(msg)


cdef object _kernel_error_exc(int rc, const char* err_msg_ptr):
    """The exception for a kernel error out of the DV* VM — rc 4 or rc 96.

    rc 96 is the kernel saying the DATA is what failed (see c_execute_dv_inner):
    the message is complete user-facing text, so it becomes an opteryx DataError
    carrying it verbatim, the same class and the same no-framing treatment the
    native engine gives it through kErrCodeDataError. rc 4 is an engine fault and
    stays a ValueError."""
    cdef object msg = (
        err_msg_ptr.decode("utf-8", "replace")
        if err_msg_ptr != NULL else "C kernel error"
    )
    if rc == 96:
        from opteryx.exceptions import DataError

        return DataError(msg)
    return ValueError(msg)


cpdef object evaluate_c_native(CompiledBytecode bc, Morsel morsel):
    """Whole-bytecode nogil DV* path for is_all_c_native predicates/expressions.

    Resolve loads (GIL) → run the entire dispatch under ONE `with nogil` block
    via the shared _dv_* helpers → materialize the arena result (GIL). On any
    nogil-inner shortfall that the GIL VM can still handle (compare fast path
    declined / unexpected string), permanently clear the flag and fall back to
    execute_bytecode — mirrors evaluate_bitmap's fall-back contract.
    """
    # Bytecodes longer than the stack cache fall back to the GIL VM (rare; keeps
    # the per-morsel path alloc-free — no malloc, no Python anchor list).
    if bc.count > 256:
        bc.is_all_c_native = False
        return execute_bytecode(bc, morsel)
    cdef const CxxMorsel* m = morsel._cxx_ptr
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef DrakenVector* dv_cache[256]
    cdef int col_idx[256]
    cdef DrakenVector* lit_dv[256]
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef int err_op = 0
    cdef const char* err_msg_ptr = NULL
    cdef int rc
    cdef bint use_substrate = (m != NULL)
    cdef VecResult* child_vr = NULL
    # GIL prepass: resolve loads. When the morsel is Cxx-backed, resolve columns
    # straight from the substrate (columns[idx].view) — no per-column Vector build;
    # otherwise the Morsel path (_cxx_column). Unresolved identity → Morsel path.
    if use_substrate:
        if _dv_cxx_resolve_caches(bc, m, col_idx, lit_dv) != 0:
            use_substrate = False
    if not use_substrate:
        _dv_native_prepass(bc, morsel, num_rows, dv_cache)
    cdef DrakenFrameArena* arena = draken_frame_arena_create()
    if arena == NULL:
        raise MemoryError("evaluate_c_native: failed to create DrakenFrameArena")
    try:
        with nogil:
            if use_substrate:
                _dv_fill_cache_cxx(bc.instrs, bc.count, m, col_idx, lit_dv, dv_cache)
            rc = c_execute_dv_inner(
                bc.instrs, bc.count, dv_cache, dv_stack, dv_store,
                arena, nbytes, <uint32_t>num_rows, &err_op, &err_msg_ptr, &child_vr)
        if rc == 0:
            if child_vr != NULL:
                # A child was produced somewhere in this program. Whether that is a
                # problem depends ENTIRELY on what the program finally RETURNS —
                # `child_vr != NULL` alone does not mean the result is an ARRAY.
                #
                # Final result IS an ARRAY (e.g. SPLIT(x) on its own): this
                # Python-Vector materialization path (_slot_to_pyobj) builds a Vector
                # from the arena DV*, which has nowhere to carry a child, so the
                # elements would be silently dropped. Fail loud — the engine's
                # ExprMultiProjectOperator path is where ARRAY results are supported.
                #
                # Final result is NOT an ARRAY (e.g. LENGTH(SPLIT(x)) -> INT64): the
                # ARRAY was a consumed INTERMEDIATE. draken_length_array reads only
                # the offsets — it needs no child, which is exactly why it composes
                # over a computed array where the element-reading kernels (SORT,
                # array containment) cannot. The child is genuinely unused, so freeing
                # it and returning the result is correct, not a silent drop. The old
                # guard tested only `child_vr != NULL` and so refused this case too.
                if dv_stack[0] != NULL and dv_stack[0].type == DRAKEN_ARRAY:
                    draken_vecresult_discard_c(child_vr)
                    raise NotImplementedError(
                        "evaluate_c_native: ARRAY function results are not supported "
                        "on the legacy Python-Vector evaluation path")
                draken_vecresult_discard_c(child_vr)
            # Gate guarantees the last op is a compute op → arena result, anchor None.
            return _slot_to_pyobj(dv_stack[0], None, arena)
        if rc == 4 or rc == 96:
            raise _kernel_error_exc(rc, err_msg_ptr)
        if rc == 1:
            raise TypeError("evaluate_c_native: NULL operand")
        if rc == 2:
            raise MemoryError("evaluate_c_native: arena alloc failed")
        # rc 3 (compare fast path declined) / 5 (unexpected string) / 99 (unknown):
        # the GIL VM handles these — clear the flag and fall back.
        bc.is_all_c_native = False
        return execute_bytecode(bc, morsel)
    finally:
        draken_frame_arena_destroy(arena)


# ---------------------------------------------------------------------------
# S3: nogil predicate/expression eval reading columns STRAIGHT from a CxxMorsel
# (no morsel._cxx_column PyObject). Columns resolve from pre-cached indices, the
# only Python-touching loads (LOAD_LIT_CONST) from pre-cached DV* — both built
# once under the GIL (schema + literals are stable). This is the primitive the
# genuine nogil filter/projection bodies (S3.2) call: the fill + inner run fully
# nogil over columns[idx].view, so the operator push can release the GIL.
# ---------------------------------------------------------------------------
cdef void _dv_fill_cache_cxx(
    BytecodeInstr* instrs, Py_ssize_t n_instrs,
    const CxxMorsel* m,
    const int* col_idx, DrakenVector** lit_dv,
    DrakenVector** dv_cache,
) noexcept nogil:
    """nogil: populate dv_cache[k] for the loads — column views straight from the
    CxxMorsel (columns[col_idx[k]].view), literals from lit_dv[k]."""
    cdef Py_ssize_t k
    cdef int opcode
    for k in range(n_instrs):
        opcode = instrs[k].opcode
        if opcode == BC_LOAD_COL:
            dv_cache[k] = <DrakenVector*>&m.columns[col_idx[k]].view
        elif opcode == BC_LOAD_LIT_CONST:
            dv_cache[k] = lit_dv[k]
        elif ((opcode == BC_CAST or opcode == BC_FUNCTION or opcode == BC_EXTRACTION)
                and (instrs[k].flags & BC_C_NATIVE_CHILD) != 0):
            # ARRAY child element vector, held by the column's VectorOwner —
            # NULL when the column has no child (kernel then fails loud).
            # BC_FUNCTION here is SORT's single-array-arg case, BC_EXTRACTION is
            # arr[i] (compiled_expression.pyx).
            dv_cache[k] = <DrakenVector*>cxx_column_child_vec(m, <uint32_t>col_idx[k])
        else:
            dv_cache[k] = NULL


cdef int _dv_cxx_resolve_caches(
    CompiledBytecode bc, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
) except -2:
    """GIL: resolve LOAD_COL column identity → column index in the CxxMorsel
    (compare bytes to m.names) and LOAD_LIT_CONST literal → DV*. Returns 0, or
    -1 if a column identity is not found (caller falls back to the Morsel path).
    Stable across morsels for a fixed pipeline schema → resolve once, reuse.
    """
    cdef Py_ssize_t k, ci, nn
    cdef BytecodeInstr* slot
    cdef bytes ident
    cdef bytes nm
    cdef object scalar_obj
    nn = <Py_ssize_t>m.names.size()
    for k in range(bc.count):
        slot = &bc.instrs[k]
        col_idx[k] = -1
        lit_dv[k] = NULL
        if slot.opcode == BC_LOAD_COL or (
                (slot.opcode == BC_CAST or slot.opcode == BC_FUNCTION)
                and (slot.flags & BC_C_NATIVE_CHILD) != 0):
            # BC_C_NATIVE_CHILD instructions (ARRAY->VARCHAR cast, SORT) carry
            # the ARRAY operand's identity so the cache fill can resolve the
            # owner-held child element vector.
            ident = <bytes>slot.column_identity
            for ci in range(nn):
                nm = m.names[ci]          # libcpp string → bytes (auto-convert)
                if nm == ident:
                    col_idx[k] = <int>ci
                    break
            if col_idx[k] < 0:
                return -1
        elif slot.opcode == BC_LOAD_LIT_CONST:
            scalar_obj = <object>slot.literal_obj
            lit_dv[k] = (<Vector>scalar_obj).unified()
    return 0


cdef inline DrakenVector** _dv_cache_for(
    Py_ssize_t count, DrakenVector** inline_cache, DrakenFrameArena* arena
) noexcept nogil:
    """Pick the dv_cache buffer for a program of ``count`` instructions.

    The spans keep a 256-slot inline array on the stack because it covers every
    ordinary expression and costs nothing. A LONGER program — a programmatically
    generated OR chain, a wide MERGE guard — takes an arena-backed cache instead.
    Before this, _dv_fill_cache_cxx wrote one entry per instruction straight off the
    end of the inline array and smashed the span's stack canary, aborting the worker
    with SIGABRT instead of failing the query.

    Returns NULL only on arena OOM; callers map that to rc 99, exactly as they do a
    failed draken_frame_arena_create."""
    if count <= 256:
        return inline_cache
    return <DrakenVector**>draken_frame_arena_alloc(
        arena, <size_t>count * sizeof(DrakenVector*))


# ---------------------------------------------------------------------------
# PoC 2026-10-08 — cache-sized expression tiling (EXPR_TILE_ROWS).
#
# A morsel is one row group (64K-262K rows), so every intermediate the DV* VM
# produces is 512 KB+ for an 8-byte type — past L1/L2. With EXPR_TILE_ROWS = T > 0
# the engine spans run the SAME VM over T-row sub-slices of the morsel: each tile's
# intermediates are T rows and live in a per-tile frame arena that is destroyed
# before the next tile, so the next tile's kernel outputs are malloc'd from the
# blocks just freed (cache-hot reuse). Each tile's result is written straight into
# the full-length output. T == 0 is the whole-morsel path, untouched. Default is
# per-arch at compile time (_EXPR_TILE_ROWS_DEFAULT): 4096 on x86-64, 0 elsewhere;
# the EXPR_TILE_ROWS env var overrides it.
#
# Eligibility is decided per morsel BEFORE anything runs (_tile_eligible): every
# loaded column must slice as a truthful view — identity-dense (data offset),
# constant (prefix of the zero selection) or a dict whose value count is below the
# tile length (selection offset, still a true dict) — and no instruction may read an
# ARRAY child. The one thing only a run can tell is the result type: tile 0's result
# must be BOOL or fixed-width with no ARRAY child; otherwise tile 0 is discarded and
# the morsel runs whole (counted in expr_tile_stats()["result_refused"]).
# Tiles start at multiples of T (T % 8 == 0, so validity/BOOL slices are byte
# aligned); the remainder joins the last tile, so every tile has >= T rows.
# ---------------------------------------------------------------------------
cdef extern from "core/buffers.h" nogil:
    int draken_is_constant(const DrakenVector* v)
    int draken_is_dict(const DrakenVector* v)
    int draken_type_is_string_storage(DrakenType t)

cdef extern from *:
    """
    static unsigned long long _expr_tile_ctr[7];
    static inline void _expr_tile_count(int k) {
        __atomic_fetch_add(&_expr_tile_ctr[k], 1ULL, __ATOMIC_RELAXED);
    }
    static inline unsigned long long _expr_tile_read(int k) {
        return __atomic_load_n(&_expr_tile_ctr[k], __ATOMIC_RELAXED);
    }
    /* Per-arch default, ruled 2026-10-09 from the 2026-10-08 A/B: x86-64 (i5-8500,
       256 KB L2/core) ran arithmetic chains 3-12% faster at 4096; Apple Silicon ran
       them 0-16% slower at every size. Everything not measured stays off. */
    #if defined(__x86_64__) || defined(_M_X64)
    #define _EXPR_TILE_ROWS_DEFAULT 4096
    #else
    #define _EXPR_TILE_ROWS_DEFAULT 0
    #endif
    static inline void _expr_tile_reset(void) {
        for (int k = 0; k < 7; ++k) __atomic_store_n(&_expr_tile_ctr[k], 0ULL, __ATOMIC_RELAXED);
    }
    """
    void _expr_tile_count(int k) noexcept nogil
    unsigned long long _expr_tile_read(int k) noexcept nogil
    void _expr_tile_reset() noexcept nogil
    int _EXPR_TILE_ROWS_DEFAULT

DEF _TILE_CTR_TILED = 0
DEF _TILE_CTR_SMALL = 1
DEF _TILE_CTR_INELIGIBLE = 2       # an input type/instruction cannot be sliced
DEF _TILE_CTR_RESULT_REFUSED = 3
DEF _TILE_CTR_BIG_DICT = 4          # a dict input with data_length >= tile length
DEF _TILE_CTR_NOT_IDENTITY = 5      # a non-dict, non-constant input without SEL_IDENTITY
DEF _TILE_CTR_SHAPE_KEPT = 6        # preserve_shape span with a compressed input

cdef uint32_t _EXPR_TILE_ROWS = 0


def set_expr_tile_rows(int rows):
    """Set the PoC tile length (0 = untiled). Must be 0 or a positive multiple of 8."""
    global _EXPR_TILE_ROWS
    if rows < 0 or (rows % 8) != 0:
        raise ValueError(f"EXPR_TILE_ROWS must be 0 or a positive multiple of 8, got {rows}")
    _EXPR_TILE_ROWS = <uint32_t>rows


def get_expr_tile_rows():
    return _EXPR_TILE_ROWS


def expr_tile_stats(bint reset=False):
    """Morsel counts per tiling outcome since the last reset (proves the knob moves)."""
    stats = {
        "tiled": _expr_tile_read(_TILE_CTR_TILED),
        "small": _expr_tile_read(_TILE_CTR_SMALL),
        "ineligible": _expr_tile_read(_TILE_CTR_INELIGIBLE),
        "result_refused": _expr_tile_read(_TILE_CTR_RESULT_REFUSED),
        "big_dict": _expr_tile_read(_TILE_CTR_BIG_DICT),
        "not_identity": _expr_tile_read(_TILE_CTR_NOT_IDENTITY),
        "shape_kept": _expr_tile_read(_TILE_CTR_SHAPE_KEPT),
    }
    if reset:
        _expr_tile_reset()
    return stats


import os as _os
set_expr_tile_rows(int(_os.environ.get("EXPR_TILE_ROWS", str(_EXPR_TILE_ROWS_DEFAULT))))


cdef int _tile_eligible(BytecodeInstr* instrs, Py_ssize_t count,
                        DrakenVector** dv_cache, uint32_t tile,
                        bint dense_inputs_only) noexcept nogil:
    """-1 = every input slices; otherwise the _TILE_CTR_* reason it does not."""
    cdef Py_ssize_t k
    cdef const DrakenVector* v
    for k in range(count):
        if (instrs[k].flags & BC_C_NATIVE_CHILD) != 0:
            return _TILE_CTR_INELIGIBLE
        if instrs[k].opcode != BC_LOAD_COL:
            continue
        v = dv_cache[k]
        if v == NULL or v.type == DRAKEN_ARRAY or v.type == DRAKEN_VECTOR_FP16:
            return _TILE_CTR_INELIGIBLE
        if (v.flags & DRAKEN_SEL_IDENTITY) != 0:
            if (v.type == DRAKEN_BOOL or v.type == DRAKEN_NULL
                    or draken_type_is_string_storage(v.type)
                    or _dv_result_elem_size(v.type) != 0):
                continue
            return _TILE_CTR_INELIGIBLE
        if draken_is_constant(v) or draken_is_dict(v):
            if dense_inputs_only:
                return _TILE_CTR_SHAPE_KEPT
            if draken_is_constant(v) or v.data_length < tile:
                continue
            return _TILE_CTR_BIG_DICT
        return _TILE_CTR_NOT_IDENTITY
    return -1


cdef inline void _tile_view(const DrakenVector* base, uint32_t off, uint32_t t,
                            DrakenVector* out, DrakenStringArena* hdr) noexcept nogil:
    """A t-row view of rows [off, off+t) of `base`; shapes per _tile_eligible."""
    out[0] = base[0]
    out.length = t
    if base.validity != NULL:
        out.validity = base.validity + (off >> 3)
    if draken_is_constant(base):
        out.flags = base.flags & ~DRAKEN_DICT_CODES_DENSE
        return
    if (base.flags & DRAKEN_SEL_IDENTITY) != 0:
        out.data_length = t
        if base.data == NULL or base.type == DRAKEN_NULL:
            return
        if base.type == DRAKEN_BOOL:
            out.data = <uint8_t*>base.data + (off >> 3)
        elif draken_type_is_string_storage(base.type):
            hdr[0] = (<DrakenStringArena*>base.data)[0]
            hdr.slots = hdr.slots + off
            hdr.length = t
            if hdr.null_bitmap != NULL:
                hdr.null_bitmap = hdr.null_bitmap + (off >> 3)
            hdr.owns_buffers = 0
            out.data = <void*>hdr
        else:
            out.data = <uint8_t*>base.data + <size_t>off * _dv_result_elem_size(base.type)
        return
    # dict with data_length < t: codes offset, value array shared
    out.selection = base.selection + off
    out.flags = base.flags & ~DRAKEN_DICT_CODES_DENSE


cdef inline int _tile_emit(const DrakenVector* r, uint32_t off, uint32_t t,
                           uint32_t num_rows, size_t es, bint bulk_identity,
                           uint8_t* out_data, uint8_t** out_validity) noexcept nogil:
    """Write one tile's result into rows [off, off+t) of the full-length output.
    The fixed-width copy mirrors the untiled boundary it replaces, so an A/B measures
    tiling and not a copy change: _dv_copy_result_dense gathers element by element,
    _dv_copy_result_preserve_shape (bulk_identity) memcpys an identity result."""
    cdef uint32_t i, phys
    cdef size_t nb = (<size_t>t + 7) >> 3
    cdef uint8_t* dst
    cdef const uint8_t* sbits
    if r.type == DRAKEN_BOOL:
        dst = out_data + (off >> 3)
        if (r.flags & DRAKEN_SEL_IDENTITY) != 0:
            memcpy(dst, r.data, nb)
        else:
            memset(dst, 0, nb)
            sbits = <const uint8_t*>r.data
            for i in range(t):
                phys = r.selection[i]
                if (sbits[phys >> 3] >> (phys & 7)) & 1:
                    dst[i >> 3] |= <uint8_t>(1 << (i & 7))
        if t & 7:
            dst[nb - 1] &= <uint8_t>((1 << (t & 7)) - 1)
    elif bulk_identity and (r.flags & DRAKEN_SEL_IDENTITY) != 0:
        memcpy(out_data + <size_t>off * es, r.data, <size_t>t * es)
    else:
        for i in range(t):
            memcpy(out_data + (<size_t>off + i) * es,
                   <const uint8_t*>r.data + <size_t>r.selection[i] * es, es)
    if r.validity != NULL:
        if out_validity[0] == NULL:
            out_validity[0] = <uint8_t*>draken_malloc(((<size_t>num_rows + 7) >> 3))
            if out_validity[0] == NULL:
                return 2
            memset(out_validity[0], 0xFF, ((<size_t>num_rows + 7) >> 3))
        memcpy(out_validity[0] + (off >> 3), r.validity, nb)
    return 0


cdef int _dv_try_tiled(
    BytecodeInstr* instrs, Py_ssize_t count, DrakenVector** dv_cache,
    uint32_t num_rows, DrakenFrameArena* outer, bint dense_inputs_only,
    DrakenVector* out, int* err_op, const char** err_msg,
) noexcept nogil:
    """Run the program tile by tile into a full-length dense result.

    Returns -1 when tiling does not apply (knob off, morsel < 2 tiles, ineligible
    inputs, or tile 0's result type is not BOOL/fixed-width) — nothing is left
    allocated and the caller runs the whole-morsel path. 0 → `out` is a dense
    identity-selection vector whose data/validity are draken_malloc'd and OWNED BY
    THE CALLER (selection is the shared global identity). Otherwise the
    c_execute_dv_inner rc with err_op/err_msg set (98 = a later tile changed result
    type, 97 = a later tile produced an ARRAY child)."""
    cdef uint32_t T = _EXPR_TILE_ROWS
    if T == 0:
        return -1
    if num_rows < 2 * T:
        _expr_tile_count(_TILE_CTR_SMALL)
        return -1
    cdef int why = _tile_eligible(instrs, count, dv_cache, T, dense_inputs_only)
    if why >= 0:
        _expr_tile_count(why)
        return -1
    cdef uint32_t ntiles = num_rows // T
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef DrakenVector* views = <DrakenVector*>draken_frame_arena_alloc(
        outer, <size_t>count * sizeof(DrakenVector))
    cdef DrakenStringArena* hdrs = <DrakenStringArena*>draken_frame_arena_alloc(
        outer, <size_t>count * sizeof(DrakenStringArena))
    cdef DrakenVector** tcache = <DrakenVector**>draken_frame_arena_alloc(
        outer, <size_t>count * sizeof(DrakenVector*))
    cdef uint8_t* out_data = NULL
    cdef uint8_t* out_validity = NULL
    cdef DrakenType out_type = DRAKEN_NULL
    cdef size_t es = 0
    cdef uint32_t tile, off, t
    cdef Py_ssize_t k
    cdef int rc = 0
    cdef VecResult* child = NULL
    cdef DrakenFrameArena* arena
    cdef const DrakenVector* r
    if views == NULL or hdrs == NULL or tcache == NULL:
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    for k in range(count):
        tcache[k] = dv_cache[k]
    for tile in range(ntiles):
        off = tile * T
        t = T if tile + 1 < ntiles else num_rows - off
        for k in range(count):
            if instrs[k].opcode == BC_LOAD_COL:
                _tile_view(dv_cache[k], off, t, &views[k], &hdrs[k])
                tcache[k] = &views[k]
        arena = draken_frame_arena_create()
        if arena == NULL:
            err_op[0] = -99
            err_msg[0] = NULL
            rc = 99
            break
        rc = c_execute_dv_inner(instrs, count, tcache, dv_stack, dv_store, arena,
                                (<Py_ssize_t>t + 7) >> 3, t, err_op, err_msg, &child)
        if rc != 0:
            if child != NULL:
                draken_vecresult_discard_c(child)
            draken_frame_arena_destroy(arena)
            break
        r = dv_stack[0]
        if tile == 0:
            if r.type == DRAKEN_BOOL:
                es = 0
            else:
                es = _dv_result_elem_size(r.type)
            if child != NULL or (r.type != DRAKEN_BOOL and es == 0):
                if child != NULL:
                    draken_vecresult_discard_c(child)
                draken_frame_arena_destroy(arena)
                _expr_tile_count(_TILE_CTR_RESULT_REFUSED)
                return -1
            out_type = r.type
            if out_type == DRAKEN_BOOL:
                out_data = <uint8_t*>draken_malloc(((<size_t>num_rows + 7) >> 3))
            else:
                out_data = <uint8_t*>draken_malloc(<size_t>num_rows * es)
            if out_data == NULL:
                draken_frame_arena_destroy(arena)
                err_op[0] = -99
                err_msg[0] = NULL
                return 99
        elif child != NULL:
            draken_vecresult_discard_c(child)
            draken_frame_arena_destroy(arena)
            err_op[0] = -97
            err_msg[0] = NULL
            rc = 97
            break
        elif r.type != out_type:
            draken_frame_arena_destroy(arena)
            err_op[0] = -98
            err_msg[0] = "expression tiling: result type changed between tiles"
            rc = 98
            break
        rc = _tile_emit(r, off, t, num_rows, es, dense_inputs_only, out_data, &out_validity)
        draken_frame_arena_destroy(arena)
        if rc != 0:
            err_op[0] = -99
            err_msg[0] = NULL
            break
    if rc != 0:
        draken_free(out_data)
        draken_free(out_validity)
        return rc
    _expr_tile_count(_TILE_CTR_TILED)
    out.data = out_data
    out.selection = draken_identity_sel(num_rows)
    out.data_length = num_rows
    out.length = num_rows
    out.validity = out_validity
    out.type = out_type
    out.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION
    return 0


cdef int _dv_filter_span_cxx(
    BytecodeInstr* instrs, int count, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
    CxxMorsel** out_filtered, int* err_op, const char** err_msg,
) noexcept nogil:
    """Pure-nogil filter span: fill the DV* cache from a PRE-RESOLVED (col_idx,
    lit_dv) pair, evaluate the predicate, and gather the surviving rows via
    cxx_mask_c. Owns its frame arena (created/destroyed here). Returns the
    c_execute rc: 0 → ``*out_filtered`` is a NEW owned CxxMorsel; 4 → kernel error
    (``*err_msg`` set, see c_execute_dv_inner); 99 → arena OOM; other → not
    applicable. (col_idx, lit_dv) are resolved ONCE by the caller via
    _dv_cxx_resolve_caches and reused across morsels — that resolve is the only
    GIL-needing step; this span has no PyObject access, so a converted operator
    can call it inside `with nogil`."""
    cdef DrakenVector* dv_cache_inline[256]
    cdef DrakenVector** dv_cache
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef Py_ssize_t num_rows = m.num_rows()
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef int rc
    cdef DrakenFrameArena* arena = draken_frame_arena_create()
    cdef VecResult* child_vr = NULL
    cdef DrakenVector tiled
    if arena == NULL:
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    dv_cache = _dv_cache_for(count, dv_cache_inline, arena)
    if dv_cache == NULL:
        draken_frame_arena_destroy(arena)
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    _dv_fill_cache_cxx(instrs, count, m, col_idx, lit_dv, dv_cache)
    rc = _dv_try_tiled(instrs, count, dv_cache, <uint32_t>num_rows, arena, False,
                       &tiled, err_op, err_msg)
    if rc == 0:
        out_filtered[0] = cxx_mask_c(m, &tiled)
        draken_free(tiled.data)
        draken_free(tiled.validity)
        draken_frame_arena_destroy(arena)
        return 0
    if rc != -1:
        draken_frame_arena_destroy(arena)
        return rc
    rc = c_execute_dv_inner(instrs, count, dv_cache, dv_stack, dv_store,
                            arena, nbytes, <uint32_t>num_rows, err_op, err_msg, &child_vr)
    if rc == 0 and child_vr != NULL:
        # Structurally unreachable: the binder only admits this span for a
        # BOOL-final predicate, and no kernel reads an ARRAY operand, so a
        # non-NULL child here means that invariant broke. Fail loud, don't leak.
        draken_vecresult_discard_c(child_vr)
        err_op[0] = -97
        err_msg[0] = NULL
        rc = 97
    elif rc == 0:
        out_filtered[0] = cxx_mask_c(m, dv_stack[0])
    draken_frame_arena_destroy(arena)
    return rc


cdef int _dv_filter_span_with_consts_cxx(
    BytecodeInstr* instrs, int count, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
    int32_t* const_col_idx, DrakenVector** const_scalar_dv, uint32_t n_consts,
    CxxMorsel** out_filtered, int* err_op, const char** err_msg,
) noexcept nogil:
    """_dv_filter_span_cxx twin for a FilterNode with `IDENTIFIER = LITERAL`
    const-replacements: gathers via cxx_mask_with_consts_c instead of cxx_mask_c,
    so a column known-constant on every surviving row (e.g. WHERE col = 7) is
    broadcast O(1) from a pre-resolved scalar DrakenVector* instead of being taken
    and then discarded. (const_col_idx, const_scalar_dv) are resolved ONCE by the
    caller (FilterNode._flt_resolve_consts) and reused across morsels — same
    resolve-once contract as (col_idx, lit_dv)."""
    cdef DrakenVector* dv_cache_inline[256]
    cdef DrakenVector** dv_cache
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef Py_ssize_t num_rows = m.num_rows()
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef int rc
    cdef DrakenFrameArena* arena = draken_frame_arena_create()
    cdef VecResult* child_vr = NULL
    cdef DrakenVector tiled
    if arena == NULL:
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    dv_cache = _dv_cache_for(count, dv_cache_inline, arena)
    if dv_cache == NULL:
        draken_frame_arena_destroy(arena)
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    _dv_fill_cache_cxx(instrs, count, m, col_idx, lit_dv, dv_cache)
    rc = _dv_try_tiled(instrs, count, dv_cache, <uint32_t>num_rows, arena, False,
                       &tiled, err_op, err_msg)
    if rc == 0:
        out_filtered[0] = cxx_mask_with_consts_c(
            m, &tiled, const_col_idx, <const DrakenVector* const*>const_scalar_dv, n_consts)
        draken_free(tiled.data)
        draken_free(tiled.validity)
        draken_frame_arena_destroy(arena)
        return 0
    if rc != -1:
        draken_frame_arena_destroy(arena)
        return rc
    rc = c_execute_dv_inner(instrs, count, dv_cache, dv_stack, dv_store,
                            arena, nbytes, <uint32_t>num_rows, err_op, err_msg, &child_vr)
    if rc == 0 and child_vr != NULL:
        # See _dv_filter_span_cxx — structurally unreachable, fail loud not leak.
        draken_vecresult_discard_c(child_vr)
        err_op[0] = -97
        err_msg[0] = NULL
        rc = 97
    elif rc == 0:
        out_filtered[0] = cxx_mask_with_consts_c(
            m, dv_stack[0], const_col_idx, <const DrakenVector* const*>const_scalar_dv, n_consts)
    draken_frame_arena_destroy(arena)
    return rc


# ── Pass-1 worker predicate (Q24 latmat) ────────────────────────────────────────
# Run the c-native predicate over decoded pass-1 columns supplied as a DrakenVector*
# ARRAY (not a CxxMorsel) and emit the survivor bitmap. Called from the rugo
# io_pipeline decode workers through an opaque C fn-ptr handed over at registration
# (get_pass1_eval_fn_ptr) — rugo stays opteryx-free (only draken's DrakenVector and
# this pointer cross). Pure nogil, no PyObject. Sources column DVs from `cols`
# and produces only the mask (the main thread
# applies it to the shipped survivor columns for top-N).

ctypedef struct Pass1PredCtx:
    BytecodeInstr*  instrs
    int             count
    const int*      col_idx    # per-instr: index into the `cols` array (BC_LOAD_COL)
    DrakenVector**  lit_dv      # per-instr: literal DV* (BC_LOAD_LIT_CONST), else NULL
    # per pass-1 COLUMN (indexed by col_idx's values, not by instr): the DrakenType
    # the PLAN says that column is. The producer of `cols` tags its views from what
    # it can see in the decoded buffers, which is the physical layout — rugo, being
    # opteryx-free by contract, cannot know that a byte_array chunk is VARBINARY
    # rather than VARCHAR. Only the plan knows. So the tag is carried here and
    # stamped on entry: the predicate always sees each column tagged as the plan
    # says it is, which is exactly what the serial fallback passes.
    const int*      col_type


ctypedef int (*Pass1EvalFn)(void*, DrakenVector**, int, uint32_t, uint8_t*) noexcept nogil


cdef int opteryx_pass1_predicate_eval(void* ctx, DrakenVector** cols, int ncols,
                                      uint32_t num_rows, uint8_t* out_mask) noexcept nogil:
    """Opaque C-ABI entry for the rugo decode worker. `ctx` is a Pass1PredCtx*
    (resolved once on the main thread, kept alive by the scan). `cols[i]` is decoded
    pass-1 predicate column i; `out_mask` is caller-owned, nbytes=(num_rows+7)//8.
    Returns 0 → out_mask is the survivor bitmap (data-bit AND validity); else the
    c_execute_dv_inner rc (worker fails loud).

    `cols[i].type` is advisory: the caller tags its views from the physical buffers,
    which is all a producer outside the plan can see. `ctx.col_type[i]` is the tag
    the PLAN gives that column, and it wins — see Pass1PredCtx. A caller that
    already tags correctly (the serial fallbacks, which pass the consumer's own
    morsel columns) matches it and no copy happens."""
    cdef Pass1PredCtx* c = <Pass1PredCtx*>ctx
    cdef BytecodeInstr* instrs = c.instrs
    cdef int count = c.count
    cdef const int* col_idx = c.col_idx
    cdef DrakenVector** lit_dv = c.lit_dv
    cdef const int* col_type = c.col_type
    cdef Py_ssize_t nbytes = <Py_ssize_t>((num_rows + 7) >> 3)
    cdef DrakenVector* dv_cache[256]
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    # Retag scratch, one slot per INSTRUCTION so it shares dv_cache's existing 256
    # bound and adds no new failure mode. Only touched for a load whose incoming tag
    # disagrees with the plan's; every other load points straight at the caller's
    # vector, untouched.
    cdef DrakenVector  col_store[256]
    cdef int ci = 0
    cdef int want = 0
    cdef int rc = 0
    cdef int err_op = 0
    cdef int opcode = 0
    cdef const char* err_msg = NULL
    cdef DrakenVector* mask_dv = NULL
    cdef uint8_t* dense = NULL
    cdef Py_ssize_t bi = 0
    cdef Py_ssize_t k = 0
    cdef DrakenFrameArena* arena = NULL
    cdef VecResult* child_vr = NULL
    cdef DrakenVector tiled
    if count > 256 or num_rows == 0:
        return -1
    arena = draken_frame_arena_create()
    if arena == NULL:
        return 99
    for k in range(count):
        opcode = instrs[k].opcode
        if opcode == BC_LOAD_COL:
            ci = col_idx[k]
            want = col_type[ci]
            if cols[ci].type == <DrakenType>want:
                dv_cache[k] = cols[ci]
            else:
                # The producer tagged this view from the physical buffers and got a
                # different type than the plan's. Stamp the plan's tag on a copy —
                # the caller's vector is left alone, and the predicate now runs over
                # exactly the operands the serial fallback would hand it.
                col_store[k] = cols[ci][0]
                col_store[k].type = <DrakenType>want
                dv_cache[k] = &col_store[k]
        elif opcode == BC_LOAD_LIT_CONST:
            dv_cache[k] = lit_dv[k]
        else:
            dv_cache[k] = NULL
    rc = _dv_try_tiled(instrs, count, dv_cache, num_rows, arena, False,
                       &tiled, &err_op, &err_msg)
    if rc == 0:
        memcpy(out_mask, tiled.data, <size_t>nbytes)
        if tiled.validity != NULL:
            c_bitmap_and_inplace(out_mask, tiled.validity, <size_t>nbytes)
        draken_free(tiled.data)
        draken_free(tiled.validity)
        draken_frame_arena_destroy(arena)
        return 0
    if rc != -1:
        draken_frame_arena_destroy(arena)
        return rc
    rc = c_execute_dv_inner(instrs, count, dv_cache, dv_stack, dv_store,
                            arena, nbytes, num_rows, &err_op, &err_msg, &child_vr)
    if rc == 0 and child_vr != NULL:
        # See _dv_filter_span_cxx — structurally unreachable, fail loud not leak.
        draken_vecresult_discard_c(child_vr)
        draken_frame_arena_destroy(arena)
        return 97
    if rc == 0:
        mask_dv = dv_stack[0]
        dense = _ensure_dense_bitmap_c(mask_dv, nbytes, num_rows, arena)
        if dense == NULL:
            draken_frame_arena_destroy(arena)
            return 99
        memcpy(out_mask, dense, <size_t>nbytes)
        if mask_dv.validity != NULL:
            c_bitmap_and_inplace(out_mask, mask_dv.validity, <size_t>nbytes)
    draken_frame_arena_destroy(arena)
    return rc


cpdef size_t get_pass1_eval_fn_ptr():
    """Address of opteryx_pass1_predicate_eval as an int, for the scan to hand to the
    rugo io_pipeline as an opaque predicate callback."""
    cdef Pass1EvalFn fn = opteryx_pass1_predicate_eval
    return <size_t><void*>fn


cdef class Pass1PredResolver:
    """Resolves a CompiledBytecode predicate to a Pass1PredCtx once (GIL) and keeps
    everything the worker callback needs alive for the scan's life: the bytecode
    (instrs + literal Vectors), the owned col_idx / lit_dv / col_type arrays.
    `col_idx[k]` maps each BC_LOAD_COL to its position in `col_names` (the predicate's
    columns in first-seen order); `col_names` are PHYSICAL names the rugo worker
    matches against its decoded result.column_names. `identity_to_type` gives each
    identity's plan DrakenType, which the eval entry stamps on the incoming view —
    the producer of those views cannot know it (see Pass1PredCtx.col_type). Only
    usable when the predicate is all-c-native."""
    cdef Pass1PredCtx ctx
    cdef CompiledBytecode _bc
    cdef int* _col_idx
    cdef DrakenVector** _lit_dv
    cdef int* _col_type
    cdef list _col_names
    cdef list _lit_anchors

    def __cinit__(self, CompiledBytecode bc, dict identity_to_physical,
                  dict identity_to_type):
        cdef int count = bc.count
        cdef int k
        cdef int pos
        cdef BytecodeInstr* instrs = bc.instrs
        cdef bytes ident
        cdef object lit_obj
        cdef dict ident_pos = {}
        self._bc = bc
        self._col_names = []
        self._lit_anchors = []
        self._col_idx = <int*>malloc(<size_t>count * sizeof(int))
        self._lit_dv = <DrakenVector**>malloc(<size_t>count * sizeof(void*))
        # Indexed by column POSITION, of which there are at most `count` (one per
        # instruction), so this bound is exact-or-generous by construction.
        self._col_type = <int*>malloc(<size_t>count * sizeof(int))
        if self._col_idx == NULL or self._lit_dv == NULL or self._col_type == NULL:
            raise MemoryError("Pass1PredResolver: malloc failed")
        for k in range(count):
            self._col_idx[k] = -1
            self._lit_dv[k] = NULL
            self._col_type[k] = 0
            if instrs[k].opcode == BC_LOAD_COL:
                ident = <bytes>instrs[k].column_identity
                if ident not in ident_pos:
                    pos = len(self._col_names)
                    ident_pos[ident] = pos
                    self._col_names.append(identity_to_physical[ident])
                    # A missing entry is a resolver bug, not a shape to tolerate: the
                    # eval entry would then stamp 0 (not a DrakenType) and every load
                    # of the column would copy-and-mistag. KeyError here, loudly.
                    self._col_type[pos] = <int>identity_to_type[ident]
                self._col_idx[k] = <int>ident_pos[ident]
            elif instrs[k].opcode == BC_LOAD_LIT_CONST:
                lit_obj = <object>instrs[k].literal_obj
                self._lit_anchors.append(lit_obj)
                self._lit_dv[k] = (<Vector>lit_obj).unified()
        self.ctx.instrs = instrs
        self.ctx.count = count
        self.ctx.col_idx = self._col_idx
        self.ctx.lit_dv = self._lit_dv
        self.ctx.col_type = self._col_type

    @property
    def col_names(self):
        return list(self._col_names)

    cpdef size_t ctx_ptr(self):
        return <size_t><void*>&self.ctx

    def __dealloc__(self):
        if self._col_idx != NULL:
            free(self._col_idx)
        if self._lit_dv != NULL:
            free(self._lit_dv)
        if self._col_type != NULL:
            free(self._col_type)


cdef size_t _dv_result_elem_size(DrakenType t) noexcept nogil:
    """Byte width of one fixed-width value; 0 for bit-packed BOOL / unsupported."""
    if t == DRAKEN_INT8 or t == DRAKEN_UINT8:
        return 1
    if t == DRAKEN_INT16 or t == DRAKEN_UINT16:
        return 2
    if (t == DRAKEN_INT32 or t == DRAKEN_UINT32 or t == DRAKEN_FLOAT32
            or t == DRAKEN_DATE32 or t == DRAKEN_TIME32):
        return 4
    if (t == DRAKEN_INT64 or t == DRAKEN_UINT64 or t == DRAKEN_FLOAT64 or t == DRAKEN_DECIMAL
            or t == DRAKEN_TIMESTAMP64 or t == DRAKEN_TIME64):
        return 8
    if t == DRAKEN_DECIMAL128 or t == DRAKEN_INTERVAL:
        return 16   # INTERVAL = {int64 months, int64 us}, a plain 16-byte value
    return 0


cdef int _dv_copy_result_string(
    const DrakenVector* src,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena,
) noexcept nogil:
    """Deep-copy a string result into a caller-owned [DrakenStringArena header |
    slots] block plus a SEPARATELY allocated byte arena — `data` still points at the
    header, exactly what draken's kernels read (buffers.h contract); `sa_out.arena`
    points at that second allocation instead of into the block.

    The arena travels out on ``out_arena`` (NULL when every slot is inline). This is
    the ExprEvalFn twin of VecResult::arena (vec_result.h): the result owns TWO
    buffers, not one, so the arena bytes are built where they land instead of being
    copied into a consolidated block. A caller that ignores ``out_arena`` leaks it —
    the whole hazard, which is why every call site takes it."""
    cdef uint32_t n = src.length
    cdef uint32_t alloc_n = n if n > 0 else 1
    cdef const DrakenStringArena* sa_in = <const DrakenStringArena*>src.data
    cdef const DrakenStringSlot* slot
    cdef size_t total_arena = 0
    cdef uint32_t i, slen
    cdef bint row_ok
    for i in range(n):
        if src.validity != NULL and not ((src.validity[i >> 3] >> (i & 7)) & 1):
            continue
        slot = &sa_in.slots[src.selection[i]]
        if not str_is_inline(slot):
            total_arena += str_length(slot)
    cdef size_t slots_off = sizeof(DrakenStringArena)
    cdef size_t blk_bytes = slots_off + <size_t>alloc_n * sizeof(DrakenStringSlot)
    cdef uint8_t* blk = <uint8_t*>draken_malloc(blk_bytes)
    if blk == NULL:
        return -1
    cdef uint8_t* arena_bytes = NULL
    if total_arena > 0:
        arena_bytes = <uint8_t*>draken_malloc(total_arena)
        if arena_bytes == NULL:
            draken_free(blk)
            return -1
    cdef DrakenStringArena* sa_out = <DrakenStringArena*>blk
    cdef DrakenStringSlot* dst = <DrakenStringSlot*>(blk + slots_off)
    sa_out.slots = dst
    sa_out.arena = arena_bytes
    sa_out.length = n
    sa_out.arena_used = total_arena
    sa_out.arena_cap = total_arena
    sa_out.null_bitmap = NULL
    sa_out.owns_buffers = 0     # block AND arena are owned by the caller's VectorOwner
    sa_out.payloads_elided = 0
    sa_out.type = src.type
    cdef size_t arena_pos = 0
    for i in range(n):
        row_ok = src.validity == NULL or ((src.validity[i >> 3] >> (i & 7)) & 1)
        if not row_ok:
            memset(&dst[i], 0, sizeof(DrakenStringSlot))
            continue
        slot = &sa_in.slots[src.selection[i]]
        if str_is_inline(slot):
            memcpy(&dst[i], slot, sizeof(DrakenStringSlot))
        else:
            slen = str_length(slot)
            memcpy(arena_bytes + arena_pos, str_data(slot, sa_in.arena), slen)
            str_clone_with_offset(&dst[i], slot, <uint32_t>arena_pos)
            arena_pos += slen
    cdef size_t vbytes = (<size_t>n + 7) >> 3
    cdef uint8_t* validity = NULL
    if src.validity != NULL:
        validity = <uint8_t*>draken_malloc(vbytes if vbytes > 0 else 1)
        if validity == NULL:
            draken_free(blk)
            draken_free(arena_bytes)
            return -1
        memcpy(validity, src.validity, vbytes if vbytes > 0 else 1)
    cdef uint32_t* sel = <uint32_t*>draken_malloc(<size_t>alloc_n * sizeof(uint32_t))
    if sel == NULL:
        draken_free(blk)
        draken_free(arena_bytes)
        if validity != NULL:
            draken_free(validity)
        return -1
    for i in range(n):
        sel[i] = i
    out_vec.data = blk
    out_vec.selection = sel
    out_vec.data_length = n
    out_vec.length = n
    out_vec.validity = validity
    out_vec.type = src.type
    out_vec.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION
    out_data[0] = blk
    out_validity[0] = validity
    out_sel[0] = sel
    out_arena[0] = arena_bytes
    return 0


cdef int _dv_copy_result_string_preserve(
    const DrakenVector* src,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena,
) noexcept nogil:
    """SHAPE-PRESERVING twin of _dv_copy_result_string: deep-copy the src.data_length
    PHYSICAL string values into a K-slot canonical block, then carry the input's
    selection (length codes) and per-logical-row validity onto the result. Dense stays
    dense (k == length), constant stays constant (k == 1), dict stays dict (k < length)
    — no gather/force-expand. Used at the ExprProject boundary for computed columns
    that feed a compression-aware consumer (GROUP BY / DISTINCT key).

    The byte arena is a SECOND caller-owned allocation handed back on ``out_arena``,
    exactly as in _dv_copy_result_string — see that docstring for the ownership."""
    cdef uint32_t n = src.length
    cdef uint32_t k = src.data_length
    cdef uint32_t alloc_k = k if k > 0 else 1
    cdef uint32_t alloc_n = n if n > 0 else 1
    cdef const DrakenStringArena* sa_in = <const DrakenStringArena*>src.data
    cdef const DrakenStringSlot* slot
    cdef size_t total_arena = 0
    cdef uint32_t j, slen
    for j in range(k):
        slot = &sa_in.slots[j]
        if not str_is_inline(slot):
            total_arena += str_length(slot)
    cdef size_t slots_off = sizeof(DrakenStringArena)
    cdef size_t blk_bytes = slots_off + <size_t>alloc_k * sizeof(DrakenStringSlot)
    cdef uint8_t* blk = <uint8_t*>draken_malloc(blk_bytes)
    if blk == NULL:
        return -1
    cdef uint8_t* arena_bytes = NULL
    if total_arena > 0:
        arena_bytes = <uint8_t*>draken_malloc(total_arena)
        if arena_bytes == NULL:
            draken_free(blk)
            return -1
    cdef DrakenStringArena* sa_out = <DrakenStringArena*>blk
    cdef DrakenStringSlot* dst = <DrakenStringSlot*>(blk + slots_off)
    sa_out.slots = dst
    sa_out.arena = arena_bytes
    sa_out.length = k
    sa_out.arena_used = total_arena
    sa_out.arena_cap = total_arena
    sa_out.null_bitmap = NULL
    sa_out.owns_buffers = 0     # block AND arena are owned by the caller's VectorOwner
    sa_out.payloads_elided = 0
    sa_out.type = src.type
    cdef size_t arena_pos = 0
    for j in range(k):
        slot = &sa_in.slots[j]
        if str_is_inline(slot):
            memcpy(&dst[j], slot, sizeof(DrakenStringSlot))
        else:
            slen = str_length(slot)
            memcpy(arena_bytes + arena_pos, str_data(slot, sa_in.arena), slen)
            str_clone_with_offset(&dst[j], slot, <uint32_t>arena_pos)
            arena_pos += slen
    cdef size_t vbytes = (<size_t>n + 7) >> 3
    cdef uint8_t* validity = NULL
    if src.validity != NULL:
        validity = <uint8_t*>draken_malloc(vbytes if vbytes > 0 else 1)
        if validity == NULL:
            draken_free(blk)
            draken_free(arena_bytes)
            return -1
        memcpy(validity, src.validity, vbytes if vbytes > 0 else 1)
    cdef uint32_t* sel = <uint32_t*>draken_malloc(<size_t>alloc_n * sizeof(uint32_t))
    if sel == NULL:
        draken_free(blk)
        draken_free(arena_bytes)
        if validity != NULL:
            draken_free(validity)
        return -1
    memcpy(sel, src.selection, <size_t>n * sizeof(uint32_t))
    out_vec.data = blk
    out_vec.selection = sel
    out_vec.data_length = k
    out_vec.length = n
    out_vec.validity = validity
    out_vec.type = src.type
    # Trust the shape hint only for a genuinely dense result (k == n); otherwise 0 =
    # "don't know" = uniform path (matches _dv_vecresult_adopt_c's flag discipline).
    out_vec.flags = src.flags if k == n else 0
    out_data[0] = blk
    out_validity[0] = validity
    out_sel[0] = sel
    out_arena[0] = arena_bytes
    return 0


cdef int _dv_copy_result_preserve_shape(
    const DrakenVector* src,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena, uint32_t vec_dim = 0,
) noexcept nogil:
    """SHAPE-PRESERVING twin of _dv_copy_result_dense: deep-copy the src.data_length
    PHYSICAL values (NOT a per-logical-row gather) plus the input's selection and
    validity into fresh caller-owned buffers, keeping the input's encoding. The arena
    is destroyed right after this returns — nothing may alias it. Fixed-width + BOOL +
    string/VARIANT only; returns -1 otherwise so the caller fails loud."""
    # Only the string family owns a separate arena; every other arm — including the
    # early -1 returns below — leaves it NULL ("the arena is wherever `data` says it
    # is", vec_result.h).
    out_arena[0] = NULL
    cdef uint32_t n = src.length
    cdef uint32_t k = src.data_length
    cdef uint32_t alloc_k = k if k > 0 else 1
    cdef uint32_t alloc_n = n if n > 0 else 1
    cdef size_t es = _dv_result_elem_size(src.type)
    # See _dv_copy_result_dense: a VECTOR_FP16 value is a dim-wide fp16 row whose
    # width only the plan knows.
    if src.type == DRAKEN_VECTOR_FP16:
        if vec_dim == 0:
            return -1
        es = <size_t>vec_dim * 2
    cdef size_t kbytes = (<size_t>k + 7) >> 3
    cdef size_t vbytes = (<size_t>n + 7) >> 3
    cdef uint8_t* validity = NULL
    cdef uint32_t* sel
    cdef uint32_t i
    cdef void* data

    # VARIANT is German-string storage (buffers.h) — same slot/arena layout as the
    # VARCHAR family. It is the result type of `->`.
    if (src.type == DRAKEN_VARCHAR or src.type == DRAKEN_NVARCHAR
            or src.type == DRAKEN_VARBINARY or src.type == DRAKEN_VARIANT):
        return _dv_copy_result_string_preserve(src, out_vec, out_data, out_validity,
                                               out_sel, out_arena)

    if src.type == DRAKEN_NULL:
        # Self-describing null (buffers.h): no data, no validity — nothing to copy.
        data = NULL
    elif src.type == DRAKEN_BOOL:
        # Physical bits are indexed by physical position (selection[i]); copy the
        # K-bit block verbatim so selection[i] still addresses the right bit.
        data = draken_malloc(kbytes if kbytes > 0 else 1)
        if data == NULL:
            return -1
        memcpy(data, src.data, kbytes if kbytes > 0 else 1)
    else:
        if es == 0:
            return -1   # strings/arrays/decimal128 — not a c-native fixed result
        data = draken_malloc(<size_t>alloc_k * es)
        if data == NULL:
            return -1
        # Copy the K PHYSICAL values contiguously (no gather); selection maps rows.
        memcpy(data, src.data, <size_t>k * es)

    if src.validity != NULL:
        validity = <uint8_t*>draken_malloc(vbytes if vbytes > 0 else 1)
        if validity == NULL:
            if data != NULL:
                draken_free(data)
            return -1
        memcpy(validity, src.validity, vbytes if vbytes > 0 else 1)

    sel = <uint32_t*>draken_malloc(<size_t>alloc_n * sizeof(uint32_t))
    if sel == NULL:
        if data != NULL:
            draken_free(data)
        if validity != NULL:
            draken_free(validity)
        return -1
    memcpy(sel, src.selection, <size_t>n * sizeof(uint32_t))

    out_vec.data = data
    out_vec.selection = sel
    out_vec.data_length = k
    out_vec.length = n
    out_vec.validity = validity
    out_vec.type = src.type
    out_vec.flags = src.flags if k == n else 0
    out_data[0] = data
    out_validity[0] = validity
    out_sel[0] = sel
    return 0


cdef int _dv_copy_result_array_offsets(
    const DrakenVector* src,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena,
) noexcept nogil:
    """DRAKEN_ARRAY twin of the fixed-width copy in _dv_copy_result_dense, for the
    identity-selection shape only (see caller). offsets[length+1] copies verbatim —
    row i's span is unaffected by re-densifying since selection is already
    i -> i. The CHILD element vector is NOT this function's concern: the caller
    (_dv_eval_span_cxx) forwards VecResult.child separately, unrelated to this
    parent-offsets buffer."""
    # Offsets, not strings: no separate arena on this result.
    out_arena[0] = NULL
    cdef uint32_t n = src.length
    cdef uint32_t alloc_n = n if n > 0 else 1
    cdef size_t vbytes = (<size_t>n + 7) >> 3
    cdef uint8_t* validity = NULL
    cdef uint32_t* sel
    cdef uint32_t i
    cdef int32_t* data = <int32_t*>draken_malloc((<size_t>alloc_n + 1) * sizeof(int32_t))
    if data == NULL:
        return -1
    memcpy(data, src.data, (<size_t>n + 1) * sizeof(int32_t))

    if src.validity != NULL:
        validity = <uint8_t*>draken_malloc(vbytes if vbytes > 0 else 1)
        if validity == NULL:
            draken_free(data)
            return -1
        memcpy(validity, src.validity, vbytes if vbytes > 0 else 1)

    sel = <uint32_t*>draken_malloc(<size_t>alloc_n * sizeof(uint32_t))
    if sel == NULL:
        draken_free(data)
        if validity != NULL:
            draken_free(validity)
        return -1
    for i in range(n):
        sel[i] = i

    out_vec.data = data
    out_vec.selection = sel
    out_vec.data_length = n
    out_vec.length = n
    out_vec.validity = validity
    out_vec.type = DRAKEN_ARRAY
    out_vec.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION
    out_data[0] = data
    out_validity[0] = validity
    out_sel[0] = sel
    return 0


cdef int _dv_copy_result_dense(
    const DrakenVector* src,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena, uint32_t vec_dim = 0,
) noexcept nogil:
    """Deep-copy an (arena-owned) expression result into fresh draken_malloc'd DENSE
    buffers the caller takes ownership of (data / validity / identity selection).
    The arena is destroyed right after this returns — nothing may alias it. Uniform
    data[selection[i]] gather; BOOL is bit-packed. Fixed-width + BOOL only (the
    is_all_c_native contract guarantees a fixed-width result); returns -1 for
    anything else so the caller can fail loud."""
    # Only the string family owns a separate arena; every other arm — including the
    # early -1 returns below — leaves it NULL ("the arena is wherever `data` says it
    # is", vec_result.h).
    out_arena[0] = NULL
    cdef uint32_t n = src.length
    cdef uint32_t alloc_n = n if n > 0 else 1
    cdef size_t es = _dv_result_elem_size(src.type)
    cdef size_t vbytes = (<size_t>n + 7) >> 3
    cdef uint8_t* data8
    cdef const uint8_t* sbits
    # A VECTOR_FP16 "value" is a whole dim-wide fp16 row, so its width is not a
    # function of DrakenType and _dv_result_elem_size returns 0 for it. The width is
    # plan-time metadata, handed in by the caller off the root instruction. The gather
    # below is otherwise identical — one es-byte value per logical row.
    if src.type == DRAKEN_VECTOR_FP16:
        if vec_dim == 0:
            return -1   # VECTOR result with no declared width — fail loud
        es = <size_t>vec_dim * 2
    cdef uint8_t* validity = NULL
    cdef uint32_t* sel
    cdef uint32_t i, phys
    cdef void* data

    # VARIANT is German-string storage (buffers.h) — same slot/arena layout as the
    # VARCHAR family. It is the result type of `->`.
    if (src.type == DRAKEN_VARCHAR or src.type == DRAKEN_NVARCHAR
            or src.type == DRAKEN_VARBINARY or src.type == DRAKEN_VARIANT):
        return _dv_copy_result_string(src, out_vec, out_data, out_validity, out_sel,
                                      out_arena)

    if src.type == DRAKEN_ARRAY:
        # data is int32_t offsets[length+1] (buffers.h) — NOT one value per row
        # the way every other fixed type is, so the generic elem_size gather
        # below cannot apply. Only the IDENTITY-selection shape is handled: a
        # true force-dense copy under a dict/constant ARRAY selection would need
        # to re-flatten the child in row order (the same gather make_array_take
        # does in draken_native.cpp) — genuinely unreachable today (the only
        # ARRAY-producing kernel, JSONB_OBJECT_KEYS, always emits identity), so
        # fail loud rather than build unexercised gather logic for it.
        if (src.flags & DRAKEN_SEL_IDENTITY) == 0:
            return -1
        return _dv_copy_result_array_offsets(src, out_vec, out_data, out_validity,
                                            out_sel, out_arena)

    if src.type == DRAKEN_NULL:
        # Self-describing null (buffers.h): type==NULL means every row is null,
        # with no data and no validity buffer at all — nothing to copy.
        data = NULL
    elif src.type == DRAKEN_BOOL:
        data = draken_malloc(vbytes if vbytes > 0 else 1)
        if data == NULL:
            return -1
        memset(data, 0, vbytes if vbytes > 0 else 1)
        data8 = <uint8_t*>data
        sbits = <const uint8_t*>src.data
        for i in range(n):
            phys = src.selection[i]
            if (sbits[phys >> 3] >> (phys & 7)) & 1:
                data8[i >> 3] |= <uint8_t>(1 << (i & 7))
    else:
        if es == 0:
            return -1   # strings/arrays/decimal128 — not a c-native fixed result
        data = draken_malloc(<size_t>alloc_n * es)
        if data == NULL:
            return -1
        for i in range(n):
            memcpy(<uint8_t*>data + <size_t>i * es,
                   <const uint8_t*>src.data + <size_t>src.selection[i] * es, es)

    if src.validity != NULL:
        validity = <uint8_t*>draken_malloc(vbytes if vbytes > 0 else 1)
        if validity == NULL:
            draken_free(data)
            return -1
        memcpy(validity, src.validity, vbytes if vbytes > 0 else 1)

    sel = <uint32_t*>draken_malloc(<size_t>alloc_n * sizeof(uint32_t))
    if sel == NULL:
        draken_free(data)
        if validity != NULL:
            draken_free(validity)
        return -1
    for i in range(n):
        sel[i] = i

    out_vec.data = data
    out_vec.selection = sel
    out_vec.data_length = n
    out_vec.length = n
    out_vec.validity = validity
    out_vec.type = src.type
    out_vec.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION
    out_data[0] = data
    out_validity[0] = validity
    out_sel[0] = sel
    return 0


cdef int _dv_eval_span_cxx(
    BytecodeInstr* instrs, int count, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
    DrakenVector* out_vec, void** out_data, uint8_t** out_validity, void** out_sel,
    uint8_t** out_arena,
    int* err_op, const char** err_msg, bint preserve_shape,
    VecResult** out_child,
) noexcept nogil:
    """Pure-nogil expression span for a COMPUTED column (the projection twin of
    _dv_filter_span_cxx): evaluate the program over pre-resolved (col_idx, lit_dv)
    and deep-copy the arena result into fresh caller-owned buffers. rc 0 → out_vec/
    out_data/out_validity/out_sel/out_arena filled (ownership transferred); 4 → kernel error
    (``*err_msg`` set, see c_execute_dv_inner); 98 → non-fixed-width result (fail
    loud upstream); 99 → arena OOM; other → the c_execute rc. No PyObject
    anywhere — callable from the engine's worker threads.

    ``preserve_shape`` selects the boundary materialization: the default (0) force-
    densifies to an identity-selection dense column (every downstream consumer sees a
    plain dense column); 1 keeps the result's compressed encoding (dict/constant
    stays compressed) and is set ONLY by the plan compiler for computed columns that
    feed a compression-aware consumer (GROUP BY / DISTINCT key).

    ``*out_arena`` is the string result's byte ARENA when the boundary copy keeps it
    as a SECOND owned allocation (see _dv_copy_result_string) — the ExprEvalFn twin of
    VecResult::arena (vec_result.h). NULL for every non-string result, for an
    all-inline string result, and on every non-zero rc; a caller that does not free it
    leaks the bulk of the column, which no test can see.

    ``*out_child`` is set to NULL, then forwarded verbatim from c_execute_dv_inner
    on rc 0 — non-NULL only for an ARRAY result (out_vec.type == DRAKEN_ARRAY),
    where it is the owned VecResult* for the element vector (vec_result.h). Not
    arena-owned (the producing kernel draken_malloc'd it standalone) — the C++
    caller (ExprMultiProjectOperator) adopts it into VectorOwner.child_owner via
    draken_vecresult_child_owner_new_c. preserve_shape's ARRAY path is NOT wired
    (its _dv_copy_result_preserve_shape branch still returns -1/rc98 for ARRAY —
    an ARRAY feeding a GROUP BY/DISTINCT key is unreached today); *out_child stays
    NULL whenever rc != 0."""
    cdef DrakenVector* dv_cache_inline[256]
    cdef DrakenVector** dv_cache
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef Py_ssize_t num_rows = m.num_rows()
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef int rc
    cdef VecResult* child_local = NULL
    cdef DrakenVector tiled
    cdef uint32_t* tiled_sel
    cdef uint32_t ti
    cdef DrakenFrameArena* arena = draken_frame_arena_create()
    out_child[0] = NULL
    out_arena[0] = NULL
    if arena == NULL:
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    dv_cache = _dv_cache_for(count, dv_cache_inline, arena)
    if dv_cache == NULL:
        draken_frame_arena_destroy(arena)
        err_op[0] = -99
        err_msg[0] = NULL
        return 99
    _dv_fill_cache_cxx(instrs, count, m, col_idx, lit_dv, dv_cache)
    # preserve_shape (computed GROUP BY / DISTINCT key): tiled only when every
    # loaded column is identity-dense, i.e. when the untiled result is dense anyway.
    rc = _dv_try_tiled(instrs, count, dv_cache, <uint32_t>num_rows, arena,
                       preserve_shape, &tiled, err_op, err_msg)
    if rc == 0:
        tiled_sel = <uint32_t*>draken_malloc(
            <size_t>(num_rows if num_rows > 0 else 1) * sizeof(uint32_t))
        if tiled_sel == NULL:
            draken_free(tiled.data)
            draken_free(tiled.validity)
            draken_frame_arena_destroy(arena)
            err_op[0] = -99
            err_msg[0] = NULL
            return 99
        for ti in range(<uint32_t>num_rows):
            tiled_sel[ti] = ti
        tiled.selection = tiled_sel
        out_vec[0] = tiled
        out_data[0] = tiled.data
        out_validity[0] = tiled.validity
        out_sel[0] = tiled_sel
        draken_frame_arena_destroy(arena)
        return 0
    if rc != -1:
        draken_frame_arena_destroy(arena)
        return rc
    rc = c_execute_dv_inner(instrs, count, dv_cache, dv_stack, dv_store,
                            arena, nbytes, <uint32_t>num_rows, err_op, err_msg, &child_local)
    if rc == 0:
        # The program is postfix, so instrs[count-1] is the ROOT — the instruction
        # whose result dv_stack[0] holds. Its bind-time-declared VECTOR width (0 for
        # every non-VECTOR result) is the one piece of plan metadata the boundary
        # materialization cannot recover from the DrakenVector itself.
        if preserve_shape:
            if _dv_copy_result_preserve_shape(
                    dv_stack[0], out_vec, out_data, out_validity, out_sel, out_arena,
                    <uint32_t>instrs[count - 1].vec_dimension) != 0:
                err_op[0] = -98
                err_msg[0] = NULL
                rc = 98
        elif _dv_copy_result_dense(
                dv_stack[0], out_vec, out_data, out_validity, out_sel, out_arena,
                <uint32_t>instrs[count - 1].vec_dimension) != 0:
            err_op[0] = -98
            err_msg[0] = NULL
            rc = 98
    if rc == 0 and child_local != NULL:
        out_child[0] = child_local
    elif child_local != NULL:
        # copy_result rejected this program's ARRAY shape (rc 98 above) — the
        # child is now orphaned; discard rather than leak.
        draken_vecresult_discard_c(child_local)
    draken_frame_arena_destroy(arena)
    return rc


cdef Py_ssize_t _gil_run(
    CompiledBytecode bc, Py_ssize_t start, Py_ssize_t end, Morsel morsel,
    DrakenFrameArena* arena, DrakenVector** dv_stack, DrakenVector* dv_store,
    list anchor, Py_ssize_t sp,
) except -1:
    """Run bc.instrs[start:end] against `morsel` on the caller's operand stack, from
    stack height `sp`; return the stack height afterwards.

    Split out of execute_bytecode so a BC_LAZY region can re-enter it over a NARROWED
    sub-morsel on the SAME stack, anchor list and frame arena — the region's result
    then sits on the stack exactly where an eagerly-evaluated branch would have."""
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (<Py_ssize_t>num_rows + 7) >> 3
    cdef Py_ssize_t skip_to = 0     # a BC_LAZY region is consumed whole by its handler
    cdef uint32_t lz_k
    cdef uint32_t* lz_rows
    cdef const DrakenVector* lz_guards[16]
    cdef Py_ssize_t lz_j, lz_g0
    cdef object lz_sub
    cdef DrakenVector lz_compact

    cdef Py_ssize_t i, j, base
    cdef int opcode
    cdef int arity
    cdef int flags
    cdef BytecodeInstr* slot
    cdef BoolVector b_result
    cdef Vector v_result
    cdef object scalar_obj
    cdef object compare_result
    cdef object legacy_result
    cdef object py_left
    cdef object py_right
    cdef int16_t left_type_code
    cdef int16_t right_type_code
    cdef object func_args
    cdef Py_ssize_t func_base
    cdef object callable_obj
    cdef bint is_nb_callable
    cdef object inlist_right
    # DV fast-path variables
    cdef DrakenVector* dv_left_ptr
    cdef DrakenVector* dv_right_ptr
    cdef DrakenVector* dv_result_ptr
    cdef void* result_data_ptr
    cdef uint8_t* result_val_ptr
    cdef uint8_t* left_data
    cdef uint8_t* left_null
    cdef uint8_t* right_data
    cdef uint8_t* right_null
    cdef uint32_t result_len_u32
    cdef DrakenType result_dtype
    cdef VecResult cast_vr
    cdef VecResult binop_vr
    cdef VecResult extr_vr
    cdef const DrakenVector* cfargs[16]   # C-native BC_FUNCTION operand scratch
    cdef Py_ssize_t _fj
    # Row-count carrier for arity-0 C-native functions (RANDOM/NORMAL): the func_fn_t
    # ABI passes only operand vectors, so a nullary kernel has no way to learn the
    # morsel row count. We hand it a synthetic length-only operand whose `length` IS
    # num_rows; the kernel reads ONLY .length (never .data/.selection/.validity — all
    # NULL here, which is why this stays confined to the arity-0 path).
    cdef DrakenVector _zeroarg_rowcount
    cdef uint32_t _eff_nargs
    cdef int dv_op
    cdef int had_null
    cdef int rc
    cdef uint8_t* cur_data
    cdef uint8_t* cur_null
    cdef uint8_t* next_data
    cdef uint8_t* next_null
    # Phase 9c: C kernel ABI dispatch
    cdef VecResult c_result
    cdef const char* error_msg

    for i in range(start, end):
        if i < skip_to:
            continue
        slot = &bc.instrs[i]
        opcode = slot.opcode

        # ----------------------------------------------------------
        # BC_LAZY — a guarded branch region. Runs the branch ONLY on the rows the
        # guard admits (see _dv_lazy_region_c, the nogil twin): every row -> the
        # branch runs as it always did; no row -> a typed NULL is pushed and the
        # branch skipped; otherwise the branch runs over a NARROWED sub-morsel and its
        # result is scattered back to full length, excluded rows NULL.
        # ----------------------------------------------------------
        if opcode == BC_LAZY:
            lz_g0 = sp - slot.bool_value
            if slot.flags < 1 or slot.flags > 16 or lz_g0 < 0:
                raise ValueError("execute_bytecode: malformed BC_LAZY")
            for lz_j in range(slot.flags):
                if dv_stack[lz_g0 + lz_j] == NULL:
                    raise TypeError("BC_LAZY: guard is not a vector (NULL slot)")
                lz_guards[lz_j] = dv_stack[lz_g0 + lz_j]
            lz_rows = <uint32_t*>draken_frame_arena_alloc(
                arena, <size_t>(num_rows if num_rows > 0 else 1) * sizeof(uint32_t))
            if lz_rows == NULL:
                raise MemoryError("execute_bytecode: BC_LAZY row list alloc failed")
            lz_k = draken_lz_rows(slot.op_code, lz_guards, <uint32_t>slot.flags,
                                  <uint32_t>num_rows, lz_rows)
            if lz_k == <uint32_t>num_rows:
                skip_to = i + 2          # every row admitted: the branch runs as usual
                continue
            if lz_k == 0:
                scalar_obj = <object>bc.instrs[i + 1].literal_obj     # the typed NULL
                dv_store[sp] = (<Vector>scalar_obj).unified()[0]
                dv_store[sp].length = <uint32_t>num_rows
                dv_store[sp].selection = draken_zero_sel(<uint32_t>num_rows)
                if dv_store[sp].validity != NULL:
                    dv_store[sp].validity = <uint8_t*>draken_zero_validity(<uint32_t>num_rows)
                dv_stack[sp] = &dv_store[sp]
                anchor[sp] = scalar_obj
                sp += 1
                skip_to = i + 1 + slot.arity
                continue
            lz_sub = morsel.take([lz_rows[lz_j] for lz_j in range(lz_k)])
            lz_g0 = sp                   # where the branch result lands
            sp = _gil_run(bc, i + 2, i + 1 + slot.arity, <Morsel>lz_sub,
                          arena, dv_stack, dv_store, anchor, sp)
            if sp != lz_g0 + 1 or dv_stack[lz_g0] == NULL:
                raise TypeError(
                    "BC_LAZY: the branch did not produce a vector result "
                    "(a lazy branch must yield a DrakenVector)")
            lz_compact = dv_stack[lz_g0][0]
            if lz_compact.type == DRAKEN_NULL:
                dv_store[lz_g0] = lz_compact
                dv_store[lz_g0].length = <uint32_t>num_rows
                dv_store[lz_g0].selection = draken_zero_sel(<uint32_t>num_rows)
                if dv_store[lz_g0].validity != NULL:
                    dv_store[lz_g0].validity = <uint8_t*>draken_zero_validity(<uint32_t>num_rows)
                dv_stack[lz_g0] = &dv_store[lz_g0]
            else:
                binop_vr = draken_lz_scatter(&lz_compact, lz_rows, lz_k, <uint32_t>num_rows)
                if binop_vr.data == NULL:
                    raise _vecresult_error_exc(&binop_vr, "lazy branch scatter failed")
                _dv_vecresult_adopt_c(&binop_vr, dv_store, dv_stack, lz_g0, arena)
            anchor[lz_g0] = None
            skip_to = i + 1 + slot.arity
            continue

        # ----------------------------------------------------------
        # BC_LOAD_COL — typed Morsel.column dispatch (cpdef)
        # ----------------------------------------------------------
        if opcode == BC_LOAD_COL:
            v_result = morsel._cxx_column(
                <bytes>slot.column_identity, <bytes>slot.column_name
            )
            if v_result is None:
                raise ColumnReferencedBeforeEvaluationError(
                    column=(<bytes>slot.column_name).decode()
                )
            anchor[sp] = v_result
            # Use _dv directly — avoids calling unified() on types (e.g. ARRAY)
            # whose Cython shim has _dv == NULL.  _slot_to_pyobj returns the
            # Python anchor directly when anc is not None, so NULL here is safe.
            # Cast away const: dv_stack holds mutable DV* but we only read
            # through it when anc is None (arena slots); borrowed slots (anc
            # is not None) are returned via anchor, never via dv_stack.
            dv_stack[sp] = <DrakenVector*>(<Vector>v_result)._dv
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LOAD_LIT_BOOL — dense bitmap materialized in arena.
        # Avoids constant-shape BoolVector; c_and_bitmap requires dense.
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_BOOL:
            result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
            if result_data_ptr == NULL:
                raise MemoryError("execute_bytecode: BC_LOAD_LIT_BOOL alloc failed")
            if slot.bool_value != 0:
                memset(<uint8_t*>result_data_ptr, 0xFF, <size_t>nbytes)
                if num_rows & 7:
                    (<uint8_t*>result_data_ptr)[nbytes - 1] = <uint8_t>((1 << (num_rows & 7)) - 1)
            else:
                memset(<uint8_t*>result_data_ptr, 0x00, <size_t>nbytes)
            dv_store[sp] = draken_vector_from_dense(
                result_data_ptr, <uint32_t>num_rows, DRAKEN_BOOL, NULL
            )
            dv_stack[sp] = &dv_store[sp]
            anchor[sp] = None
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LOAD_LIT_SET — non-DV slot (set/CarcharSet objects)
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_SET:
            anchor[sp] = <object>slot.literal_obj
            dv_stack[sp] = NULL
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_LOAD_LIT_SCALAR — IN-list collection / set literal.
        #
        # Genuine scalar literals are pre-materialised at bind time and use
        # BC_LOAD_LIT_CONST. Only set/list/tuple membership literals remain
        # here: they are never DrakenVector* and are pushed as a Python anchor
        # for a downstream BC_COMPARE. Anything else is an internal invariant
        # violation — fail fast (CLAUDE.md §1).
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_SCALAR:
            scalar_obj = <object>slot.literal_obj
            if isinstance(scalar_obj, (_CarcharSetWrapper, _PerfectHashSet,
                                       list, tuple, set, frozenset)):
                anchor[sp] = scalar_obj
                dv_stack[sp] = NULL
                sp += 1
                continue
            raise TypeError(
                "execute_bytecode: BC_LOAD_LIT_SCALAR expected an in-list "
                f"collection/set literal, got {type(scalar_obj).__name__}"
            )

        # ----------------------------------------------------------
        # BC_LOAD_LIT_CONST — pre-materialised scalar constant.
        #
        # The cached Vector is constant-shape (data_length==1), built ONCE at
        # bind time. Re-stamp ONLY the logical length onto a stack-local DV
        # copy — zero alloc, no Python object, no isinstance, no re-encode.
        # selection/validity are refreshed to the shared globals sized for N
        # rows (the bind-time pointers were sized for length 1). The cached
        # Vector anchors the borrowed data; _slot_to_pyobj lazily builds a
        # length-N view if a Python-fallback kernel needs the object.
        # ----------------------------------------------------------
        if opcode == BC_LOAD_LIT_CONST:
            scalar_obj = <object>slot.literal_obj
            dv_store[sp] = (<Vector>scalar_obj).unified()[0]
            dv_store[sp].length = <uint32_t>num_rows
            dv_store[sp].selection = draken_zero_sel(<uint32_t>num_rows)
            if dv_store[sp].validity != NULL:
                dv_store[sp].validity = <uint8_t*>draken_zero_validity(<uint32_t>num_rows)
            dv_stack[sp] = &dv_store[sp]
            anchor[sp] = scalar_obj
            sp += 1
            continue

        # ----------------------------------------------------------
        # Boolean combinators — C-level bitmap kernels.
        #
        # _ensure_dense_bitmap handles dense (no-copy) and constant-shape
        # (expand in arena) inputs.  Non-dense non-constant shapes raise —
        # fail fast per CLAUDE.md §1.  No Python fallback.
        # ----------------------------------------------------------
        if opcode == BC_AND:
            rc = _dv_bool_binop_c(0, dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_AND: operand is not a boolean DV* (NULL slot)")
            if rc == 2:
                raise MemoryError("execute_bytecode: BC_AND alloc failed")
            anchor[sp - 1] = None
            continue

        if opcode == BC_OR:
            rc = _dv_bool_binop_c(1, dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_OR: operand is not a boolean DV* (NULL slot)")
            if rc == 2:
                raise MemoryError("execute_bytecode: BC_OR alloc failed")
            anchor[sp - 1] = None
            continue

        if opcode == BC_XOR:
            rc = _dv_bool_binop_c(2, dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_XOR: operand is not a boolean DV* (NULL slot)")
            if rc == 2:
                raise MemoryError("execute_bytecode: BC_XOR alloc failed")
            anchor[sp - 1] = None
            continue

        if opcode == BC_NOT:
            rc = _dv_not_c(dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_NOT: operand is not a boolean DV* (NULL slot)")
            if rc == 2:
                raise MemoryError("execute_bytecode: BC_NOT alloc failed")
            anchor[sp - 1] = None
            continue

        # ----------------------------------------------------------
        # Variadic AND/OR — DNF (AND-of-terms) / CNF (OR-of-terms).
        #
        # Native bitmap loop: no Python objects.  Ping-pong between
        # two arena buffer pairs — cur_{data,null} accumulates the
        # result; next_{data,null} is the per-step output.
        # After the loop the final pair is stored in dv_store[base].
        # ----------------------------------------------------------
        if opcode == BC_DNF:
            rc = _dv_variadic_bool_c(0, slot.arity, dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_DNF: operand is NULL")
            if rc == 2:
                raise MemoryError("BC_DNF: alloc failed")
            anchor[sp - 1] = None
            continue

        if opcode == BC_CNF:
            rc = _dv_variadic_bool_c(1, slot.arity, dv_stack, dv_store, &sp, arena, nbytes, <uint32_t>num_rows)
            if rc == 1:
                raise TypeError("BC_CNF: operand is NULL")
            if rc == 2:
                raise MemoryError("BC_CNF: alloc failed")
            anchor[sp - 1] = None
            continue

        # ----------------------------------------------------------
        # BC_COMPARE — typed draken_compare (cpdef)
        #
        # Two shapes:
        #   Normal (flags & BC_CMP_INLIST_INLINE == 0):
        #     pop right DV*, pop left DV*, compare, push result DV*.
        #     Phase 4/5 fast path: draken_compare_dv for EQ/NE/LT/GT/LE/GE;
        #     result DV* stored in dv_stack — no from_decoded until needed.
        #   Inline IN-list (flags & BC_CMP_INLIST_INLINE != 0):
        #     right operand folded into slot.literal_obj — pop left DV* only.
        # ----------------------------------------------------------
        if opcode == BC_COMPARE:
            flags = slot.flags
            left_type_code = slot.left_type_code
            right_type_code = slot.right_type_code

            if flags & BC_CMP_INLIST_INLINE:
                # Right is an inline set literal — pop ONE item.
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                inlist_right = <object>slot.literal_obj
                if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(py_left):
                    py_left = _coerce_temporal_scalar_for_arrow(
                        py_left,
                        _CT_DATE if left_type_code == BC_TYPE_DATE else _CT_TIMESTAMP,
                    )
                compare_result = draken_compare_int(
                    slot.op_code, py_left, inlist_right, left_type_code, right_type_code
                )
            else:
                # Normal case — C-level fast path for ordinal EQ/NE/LT/GT/LE/GE
                # via the shared nogil helper (_dv_compare_c → draken_compare_dv,
                # no Python objects). rc 0 = result pushed; rc 3 = fast path N/A,
                # sp left decremented so the Python fallback re-reads operands.
                dv_op = -1
                if 0 < slot.op_code < 19:
                    dv_op = _DRAKEN_CMP_OP[slot.op_code]
                rc = _dv_compare_c(
                    dv_op, dv_stack, &sp,
                    slot.left_type_code, slot.right_type_code,
                    <uint32_t>num_rows, arena)
                if rc == 0:
                    anchor[sp - 1] = None
                    continue

                # Python fallback (unsupported types, LIKE/RLIKE/IN_LIST).
                dv_left_ptr = dv_stack[sp]
                dv_right_ptr = dv_stack[sp + 1]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                py_right = _slot_to_pyobj(dv_right_ptr, anchor[sp + 1], arena)
                if flags != 0:
                    if (flags & BC_CMP_LEFT_TEMPORAL) and _is_scalar_value(py_left):
                        py_left = _coerce_temporal_scalar_for_arrow(
                            py_left,
                            _CT_DATE if left_type_code == BC_TYPE_DATE else _CT_TIMESTAMP,
                        )
                    if (flags & BC_CMP_RIGHT_TEMPORAL) and _is_scalar_value(py_right):
                        py_right = _coerce_temporal_scalar_for_arrow(
                            py_right,
                            _CT_DATE if right_type_code == BC_TYPE_DATE else _CT_TIMESTAMP,
                        )
                compare_result = draken_compare_int(
                    slot.op_code, py_left, py_right, left_type_code, right_type_code
                )
            anchor[sp] = compare_result
            dv_stack[sp] = (<Vector>compare_result).unified()
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_BINARY_OP — arithmetic / string / date ops on two vecs.
        #
        # Phase 4/5 fast path: draken_arithmetic_dv for PLUS..MODULO.
        # Result DV* stored in dv_stack — no vec_from_decoded until needed.
        # ----------------------------------------------------------
        if opcode == BC_BINARY_OP:
            sp -= 1
            dv_right_ptr = dv_stack[sp]
            sp -= 1
            dv_left_ptr = dv_stack[sp]

            # P9.1 C-native binop: when the binder routed this (op, types) to the
            # unified draken_binop kernel (BC_INSTR_C_NATIVE), dispatch it directly
            # — no closure, no Python objects. Fixed-width result folds into the
            # frame arena as a dense DV* (mirrors the BC_CAST C-native path). On a
            # kernel error sentinel we raise (fail-loud, no silent fallback).
            if ((slot.flags & BC_INSTR_C_NATIVE) != 0
                    and dv_left_ptr != NULL and dv_right_ptr != NULL):
                rc = _dv_binop_kernel_c(
                    slot.kernel_fn, <void*>slot.ctx_ptr,
                    dv_left_ptr, dv_right_ptr,
                    dv_store, dv_stack, sp, arena, &binop_vr)
                if rc == 4:
                    raise _vecresult_error_exc(&binop_vr, "C binop kernel error")
                if rc == 5:
                    # String result (e.g. ||): consolidated block with embedded
                    # validity — own it as a Vector (the canonical owner). Stays
                    # on the GIL path (string ownership can't fold into the arena).
                    legacy_result = Vector(draken_vecresult_own_c(binop_vr))
                    anchor[sp] = legacy_result
                    dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
                else:
                    # rc == 0: fixed-width result already folded into the arena.
                    anchor[sp] = None
                sp += 1
                continue

            # `/` (BOP_DIVIDE) is TRUE division: when either operand is an
            # integer, skip the native (truncating) path and fall through to
            # the resolved kernel, which promotes integers to FLOAT64 so
            # int / int yields a float. Float / float stays on the fast path.
            if (BOP_PLUS <= slot.op_code <= BOP_MODULO
                    and dv_left_ptr != NULL and dv_right_ptr != NULL
                    and not (slot.op_code == BOP_DIVIDE
                             and (dv_left_ptr.type == DRAKEN_INT8
                                  or dv_left_ptr.type == DRAKEN_INT16
                                  or dv_left_ptr.type == DRAKEN_INT32
                                  or dv_left_ptr.type == DRAKEN_INT64
                                  or dv_right_ptr.type == DRAKEN_INT8
                                  or dv_right_ptr.type == DRAKEN_INT16
                                  or dv_right_ptr.type == DRAKEN_INT32
                                  or dv_right_ptr.type == DRAKEN_INT64))):
                # Executor short-circuit: detect all-null inputs (DRAKEN_NULL constant)
                # and return null result without calling kernel (Defect 2 fix).
                if (dv_left_ptr.type == DRAKEN_NULL or dv_right_ptr.type == DRAKEN_NULL):
                    dv_result_ptr = Vector(_draken_native.vector_null_from_length(num_rows)).unified()
                    dv_stack[sp] = dv_result_ptr
                    anchor[sp] = None
                    sp += 1
                    continue

                dv_result_ptr = draken_arithmetic_dv(
                    slot.op_code,
                    dv_left_ptr, dv_right_ptr,
                    <uint32_t>num_rows, arena,
                )
                if dv_result_ptr != NULL:
                    dv_stack[sp] = dv_result_ptr
                    anchor[sp] = None
                    sp += 1
                    continue

            # Single path: Phase 6 Python kernel (pre-9c, last-correct state).
            # CAST and EXTRACTION retain C-native dispatch; binop reverts to resolved kernel.
            py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
            py_right = _slot_to_pyobj(dv_right_ptr, anchor[sp + 1], arena)
            legacy_result = (<object>slot.callable_ref)(py_left, py_right)

            # Phase 1 result-wrap pattern: check flags set at bind time.
            if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
                if slot.flags & BC_RESULT_WRAP_AS_BOOL:
                    legacy_result = BoolVector(legacy_result)
                else:
                    legacy_result = Vector(legacy_result)

            anchor[sp] = legacy_result
            if isinstance(legacy_result, Vector):
                dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
            else:
                dv_stack[sp] = NULL
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_UNARY_OP — IS NULL / IS NOT NULL / bitwise-not / etc.
        # ----------------------------------------------------------
        if opcode == BC_UNARY_OP:
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
            legacy_result = _unary_op_kernel(slot.op_code, py_left)
            anchor[sp] = legacy_result
            dv_stack[sp] = (<Vector>legacy_result).unified()
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_FUNCTION — call pre-resolved kernel callable.
        #
        # nb_func callables receive raw nanobind Vectors (_nb unwrapped
        # via typed (<Vector>item)._nb — C-level struct access).
        # Non-nb callables receive Cython Vector shims.
        # _slot_to_pyobj materializes arena DV* slots on demand; zero
        # cost when anchor is not None (the common case).
        # ----------------------------------------------------------
        if opcode == BC_FUNCTION:
            arity = slot.arity

            # C-native function kernel (func_fn_t): the compiler lowered this
            # to a draken_* kernel (EXTRACT / LIKE / IN-list / CASE-blend) with
            # NO callable_ref. Dispatch it directly — mirrors the BC_BINARY_OP
            # C-native path above. A fixed-width/BOOL result folds into the
            # arena; a canonical-string result is owned as a Vector.
            if (slot.flags & BC_INSTR_C_NATIVE) != 0:
                if arity < 0 or arity > 16:
                    raise ValueError("BC_FUNCTION: bad arity for C-native kernel")
                func_base = sp - arity
                for _fj in range(arity):
                    if dv_stack[func_base + _fj] == NULL:
                        raise ValueError("BC_FUNCTION: NULL operand for C-native kernel")
                    cfargs[_fj] = dv_stack[func_base + _fj]
                sp = func_base
                _eff_nargs = <uint32_t>arity
                if arity == 0:
                    # Nullary C-native function (RANDOM/NORMAL): synthesize a
                    # length-only operand carrying num_rows so the kernel can size
                    # its output. data/selection/validity stay NULL — the kernel
                    # contract for arity-0 functions is to read ONLY .length.
                    _zeroarg_rowcount.data = NULL
                    _zeroarg_rowcount.selection = NULL
                    _zeroarg_rowcount.validity = NULL
                    _zeroarg_rowcount.data_length = num_rows
                    _zeroarg_rowcount.length = num_rows
                    _zeroarg_rowcount.type = DRAKEN_FLOAT64
                    cfargs[0] = &_zeroarg_rowcount
                    _eff_nargs = 1
                rc = _dv_function_kernel_c(
                    slot.kernel_fn, <void*>slot.ctx_ptr, cfargs,
                    _eff_nargs, dv_store, dv_stack, sp, arena, &binop_vr, 1)
                if rc == 4:
                    raise _vecresult_error_exc(&binop_vr, "C function kernel error")
                if rc == 5 or rc == 6:
                    # rc 6 = ARRAY result: owned rather than arena-folded so the
                    # elements on VecResult.child survive (vecresult_to_owner
                    # adopts the child recursively). Anchoring the Vector is what
                    # lets a following arr[i] reach them — an arena DV* cannot
                    # carry a child. Identical handling to rc 5, which likewise
                    # owns rather than folds.
                    legacy_result = Vector(draken_vecresult_own_c(binop_vr))
                    anchor[sp] = legacy_result
                    dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
                else:
                    anchor[sp] = None   # rc 0: folded into the arena
                sp += 1
                continue

            callable_obj = <object>slot.callable_ref
            is_nb_callable = slot.bool_value != 0

            if arity == 0:
                legacy_result = callable_obj(num_rows)
            else:
                func_base = sp - arity
                sp = func_base

                if is_nb_callable:
                    # CHECKED cast (<Vector?>): an nb kernel operand must be a
                    # materialized Vector. A constant ARRAY/collection literal is
                    # NOT materialized into a DrakenVector (there is no ARRAY case
                    # in _materialise_constant_literal), so it arrives here as a
                    # bare Python list. The old unchecked <Vector> cast then read
                    # ._nb off list memory and handed garbage to the kernel —
                    # SIGSEGV (e.g. GREATEST([1,5,3]) folded at plan time). The
                    # checked cast fails loud with TypeError instead of corrupting
                    # memory. (Making such a literal actually evaluate needs a new
                    # ARRAY-literal constant path — an architect decision.)
                    if arity == 1:
                        legacy_result = callable_obj(
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                        )
                    elif arity == 2:
                        legacy_result = callable_obj(
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                        )
                    elif arity == 3:
                        legacy_result = callable_obj(
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base + 2], anchor[func_base + 2], arena))._nb,
                        )
                    else:
                        func_args = [
                            (<Vector?>_slot_to_pyobj(dv_stack[func_base + j], anchor[func_base + j], arena))._nb
                            for j in range(arity)
                        ]
                        legacy_result = callable_obj(*func_args)
                else:
                    if arity == 1:
                        legacy_result = callable_obj(
                            _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena)
                        )
                    elif arity == 2:
                        legacy_result = callable_obj(
                            _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena),
                            _slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena),
                        )
                    elif arity == 3:
                        legacy_result = callable_obj(
                            _slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena),
                            _slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena),
                            _slot_to_pyobj(dv_stack[func_base + 2], anchor[func_base + 2], arena),
                        )
                    else:
                        func_args = [
                            _slot_to_pyobj(dv_stack[func_base + j], anchor[func_base + j], arena)
                            for j in range(arity)
                        ]
                        legacy_result = callable_obj(*func_args)

            # Wrap nanobind result based on flags set at bind time.
            # BC_RESULT_NEEDS_NB_WRAP: result is raw nanobind Vector → wrap.
            # BC_RESULT_WRAP_AS_BOOL: wrap as BoolVector (else Vector).
            if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
                if slot.flags & BC_RESULT_WRAP_AS_BOOL:
                    legacy_result = BoolVector(legacy_result)
                else:
                    legacy_result = Vector(legacy_result)
            anchor[sp] = legacy_result
            if slot.flags & BC_RESULT_NO_DV:
                dv_stack[sp] = NULL
            else:
                dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_EXTRACTION — the bind-time-resolved C-ABI kernel, called
        # directly, exactly as the engine VM calls it (c_execute_dv_inner).
        # `->`, `->>` and str[i] consume ONE operand — the path/index rides
        # in extraction_ctx — so there is nothing to marshal: no nanobind
        # wrapper, no Python Vector per morsel. Mirrors the BC_CAST arm below.
        #
        # arr[i] is the one sub-op that still needs a Python object here.
        # Its element vector hangs off the column owner, not off
        # DrakenVector, and this VM has no dv_cache to resolve it from
        # (the reason BC_CAST's ARRAY->VARCHAR arm refuses outright); only
        # the anchor's nanobind Vector can reach the child.
        # ----------------------------------------------------------
        if opcode == BC_EXTRACTION:
            sp -= 1
            dv_left_ptr = dv_stack[sp]

            if slot.op_code == BC_EXTR_MAP_ARRAY:
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                # Unwrap Cython shim to nanobind Vector for the native call.
                if isinstance(py_left, Vector):
                    py_left_nb = (<Vector>py_left)._nb
                else:
                    py_left_nb = py_left
                legacy_result = _vector_array_map_access(py_left_nb, <int64_t>slot.bool_value)
                if not isinstance(legacy_result, Vector):
                    legacy_result = Vector(legacy_result)
                anchor[sp] = legacy_result
                dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
                sp += 1
                continue

            # Every other sub-op is resolved to a kernel at bind time or the
            # lowering raises (compiled_expression.pyx), so a missing kernel
            # here is a compiler bug — fail loud, never marshal a fallback.
            if (slot.flags & BC_INSTR_C_NATIVE) == 0:
                raise ValueError(
                    f"execute_bytecode: BC_EXTRACTION sub-op {slot.op_code} carries "
                    "no resolved kernel"
                )
            if dv_left_ptr == NULL:
                raise ValueError(
                    "execute_bytecode: BC_EXTRACTION operand is not a vector"
                )
            rc = _dv_extraction_kernel_c(
                slot.kernel_fn, <void*>slot.ctx_ptr, dv_left_ptr, NULL,
                dv_store, dv_stack, sp, arena, &extr_vr)
            if rc == 4:
                raise _vecresult_error_exc(&extr_vr, "C extraction kernel error")
            # rc 0: the result (string canonical block or element-typed) is
            # adopted into the frame arena; _slot_to_pyobj builds a Vector
            # lazily only if something downstream consumes the object.
            anchor[sp] = None
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_CAST — pre-resolved kernel/closure, pop 1 push 1
        # Phase 5: no per-morsel dispatch; kernel return type is deterministic.
        # ----------------------------------------------------------
        if opcode == BC_CAST:
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            # Y (executor flip): when a C-native kernel is wired (fixed-width
            # result) and the input is a real DV*, call it directly — no closure
            # call, no input/output Python Vector. The kernel's draken_malloc'd
            # buffers are adopted into the frame arena and exposed as a dense
            # DV*; the result Vector is materialized lazily only if consumed at
            # frame exit (_slot_to_pyobj). Zero Python objects per morsel.
            if (slot.flags & BC_C_NATIVE_CHILD) != 0:
                # ARRAY->VARCHAR is engine-only: this VM evaluates Python
                # Morsels and cannot resolve the owner-held child vector.
                raise ValueError(
                    "ARRAY->VARCHAR cast reached the Morsel VM — engine-only "
                    "instruction (BC_C_NATIVE_CHILD); fail loud")
            if (slot.flags & BC_INSTR_C_NATIVE) != 0 and dv_left_ptr != NULL:
                rc = _dv_cast_kernel_c(
                    slot.kernel_fn, <void*>slot.ctx_ptr, dv_left_ptr,
                    dv_store, dv_stack, sp, arena, &cast_vr, 1)
                if rc == 4:
                    raise _vecresult_error_exc(&cast_vr, "C cast kernel error")
                if rc == 5 or rc == 6:
                    # String result: consolidated block with embedded validity —
                    # own it as a Vector (the canonical owner; carries the block).
                    # Stays on the GIL path (string ownership can't fold to arena).
                    # rc 6 = CAST(json AS ARRAY<T>): owned for the same reason, so
                    # the elements on VecResult.child survive the arena boundary
                    # and a following arr[i] can reach them.
                    legacy_result = Vector(draken_vecresult_own_c(cast_vr))
                    anchor[sp] = legacy_result
                    dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
                else:
                    # rc == 0: fixed-width result already folded into the arena.
                    anchor[sp] = None
                sp += 1
                continue
            py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
            # X (thin closures): when the resolved kernel is a raw-nanobind cast
            # fn (slot.bool_value != 0), hand it the unwrapped ._nb directly —
            # no Python getattr, mirrors the BC_EXTRACTION unwrap.
            if slot.bool_value != 0 and isinstance(py_left, Vector):
                py_left = (<Vector>py_left)._nb
            legacy_result = (<object>slot.callable_ref)(py_left)
            # Phase 5: wrap based on flags set at bind time.
            if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
                if slot.flags & BC_RESULT_WRAP_AS_BOOL:
                    legacy_result = BoolVector(legacy_result)
                else:
                    legacy_result = Vector(legacy_result)
            anchor[sp] = legacy_result
            if isinstance(legacy_result, Vector):
                dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
            else:
                dv_stack[sp] = NULL
            sp += 1
            continue

        # ----------------------------------------------------------
        # BC_CASE — pre-compiled CASE WHEN closure, push 1.
        # callable_ref holds the closure built by build_case_fn at bind
        # time; conditions and results are already CompiledBytecode.
        # ----------------------------------------------------------
        if opcode == BC_CASE:
            legacy_result = (<object>slot.callable_ref)(morsel)
            # See BC_EXTRACTION above: CASE assemble return type is not
            # reliably nanobind.  TODO(Phase-7): delete the gate; trust the flag.
            if (slot.flags & BC_RESULT_NEEDS_NB_WRAP) and not isinstance(legacy_result, Vector):
                if slot.flags & BC_RESULT_WRAP_AS_BOOL:
                    legacy_result = BoolVector(legacy_result)
                else:
                    legacy_result = Vector(legacy_result)
            anchor[sp] = legacy_result
            if isinstance(legacy_result, Vector):
                dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
            else:
                dv_stack[sp] = NULL
            sp += 1
            continue

        raise NotImplementedError(
            f"execute_bytecode: unknown opcode {opcode}"
        )

    return sp


cpdef execute_bytecode(CompiledBytecode bc, Morsel morsel):
    """Execute a typed bytecode against `morsel`. Returns a Vector.

    If bc.is_pure_bitmap, delegates to evaluate_bitmap (nogil bitmap path).
    Otherwise uses a C-array DV* operand stack backed by a parallel Python
    anchor list. CLAUDE.md §2/§3.

    Phase 5 — DV* stack: every stack slot is a (DrakenVector*, Python anchor) pair.
    - dv_stack[sp]: raw DrakenVector* — borrowed (from Python Vector.unified()) or
      arena-allocated (from draken_compare_dv / draken_arithmetic_dv / combinator).
      NULL for non-vector slots (sets, CarcharSet, etc.).
    - anchor[sp]: Python object keeping the vector alive (None for arena results).

    Boolean combinators (BC_AND/OR/XOR/NOT) call the C-level bitmap kernels
    (c_and_bitmap etc.) directly on dv->data, avoiding intermediate Python
    BoolVector object creation. BC_COMPARE and BC_BINARY_OP fast paths push
    DV* from draken_compare_dv/draken_arithmetic_dv without from_decoded.
    BC_DNF/CNF use a native ping-pong bitmap loop (no Python objects).

    Promoted to cpdef so callers within the _operators compilation unit dispatch
    at C level — no Python function call boundary.
    """
    if bc.is_pure_bitmap:
        return evaluate_bitmap(bc, morsel)

    # S2: whole-bytecode nogil DV* path (numeric/bool arith + compare + cast).
    # Guarded num_rows > 0 (the empty-morsel zero-byte arena edge stays on the
    # GIL loop, which already handles it).
    if bc.is_all_c_native and morsel.ptr.num_rows > 0:
        return evaluate_c_native(bc, morsel)

    cdef Py_ssize_t n_instrs = bc.count
    cdef Py_ssize_t cap = bc.max_stack_depth
    if cap < 1:
        cap = 1
    if cap > 64:
        raise ValueError(
            f"execute_bytecode: expression stack depth {cap} exceeds maximum 64"
        )

    # DV* operand stack — C array of pointers.
    # dv_store: inline DrakenVector struct storage for combinator results
    # (bitmap data/validity are arena-allocated; the struct lives here).
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef list anchor = [None] * cap
    cdef Py_ssize_t ki
    for ki in range(64):
        dv_stack[ki] = NULL

    cdef Py_ssize_t sp = 0
    cdef DrakenFrameArena* arena = NULL

    arena = draken_frame_arena_create()
    if arena == NULL:
        raise MemoryError("execute_bytecode: failed to create DrakenFrameArena")

    try:
        sp = _gil_run(bc, 0, n_instrs, morsel, arena, dv_stack, dv_store, anchor, 0)
        if sp != 1:
            raise ValueError(
                f"execute_bytecode: expected 1 result on stack, got {sp}"
            )

        return _slot_to_pyobj(dv_stack[0], anchor[0], arena)

    finally:
        draken_frame_arena_destroy(arena)


# Wire the trampoline into the global function pointer so C++ worker threads
# can call it without holding the GIL. Done once at module import time.
opteryx_set_worker_fn(_c_bytecode_worker_trampoline)
