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
from opteryx.compiled.nanobind.vector_bitwise import vector_bitwise_not as _vector_bitwise_not
from opteryx.compiled.nanobind.vector_accessors import (
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
from opteryx.compiled.nanobind.vector_bool_ops import vector_uint64_eq_scalar as _vector_uint64_eq_scalar
from opteryx.compiled.nanobind.vector_special import vector_map_access_string as _vector_map_access_string
from opteryx.compiled.nanobind.vector_json import vector_json_extract as _vector_json_extract
from opteryx.compiled.nanobind.vector_json import vector_json_extract_text as _vector_json_extract_text
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
from draken.core.buffers cimport DrakenVector, DRAKEN_SEL_IDENTITY
from draken.vectors.bool_vector cimport (
    BoolVector,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport simd_popcount

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

# Telemetry: count C-native kernel calls for regression detection (Phase 9c).
cdef uint64_t _c_native_kernel_call_count = 0


def get_c_native_kernel_call_count():
    """Return the current count of C-native kernel calls (telemetry).

    This counter increments each time the executor dispatches to a C ABI kernel.
    Used for regression detection to ensure C paths are exercised.
    """
    return _c_native_kernel_call_count


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
    left_sc = getattr(left_node, "schema_column", None)
    right_sc = getattr(right_node, "schema_column", None)
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

    try:
        result_obj = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if l_scratch != NULL: free(l_scratch)
        if r_scratch != NULL: free(r_scratch)

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

    try:
        result_obj = bool_vector_from_bits(out_data, out_null if had_null else NULL, num_rows)
    finally:
        free(out_data)
        free(out_null)
        if src_scratch != NULL: free(src_scratch)

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


cdef BoolVector _is_null_from_dv(DrakenVector* dv, bint negate) noexcept:
    """Produce a BoolVector of IS NULL / IS NOT NULL from a DrakenVector's validity bitmap.

    negate=0: IS NULL — output bit = 1 where input is null (validity bit = 0)
    negate=1: IS NOT NULL — output bit = 1 where input is valid (validity bit = 1)

    Cases:
      - dv.validity == NULL: all rows valid
        - IS NULL: all zeros
        - IS NOT NULL: all ones (with tail masked)
      - dv.validity != NULL: copy validity, optionally invert
    """
    cdef uint32_t num_rows = dv.length
    cdef Py_ssize_t nbytes = (<Py_ssize_t>num_rows + 7) >> 3
    cdef uint8_t* out_data = <uint8_t*>malloc(<size_t>nbytes)
    cdef const uint8_t* validity = dv.validity
    cdef object result_obj
    cdef Py_ssize_t k
    cdef uint8_t tail_mask

    if out_data == NULL:
        raise MemoryError("_is_null_from_dv: malloc failed")

    try:
        if validity == NULL:
            # All rows are valid (no nulls in the input)
            if negate:
                # IS NOT NULL: all output bits = 1
                memset(out_data, 0xFF, <size_t>nbytes)
            else:
                # IS NULL: all output bits = 0
                memset(out_data, 0x00, <size_t>nbytes)
        else:
            # Copy the validity bitmap and optionally invert
            memcpy(out_data, <void*>validity, <size_t>nbytes)
            if negate:
                # IS NOT NULL: output = validity (1=valid, 1 in output)
                pass  # Already copied validity
            else:
                # IS NULL: output = ~validity (invert: 1=valid→0, 0=null→1)
                for k in range(nbytes):
                    out_data[k] = ~out_data[k]

        # Mask tail bits beyond num_rows
        if num_rows & 7u:
            tail_mask = <uint8_t>((1u << (num_rows & 7u)) - 1u)
            out_data[nbytes - 1] &= tail_mask

        # Result has no nulls — IS NULL/NOT NULL always yields a definite answer
        result_obj = bool_vector_from_bits(out_data, NULL, num_rows)
    finally:
        free(out_data)

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
        result_obj = bool_vector_from_bits(out_data, NULL, num_rows)
    finally:
        free(out_data)
        if scratch != NULL:
            free(scratch)

    return _BoolVector(result_obj)


cpdef execute_and_append(list compiled_evals, morsel):
    """Execute pre-compiled (identity, CompiledBytecode) pairs and append results.

    Successor to the tree-walker evaluate_and_append_draken.  Filtering
    (_PASSTHRU, should_evaluate) and ordering (prioritize_evaluation) must
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
# Native nodes: pop `arity` vectors, push one result.
# Legacy nodes: call _eval_value(source_node, morsel), push one result.
# ---------------------------------------------------------------------------

from opteryx.compiled.expression.compiled_expression cimport (
    BC_AND,
    BC_BETWEEN,
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
    # Extraction op codes
    BC_EXTR_UNKNOWN, BC_EXTR_MAP_STRING, BC_EXTR_MAP_ARRAY,
    BC_EXTR_JSON_PTR, BC_EXTR_JSON_KEY,
)
from libc.stdint cimport uint8_t, int8_t, int16_t, int64_t, uintptr_t, uint32_t

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_BOOL, DRAKEN_NULL, draken_vector_from_dense
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY
from draken.core.buffers cimport DRAKEN_DECIMAL, DRAKEN_DECIMAL128, DRAKEN_TIMESTAMP64
from draken.core.buffers cimport draken_zero_sel, draken_zero_validity
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

from draken.morsels.morsel cimport Morsel, cxx_to_morsel
from draken.morsels.cxx_morsel cimport CxxMorsel, cxx_mask_c
from libcpp.memory cimport shared_ptr
from draken.vectors.bool_vector cimport (
    BoolVector,
    from_decoded,
    c_and_bitmap,
    c_not_bitmap,
    c_or_bitmap,
    c_xor_bitmap,
    c_get_bitmap_ptrs,
    bool_vector_from_bits,
)
from draken.vectors.vector cimport Vector, simd_popcount, from_decoded as vec_from_decoded
from draken.core.frame_arena cimport (
    DrakenFrameArena,
    draken_frame_arena_create,
    draken_frame_arena_destroy,
    draken_frame_arena_alloc,
    draken_frame_arena_release,
    draken_frame_arena_adopt,
)
from draken.ops.compare_dv cimport draken_compare_dv
from draken.ops.arithmetic_dv cimport draken_arithmetic_dv

# Phase 9c: C kernel ABI — function-pointer signatures for binary ops, casts, extractions
cdef extern from "ops/vec_result.h":
    ctypedef struct VecResult:
        void*             data
        uint8_t*          validity
        const uint32_t*   selection
        bint              owns_selection
        uint32_t          data_length
        uint32_t          length
        DrakenType        type
        uint8_t           flags

# Function-pointer typedefs per Decision 3 (Phase 9 design, §Post-design)
ctypedef VecResult (*binop_fn_t)(void* ctx, const DrakenVector* left, const DrakenVector* right) nogil
ctypedef VecResult (*cast_fn_t)(void* ctx, const DrakenVector* v) nogil
ctypedef VecResult (*extr_fn_t)(void* ctx, const DrakenVector* v, const DrakenVector* key) nogil
ctypedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs) nogil
ctypedef VecResult (*case_fn_t)(void* ctx, void* morsel) nogil

# Error handling for kernel results
cdef extern from "ops/kernels/error_handling.h":
    const char* draken_get_error_message() nogil
    void draken_error_message_clear() nogil
    bint draken_has_error() nogil

# VecResult → Python Vector (VectorOwner) trampoline. Declared returning `object`
# so Cython manages the new reference; honors validity_embedded + ts_unit, which a
# bare arena DV* cannot carry (string consolidated block / timestamp unit descriptor).
cdef extern from "core/draken_bridge.h":
    object draken_vecresult_own_c(VecResult res)


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
    cdef uint8_t* left_data
    cdef uint8_t* right_data
    cdef void* result_data_ptr
    cdef uint8_t* result_val_ptr
    cdef int had_null
    sp -= 1
    dv_right_ptr = dv_stack[sp]
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    if dv_left_ptr == NULL or dv_right_ptr == NULL:
        return 1
    left_data  = _ensure_dense_bitmap_c(dv_left_ptr,  nbytes, num_rows, arena)
    right_data = _ensure_dense_bitmap_c(dv_right_ptr, nbytes, num_rows, arena)
    if left_data == NULL or right_data == NULL:
        return 2
    result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
    result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    if result_data_ptr == NULL or result_val_ptr == NULL:
        return 2
    if op == 0:
        had_null = c_and_bitmap(
            <uint8_t*>result_data_ptr, result_val_ptr,
            left_data, dv_left_ptr.validity, right_data, dv_right_ptr.validity,
            <size_t>nbytes, num_rows)
    elif op == 1:
        had_null = c_or_bitmap(
            <uint8_t*>result_data_ptr, result_val_ptr,
            left_data, dv_left_ptr.validity, right_data, dv_right_ptr.validity,
            <size_t>nbytes, num_rows)
    else:
        had_null = c_xor_bitmap(
            <uint8_t*>result_data_ptr, result_val_ptr,
            left_data, dv_left_ptr.validity, right_data, dv_right_ptr.validity,
            <size_t>nbytes, num_rows)
    dv_store[sp] = draken_vector_from_dense(
        result_data_ptr, num_rows, DRAKEN_BOOL,
        result_val_ptr if had_null else NULL)
    dv_stack[sp] = &dv_store[sp]
    sp += 1
    sp_io[0] = sp
    return 0


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
    cdef uint8_t* left_data
    cdef void* result_data_ptr
    cdef uint8_t* result_val_ptr
    cdef int had_null
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    if dv_left_ptr == NULL:
        return 1
    left_data = _ensure_dense_bitmap_c(dv_left_ptr, nbytes, num_rows, arena)
    if left_data == NULL:
        return 2
    result_data_ptr = draken_frame_arena_alloc(arena, <size_t>nbytes)
    result_val_ptr  = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    if result_data_ptr == NULL or result_val_ptr == NULL:
        return 2
    had_null = c_not_bitmap(
        <uint8_t*>result_data_ptr, result_val_ptr,
        left_data, dv_left_ptr.validity, <size_t>nbytes, num_rows)
    dv_store[sp] = draken_vector_from_dense(
        result_data_ptr, num_rows, DRAKEN_BOOL,
        result_val_ptr if had_null else NULL)
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
    cdef Py_ssize_t sp = sp_io[0]
    cdef Py_ssize_t base = sp - arity
    cdef Py_ssize_t j
    cdef DrakenVector* dv_left_ptr = dv_stack[base]
    cdef DrakenVector* dv_right_ptr
    cdef uint8_t* cur_data
    cdef uint8_t* cur_null
    cdef uint8_t* right_data
    cdef uint8_t* next_data
    cdef uint8_t* next_null
    cdef uint8_t* dense
    cdef int had_null = 0
    sp = base
    if dv_left_ptr == NULL:
        return 1
    # Accumulator: copy first operand's bitmap (+validity).
    cur_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    cur_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
    if cur_data == NULL or cur_null == NULL:
        return 2
    dense = _ensure_dense_bitmap_c(dv_left_ptr, nbytes, num_rows, arena)
    if dense == NULL:
        return 2
    memcpy(cur_data, dense, <size_t>nbytes)
    if dv_left_ptr.validity != NULL:
        memcpy(cur_null, dv_left_ptr.validity, <size_t>nbytes)
    else:
        memset(cur_null, 0, <size_t>nbytes)
    for j in range(1, arity):
        dv_right_ptr = dv_stack[base + j]
        if dv_right_ptr == NULL:
            return 1
        right_data = _ensure_dense_bitmap_c(dv_right_ptr, nbytes, num_rows, arena)
        if right_data == NULL:
            return 2
        next_data = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
        next_null = <uint8_t*>draken_frame_arena_alloc(arena, <size_t>nbytes)
        if next_data == NULL or next_null == NULL:
            return 2
        if op == 0:
            had_null = c_and_bitmap(
                next_data, next_null, cur_data, cur_null,
                right_data, dv_right_ptr.validity, <size_t>nbytes, num_rows)
        else:
            had_null = c_or_bitmap(
                next_data, next_null, cur_data, cur_null,
                right_data, dv_right_ptr.validity, <size_t>nbytes, num_rows)
        cur_data = next_data
        cur_null = next_null
    dv_store[sp] = draken_vector_from_dense(
        cur_data, num_rows, DRAKEN_BOOL, cur_null if had_null else NULL)
    dv_stack[sp] = &dv_store[sp]
    sp += 1
    sp_io[0] = sp
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
    draken_frame_arena_adopt(arena, vr.data)
    if vr.validity != NULL:
        draken_frame_arena_adopt(arena, vr.validity)
    dv_store[slot_idx] = draken_vector_from_dense(vr.data, vr.length, vr.type, vr.validity)
    dv_stack[slot_idx] = &dv_store[slot_idx]
    return 0


cdef inline int _dv_cast_kernel_c(
    void* kernel_fn,
    void* ctx_ptr,
    DrakenVector* dv_left_ptr,
    DrakenVector* dv_store,
    DrakenVector** dv_stack,
    Py_ssize_t slot_idx,          # result slot (== sp after the one pop)
    DrakenFrameArena* arena,
    VecResult* out_vr,
) noexcept nogil:
    """C-native BC_CAST kernel dispatch (unary; mirrors _dv_binop_kernel_c).
    rc 0 = fixed-width result folded into the arena and pushed; rc 4 = kernel
    error; rc 5 = string result in out_vr (GIL caller wraps as a Vector)."""
    cdef VecResult vr = (<cast_fn_t>kernel_fn)(ctx_ptr, dv_left_ptr)
    out_vr[0] = vr
    if vr.data == NULL:
        return 4
    if (vr.type == DRAKEN_VARCHAR or vr.type == DRAKEN_NVARCHAR
            or vr.type == DRAKEN_VARBINARY):
        return 5
    draken_frame_arena_adopt(arena, vr.data)
    if vr.validity != NULL:
        draken_frame_arena_adopt(arena, vr.validity)
    dv_store[slot_idx] = draken_vector_from_dense(vr.data, vr.length, vr.type, vr.validity)
    dv_stack[slot_idx] = &dv_store[slot_idx]
    return 0


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
    draken_frame_arena_release(arena, dp)
    if vp != NULL:
        draken_frame_arena_release(arena, vp)
    if dv.type == DRAKEN_BOOL:
        return from_decoded(dp, vp, <size_t>dv.length)
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


cdef int c_execute_dv_inner(
    BytecodeInstr* instrs, Py_ssize_t n_instrs,
    DrakenVector** dv_cache,
    DrakenVector** dv_stack, DrakenVector* dv_store,
    DrakenFrameArena* arena,
    Py_ssize_t nbytes, uint32_t num_rows,
    int* err_op,
) noexcept nogil:
    """Nogil DV* VM inner loop for is_all_c_native bytecodes.

    Loads read pre-resolved DV* from dv_cache; compute ops call the shared
    _dv_* helpers. Returns 0 on success (result at dv_stack[0]); otherwise the
    helper rc (1 NULL-operand, 2 alloc, 3 compare-N/A, 4 kernel-error, 5 string)
    with *err_op set to the failing opcode. No PyObject, no anchor — the GIL
    caller materializes dv_stack[0] (an arena result) and maps any error.
    """
    cdef Py_ssize_t sp = 0
    cdef Py_ssize_t i
    cdef int opcode, rc, dv_op
    cdef BytecodeInstr* slot
    cdef DrakenVector* dv_left_ptr
    cdef DrakenVector* dv_right_ptr
    cdef void* result_data_ptr
    cdef VecResult vr
    for i in range(n_instrs):
        slot = &instrs[i]
        opcode = slot.opcode

        if opcode == BC_LOAD_COL:
            dv_stack[sp] = dv_cache[i]
            sp += 1
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
            if rc == 0:
                sp += 1
        elif opcode == BC_CAST:
            sp -= 1
            dv_left_ptr = dv_stack[sp]
            if dv_left_ptr == NULL:
                err_op[0] = opcode
                return 1
            rc = _dv_cast_kernel_c(slot.kernel_fn, <void*>slot.ctx_ptr,
                                   dv_left_ptr, dv_store, dv_stack, sp, arena, &vr)
            if rc == 0:
                sp += 1
        else:
            err_op[0] = opcode
            return 99
        if rc != 0:
            err_op[0] = opcode
            return rc
    return 0


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
    cdef int rc
    cdef bint use_substrate = (m != NULL)
    cdef object err_msg
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
                arena, nbytes, <uint32_t>num_rows, &err_op)
        if rc == 0:
            # Gate guarantees the last op is a compute op → arena result, anchor None.
            return _slot_to_pyobj(dv_stack[0], None, arena)
        if rc == 4:
            err_msg = (
                draken_get_error_message().decode("utf-8", "replace")
                if draken_has_error() else "C kernel error"
            )
            draken_error_message_clear()
            raise ValueError(err_msg)
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
        if slot.opcode == BC_LOAD_COL:
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


cdef int _dv_filter_span_cxx(
    BytecodeInstr* instrs, int count, const CxxMorsel* m,
    int* col_idx, DrakenVector** lit_dv,
    CxxMorsel** out_filtered, int* err_op,
) noexcept nogil:
    """Pure-nogil filter span: fill the DV* cache from a PRE-RESOLVED (col_idx,
    lit_dv) pair, evaluate the predicate, and gather the surviving rows via
    cxx_mask_c. Owns its frame arena (created/destroyed here). Returns the
    c_execute rc: 0 → ``*out_filtered`` is a NEW owned CxxMorsel; 4 → kernel error;
    99 → arena OOM; other → not applicable. (col_idx, lit_dv) are resolved ONCE by
    the caller via _dv_cxx_resolve_caches and reused across morsels — that resolve is
    the only GIL-needing step; this span has no PyObject access, so a converted
    operator can call it inside `with nogil`."""
    cdef DrakenVector* dv_cache[256]
    cdef DrakenVector* dv_stack[64]
    cdef DrakenVector  dv_store[64]
    cdef Py_ssize_t num_rows = m.num_rows()
    cdef Py_ssize_t nbytes = (num_rows + 7) >> 3
    cdef int rc
    cdef DrakenFrameArena* arena = draken_frame_arena_create()
    if arena == NULL:
        err_op[0] = -99
        return 99
    _dv_fill_cache_cxx(instrs, count, m, col_idx, lit_dv, dv_cache)
    rc = c_execute_dv_inner(instrs, count, dv_cache, dv_stack, dv_store,
                            arena, nbytes, <uint32_t>num_rows, err_op)
    if rc == 0:
        out_filtered[0] = cxx_mask_c(m, dv_stack[0])
    draken_frame_arena_destroy(arena)
    return rc


cpdef object filter_morsel_c_native(CompiledBytecode bc, Morsel morsel):
    """S3.2: evaluate an all-c-native predicate AND apply its mask in ONE nogil
    span over the CxxMorsel — the predicate result DV* feeds straight into
    cxx_mask_c, no Python BoolVector materialized, no nanobind mask crossing, one
    GIL release for the whole filter. Returns the filtered (Cxx-backed) Morsel,
    or None when not applicable (caller falls back to execute_bytecode +
    filter_mask). Raises on a genuine C kernel error (rc 4)."""
    cdef const CxxMorsel* m = morsel._cxx_ptr
    if m == NULL or bc.count > 256:
        return None
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    if num_rows == 0:
        return None
    cdef int col_idx[256]
    cdef DrakenVector* lit_dv[256]
    cdef int err_op = 0
    cdef int rc
    cdef CxxMorsel* filtered = NULL
    cdef object err_msg
    if _dv_cxx_resolve_caches(bc, m, col_idx, lit_dv) != 0:
        return None
    with nogil:
        rc = _dv_filter_span_cxx(bc.instrs, bc.count, m, col_idx, lit_dv, &filtered, &err_op)
    if rc == 0:
        return cxx_to_morsel(shared_ptr[CxxMorsel](filtered))
    if rc == 4:
        err_msg = (
            draken_get_error_message().decode("utf-8", "replace")
            if draken_has_error() else "C kernel error"
        )
        draken_error_message_clear()
        raise ValueError(err_msg)
    # rc 1/2/3/5/99: signal the caller to use the Morsel VM + filter_mask path.
    return None


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
    cdef Py_ssize_t i, j, base
    cdef int opcode
    cdef int arity
    cdef int flags
    cdef BytecodeInstr* slot
    cdef BoolVector b_result
    cdef Vector v_result
    cdef Py_ssize_t num_rows = morsel.ptr.num_rows
    cdef Py_ssize_t nbytes = (<Py_ssize_t>num_rows + 7) >> 3
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
    cdef DrakenFrameArena* arena = NULL
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
    cdef object cast_err_msg
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

    arena = draken_frame_arena_create()
    if arena == NULL:
        raise MemoryError("execute_bytecode: failed to create DrakenFrameArena")

    try:
        for i in range(n_instrs):
            slot = &bc.instrs[i]
            opcode = slot.opcode

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
            # BC_BETWEEN — typed draken_between (cpdef)
            # ----------------------------------------------------------
            if opcode == BC_BETWEEN:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
                compare_result = draken_between(
                    py_left,
                    <object>slot.literal_obj if slot.literal_obj != NULL else None,
                    <object>slot.literal_obj2 if slot.literal_obj2 != NULL else None,
                    slot.op_code != 0,
                    slot.bool_value != 0,
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
                        cast_err_msg = (
                            draken_get_error_message().decode("utf-8", "replace")
                            if draken_has_error() else "C binop kernel error"
                        )
                        draken_error_message_clear()
                        raise ValueError(cast_err_msg)
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
                        global _c_native_kernel_call_count
                        _c_native_kernel_call_count += 1  # Count as C-native dispatch
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
                callable_obj = <object>slot.callable_ref
                is_nb_callable = slot.bool_value != 0

                if arity == 0:
                    legacy_result = callable_obj(num_rows)
                else:
                    func_base = sp - arity
                    sp = func_base

                    if is_nb_callable:
                        if arity == 1:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                            )
                        elif arity == 2:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                            )
                        elif arity == 3:
                            legacy_result = callable_obj(
                                (<Vector>_slot_to_pyobj(dv_stack[func_base], anchor[func_base], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 1], anchor[func_base + 1], arena))._nb,
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + 2], anchor[func_base + 2], arena))._nb,
                            )
                        else:
                            func_args = [
                                (<Vector>_slot_to_pyobj(dv_stack[func_base + j], anchor[func_base + j], arena))._nb
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
            # BC_EXTRACTION — Phase 3: direct native kernel calls.
            # Sub-op code in slot.op_code (BC_EXTR_MAP_STRING, etc.)
            # Key stored in slot.literal_obj (bytes or Vector) or slot.bool_value (scalar int).
            # ----------------------------------------------------------
            if opcode == BC_EXTRACTION:
                sp -= 1
                dv_left_ptr = dv_stack[sp]
                py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)

                # Unwrap Cython shim to nanobind Vector for native kernel calls.
                if isinstance(py_left, Vector):
                    py_left_nb = (<Vector>py_left)._nb
                else:
                    py_left_nb = py_left

                # Dispatch to the resolved native kernel based on sub-op code.
                if slot.op_code == BC_EXTR_MAP_STRING:
                    legacy_result = _vector_map_access_string(py_left_nb, <object>slot.literal_obj)
                elif slot.op_code == BC_EXTR_MAP_ARRAY:
                    legacy_result = _vector_array_map_access(py_left_nb, <int64_t>slot.bool_value)
                elif slot.op_code == BC_EXTR_JSON_PTR:
                    # `->` → VARIANT (JSON value)
                    legacy_result = _vector_json_extract(py_left_nb, <object>slot.literal_obj)
                elif slot.op_code == BC_EXTR_JSON_KEY:
                    # `->>` → NVARCHAR (text; JSON strings unquoted)
                    legacy_result = _vector_json_extract_text(py_left_nb, <object>slot.literal_obj)
                else:
                    raise NotImplementedError(f"BC_EXTRACTION: unknown sub-op {slot.op_code}")

                # Result wrap — kernel returns nanobind Vector, wrap as needed.
                if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
                    if not isinstance(legacy_result, Vector):
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
                if (slot.flags & BC_INSTR_C_NATIVE) != 0 and dv_left_ptr != NULL:
                    rc = _dv_cast_kernel_c(
                        slot.kernel_fn, <void*>slot.ctx_ptr, dv_left_ptr,
                        dv_store, dv_stack, sp, arena, &cast_vr)
                    if rc == 4:
                        cast_err_msg = (
                            draken_get_error_message().decode("utf-8", "replace")
                            if draken_has_error() else "C cast kernel error"
                        )
                        draken_error_message_clear()
                        raise ValueError(cast_err_msg)
                    if rc == 5:
                        # String result: consolidated block with embedded validity —
                        # own it as a Vector (the canonical owner; carries the block).
                        # Stays on the GIL path (string ownership can't fold to arena).
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
