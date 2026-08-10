# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""C++ CompiledExpression arena + typed bytecode builder.

Two public entry points:
  lower(node)            — lower a Python Node tree into a CompiledExpressionHandle
  build_bytecode(handle) — linearise the arena tree into a typed CompiledBytecode

CompiledBytecode is consumed by execute_bytecode() in the evaluator package.
Hot-path types (BytecodeInstr, CompiledBytecode internals) live in the .pxd
so the executor can read C struct fields directly with no Python protocol.

CLAUDE.md §2/§3: no `object`-typed fields in the hot data layout, no Python
list as instruction store, no method dispatch through PyObject during exec.
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.ref cimport PyObject, Py_INCREF
from libc.stdlib cimport free
from libc.string cimport memset

import draken.draken_native as _draken_native
from opteryx.exceptions import IncorrectTypeError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import InvalidFunctionParameterError

import datetime as _datetime
import decimal as _decimal
import struct as _struct

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY, DRAKEN_INTERVAL, DRAKEN_DATE32, DRAKEN_TIMESTAMP64, DRAKEN_VECTOR_FP16, DRAKEN_ARRAY
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_UINT8, DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64
from draken.core.buffers cimport DRAKEN_DECIMAL128, DRAKEN_DECIMAL, DRAKEN_TIME32, DRAKEN_TIME64

# Epoch anchor for DATE literal → days-since-1970 conversion (bind time only).
cdef object _EPOCH_DATE = _datetime.date(1970, 1, 1)


cdef Vector _materialise_constant_literal(object value, int physical_type,
                                           int precision=-1, int scale=-1,
                                           object logical=None):
    """Materialise a scalar literal into a constant-shape Draken Vector ONCE at
    bind time (data_length==1, length==1). The executor re-stamps only the
    logical length per morsel — no per-morsel Python object, isinstance, or
    re-encode (CLAUDE.md §2/§3).

    Strings are bound to bytes here: a Python str is encoded to UTF-8 exactly
    once so it never reaches the Draken edge. The string subtype is selected from
    physical_type — VARCHAR/VARBINARY store the bytes verbatim; NVARCHAR validates
    UTF-8 inside the native constructor. Non-string scalars dispatch on the Python
    value type, preserving the existing INT64/FLOAT64/DECIMAL/DATE32/TIMESTAMP
    mappings exactly.

    precision/scale (Decimal only): the bind-time DECLARED (precision, scale) —
    e.g. from `CAST(x AS DECIMAL(38,6))` — passed by the caller when known.
    -1 means undeclared: fall back to deriving them from the value's own digit
    count. This matters because the parsed Decimal value is NOT re-quantized to
    the declared scale upstream (parser_for(DECIMAL) leaves '1.23' as-is even
    when CAST to DECIMAL(38,6)) — deriving from the value alone silently picks
    the wrong physical tier (DECIMAL vs DECIMAL128) and the wrong stored
    magnitude whenever the value's natural scale differs from the declared one.
    """
    cdef long long ordinal
    cdef bytes raw
    cdef object int_vec
    cdef int use_precision, use_scale
    if value is None:
        # A typed NULL literal (e.g. CAST(NULL AS VARCHAR)) must materialise a
        # null constant of its declared physical type, NOT an untyped DRAKEN_NULL
        # vector. String-family kernels (concat, LIKE, ...) dispatch on the
        # operand's bind type and read the string arena directly; handed a
        # DRAKEN_NULL vector (data==NULL, validity==NULL ⇒ all-valid) they read
        # garbage slots and emit non-null junk. The typed null constant carries a
        # real all-null validity bitmap and the correct slot layout. Numeric and
        # untyped (physical_type == -1) NULLs keep DRAKEN_NULL — their kernels
        # short-circuit on the DRAKEN_NULL tag.
        if physical_type == <int>DRAKEN_VARCHAR:
            return Vector(_draken_native.vector_varchar_from_constant(None, 1))
        if physical_type == <int>DRAKEN_NVARCHAR:
            return Vector(_draken_native.vector_nvarchar_from_constant(None, 1))
        if physical_type == <int>DRAKEN_VARBINARY:
            return Vector(_draken_native.vector_varbinary_from_constant(None, 1))
        # Scalar numeric/date physical tags — e.g. a FULL OUTER JOIN rewritten as a
        # UNION synthesises a typed NULL for the non-preserved side's columns (see
        # opteryx/planner/binder/set_ops.py's `_cast_leg_columns_to`), which retypes
        # the literal to the leg's coerced column type. Concatenating that leg's
        # output with the other leg's real column of the same type (Morsel.combine)
        # requires both sides to carry the SAME physical tag, not an untyped
        # DRAKEN_NULL. Each of these constant constructors already accepts
        # value=None (an all-null constant of its own type); no descriptor
        # (precision/scale/unit) is needed for any of them.
        if physical_type == <int>DRAKEN_INT8:
            return Vector(_draken_native.vector_int8_from_constant(None, 1))
        if physical_type == <int>DRAKEN_INT16:
            return Vector(_draken_native.vector_int16_from_constant(None, 1))
        if physical_type == <int>DRAKEN_INT32:
            return Vector(_draken_native.vector_int32_from_constant(None, 1))
        if physical_type == <int>DRAKEN_INT64:
            return Vector(_draken_native.vector_from_constant(None, 1))
        if physical_type == <int>DRAKEN_UINT8:
            return Vector(_draken_native.vector_uint8_from_constant(None, 1))
        if physical_type == <int>DRAKEN_UINT16:
            return Vector(_draken_native.vector_uint16_from_constant(None, 1))
        if physical_type == <int>DRAKEN_UINT32:
            return Vector(_draken_native.vector_uint32_from_constant(None, 1))
        if physical_type == <int>DRAKEN_UINT64:
            return Vector(_draken_native.vector_uint64_from_constant(None, 1))
        if physical_type == <int>DRAKEN_FLOAT32:
            return Vector(_draken_native.vector_float32_from_constant(None, 1))
        if physical_type == <int>DRAKEN_FLOAT64:
            return Vector(_draken_native.vector_float64_from_constant(None, 1))
        if physical_type == <int>DRAKEN_DATE32:
            return Vector(_draken_native.vector_date32_from_constant(None, 1))
        # DECIMAL/DECIMAL128/TIMESTAMP64/TIME32/TIME64/ARRAY are the PARAMETERIZED
        # and STRUCTURED physical types (§14): their tag alone is not a full type,
        # so a NULL literal for one of them used to fall through to untyped
        # DRAKEN_NULL rather than guess a descriptor.
        #
        # That is safe for a kernel (they short-circuit on the DRAKEN_NULL tag) but
        # NOT for a concat. FullOuterToUnionStrategy rewrites FULL OUTER JOIN into
        # (LEFT OUTER) UNION (LEFT ANTI + synthesized NULL literals for the
        # non-preserved side); the union then concatenates the real column against
        # those literals, and `vector_concat` requires ONE type. Every FULL OUTER
        # JOIN projecting a DECIMAL, TIMESTAMP, TIME or ARRAY column died with
        # "concat: all inputs must share one type". Unparameterized types were
        # unaffected — they already had a typed-null constructor above.
        #
        # `logical` carries the declared descriptor when the caller knows it, which
        # is exactly the union case (set_ops retypes the NULL literal to the other
        # leg's ColumnType). Without a descriptor we still cannot invent one, so the
        # untyped fall-through below is kept for that case.
        if physical_type == <int>DRAKEN_DECIMAL and precision >= 1:
            return Vector(_draken_native.vector_decimal_from_constant(
                None, 1, precision, scale if scale >= 0 else 0))
        if physical_type == <int>DRAKEN_DECIMAL128 and precision >= 1:
            return Vector(_draken_native.vector_decimal128_from_constant(
                None, 1, precision, scale if scale >= 0 else 0))
        if physical_type == <int>DRAKEN_ARRAY:
            # One null ARRAY row; the child type rides on `logical`/the schema, and
            # a null row has no elements to type either way.
            return Vector(_draken_native.vector_array_from_sequence([None]))
        if physical_type == <int>DRAKEN_TIMESTAMP64:
            null_ts = _draken_native.vector_reinterpret_as_timestamp64(
                _draken_native.vector_from_constant(None, 1))
            if logical is not None:
                _draken_native.vector_attach_logical_type(null_ts, logical)
            return Vector(null_ts)
        if physical_type == <int>DRAKEN_TIME32:
            null_t = _draken_native.vector_reinterpret_as_time32(
                _draken_native.vector_from_constant(None, 1))
            if logical is not None:
                _draken_native.vector_attach_logical_type(null_t, logical)
            return Vector(null_t)
        if physical_type == <int>DRAKEN_TIME64:
            null_t = _draken_native.vector_reinterpret_as_time64(
                _draken_native.vector_from_constant(None, 1))
            if logical is not None:
                _draken_native.vector_attach_logical_type(null_t, logical)
            return Vector(null_t)
        # BOOL has no constant constructor in draken_native at all. Numeric and
        # genuinely untyped (physical_type == -1) NULLs keep DRAKEN_NULL — their
        # kernels short-circuit on the DRAKEN_NULL tag.
        return Vector(_draken_native.vector_null_from_length(1))
    if isinstance(value, bool):
        # Bools are handled upstream by BC_LOAD_LIT_BOOL; reaching here is a bug.
        raise InvalidInternalStateError(
            "_materialise_constant_literal: bool literal must use BC_LOAD_LIT_BOOL"
        )
    if isinstance(value, int):
        # Temporal literals are carried as raw integers (DATE32 = days-since-epoch,
        # TIMESTAMP64 = microseconds-since-epoch) but tagged with their temporal
        # physical type — this is how the planner folds CAST(<str> AS DATE/TIMESTAMP)
        # and stores typed temporal literals. Reinterpret the int constant as the
        # tagged temporal type so the materialised vector carries DATE32/TIMESTAMP64
        # (mirrors the datetime.date / datetime.datetime branches below). Without
        # this the constant would surface as a bare INT64 and downstream temporal
        # kernels (e.g. DATE + INTERVAL) would reject it.
        # Narrow-int / float physical tags are set by the comparison binder when a
        # scalar literal is coerced to a column's physical type so the c-native
        # compare fast path (draken_compare_dv, identical-type only) can fire. The
        # value-fits check is enforced by the binder before tagging; the narrow
        # constructors raise on overflow as a backstop.
        if physical_type == <int>DRAKEN_INT8:
            return Vector(_draken_native.vector_int8_from_constant(value, 1))
        if physical_type == <int>DRAKEN_INT16:
            return Vector(_draken_native.vector_int16_from_constant(value, 1))
        if physical_type == <int>DRAKEN_INT32:
            return Vector(_draken_native.vector_int32_from_constant(value, 1))
        # E33: UINT64 must be materialised directly — a value can exceed
        # INT64_MAX (the fallback `vector_from_constant` below is INT64-only
        # and would std::bad_cast on nb::cast<int64_t> for such a value).
        # UINT8/16/32 route here too for directness, though their range always
        # fits the INT64 fallback.
        if physical_type == <int>DRAKEN_UINT8:
            return Vector(_draken_native.vector_uint8_from_constant(value, 1))
        if physical_type == <int>DRAKEN_UINT16:
            return Vector(_draken_native.vector_uint16_from_constant(value, 1))
        if physical_type == <int>DRAKEN_UINT32:
            return Vector(_draken_native.vector_uint32_from_constant(value, 1))
        if physical_type == <int>DRAKEN_UINT64:
            return Vector(_draken_native.vector_uint64_from_constant(value, 1))
        if physical_type == <int>DRAKEN_FLOAT32:
            return Vector(_draken_native.vector_float32_from_constant(value, 1))
        if physical_type == <int>DRAKEN_FLOAT64:
            return Vector(_draken_native.vector_float64_from_constant(value, 1))
        int_vec = _draken_native.vector_from_constant(value, 1)
        if physical_type == <int>DRAKEN_DATE32:
            return Vector(_draken_native.vector_reinterpret_as_date32(int_vec))
        if physical_type == <int>DRAKEN_TIMESTAMP64:
            return Vector(_draken_native.vector_reinterpret_as_timestamp64(int_vec))
        return Vector(int_vec)
    if isinstance(value, float):
        if physical_type == <int>DRAKEN_FLOAT32:
            return Vector(_draken_native.vector_float32_from_constant(value, 1))
        return Vector(_draken_native.vector_float64_from_constant(value, 1))
    if isinstance(value, (str, bytes)):
        if isinstance(value, str):
            raw = (<str>value).encode("utf-8")
        else:
            raw = <bytes>value
        if physical_type == <int>DRAKEN_NVARCHAR:
            return Vector(_draken_native.vector_nvarchar_from_constant(raw, 1))
        if physical_type == <int>DRAKEN_VARBINARY:
            return Vector(_draken_native.vector_varbinary_from_constant(raw, 1))
        # Default (VARCHAR and any unspecified physical tag): raw bytes verbatim.
        return Vector(_draken_native.vector_varchar_from_constant(raw, 1))
    if isinstance(value, _decimal.Decimal):
        if precision >= 1:
            use_precision = precision
            use_scale = scale if scale >= 0 else 0
        else:
            sign, digits, exponent = value.as_tuple()
            use_scale = max(0, -int(exponent))
            use_precision = max(len(digits), use_scale + 1)
        if physical_type == <int>DRAKEN_DECIMAL128 or use_precision > 18:
            return Vector(_draken_native.vector_decimal128_from_constant(value, 1, use_precision, use_scale))
        return Vector(_draken_native.vector_decimal_from_constant(value, 1, use_precision, use_scale))
    if physical_type == <int>DRAKEN_INTERVAL:
        # INTERVAL literals are (months, microseconds) tuples (see
        # logical_planner_builders.literal_interval). Materialise a constant-shape
        # INTERVAL Vector so the temporal arithmetic kernels receive a real vector
        # rather than a raw Python tuple.
        return Vector(_draken_native.vector_interval_from_constant(value, 1))
    if physical_type == <int>DRAKEN_ARRAY:
        # An ARRAY literal's value is a Python list/tuple of element values — ONE
        # array row. vector_array_from_sequence takes a LIST OF ROWS, so wrap it as
        # a single-row sequence [[...]]. The result is a length-1 ARRAY Vector with
        # a real child, which the executor broadcasts per morsel (the same
        # constant-shape contract as every other constant here). This is what lets
        # a folded ARRAY-literal argument (GREATEST([1,5,3])) reach a reducer as a
        # Vector rather than a bare Python list.
        return Vector(_draken_native.vector_array_from_sequence([list(value)]))
    if physical_type == <int>DRAKEN_VECTOR_FP16:
        # CAST(<array literal> AS VECTOR(n)) folds to a VECTOR-typed literal whose
        # value is the element list — a genuine scalar (ONE vector), not an in-list
        # collection, exactly like INTERVAL's tuple above. One row in, so the result is
        # the constant-shape (data_length==1) vector the cosine kernels read via the
        # uniform data[selection[i]]. The width is the row's own length: the fold in
        # _cast_literal_value already checked it against the DECLARED VECTOR(n).
        return Vector(
            _draken_native.vector_fp16_from_sequence([list(value)], len(value)))
    if isinstance(value, _datetime.date) and not isinstance(value, _datetime.datetime):
        ordinal = (value - _EPOCH_DATE).days
        int_vec = _draken_native.vector_from_constant(ordinal, 1)
        return Vector(_draken_native.vector_reinterpret_as_date32(int_vec))
    if isinstance(value, _datetime.datetime):
        return Vector(_draken_native.vector_timestamp_from_constant(value, 1))
    if isinstance(value, _datetime.time):
        return Vector(_draken_native.vector_time64_from_constant(value, 1))
    raise InvalidInternalStateError(
        f"_materialise_constant_literal: cannot materialise constant for literal "
        f"{value!r} (type {type(value).__name__})"
    )


cdef bint _exact_as_float32(object v) except -1:
    """True if numeric ``v`` round-trips exactly through IEEE-754 binary32.

    Bind-time only (one call per coerced literal). The magnitude guard keeps the
    pack/unpack in range so it never raises — a value outside binary32 range or
    a NaN literal returns False and is left to the Python compare fallback.
    """
    if v != v:                              # NaN literal — fallback handles it
        return False
    if v < -3.4e38 or v > 3.4e38:           # outside binary32 range
        return False
    return _struct.unpack("<f", _struct.pack("<f", v))[0] == v


def _column_type_for_physical(int physical_code):
    """Canonical unparameterized ColumnType for a DrakenType int. Used to report a
    coerced literal's new physical type to the c-native gate."""
    from draken.draken_native import DrakenType as _DT
    from opteryx.types.logical_type import ColumnType as _CT
    return _CT(_DT(physical_code))


cdef int _coerce_literal_physical(object col_type, object lit_value) except -99:
    """Decide the DrakenType a scalar literal must be materialised as so it shares
    the column's physical type and the c-native compare fast path
    (``draken_compare_dv``, identical-type only) can fire instead of declining to
    the Python fallback. Returns the DrakenType int, or -1 to leave the literal at
    its natural type.

    Only value-EXACT numeric coercions are returned: a literal that cannot be
    represented in the column's physical type without loss is left untouched, so
    the fallback yields the same answer it does today (CLAUDE.md §1 correctness).
    INT64 column vs int literal and FLOAT64 column vs float literal already share a
    type, so they return -1 (no re-materialisation needed).
    """
    if col_type is None or lit_value is None:
        return -1
    if isinstance(lit_value, bool):             # bool → BC_LOAD_LIT_BOOL, never here
        return -1
    cdef object phys = col_type.physical
    cdef str pname = getattr(phys, "name", None)
    if pname is None:
        return -1
    if isinstance(lit_value, int):
        if pname == "INT8":
            return <int>DRAKEN_INT8 if -128 <= lit_value <= 127 else -1
        if pname == "INT16":
            return <int>DRAKEN_INT16 if -32768 <= lit_value <= 32767 else -1
        if pname == "INT32":
            return <int>DRAKEN_INT32 if -2147483648 <= lit_value <= 2147483647 else -1
        # Unsigned columns need the same treatment: draken_compare_dv is
        # identical-type only, so an INT64 literal against a UINT column declines
        # to the fallback (and, on the relocated native ExprFilter, raised
        # err_op=11 — which is why unsigned predicate inputs used to fail the scan
        # closed). A negative literal can never equal an unsigned value and does
        # not fit the type, so it stays untouched and takes the fallback, which
        # compares correctly.
        if pname == "UINT8":
            return <int>DRAKEN_UINT8 if 0 <= lit_value <= 255 else -1
        if pname == "UINT16":
            return <int>DRAKEN_UINT16 if 0 <= lit_value <= 65535 else -1
        if pname == "UINT32":
            return <int>DRAKEN_UINT32 if 0 <= lit_value <= 4294967295 else -1
        if pname == "UINT64":
            return <int>DRAKEN_UINT64 if 0 <= lit_value <= 18446744073709551615 else -1
        if pname == "FLOAT64":
            # every |int| <= 2**53 is exactly representable in binary64
            return <int>DRAKEN_FLOAT64 if -9007199254740992 <= lit_value <= 9007199254740992 else -1
        if pname == "FLOAT32":
            return <int>DRAKEN_FLOAT32 if _exact_as_float32(lit_value) else -1
        return -1
    if isinstance(lit_value, float):
        if pname == "FLOAT32":
            return <int>DRAKEN_FLOAT32 if _exact_as_float32(lit_value) else -1
        return -1
    return -1


# ---------------------------------------------------------------------------
# Phase 9b: Kernel resolution and context wrapper
# ---------------------------------------------------------------------------

cdef class _KernelContextWrapper:
    """Wraps a C context pointer for lifetime management.
    Ensures the context is freed when the wrapper is garbage collected."""
    cdef public unsigned long long ctx_ptr

    def __cinit__(self, unsigned long long ctx_ptr):
        self.ctx_ptr = ctx_ptr

    def __dealloc__(self):
        # No Python here — __dealloc__ can run during interpreter shutdown when
        # the import system is already torn down. Every kernel context is a single
        # malloc block (see kernel_context.h / kernel_registry.cpp), so draken's
        # kernel_free_context() is exactly free(ctx). Call libc free directly at
        # the C level: no import, no cross-module linkage on the dealloc path.
        if self.ctx_ptr != 0:
            free(<void*>self.ctx_ptr)

    def __repr__(self):
        return f"<KernelContextWrapper {self.ctx_ptr}>"


# Kernels that live in OTHER compiled modules and self-register with the draken
# registry at their module's import — kernel name -> home module to import on a
# bind-time lookup miss (see _resolve_kernel_and_context).
cdef dict _EXTERNAL_KERNEL_HOMES = {
    "draken__dfa_extract": "opteryx.compiled.vector_ops",
}


def _resolve_kernel_and_context(str kernel_name, context_allocator=None, context_arg=None):
    """Resolve a kernel by name and allocate context if needed.

    Returns (kernel_fn_ptr, context_wrapper_or_none).
    Returns (None, None) if kernel not found — no exception.
    Raises ValueError if context allocation fails (control flow, not fallback).
    """
    from draken.ops.kernels._kernel_registry import lookup_kernel

    fn_ptr, ctx_ptr = lookup_kernel(kernel_name)
    if fn_ptr is None and kernel_name in _EXTERNAL_KERNEL_HOMES:
        # Kernels compiled OUTSIDE the draken registry .so register themselves at
        # their module's import. Import the declared home module and retry ONCE —
        # this makes bind-time resolution independent of import order (a lookup
        # miss for a known-external kernel was observed intermittently when the
        # home module had not been pulled in yet by the plan path).
        import importlib
        importlib.import_module(_EXTERNAL_KERNEL_HOMES[kernel_name])
        fn_ptr, ctx_ptr = lookup_kernel(kernel_name)
    if fn_ptr is None:
        return None, None

    context_wrapper = None
    if context_allocator is not None:
        if isinstance(context_arg, tuple):
            ctx_ptr = context_allocator(*context_arg)   # multi-arg ctx (binop scales/units)
        elif context_arg is not None:
            ctx_ptr = context_allocator(context_arg)
        else:
            ctx_ptr = context_allocator()
        if ctx_ptr is None:
            raise ValueError(f"Failed to allocate context for kernel '{kernel_name}'")
        context_wrapper = _KernelContextWrapper(ctx_ptr)

    return fn_ptr, context_wrapper


# ---------------------------------------------------------------------------
# NodeType integer constants — must mirror NodeType IntEnum in
# opteryx/expression/__init__.py (verified at startup).
# ---------------------------------------------------------------------------
DEF _NT_AND = 17
DEF _NT_OR = 18
DEF _NT_XOR = 19
DEF _NT_NOT = 20
DEF _NT_DNF = 21
DEF _NT_CNF = 22
DEF _NT_CASE = 32
DEF _NT_COMPARISON_OPERATOR = 34
DEF _NT_BINARY_OPERATOR = 35
DEF _NT_UNARY_OPERATOR = 36
DEF _NT_FUNCTION = 37
DEF _NT_IDENTIFIER = 38
DEF _NT_NESTED = 40
DEF _NT_AGGREGATOR = 41
DEF _NT_LITERAL = 42
DEF _NT_EVALUATED = 44
DEF _NT_CAST = 45

# CAST ... FORMAT '<pattern>' (default ISO-8601 when absent) — kernel names that
# read a format_ctx (draken/ops/kernels/kernel_context.h). Referenced both to
# fail loud when FORMAT is given for an unsupported pairing, and to build the
# ctx for the ones that do support it — see the _NT_CAST case below.
cdef frozenset _CAST_FORMAT_AWARE_KERNELS = frozenset((
    "draken_cast_date_to_string", "draken_cast_date_to_blob",
    "draken_cast_timestamp_to_string", "draken_cast_timestamp_to_blob",
    "draken_cast_string_to_date32", "draken_cast_string_to_timestamp",
    "draken_cast_interval_to_string", "draken_cast_interval_to_blob",
))
DEF _NT_EXTRACTION_OPERATOR = 46
# NT_BETWEEN (47) has no lowering arm — `lower()` expands BETWEEN into a pair of
# compares before the C node tree is built (see `expand_between`).


# ---------------------------------------------------------------------------
# Bind-time lookups built lazily on first use.
# ---------------------------------------------------------------------------

cdef dict _OP_CODES = None
cdef object _LogicalCategory_DATE = None
cdef object _LogicalCategory_TIMESTAMP = None
cdef object _LogicalCategory_BOOLEAN = None
cdef object _LogicalCategory_VARCHAR = None
cdef object _LogicalCategory_ARRAY = None
cdef object _LogicalCategory_BLOB = None
cdef object _LogicalCategory_VARIANT = None
# LogicalKind, not LogicalCategory: IPv4's category is deliberately INTEGER (so
# ordering/grouping/joining run on the raw uint32), which makes the DESCRIPTOR the
# only thing that distinguishes an address column from a plain unsigned one.
cdef object _LogicalKind_IPV4 = None
cdef tuple _STRING_FAMILY = ()

# draken_date_part bind-time part ids (kernel contract — function_kernels.cpp).
cdef dict _EXTRACT_PARTS = {"YEAR": 1, "MONTH": 2, "DAY": 3, "QUARTER": 4,
                            "HOUR": 5, "MINUTE": 6, "SECOND": 7}

# draken_like bind-time modes (bit0 = negate, bit1 = case-insensitive).
cdef dict _LIKE_MODES = {"Like": 0, "NotLike": 1, "ILike": 2, "NotILike": 3}

# Average string length (bytes) below which the length-adaptive LIKE kernel walks
# a plan-time LIKE-DFA instead of the glob matcher. Measured crossover on real
# data: the DFA wins ~2.2x below this and converges to parity above it, so short
# columns take the DFA, long columns (text) stay on the glob. The dispatch is
# correctness-neutral (both matchers verified equivalent) — this only tunes speed.
DEF _LIKE_DFA_LEN_THRESHOLD = 64

# draken_contains modes — the optimizer's `LIKE '%x%'` → InStr rewrite family.
cdef dict _CONTAINS_MODES = {"InStr": 0, "NotInStr": 1, "IInStr": 2, "NotIInStr": 3}

# draken_rlike bind-time modes (bit0 = negate; no case-insensitive bit — RLIKE
# has no ILIKE-style SQL variant). The pattern operand at this point is always
# a pre-compiled DFA blob (predicate_rewriter.py's _rewrite_rlike_to_dfa runs
# at optimize time, before bind-time lowering ever sees the node) — a
# non-literal or uncompilable pattern raises NotSupportedError there, so this
# arm never needs to handle a raw pattern.
cdef dict _RLIKE_MODES = {"RLike": 0, "NotRLike": 1}

# LIKE ANY / ILIKE ANY (+ NOT) -> (case_insensitive, negate). Both flags are
# baked into the matcher blob by compile_like_any, not into a binary_op_ctx —
# the blob rides in ctx (like draken_in_list's set). The pattern set is a
# plan-time constant list; a per-row (column-sourced) pattern set is not handled
# by this arm (it has no constant blob to compile) and falls through.
cdef dict _LIKE_ANY_MODES = {
    "AnyOpLike": (False, False),
    "AnyOpNotLike": (False, True),
    "AnyOpILike": (True, False),
    "AnyOpNotILike": (True, True),
}

# draken_humanize mode ids (kernel contract — string_humanize.cpp HzMode). This
# table IS the closed set: a mode not spelled here is rejected at PLAN time by the
# HUMANIZE lowering arm, so the kernel never sees an id it cannot dispatch.
# Canonical spellings only — no aliases, no plurals, deliberately (an alias pool
# is a second vocabulary to keep in step with the docs and the kernel). Matching
# is case-insensitive on the literal, as it is for every other unit literal here.
cdef dict _HUMANIZE_MODES = {
    "WORDS": 0, "COMPACT": 1, "BYTES": 2, "SI": 3,
    "TIME": 4, "CLOCK": 5, "PERCENT": 6, "ODDS": 7,
}

# draken_date_trunc unit ids (kernel contract — function_kernels.cpp FK_TR_*).
# singular/plural, case-insensitive — mirrors _DIFF_PARTS below.
cdef dict _TRUNC_PARTS = {
    "SECOND": 1, "SECONDS": 1, "MINUTE": 2, "MINUTES": 2, "HOUR": 3, "HOURS": 3,
    "DAY": 4, "DAYS": 4, "WEEK": 5, "WEEKS": 5, "MONTH": 6, "MONTHS": 6,
    "QUARTER": 7, "QUARTERS": 7, "YEAR": 8, "YEARS": 8,
}

# draken_datediff/draken_timediff bind-time diff-kind ids (kernel contract —
# date_diff_batch, draken/ops/temporal_arith.h; singular/plural, case-insensitive).
cdef dict _DIFF_PARTS = {
    "MICROSECOND": 0, "MICROSECONDS": 0, "MILLISECOND": 1, "MILLISECONDS": 1,
    "SECOND": 2, "SECONDS": 2, "MINUTE": 3, "MINUTES": 3, "HOUR": 4, "HOURS": 4,
    "DAY": 5, "DAYS": 5, "WEEK": 6, "WEEKS": 6, "MONTH": 7, "MONTHS": 7,
    "QUARTER": 8, "QUARTERS": 8, "YEAR": 9, "YEARS": 9,
}
# TIMEDIFF hardcodes 'hours' (matches the nanobind dispatch_time_diff reference).
DEF _TIMEDIFF_DIFF_KIND = 4

# draken_time_bucket bind-time unit-kind ids (kernel contract — function_temporal.cpp).
cdef dict _BUCKET_UNITS = {
    "SECOND": 1, "SECONDS": 1, "MINUTE": 2, "MINUTES": 2,
    "HOUR": 3, "HOURS": 3, "DAY": 4, "DAYS": 4,
    "WEEK": 5, "WEEKS": 5, "MONTH": 6, "MONTHS": 6,
    "QUARTER": 7, "QUARTERS": 7, "YEAR": 8, "YEARS": 8,
}


def _pack_membership_blob(vals, int kind, int negate):
    """Serialize a value collection into the in_list_ctx blob — the kernel contract
    in kernel_context.h: [u32 count][u8 kind][u8 negate][u16 pad][payload].

    kind 0 payload is int64 SORTED ASCENDING (draken_in_list binary-searches it, so
    the sortedness is load-bearing, not cosmetic); kind 1 is (u32 len + bytes)
    entries; kind 2 is float64 in GIVEN order (draken_array_contains's only
    consumer never has more than one entry, so there is no binary-search
    invariant to preserve — draken_in_list has no kind-2 arm); kind 3 is uint64
    SORTED ASCENDING (UNSIGNED int family — a separate kind from 0 so a value
    above INT64_MAX is never reinterpreted as negative). Shared by the
    IN-list lowering and the ARRAY_CONTAINS_ANY/ALL/single-item lowerings so
    they cannot drift on the byte format or the kind-0 sort invariant.
    Duplicates are collapsed (set()) — membership is unaffected, and for
    ARRAY_CONTAINS_ALL a de-duplicated needle set is exactly the subset test.
    """
    import struct as _struct

    if kind == 0:
        items = sorted(set(int(v) for v in vals))
        blob = _struct.pack("<IBBH", len(items), 0, negate, 0)
        return blob + b"".join(_struct.pack("<q", v) for v in items)
    if kind == 3:
        items = sorted(set(int(v) for v in vals))
        blob = _struct.pack("<IBBH", len(items), 3, negate, 0)
        return blob + b"".join(_struct.pack("<Q", v) for v in items)
    if kind == 2:
        items = sorted(set(float(v) for v in vals))
        blob = _struct.pack("<IBBH", len(items), 2, negate, 0)
        return blob + b"".join(_struct.pack("<d", v) for v in items)
    entries = sorted(set(
        v.encode("utf-8") if isinstance(v, str) else bytes(v) for v in vals))
    blob = _struct.pack("<IBBH", len(entries), 1, negate, 0)
    for e in entries:
        blob += _struct.pack("<I", len(e)) + e
    return blob


def _membership_values(values):
    """Common shape gate for both membership blob builders: a plain literal
    collection, non-empty, no NULLs (three-valued semantics the blob cannot
    carry). Returns the value list, or None to keep the caller on the Python path.
    CarcharSet / PerfectHashSet wrappers are not literal collections → None."""
    if not isinstance(values, (list, tuple, set, frozenset)):
        return None
    vals = list(values)
    if len(vals) == 0 or any(v is None for v in vals):
        return None
    return vals


def _build_in_list_blob(values, left_type, int negate):
    """Serialize a literal IN-list into the in_list_ctx blob (kernel contract —
    kernel_context.h). Returns None when the list shape is outside the kernel's
    contract (NULL entries, mixed/unsupported types) — caller falls through."""
    vals = _membership_values(values)
    if vals is None:
        return None
    phys = ""
    if left_type is not None and left_type.physical is not None:
        phys = getattr(left_type.physical, "name", "")
    if phys in ("INT8", "INT16", "INT32", "INT64") and all(
            isinstance(v, int) and not isinstance(v, bool) for v in vals):
        return _pack_membership_blob(vals, 0, negate)
    if phys in ("UINT8", "UINT16", "UINT32", "UINT64") and all(
            isinstance(v, int) and not isinstance(v, bool) for v in vals):
        # An unsigned column can never hold a value outside [0, UINT64_MAX] —
        # dropping such literals from the set is exact (they can never match),
        # not a narrowing. An empty resulting set still lowers correctly: kind-3
        # with count=0 is "matches nothing" (negate=True → "matches everything").
        in_range = [v for v in vals if 0 <= v <= 0xFFFFFFFFFFFFFFFF]
        return _pack_membership_blob(in_range, 3, negate)
    if phys in ("VARCHAR", "NVARCHAR") and all(
            isinstance(v, (str, bytes)) for v in vals):
        return _pack_membership_blob(vals, 1, negate)
    return None


def _build_array_membership_blob(values):
    """ARRAY_CONTAINS_ANY/ALL needle set → in_list_ctx blob (negate always 0; the
    ANY/ALL distinction is the KERNEL, not a blob flag).

    Unlike _build_in_list_blob there is no column type to gate on: the binder
    leaves ARRAY columns UNTYPED (schema_column.column_type is None — see the
    ARRAY->VARCHAR cast's comment), so the element type is simply not knowable
    here. The blob kind is therefore inferred from the LITERAL values, and the
    kernel verifies it against the actual child element type at RUN time, failing
    loud on a mismatch (e.g. ARRAY_CONTAINS_ANY(int_array, ('a','b'))) rather
    than silently answering false."""
    vals = _membership_values(values)
    if vals is None:
        return None
    if all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
        return _pack_membership_blob(vals, 0, 0)
    if all(isinstance(v, (str, bytes)) for v in vals):
        return _pack_membership_blob(vals, 1, 0)
    return None   # mixed / unsupported literal types stay on the Python path


# TimestampUnit (draken/logical_type.h): SECONDS=0, MILLISECONDS=1,
# MICROSECONDS=2, NANOSECONDS=3. Literal TIMESTAMP values are always carried
# as raw MICROSECONDS (_materialise_constant_literal's int branch, and
# vector_timestamp_from_constant's unit="us" default) regardless of the
# target array's storage unit.
def _micros_to_array_unit(long long micros, int unit_value):
    """Convert a microsecond instant to the array child's raw storage unit.
    Returns None when the conversion is LOSSY (the literal has finer
    precision than the array's granularity) — such an item can provably never
    equal any element (every stored value is already a whole tick of that
    unit), but the caller has no "guaranteed false" blob kind, so it declines
    rather than silently rounding to a DIFFERENT stored value's raw int."""
    if unit_value == 2:
        return micros
    if unit_value == 3:
        return micros * 1000
    if unit_value == 1:
        return micros // 1000 if micros % 1000 == 0 else None
    if unit_value == 0:
        return micros // 1_000_000 if micros % 1_000_000 == 0 else None
    return None


def _build_single_item_blob(value, element_ct):
    """Serialize ONE scalar item — the LEFT (item) side of ARRAY_CONTAINS's
    lowered `item = ANY(arr)` — into a one-element in_list_ctx blob. Kind is
    inferred from the item's Python/bind-time type:
      bool/int  -> kind 0 (int64)
      float     -> kind 2 (float64)
      str/bytes -> kind 1
      TIMESTAMP64-tagged int (see _materialise_constant_literal) -> kind 0,
        raw value converted from microseconds to the ARRAY's own storage
        unit — draken_array_contains does a plain int64 compare, no runtime
        unit ctx. `element_ct` is node.right.schema_column.column_type.element
        (the array's element type — populated for a real column, unlike a
        literal array; see _build_array_membership_blob's note on that).
    Returns None (not eligible — caller falls through to plan-time refusal)
    for anything else: NULL, DATE (DATE32 array children are not yet
    unit-retagged — see draken_array_contains's DECIMAL/DATE32 note),
    DECIMAL (array children unreachable), or a TIMESTAMP item whose precision
    the array's unit cannot represent losslessly.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return _pack_membership_blob([int(value)], 0, 0)
    if isinstance(value, float):
        return _pack_membership_blob([value], 2, 0)
    if isinstance(value, int):
        if element_ct is not None and getattr(
                getattr(element_ct, "physical", None), "name", "") == "TIMESTAMP64":
            if element_ct.logical is None or element_ct.logical.unit is None:
                return None
            raw = _micros_to_array_unit(value, int(element_ct.logical.unit.value))
            if raw is None:
                return None
            return _pack_membership_blob([raw], 0, 0)
        return _pack_membership_blob([value], 0, 0)
    if isinstance(value, (str, bytes)):
        return _pack_membership_blob([value], 1, 0)
    return None
cdef type _CarcharSetWrapper_t = None
cdef type _PerfectHashSet_t = None

# Result-handling flag bits (read by execute_bytecode after kernel return).
# Set at bind time; used to dispatch result wrapping without isinstance/type checks.
BC_RESULT_NEEDS_NB_WRAP = 0x10  # result is a raw nanobind Vector → wrap in Cython shim
BC_RESULT_WRAP_AS_BOOL  = 0x20  # nb path: wrap as BoolVector (with NEEDS_NB_WRAP);
                                # C-native path: marks a bool-mask-returning kernel
                                # (LIKE family) for filter admission in _operators
BC_RESULT_NO_DV         = 0x40  # result has no DV* (constant / scalar / not a vector) → store NULL in dv_stack

# Binary op string → BCBinaryOpCode. Built once at first use.
_BOP_CODE = {
    "Plus":          BOP_PLUS,
    "Minus":         BOP_MINUS,
    "Multiply":      BOP_MULTIPLY,
    "Divide":        BOP_DIVIDE,
    "Modulo":        BOP_MODULO,
    "MyIntegerDivide": BOP_INT_DIVIDE,
    "StringConcat":  BOP_STRING_CONCAT,
    "BitwiseOr":     BOP_BITWISE_OR,
    "BitwiseAnd":    BOP_BITWISE_AND,
    "BitwiseXor":    BOP_BITWISE_XOR,
    "ShiftLeft":     BOP_SHIFT_LEFT,
    "ShiftRight":    BOP_SHIFT_RIGHT,
}

# P9.1: single source of truth for which (op, operand-types) dispatch C-native
# via the unified draken_binop kernel. Increment 1 covers integer/float
# ARITHMETIC and integer BITWISE — fixed-width results, ctx carries op_code only
# (no decimal scales). DECIMAL, string concat, temporal and IP are NOT listed yet
# and stay on the resolve_binary_op closure until their families are ported. This
# is a deterministic, fail-loud routing decision (no silent fallback): a binop is
# either explicitly C-native here or explicitly on the closure.
_BINOP_NATIVE_INT = frozenset({
    "INT8", "INT16", "INT32", "INT64",
    # E33: draken_binop routes these to fi_int_arith (mixed with a narrow signed
    # int, or narrow-unsigned-vs-narrow-unsigned) or fi_uint_arith (all-unsigned).
    # UINT64 combined with a SIGNED int is not yet native (needs the DECIMAL128
    # escape) — draken_binop fails loud for that specific combination rather than
    # silently miscomputing, so it's still safe to list UINT64 here.
    "UINT8", "UINT16", "UINT32", "UINT64",
})
_BINOP_NATIVE_FLOAT = frozenset({"FLOAT32", "FLOAT64"})
_BINOP_NATIVE_STRING = frozenset({"VARCHAR", "NVARCHAR", "VARBINARY"})
_BINOP_NATIVE_INTERVAL = frozenset({"INTERVAL"})
_BINOP_NATIVE_DECIMAL = frozenset({"DECIMAL", "DECIMAL128"})
_BINOP_NATIVE_TEMPORAL = frozenset({"DATE32", "TIMESTAMP64"})
# Signed int widths draken_binop accepts paired with a DECIMAL/DECIMAL128 operand.
# INT8/16/32 widen to INT64 (draken_binop's own widening block, right after it reads
# left/right type) before falling into the SAME dec_*/dec128_* kernels INT64 already
# uses — one native path for every width, not a per-width kernel.
_BINOP_NATIVE_DECIMAL_INT_WIDTHS = frozenset({"INT8", "INT16", "INT32", "INT64"})


def _binop_dec_scale(ct):
    """DECIMAL/DECIMAL128 scale off a bound ColumnType (0 if not decimal)."""
    if ct is None or ct.logical is None:
        return 0
    s = getattr(ct.logical, "scale", None)
    return int(s) if s is not None else 0


def _binop_dec_precision(ct):
    """DECIMAL/DECIMAL128 precision off a bound ColumnType (0 if not decimal)."""
    if ct is None or ct.logical is None:
        return 0
    p = getattr(ct.logical, "precision", None)
    return int(p) if p is not None else 0


def _binop_ts_unit(ct):
    """TimestampUnit int (0=s,1=ms,2=us,3=ns) off a bound TIMESTAMP/TIME ColumnType
    (0 otherwise; date32 has no unit)."""
    if ct is None or ct.logical is None:
        return 0
    u = getattr(ct.logical, "unit", None)
    if u is None:
        return 0
    v = getattr(u, "value", None)
    return int(v) if v is not None else int(u)

def _c_native_binop(int op_code, left_phys, right_phys, result_phys=None):
    """True iff draken_binop handles (op_code, left_phys, right_phys) today.

    result_phys (the bound result physical type) guards every kernel family whose
    output type is fixed by its operands rather than by the binder. The dec_*/dec128_*
    kernels output the SAME kind as their operands, so if the binder promotes the
    result to a different kind (e.g. DECIMAL × DECIMAL whose precision exceeds 18 →
    DECIMAL128), the kernel's physical output disagrees with the bound type and a
    downstream op reads the wrong width. int_bitwise is the same shape of promise:
    width-preserving, so its result must equal its (already equal) operand types.
    Stay on the closure for those. When result_phys is None (introspection) the
    guard is skipped."""
    if left_phys is None or right_phys is None:
        return False
    cdef bint l_int = left_phys in _BINOP_NATIVE_INT
    cdef bint r_int = right_phys in _BINOP_NATIVE_INT
    cdef bint l_num = l_int or (left_phys in _BINOP_NATIVE_FLOAT)
    cdef bint r_num = r_int or (right_phys in _BINOP_NATIVE_FLOAT)
    # Arithmetic PLUS/MINUS/MULTIPLY/DIVIDE/MODULO/INT_DIVIDE over numeric
    # (draken_binop handles cross-width widening + int/float promotion + true div).
    # INTERVAL ± INTERVAL → INTERVAL (S-A.1). MUST precede the numeric range check
    # below: PLUS/MINUS fall in [BOP_PLUS, BOP_INT_DIVIDE], whose `return l_num and
    # r_num` would short-circuit to False for interval operands. draken_binop wires
    # the existing component-wise interval_add/sub kernels (same as the closure).
    # PLUS/MINUS only; interval mul/div/mod are unsupported (stay on the closure).
    if op_code == BOP_PLUS or op_code == BOP_MINUS:
        if left_phys in _BINOP_NATIVE_INTERVAL and right_phys in _BINOP_NATIVE_INTERVAL:
            return True
    # DECIMAL (S-A.2): same-kind DECIMAL/DECIMAL128, or decimal × float → FLOAT64.
    # + - * / % only (not INT_DIVIDE). All same-kind, cross-kind (DECIMAL×DECIMAL128),
    # decimal×float and decimal×(any signed int width) are now c-native — incl.
    # promotion to DECIMAL128 (draken_binop widens int64 operands to int128, and an
    # INT8/16/32 operand to INT64 first). Results carry their precision/scale
    # descriptor across the c-native wrap (binder stamps it via ctx; executor
    # reattaches it). MUST precede the numeric range check (decimal operands are not
    # l_num/r_num → would short-circuit).
    if BOP_PLUS <= op_code <= BOP_MODULO:
        l_dec = left_phys in _BINOP_NATIVE_DECIMAL
        r_dec = right_phys in _BINOP_NATIVE_DECIMAL
        # Same-kind DECIMAL×DECIMAL / DECIMAL128×DECIMAL128. The result is the operand
        # kind when it fits the int64 tier; a DECIMAL pair whose precision exceeds 18
        # promotes to DECIMAL128 (draken_binop widens both operands to int128 and runs
        # dec128_*). Both carry their precision/scale descriptor across the c-native wrap.
        if l_dec and r_dec and left_phys == right_phys \
                and (result_phys is None or result_phys == left_phys
                     or (left_phys == "DECIMAL" and result_phys == "DECIMAL128")):
            return True
        if (l_dec and right_phys in _BINOP_NATIVE_FLOAT) or \
                (r_dec and left_phys in _BINOP_NATIVE_FLOAT):
            return True
        # DECIMAL(int64) × (INT8/16/32/64) → DECIMAL or DECIMAL128 (S-A.3). Any signed
        # int operand is a scale-0 decimal; draken_binop widens INT8/16/32 to INT64 up
        # front, then runs dec_* (int64-tier result, ≤18) or widens both operands to
        # int128 and runs dec128_* (result DECIMAL128, >18) — same as the INT64 case.
        if ((left_phys == "DECIMAL" and right_phys in _BINOP_NATIVE_DECIMAL_INT_WIDTHS) or
                (left_phys in _BINOP_NATIVE_DECIMAL_INT_WIDTHS and right_phys == "DECIMAL")) and \
                (result_phys is None or result_phys == "DECIMAL"
                 or result_phys == "DECIMAL128"):
            return True
        # DECIMAL128 promotion (S-A.3 completion): DECIMAL128 × (INT8/16/32/64) (either
        # order) and cross-kind DECIMAL × DECIMAL128 (either order). draken_binop widens
        # the narrow-int/int64-backed operand to int128 (narrow ints via INT64 first)
        # and runs dec128_*; the result is always DECIMAL128 (the rc-5 wrap reattaches
        # precision/scale).
        l128 = left_phys == "DECIMAL128"
        r128 = right_phys == "DECIMAL128"
        l_i64dec = left_phys == "DECIMAL" or left_phys in _BINOP_NATIVE_DECIMAL_INT_WIDTHS
        r_i64dec = right_phys == "DECIMAL" or right_phys in _BINOP_NATIVE_DECIMAL_INT_WIDTHS
        if ((l128 and r_i64dec) or (r128 and l_i64dec)) and \
                (result_phys is None or result_phys == "DECIMAL128"):
            return True
        # E33 — UINT64 x INT64 (either order): the DECIMAL128 escape valve from
        # the design matrix (no fixed-width signed type holds UINT64's full
        # range). PLUS/MINUS/MULTIPLY only — draken_binop degenerates these to
        # plain int128 arithmetic at scale 0; INT_DIVIDE/MODULO stay on the
        # closure (dec128_div/mod have different semantics — scale-expanding
        # true division, and a divide-by-zero convention that diverges from
        # plain integer DIV/MOD — not a safe reuse without dedicated design).
        # UINT64 x narrower signed int (INT8/16/32) is NOT covered — mirrors
        # the existing narrow-int restriction on the DECIMAL128 block above.
        if op_code == BOP_PLUS or op_code == BOP_MINUS or op_code == BOP_MULTIPLY:
            if (left_phys == "UINT64" and right_phys == "INT64") or \
                    (left_phys == "INT64" and right_phys == "UINT64"):
                if result_phys is None or result_phys == "DECIMAL128":
                    return True
    # TEMPORAL (S-A.2): date/ts ± interval → TIMESTAMP(µs); date/ts − date/ts → INTERVAL.
    # The TIMESTAMP result carries its unit descriptor across the c-native wrap.
    if op_code == BOP_PLUS or op_code == BOP_MINUS:
        l_tmp = left_phys in _BINOP_NATIVE_TEMPORAL
        r_tmp = right_phys in _BINOP_NATIVE_TEMPORAL
        if l_tmp and right_phys in _BINOP_NATIVE_INTERVAL:
            return True
        if op_code == BOP_PLUS and left_phys in _BINOP_NATIVE_INTERVAL and r_tmp:
            return True
        if op_code == BOP_MINUS and l_tmp and r_tmp:
            return True
    if BOP_PLUS <= op_code <= BOP_INT_DIVIDE:
        return l_num and r_num
    # Bitwise OR/AND/XOR/SHIFT over SAME-type integers (int_bitwise requires it;
    # mismatch would return a loud error sentinel, so require equality up front).
    #
    # The RESULT type is checked too, not just the operands. int_bitwise is
    # width-preserving — it stamps the shared operand type on its output — so a
    # binder that declared any other width has mis-described this expression to
    # everything downstream. That used to go unnoticed here and detonate one level
    # up instead: the declared width fed the enclosing comparison's literal-coercion
    # decision, and draken_compare_dv met a narrower vector than it was promised and
    # died with a bare `err_op=11` carrying no message. Refuse at plan time instead,
    # where the operator and both types are still in hand to report.
    if BOP_BITWISE_OR <= op_code <= BOP_SHIFT_RIGHT:
        return (l_int and r_int and left_phys == right_phys
                and (result_phys is None or result_phys == left_phys))
    # String concat over SAME-type string columns (VARCHAR/NVARCHAR/VARBINARY).
    # Mixed/non-string operands stay on the closure (which coerces) — the kernel
    # only sees string||string of one type, result type = that type.
    #
    # An untyped NULL operand (`x || NULL`, `NULL || x`) is also C-native: the
    # kernel short-circuits it to an all-NULL column of the string operand's type
    # (DuckDB semantics), which is what the binder now types the result as. This
    # MUST be admitted rather than left to the closure — the closure is not a
    # fallback the hard-cutover native engine can reach, so refusing it here is a
    # plan-time NotSupportedError, not a slow path. NULL || NULL is NOT admitted:
    # there is no string operand whose type the result could adopt.
    if op_code == BOP_STRING_CONCAT:
        if left_phys in _BINOP_NATIVE_STRING and left_phys == right_phys:
            return True
        if left_phys == "NULL" and right_phys in _BINOP_NATIVE_STRING:
            return True
        if right_phys == "NULL" and left_phys in _BINOP_NATIVE_STRING:
            return True
        return False
    return False

# Unary op string → BCUnaryOpCode. Built once at module load.
_UOP_CODE = {
    "IsNull":      UOP_IS_NULL,
    "IsNotNull":   UOP_IS_NOT_NULL,
    "IsEmpty":     UOP_IS_EMPTY,
    "IsNotEmpty":  UOP_IS_NOT_EMPTY,
    "BitwiseNot":  UOP_BITWISE_NOT,
    "IsTrue":      UOP_IS_TRUE,
    "IsNotFalse":  UOP_IS_NOT_FALSE,
    "IsFalse":     UOP_IS_FALSE,
    "IsNotTrue":   UOP_IS_NOT_TRUE,
}


cdef inline int16_t _sql_type_to_code(object sql_type):
    """Convert a ColumnType to a BCTypeCode integer. Returns BC_TYPE_NONE for None or non-temporal."""
    if sql_type is None:
        return <int16_t>BC_TYPE_NONE
    _ensure_sql_types()
    cdef object cat = sql_type.category
    if cat is _LogicalCategory_DATE:
        return <int16_t>BC_TYPE_DATE
    if cat is _LogicalCategory_TIMESTAMP:
        return <int16_t>BC_TYPE_TIMESTAMP
    return <int16_t>BC_TYPE_NONE


cdef inline dict _get_op_codes():
    global _OP_CODES
    if _OP_CODES is None:
        from opteryx.expression.evaluator import _OP_CODE
        _OP_CODES = _OP_CODE
    return _OP_CODES


cdef inline _ensure_sql_types():
    global _LogicalCategory_DATE, _LogicalCategory_TIMESTAMP, _LogicalCategory_BOOLEAN
    global _LogicalCategory_VARCHAR, _LogicalCategory_ARRAY, _LogicalCategory_BLOB
    global _STRING_FAMILY
    global _LogicalCategory_VARIANT
    global _LogicalKind_IPV4
    if _LogicalCategory_DATE is None:
        from draken.draken_native import LogicalKind
        from opteryx.types.logical_type import LogicalCategory
        _LogicalKind_IPV4 = LogicalKind.IPV4
        _LogicalCategory_DATE = LogicalCategory.DATE
        _LogicalCategory_TIMESTAMP = LogicalCategory.TIMESTAMP
        _LogicalCategory_BOOLEAN = LogicalCategory.BOOLEAN
        _LogicalCategory_VARCHAR = LogicalCategory.VARCHAR
        _LogicalCategory_ARRAY = LogicalCategory.ARRAY
        _LogicalCategory_BLOB = LogicalCategory.VARBINARY
        _LogicalCategory_VARIANT = LogicalCategory.VARIANT
        # Types valid as the LEFT operand of an extraction operator (-> ->> [i]).
        # Includes VARIANT so JSON access chains (a -> b ->> c) with no user cast.
        # NOTE: MapAccess excludes VARIANT explicitly - see the guard in _linearize.
        _STRING_FAMILY = (_LogicalCategory_VARCHAR, LogicalCategory.NVARCHAR, _LogicalCategory_BLOB, LogicalCategory.VARIANT)


cdef inline _ensure_set_types():
    global _CarcharSetWrapper_t, _PerfectHashSet_t
    if _CarcharSetWrapper_t is None:
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
        from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet
        _CarcharSetWrapper_t = CarcharSetWrapper
        _PerfectHashSet_t = PerfectHashSet


# ---------------------------------------------------------------------------
# CompiledBytecode container
# ---------------------------------------------------------------------------

DEF _INITIAL_CAP = 16


cdef class CompiledBytecode:
    """Owns a contiguous C array of BytecodeInstr and the Python refs the
    instructions point at.

    The executor (execute_bytecode in evaluation.pyx) reads instrs[i].opcode
    and the typed PyObject* slots directly. CompiledBytecode never INCREFs
    during execution; all owning refs live in _held_refs.
    """

    def __cinit__(self):
        self.instrs = NULL
        self.count = 0
        self.capacity = 0
        self.max_stack_depth = 0
        self._held_refs = []

    def __dealloc__(self):
        if self.instrs != NULL:
            PyMem_Free(self.instrs)
            self.instrs = NULL

    @property
    def length(self):
        return self.count

    @property
    def stack_depth(self):
        return self.max_stack_depth

    cdef BytecodeInstr* _push_instr(self) except NULL:
        cdef Py_ssize_t new_cap
        cdef BytecodeInstr* new_buf
        if self.count == self.capacity:
            new_cap = _INITIAL_CAP if self.capacity == 0 else self.capacity * 2
            new_buf = <BytecodeInstr*>PyMem_Realloc(
                self.instrs, <size_t>(new_cap * sizeof(BytecodeInstr))
            )
            if new_buf == NULL:
                raise MemoryError("CompiledBytecode: failed to grow instr array")
            self.instrs = new_buf
            self.capacity = new_cap
        cdef BytecodeInstr* slot = &self.instrs[self.count]
        memset(<void*>slot, 0, sizeof(BytecodeInstr))
        self.count += 1
        return slot

    cdef inline void _hold(self, object obj):
        # Anchor obj in the held-refs list. The slot reads <PyObject*>obj
        # which is a borrowed pointer; the list holds the strong ref.
        if obj is not None:
            self._held_refs.append(obj)


# ---------------------------------------------------------------------------
# Postfix lineariser
# ---------------------------------------------------------------------------

cdef Py_ssize_t _linearize(
    CompiledExpression* node,
    CompiledBytecode bc,
    Py_ssize_t depth,
) except -1:
    """Recursive postfix walk. Returns the resulting stack height after
    executing the emitted instructions for this subtree. `depth` is the
    stack height BEFORE this subtree. The maximum height seen is recorded
    on bc.max_stack_depth.

    Native opcodes pop their inputs and push one result (depth = depth+1
    after the instruction). Legacy opcodes are arity=0 (no children
    traversed) and also push one result.
    """
    cdef int nt = node.node_type
    cdef BytecodeInstr* slot
    cdef Py_ssize_t n, i, sub_depth
    cdef object value_obj
    cdef object schema_col
    cdef object identity_obj
    cdef object name_obj
    cdef bytes identity_bytes
    cdef bytes name_bytes
    cdef object left_sc, right_sc, left_type, right_type
    cdef object op_str
    cdef dict op_codes
    cdef int op_code_val
    cdef int flags
    # Variables for new native opcodes
    cdef object between_val, lower_obj, upper_obj
    cdef bint lower_incl, upper_incl
    cdef object bin_left_sc, bin_right_sc, bin_left_type, bin_right_type, bin_op_str
    cdef object unary_op_str
    cdef object func_val, func_ref_obj, func_py_node, func_ref_meta, callable_obj
    cdef object _co_params
    cdef Py_ssize_t _co_i
    cdef object extr_op_str, extr_key, extr_callable, extr_key_vec
    cdef bint right_is_inlist_literal
    cdef object inlist_set_obj
    cdef int coerce_phys
    cdef object coerce_lit_val
    cdef object cast_target_type, cast_unit, cast_params, cast_kernel, cast_py_node
    cdef object src
    cdef object const_lit

    # ------------------------------------------------------------------
    # NT_NESTED — transparent, no instruction emitted
    # ------------------------------------------------------------------
    if nt == _NT_NESTED:
        if node.centre != NULL:
            return _linearize(node.centre, bc, depth)
        return depth

    # ------------------------------------------------------------------
    # NT_LITERAL — pre-categorise into bool / set / scalar
    # ------------------------------------------------------------------
    if nt == _NT_LITERAL:
        value_obj = <object>node.value
        slot = bc._push_instr()
        if isinstance(value_obj, bool):
            slot.opcode = BC_LOAD_LIT_BOOL
            slot.bool_value = 1 if value_obj else 0
        else:
            _ensure_set_types()
            if isinstance(value_obj, _CarcharSetWrapper_t) or isinstance(value_obj, _PerfectHashSet_t):
                # Set / hash-set literal — consumed as a Python object by a
                # downstream BC_COMPARE; never a DrakenVector on the stack.
                slot.opcode = BC_LOAD_LIT_SET
                bc._hold(value_obj)
                slot.literal_obj = <PyObject*>value_obj
            elif node.physical_type == <int>DRAKEN_VECTOR_FP16:
                # A VECTOR literal's value is a list of floats, but it is ONE vector,
                # not a membership set — it must precede the list/tuple in-list branch
                # below for the same reason INTERVAL does, or it would be pushed as a
                # raw Python object and fall out of the c-native set.
                const_lit = _materialise_constant_literal(value_obj, node.physical_type)
                slot.opcode = BC_LOAD_LIT_CONST
                bc._hold(const_lit)
                slot.literal_obj = <PyObject*>const_lit
            elif node.physical_type == <int>DRAKEN_INTERVAL:
                # INTERVAL literal — its value is a (months, microseconds) tuple,
                # but it is a genuine scalar, not an in-list collection. Materialise
                # a constant INTERVAL Vector (must precede the tuple/in-list branch
                # below, which would otherwise mis-handle it as a membership set).
                const_lit = _materialise_constant_literal(value_obj, node.physical_type)
                slot.opcode = BC_LOAD_LIT_CONST
                bc._hold(const_lit)
                slot.literal_obj = <PyObject*>const_lit
            elif node.physical_type == <int>DRAKEN_ARRAY \
                    and isinstance(value_obj, (list, tuple)):
                # An ARRAY-typed literal is ONE array VALUE (e.g. the [1,5,3] in
                # GREATEST([1,5,3])), not a membership collection — membership
                # lists (x IN (...), x = ANY(...)) are consumed by the BC_COMPARE
                # arm and never linearized here, so this branch cannot capture one.
                # Materialise a real constant-shape ARRAY Vector so a reducer / nb
                # callable receives a Vector, not a bare Python list (the bare list
                # was the un-materialized operand the nb trampoline used to
                # unchecked-cast to Vector → SIGSEGV). Must precede the generic
                # list/tuple in-list branch below, exactly as INTERVAL/VECTOR do.
                const_lit = _materialise_constant_literal(value_obj, node.physical_type)
                slot.opcode = BC_LOAD_LIT_CONST
                bc._hold(const_lit)
                slot.literal_obj = <PyObject*>const_lit
            elif isinstance(value_obj, (list, tuple, set, frozenset)):
                # In-list collection literal — stays a Python object on the stack
                # for a downstream BC_COMPARE membership test.
                slot.opcode = BC_LOAD_LIT_SCALAR
                bc._hold(value_obj)
                slot.literal_obj = <PyObject*>value_obj
            else:
                # Genuine scalar literal — materialise the native constant ONCE.
                # The executor re-stamps only the logical length per morsel.
                # Thread the bind-time DECLARED descriptor through for both a
                # Decimal value and a NULL.
                #
                # Decimal: the DECLARED (precision, scale) — e.g.
                # CAST(1.23 AS DECIMAL(38,6)) — since the parsed Decimal value
                # itself is never re-quantized to it upstream. Without this,
                # _materialise_constant_literal falls back to deriving
                # (precision, scale) from the value's own digit count, which
                # silently picks the wrong physical tier (DECIMAL vs DECIMAL128)
                # and the wrong stored magnitude.
                #
                # NULL: a None with a parameterized physical tag (DECIMAL,
                # TIMESTAMP64, TIME32/64) needs the same descriptor to
                # materialise as a TYPED null rather than untyped DRAKEN_NULL —
                # see the None branch of _materialise_constant_literal. The
                # union leg of a rewritten FULL OUTER JOIN depends on it.
                if isinstance(value_obj, _decimal.Decimal) or value_obj is None:
                    _lit_py_node = <object>node.source_node
                    _lit_ct = getattr(_lit_py_node, "type", None)
                    _lit_lg = getattr(_lit_ct, "logical", None) if _lit_ct is not None else None
                    if _lit_lg is not None and getattr(_lit_lg, "precision", None):
                        const_lit = _materialise_constant_literal(
                            value_obj, node.physical_type,
                            int(_lit_lg.precision), int(_lit_lg.scale), _lit_lg)
                    else:
                        const_lit = _materialise_constant_literal(
                            value_obj, node.physical_type, -1, -1, _lit_lg)
                else:
                    const_lit = _materialise_constant_literal(value_obj, node.physical_type)
                slot.opcode = BC_LOAD_LIT_CONST
                bc._hold(const_lit)
                slot.literal_obj = <PyObject*>const_lit
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # NT_IDENTIFIER / NT_EVALUATED / NT_AGGREGATOR — pre-resolve column
    # identity + encoded name bytes
    # ------------------------------------------------------------------
    if nt == _NT_IDENTIFIER or nt == _NT_EVALUATED or nt == _NT_AGGREGATOR:
        schema_col = <object>node.schema_column
        if schema_col is None:
            raise ValueError("compiled_expression: IDENTIFIER node missing schema_column")
        identity_obj = schema_col.identity
        name_obj = schema_col.name
        if isinstance(name_obj, bytes):
            name_bytes = <bytes>name_obj
        else:
            name_bytes = (<str>name_obj).encode()
        if isinstance(identity_obj, bytes):
            identity_bytes = <bytes>identity_obj
        else:
            # morsel.column accepts the identity object directly; we still
            # need to anchor it. Most callers use bytes already.
            identity_bytes = identity_obj
        slot = bc._push_instr()
        slot.opcode = BC_LOAD_COL
        bc._hold(identity_bytes)
        bc._hold(name_bytes)
        slot.column_identity = <PyObject*>identity_bytes
        slot.column_name = <PyObject*>name_bytes
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # Binary boolean combinators: AND / OR / XOR
    # ------------------------------------------------------------------
    if nt == _NT_AND or nt == _NT_OR or nt == _NT_XOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: binary boolean op missing operand")
        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)
        slot = bc._push_instr()
        if nt == _NT_AND:
            slot.opcode = BC_AND
        elif nt == _NT_OR:
            slot.opcode = BC_OR
        else:
            slot.opcode = BC_XOR
        return sub_depth - 1   # pop 2, push 1 → net -1

    # ------------------------------------------------------------------
    # Unary NOT
    # ------------------------------------------------------------------
    if nt == _NT_NOT:
        if node.centre == NULL:
            raise ValueError("compiled_expression: NOT missing operand")
        sub_depth = _linearize(node.centre, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_NOT
        return sub_depth          # pop 1, push 1 → net 0

    # ------------------------------------------------------------------
    # Variadic AND/OR — DNF / CNF
    # ------------------------------------------------------------------
    if nt == _NT_DNF or nt == _NT_CNF:
        n = <Py_ssize_t>node.parameters.size()
        if n == 0:
            raise ValueError("compiled_expression: DNF/CNF with no parameters")
        sub_depth = depth
        for i in range(n):
            if node.parameters[i] == NULL:
                raise ValueError("compiled_expression: DNF/CNF parameter NULL")
            sub_depth = _linearize(node.parameters[i], bc, sub_depth)
        slot = bc._push_instr()
        slot.opcode = BC_DNF if nt == _NT_DNF else BC_CNF
        slot.arity = <int>n
        return sub_depth - n + 1   # pop n, push 1

    # ------------------------------------------------------------------
    # NT_COMPARISON_OPERATOR — resolve op string to integer code,
    # pre-read schema types, run temporal validation once.
    #
    # IN-list fold (BC_CMP_INLIST_INLINE): when the right operand is an
    # NT_LITERAL whose value is a set/list/CarcharSet we fold it directly
    # into the BC_COMPARE instruction (slot.literal_obj) instead of
    # emitting a separate BC_LOAD_LIT_SET instruction.  This removes the
    # set from the execution stack entirely — sets cannot become
    # DrakenVector* and must not appear as stack operands.
    # Folded BC_COMPARE pops ONE item (left) and pushes one result;
    # non-folded pops TWO items (left + right) and pushes one result.
    # ------------------------------------------------------------------
    if nt == _NT_COMPARISON_OPERATOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: COMPARISON missing operand")

        # Read schema types from children BEFORE linearising them.
        left_sc = <object>node.left.schema_column
        right_sc = <object>node.right.schema_column
        left_type = left_sc.column_type if left_sc is not None else None
        right_type = right_sc.column_type if right_sc is not None else None
        op_str = <object>node.value
        _validate_temporal_at_bind(
            node.left.node_type, left_type,
            node.right.node_type, right_type,
            op_str,
        )

        # LIKE-family and InStr-family (the optimizer's `LIKE '%x%'` → contains
        # rewrite) — bind-time lowering to the C-ABI draken_like / draken_contains
        # kernels (mode in binary_op_ctx.op_code: bit0 negate, bit1 ci; the
        # pattern/needle travels as the second vector argument, so constant AND
        # per-row patterns share one path).
        _like_kernel = "draken_like"
        _like_mode = _LIKE_MODES.get(op_str, -1)
        if _like_mode < 0:
            _like_mode = _CONTAINS_MODES.get(op_str, -1)
            _like_kernel = "draken_contains"
        # Length-adaptive LIKE: a case-sensitive general-glob LIKE over a VARCHAR
        # column, whose ASCII pattern compiles to a LIKE-DFA, dispatches to
        # draken_like_adaptive — it picks the DFA (short columns) or the glob
        # (long) at run time from the column's avg string length. Pure prefix/
        # suffix/contains/exact never reach here (already affix/=-rewritten), and
        # ILIKE/NVARCHAR/non-ASCII fall through to plain draken_like below.
        if _like_kernel == "draken_like" and op_str in ("Like", "NotLike") \
                and node.right != NULL and node.right.node_type == _NT_LITERAL \
                and left_type is not None:
            _ensure_sql_types()
            _ld_pat = <object>node.right.value
            if left_type.category is _LogicalCategory_VARCHAR \
                    and isinstance(_ld_pat, (str, bytes)):
                _ld_bytes = _ld_pat.encode("utf-8") if isinstance(_ld_pat, str) else _ld_pat
                from opteryx.compiled import vector_ops as _ld_vops
                # Prefer the SIMD op-program (draken_like_program) when the glob
                # decomposes to anchored-fixed + %-floating-literals + suffix —
                # O(segments) SIMD scans, beats both the transition-table DFA and
                # glob on short and long columns. Non-decomposable globs return
                # None and fall through to the length-adaptive DFA/glob path.
                _lp_blob = _ld_vops.compile_like_program(_ld_bytes)
                if _lp_blob is not None:
                    from draken.ops.kernels._kernel_registry import alloc_like_dfa_ctx as _lp_alloc
                    _lp_fn, _lp_ctx = _resolve_kernel_and_context(
                        "draken_like_program", _lp_alloc,
                        (_like_mode, 0, _lp_blob))
                    if _lp_fn is not None:
                        sub_depth = _linearize(node.left, bc, depth)
                        sub_depth = _linearize(node.right, bc, sub_depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 2
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                        slot.kernel_fn = <void*>(<unsigned long long>_lp_fn)
                        if _lp_ctx is not None:
                            bc._hold(_lp_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_lp_ctx.ctx_ptr)
                        return sub_depth - 2 + 1
                _ld_blob = _ld_vops.compile_like_dfa(_ld_bytes)
                if _ld_blob is not None:
                    from draken.ops.kernels._kernel_registry import alloc_like_dfa_ctx as _ld_alloc
                    _ld_fn, _ld_ctx = _resolve_kernel_and_context(
                        "draken_like_adaptive", _ld_alloc,
                        (_like_mode, _LIKE_DFA_LEN_THRESHOLD, _ld_blob))
                    if _ld_fn is not None:
                        sub_depth = _linearize(node.left, bc, depth)
                        sub_depth = _linearize(node.right, bc, sub_depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 2
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                        slot.kernel_fn = <void*>(<unsigned long long>_ld_fn)
                        if _ld_ctx is not None:
                            bc._hold(_ld_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_ld_ctx.ctx_ptr)
                        return sub_depth - 2 + 1

        if _like_mode >= 0:
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _lk_alloc
            _lk_fn, _lk_ctx = _resolve_kernel_and_context(
                _like_kernel, _lk_alloc, (_like_mode, 0, 0, 0, 0, 0, 0))
            if _lk_fn is not None:
                sub_depth = _linearize(node.left, bc, depth)
                sub_depth = _linearize(node.right, bc, sub_depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                # WRAP_AS_BOOL marks the result as a boolean mask — the filter
                # admission (bytecode_is_c_native_predicate) keys on it.
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_lk_fn)
                if _lk_ctx is not None:
                    bc._hold(_lk_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_lk_ctx.ctx_ptr)
                return sub_depth - 2 + 1
            # kernel unavailable — fall through to the Python compare path

        # RLIKE/NOT RLIKE — draken_rlike, same BC_FUNCTION|WRAP_AS_BOOL shape as
        # LIKE above. Pattern operand is a pre-compiled DFA blob by this point
        # (see _RLIKE_MODES's comment) — always pushed as the second operand,
        # never baked into ctx, so a per-row (non-constant) blob would still be
        # structurally valid here; in practice the optimizer only ever produces
        # a constant blob since only literal patterns compile.
        _rlike_mode = _RLIKE_MODES.get(op_str, -1)
        if _rlike_mode >= 0:
            # The pattern operand is a pre-compiled blob (predicate_rewriter):
            # version 2 = SIMD op-program -> draken_like_program (ctx-carried
            # blob, same executor as LIKE); version 1 = transition-table DFA ->
            # draken_rlike. Peek the version byte to pick the kernel.
            _rl_blob = None
            if node.right != NULL and node.right.node_type == _NT_LITERAL:
                _rl_rawpat = <object>node.right.value
                if isinstance(_rl_rawpat, (bytes, bytearray)) and len(_rl_rawpat) >= 1 \
                        and _rl_rawpat[0] == 2:
                    _rl_blob = bytes(_rl_rawpat)
            if _rl_blob is not None:
                from draken.ops.kernels._kernel_registry import alloc_like_dfa_ctx as _rlp_alloc
                _rlp_fn, _rlp_ctx = _resolve_kernel_and_context(
                    "draken_like_program", _rlp_alloc, (_rlike_mode, 0, _rl_blob))
                if _rlp_fn is not None:
                    sub_depth = _linearize(node.left, bc, depth)
                    sub_depth = _linearize(node.right, bc, sub_depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 2
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                    slot.kernel_fn = <void*>(<unsigned long long>_rlp_fn)
                    if _rlp_ctx is not None:
                        bc._hold(_rlp_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_rlp_ctx.ctx_ptr)
                    return sub_depth - 2 + 1
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _rl_alloc
            _rl_fn, _rl_ctx = _resolve_kernel_and_context(
                "draken_rlike", _rl_alloc, (_rlike_mode, 0, 0, 0, 0, 0, 0))
            if _rl_fn is not None:
                sub_depth = _linearize(node.left, bc, depth)
                sub_depth = _linearize(node.right, bc, sub_depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_rl_fn)
                if _rl_ctx is not None:
                    bc._hold(_rl_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_rl_ctx.ctx_ptr)
                return sub_depth - 2 + 1
            # kernel unavailable (should not happen — draken_rlike is a static
            # built-in, not an external-kernel-home lookup) — fall through to
            # the generic compare path, which the c-native gate will refuse.

        # LIKE ANY / ILIKE ANY (+ NOT) — bind-time compile the constant pattern
        # set into a matcher blob (compile_like_any) and dispatch draken_like_any.
        # The pattern set NEVER becomes a Draken vector (which is exactly why the
        # old RE2 path errored: _eval_value could not construct one). Subject is
        # scalar string OR ARRAY<string> (any element matches → BC_C_NATIVE_CHILD,
        # like draken_array_contains). Zero RE2, zero Python at run time.
        _la_modes = _LIKE_ANY_MODES.get(op_str, None)
        if _la_modes is not None and node.right != NULL \
                and node.right.node_type == _NT_LITERAL:
            _la_patterns = <object>node.right.value
            if isinstance(_la_patterns, (list, tuple)):
                _ensure_sql_types()
                _la_is_array = (left_type is not None
                                and left_type.category is _LogicalCategory_ARRAY)
                # An ARRAY subject needs a resolvable child (per-morsel), so it
                # must be a direct column load; a computed-array subject falls
                # through to the generic path.
                if not (_la_is_array and node.left.node_type != _NT_IDENTIFIER):
                    from opteryx.compiled import vector_ops as _la_vops
                    _la_blob = _la_vops.compile_like_any(
                        tuple(_la_patterns), _la_modes[0], _la_modes[1])
                    from draken.ops.kernels._kernel_registry import alloc_like_any_ctx as _la_alloc
                    _la_fn, _la_ctx = _resolve_kernel_and_context(
                        "draken_like_any", _la_alloc, _la_blob)
                    if _la_fn is not None:
                        sub_depth = _linearize(node.left, bc, depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 1
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                        if _la_is_array:
                            slot.flags = slot.flags | BC_C_NATIVE_CHILD
                            # bc.count - 2 is the array's BC_LOAD_COL (this FUNCTION
                            # slot is bc.count - 1) — same indexing array_contains uses.
                            slot.column_identity = bc.instrs[bc.count - 2].column_identity
                        slot.kernel_fn = <void*>(<unsigned long long>_la_fn)
                        if _la_ctx is not None:
                            bc._hold(_la_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_la_ctx.ctx_ptr)
                        return sub_depth   # pops 1 (subject), pushes mask — net 0

        # ARRAY_CONTAINS(arr, item) lowers to `item = ANY(arr)` (AnyOpEq) at
        # plan-build time; bare `x = ANY(arr)` produces the same node. Two
        # native shapes, both GIL-free (no Python `anyop_eq`/`vector_anyop_eq`
        # is ever reached — that GIL path is retired for this node type):
        #
        # (a) node.right is a DIRECT ARRAY column, node.left a literal item —
        #     pack the item into a one-element in_list_ctx blob and dispatch
        #     draken_array_contains via the SAME BC_C_NATIVE_CHILD +
        #     WRAP_AS_BOOL path ARRAY_CONTAINS_ANY uses, so the compare
        #     admission gate (which refuses AnyOpEq) is never touched.
        # (b) node.right is a LITERAL array (`x = ANY([1,2,3])` /
        #     `ARRAY_CONTAINS([1,2,3], x)`), node.left anything (literal or
        #     column) — this is exactly an IN-list test, so it reuses
        #     draken_in_list directly: no new kernel, no new blob kind, and a
        #     fully-literal expression (both sides) now constant-folds through
        #     THIS same native instruction (fold_constants calls
        #     execute_bytecode on the compiled bytecode) instead of the old
        #     GIL AnyOpEq fallback that could only see materialised Vectors,
        #     not bare Python lists.
        #
        # A computed (non-column, non-literal) array, or an item type this
        # kernel doesn't cover (DECIMAL, DATE, mixed types), is not eligible
        # and falls through to the generic compare, which the native engine
        # then refuses (no fallback) — never a silent wrong answer.
        if op_str == "AnyOpEq" and node.left != NULL and node.right != NULL:
            if node.right.node_type == _NT_LITERAL and node.right.value != NULL:
                _ari_obj = <object>node.right.value
                if isinstance(_ari_obj, (list, tuple)):
                    _ari_blob = _build_in_list_blob(_ari_obj, left_type, 0)
                    if _ari_blob is not None:
                        from draken.ops.kernels._kernel_registry import alloc_in_list_ctx as _ari_alloc
                        _ari_fn, _ari_ctx = _resolve_kernel_and_context(
                            "draken_in_list", _ari_alloc, _ari_blob)
                        if _ari_fn is not None:
                            sub_depth = _linearize(node.left, bc, depth)
                            slot = bc._push_instr()
                            slot.opcode = BC_FUNCTION
                            slot.arity = 1
                            slot.bool_value = 0
                            slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                            slot.kernel_fn = <void*>(<unsigned long long>_ari_fn)
                            if _ari_ctx is not None:
                                bc._hold(_ari_ctx)
                                slot.ctx_ptr = <void*>(<unsigned long long>_ari_ctx.ctx_ptr)
                            return sub_depth   # pops the value, pushes the mask — net 0
            elif node.left.node_type == _NT_LITERAL and node.left.value != NULL \
                    and (node.right.node_type == _NT_IDENTIFIER
                         or node.right.node_type == _NT_EVALUATED
                         or node.right.node_type == _NT_AGGREGATOR):
                _ac_right_ct = (<object>node.right.schema_column).column_type \
                    if node.right.schema_column != NULL else None
                _ac_element_ct = getattr(_ac_right_ct, "element", None) if _ac_right_ct is not None else None
                _ac_blob = _build_single_item_blob(<object>node.left.value, _ac_element_ct)
                if _ac_blob is not None:
                    from draken.ops.kernels._kernel_registry import alloc_in_list_ctx as _ac_alloc
                    _ac_fn, _ac_ctx = _resolve_kernel_and_context(
                        "draken_array_contains", _ac_alloc, _ac_blob)
                    if _ac_fn is not None:
                        sub_depth = _linearize(node.right, bc, depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 1
                        slot.bool_value = 0
                        slot.flags = (BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                                      | BC_C_NATIVE_CHILD)
                        slot.kernel_fn = <void*>(<unsigned long long>_ac_fn)
                        # bc.count - 2 is the array's BC_LOAD_COL (this FUNCTION slot is
                        # bc.count - 1) — same indexing SORT's child path uses.
                        slot.column_identity = bc.instrs[bc.count - 2].column_identity
                        if _ac_ctx is not None:
                            bc._hold(_ac_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_ac_ctx.ctx_ptr)
                        return sub_depth   # value pushed, fn pops 1 pushes 1 — net 0

        # `arr @> (…)` (AtArrow, contains-ANY) and `arr @>> (…)` (ArrayContainsAll,
        # contains-ALL) — the OPERATOR spellings of ARRAY_CONTAINS_ANY/ALL, lowered
        # to the same draken_array_contains_any/all kernels through the same
        # BC_C_NATIVE_CHILD + WRAP_AS_BOOL shape (see the FUNCTION lowering below
        # for why only the array operand is pushed and the needles ride the ctx).
        #
        # These reach the filter from two directions, and BOTH were unrunnable while
        # only the function spelling was lowered: written directly by a user, and
        # synthesised by predicate_rewriter, which folds OR'd / AND'd `x = ANY(col)`
        # over one column into a single AtArrow / ArrayContainsAll node. So a query
        # the rewriter had just made cheaper became a query the engine refused.
        #
        # The needle literal is whatever the producer built — the rewriter emits a
        # list for AtArrow and a set for ArrayContainsAll — and
        # _build_array_membership_blob takes any of list/tuple/set/frozenset.
        if op_str in ("AtArrow", "ArrayContainsAll") and node.left != NULL \
                and node.right != NULL \
                and node.right.node_type == _NT_LITERAL and node.right.value != NULL \
                and (node.left.node_type == _NT_IDENTIFIER
                     or node.left.node_type == _NT_EVALUATED
                     or node.left.node_type == _NT_AGGREGATOR):
            _aco_blob = _build_array_membership_blob(<object>node.right.value)
            if _aco_blob is not None:
                from draken.ops.kernels._kernel_registry import alloc_in_list_ctx as _aco_alloc
                _aco_fn, _aco_ctx = _resolve_kernel_and_context(
                    "draken_array_contains_any" if op_str == "AtArrow"
                    else "draken_array_contains_all", _aco_alloc, _aco_blob)
                if _aco_fn is not None:
                    sub_depth = _linearize(node.left, bc, depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 1
                    slot.bool_value = 0
                    slot.flags = (BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                                  | BC_C_NATIVE_CHILD)
                    slot.kernel_fn = <void*>(<unsigned long long>_aco_fn)
                    # bc.count - 2 is the array's BC_LOAD_COL (this FUNCTION slot is
                    # bc.count - 1) — same indexing the ARRAY_CONTAINS child path uses.
                    slot.column_identity = bc.instrs[bc.count - 2].column_identity
                    if _aco_ctx is not None:
                        bc._hold(_aco_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_aco_ctx.ctx_ptr)
                    return sub_depth   # array pushed, fn pops 1 pushes 1 — net 0

        # IN-list — bind-time lowering to the C-ABI draken_in_list kernel for
        # plain literal collections over integer-family or string columns.
        # Lists containing NULL keep the Python path (three-valued IN semantics
        # the blob cannot carry — engine admission then rejects loud).
        if op_str in ("InList", "NotInList") and node.right != NULL \
                and node.right.node_type == _NT_LITERAL:
            _il_obj = <object>node.right.value
            _il_blob = _build_in_list_blob(
                _il_obj, left_type, 1 if op_str == "NotInList" else 0)
            if _il_blob is not None:
                from draken.ops.kernels._kernel_registry import alloc_in_list_ctx as _il_alloc
                _il_fn, _il_ctx = _resolve_kernel_and_context(
                    "draken_in_list", _il_alloc, _il_blob)
                if _il_fn is not None:
                    sub_depth = _linearize(node.left, bc, depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 1
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                    slot.kernel_fn = <void*>(<unsigned long long>_il_fn)
                    if _il_ctx is not None:
                        bc._hold(_il_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_il_ctx.ctx_ptr)
                    return sub_depth   # pops the value, pushes the mask — net 0

        # Detect set/list literal on the right — fold if found.
        right_is_inlist_literal = False
        inlist_set_obj = None
        if node.right != NULL and node.right.node_type == _NT_LITERAL:
            inlist_set_obj = <object>node.right.value
            _ensure_set_types()
            if (
                isinstance(inlist_set_obj, _CarcharSetWrapper_t)
                or isinstance(inlist_set_obj, _PerfectHashSet_t)
            ):
                right_is_inlist_literal = True
            elif isinstance(inlist_set_obj, (list, tuple, set, frozenset)):
                right_is_inlist_literal = True
                # The string in-list edge (vector_in_list) is bytes-only — encode
                # str members to bytes ONCE at bind (str must not reach the Draken
                # edge). Non-str members (ints, etc.) pass through unchanged.
                inlist_set_obj = [
                    e.encode("utf-8") if isinstance(e, str) else e
                    for e in inlist_set_obj
                ]
            else:
                inlist_set_obj = None  # scalar literal — don't fold

        # C-native compare fast path enablement: when comparing a column (or any
        # non-literal expression) against a scalar literal, materialise the literal
        # in the column's PHYSICAL type so draken_compare_dv (identical-type only)
        # fires instead of declining to the Python fallback (which permanently
        # clears is_all_c_native). Only value-exact numeric coercions are applied;
        # everything else is left to the fallback, unchanged. The literal node's
        # physical_type is consumed by _materialise_constant_literal below.
        if not right_is_inlist_literal:
            if (node.right.node_type == _NT_LITERAL
                    and node.left.node_type != _NT_LITERAL
                    and node.right.value != NULL):
                coerce_lit_val = <object>node.right.value
                coerce_phys = _coerce_literal_physical(left_type, coerce_lit_val)
                if coerce_phys >= 0:
                    node.right.physical_type = coerce_phys
            elif (node.left.node_type == _NT_LITERAL
                    and node.right.node_type != _NT_LITERAL
                    and node.left.value != NULL):
                coerce_lit_val = <object>node.left.value
                coerce_phys = _coerce_literal_physical(right_type, coerce_lit_val)
                if coerce_phys >= 0:
                    node.left.physical_type = coerce_phys

        sub_depth = _linearize(node.left, bc, depth)
        if not right_is_inlist_literal:
            sub_depth = _linearize(node.right, bc, sub_depth)

        op_codes = _get_op_codes()
        op_code_val = <int>op_codes.get(op_str, 0)
        if op_code_val == 0:
            raise NotImplementedError(
                f"compiled_expression: unknown comparison operator {op_str!r}"
            )
        _ensure_sql_types()

        # IPv4 CIDR containment (`<<=` / `>>=`). Routed to its own C-ABI kernel
        # rather than draken_compare_dv: this is not an ordering comparison at
        # all, it is a mask-and-compare against a network parsed once per morsel.
        #
        # No ctx and no operand reordering — draken_ipv4_in_cidr discriminates on
        # operand TYPE (the UINT32 side is the address), so both spellings share
        # one kernel and one code path here.
        if op_str == "IPContainedBy" or op_str == "IPContains":
            _ip_fn, _ip_ctx = _resolve_kernel_and_context("draken_ipv4_in_cidr")
            if _ip_fn is None:
                raise NotImplementedError(
                    "compiled_expression: draken_ipv4_in_cidr kernel is not registered"
                )
            slot = bc._push_instr()
            slot.opcode = BC_FUNCTION
            slot.arity = 2
            slot.bool_value = 0
            slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
            slot.kernel_fn = <void*>(<unsigned long long>_ip_fn)
            return sub_depth - 2 + 1

        # Mixed-type NUMERIC comparison — draken_compare_dv declines any type
        # mismatch (DECIMAL vs FLOAT64 from a float-literal product, DECIMAL vs
        # DECIMAL128, INT vs FLOAT, cross-scale DECIMAL) and would error with no
        # fallback. Route to the ctx-carrying draken_numeric_cmp: BOTH-decimal
        # pairs rescale exactly in int128, any-float pairs promote to double.
        # Fires ONLY when the two numeric operands differ in (type, scale) — a
        # matched same-type-same-scale pair stays on the fast draken_compare_dv.
        _NUM_PHYS = ("INT8", "INT16", "INT32", "INT64",
                     "FLOAT32", "FLOAT64", "DECIMAL", "DECIMAL128")
        _lphys = getattr(left_type.physical, "name", "") if left_type is not None else ""
        _rphys = getattr(right_type.physical, "name", "") if right_type is not None else ""
        _lscale = (int(left_type.logical.scale)
                   if left_type is not None and left_type.logical is not None
                   and _lphys in ("DECIMAL", "DECIMAL128") else 0)
        _rscale = (int(right_type.logical.scale)
                   if right_type is not None and right_type.logical is not None
                   and _rphys in ("DECIMAL", "DECIMAL128") else 0)
        if (not right_is_inlist_literal and op_code_val in (1, 2, 3, 4, 5, 6)
                and _lphys in _NUM_PHYS and _rphys in _NUM_PHYS
                and (_lphys != _rphys or _lscale != _rscale)):
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _dc_alloc
            _dc_fn, _dc_ctx = _resolve_kernel_and_context(
                "draken_numeric_cmp", _dc_alloc,
                (op_code_val, _lscale, _rscale, 0, 0, 0, 0))
            if _dc_fn is not None:
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_dc_fn)
                if _dc_ctx is not None:
                    bc._hold(_dc_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_dc_ctx.ctx_ptr)
                return sub_depth - 2 + 1

        # Mixed-domain TEMPORAL comparison — draken_compare_dv declines a DATE32-vs-
        # TIMESTAMP64 type mismatch (no fallback on the native engine → hard error)
        # and, worse, silently mis-compares two TIMESTAMP64 operands carried at
        # DIFFERENT units (its raw-int ordering is unit-blind). Route BOTH to the
        # unit-aware draken_temporal_cmp, which promotes each side to nanoseconds
        # (DATE days×86.4e12; TIMESTAMP scaled by its bind-time unit) before
        # comparing. Fires ONLY when the two temporal operands differ in physical
        # type OR unit — a matched date/date or same-unit ts/ts pair stays on the
        # fast draken_compare_dv. Mirrors the numeric-mismatch routing above.
        _TEMPORAL_PHYS = ("DATE32", "TIMESTAMP64")
        _lunit = _binop_ts_unit(left_type)
        _runit = _binop_ts_unit(right_type)
        if (not right_is_inlist_literal and op_code_val in (1, 2, 3, 4, 5, 6)
                and _lphys in _TEMPORAL_PHYS and _rphys in _TEMPORAL_PHYS
                and (_lphys != _rphys or _lunit != _runit)):
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _tc_alloc
            _tc_fn, _tc_ctx = _resolve_kernel_and_context(
                "draken_temporal_cmp", _tc_alloc,
                (op_code_val, 0, 0, 0, 0, _lunit, _runit))
            if _tc_fn is not None:
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_tc_fn)
                if _tc_ctx is not None:
                    bc._hold(_tc_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_tc_ctx.ctx_ptr)
                return sub_depth - 2 + 1

        flags = 0
        _left_cat = left_type.category if left_type is not None else None
        _right_cat = right_type.category if right_type is not None else None
        if _left_cat is _LogicalCategory_DATE or _left_cat is _LogicalCategory_TIMESTAMP:
            flags |= BC_CMP_LEFT_TEMPORAL
        if _right_cat is _LogicalCategory_DATE or _right_cat is _LogicalCategory_TIMESTAMP:
            flags |= BC_CMP_RIGHT_TEMPORAL
        if right_is_inlist_literal:
            flags |= BC_CMP_INLIST_INLINE

        slot = bc._push_instr()
        slot.opcode = BC_COMPARE
        slot.op_code = op_code_val
        slot.flags = flags
        slot.left_type_code = _sql_type_to_code(left_type)
        slot.right_type_code = _sql_type_to_code(right_type)
        if right_is_inlist_literal:
            bc._hold(inlist_set_obj)
            slot.literal_obj = <PyObject*>inlist_set_obj
            return sub_depth      # pop 1 push 1 — net 0
        return sub_depth - 1      # pop 2 push 1 — net -1

    # ------------------------------------------------------------------
    # NT_BINARY_OPERATOR — Phase 6: resolve kernel at bind time, store
    # callable ref. Operand types stored for introspection/debugging.
    # ------------------------------------------------------------------
    if nt == _NT_BINARY_OPERATOR:
        if node.left == NULL or node.right == NULL:
            raise ValueError("compiled_expression: BINARY_OPERATOR missing operand")
        bin_left_sc = <object>node.left.schema_column if node.left.schema_column != NULL else None
        bin_right_sc = <object>node.right.schema_column if node.right.schema_column != NULL else None
        bin_left_type = bin_left_sc.column_type if bin_left_sc is not None else None
        bin_right_type = bin_right_sc.column_type if bin_right_sc is not None else None
        bin_result_sc = <object>node.schema_column if node.schema_column != NULL else None
        bin_result_type = bin_result_sc.column_type if bin_result_sc is not None else None
        bin_op_str = <object>node.value

        # Bitwise/shift ops require IDENTICAL physical operand types — draken's
        # int_bitwise returns a loud error sentinel on a mismatch, so the gate
        # below demands equality up front. An integer LITERAL, though, binds at
        # its own natural width, so `id | 1` against an INT8 column was INT8 | INT64
        # and got refused at plan time with "outside the c-native kernel set" —
        # while `id | id` worked. Arithmetic never hit this because its gate asks
        # only that both sides be numeric.
        #
        # Fixed the same way the comparison path already fixes it: materialise the
        # literal in the COLUMN's physical type. _coerce_literal_physical returns
        # only value-exact coercions (a literal that will not fit is left alone and
        # the expression stays non-c-native rather than silently wrapping), so this
        # cannot change an answer.
        #
        # The coerced type has to be reported to the gate as well as set on the
        # node: the node drives materialisation, the gate reads the physical names.
        bin_op_code = <int>_BOP_CODE.get(bin_op_str, BOP_UNKNOWN)
        if BOP_BITWISE_OR <= bin_op_code <= BOP_SHIFT_RIGHT:
            # Fires only column-op-literal. literal-op-literal is constant folding's
            # job and has no column type to adopt; column-op-column already matches
            # or genuinely cannot.
            if (node.right.node_type == _NT_LITERAL
                    and node.left.node_type != _NT_LITERAL
                    and node.right.value != NULL and bin_left_type is not None):
                bw_coerce_phys = _coerce_literal_physical(
                    bin_left_type, <object>node.right.value)
                if bw_coerce_phys >= 0:
                    node.right.physical_type = bw_coerce_phys
                    bin_right_type = _column_type_for_physical(bw_coerce_phys)
            elif (node.left.node_type == _NT_LITERAL
                    and node.right.node_type != _NT_LITERAL
                    and node.left.value != NULL and bin_right_type is not None):
                bw_coerce_phys = _coerce_literal_physical(
                    bin_right_type, <object>node.left.value)
                if bw_coerce_phys >= 0:
                    node.left.physical_type = bw_coerce_phys
                    bin_left_type = _column_type_for_physical(bw_coerce_phys)

        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)

        slot = bc._push_instr()
        slot.opcode = BC_BINARY_OP
        slot.op_code = bin_op_code
        if slot.op_code == BOP_UNKNOWN:
            raise NotImplementedError(f"compiled_expression: unknown binary op {bin_op_str!r}")

        # Phase 6: resolve the kernel at bind time.
        from opteryx.expression.evaluator.arithmetic import resolve_binary_op
        binop_kernel = resolve_binary_op(slot.op_code, bin_left_type, bin_right_type)
        bc._hold(binop_kernel)
        slot.callable_ref = <PyObject*>binop_kernel

        # P9.1 (executor flip): route C-native families to the unified draken_binop
        # kernel. When _c_native_binop allow-lists this (op, types), the executor
        # dispatches it directly via BC_INSTR_C_NATIVE (no closure, no Python
        # objects). resolve_binary_op stays in callable_ref as the path for every
        # binop not yet C-native. _c_native_binop is the single source of truth.
        bin_left_phys = getattr(bin_left_type.physical, "name", None) if bin_left_type is not None else None
        bin_right_phys = getattr(bin_right_type.physical, "name", None) if bin_right_type is not None else None
        bin_result_phys = getattr(bin_result_type.physical, "name", None) if bin_result_type is not None else None
        if _c_native_binop(slot.op_code, bin_left_phys, bin_right_phys, bin_result_phys):
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx
            # S-A.2 ctx metadata: DECIMAL scales + TIMESTAMP units from the bound
            # logical types (the physical DrakenVector carries neither). result_scale
            # is the binder's own resolved decimal result scale (read, not re-derived,
            # so byte-identical to the operator_map). All zero for numeric/interval/
            # string/bitwise, which don't read them.
            # result_scale/result_precision are the binder's own resolved decimal
            # result descriptor (read off bin_result_type, not re-derived). The kernel
            # stamps them onto the VecResult so the executor wrap reattaches the
            # LogicalType (precision/scale for DECIMAL; unit for TIMESTAMP).
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                "draken_binop", alloc_binary_op_ctx,
                (slot.op_code,
                 _binop_dec_scale(bin_left_type), _binop_dec_scale(bin_right_type),
                 _binop_dec_scale(bin_result_type), _binop_dec_precision(bin_result_type),
                 _binop_ts_unit(bin_left_type), _binop_ts_unit(bin_right_type)))
            if fn_ptr is not None and ctx_wrapper is not None:
                slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                bc._hold(ctx_wrapper)   # keep ctx alive for the bytecode's lifetime
                slot.flags |= BC_INSTR_C_NATIVE
                # S2: a C-native binop is BC_C_NATIVE_FIXED (arena-foldable, nogil)
                # EXCEPT when the result needs a Python-side descriptor wrap: string
                # concat (string owner) and parameterized DECIMAL/TIMESTAMP results
                # (precision/scale or unit reattached via the rc-5 wrap). Those are
                # excluded from the nogil whole-expression fast path.
                if slot.op_code != BOP_STRING_CONCAT and \
                        bin_result_phys not in ("DECIMAL", "DECIMAL128", "TIMESTAMP64"):
                    slot.flags |= BC_C_NATIVE_FIXED
                elif slot.op_code == BOP_STRING_CONCAT:
                    # canonical-block string result — nogil-foldable (S6)
                    slot.flags |= BC_C_NATIVE_STRING
                elif bin_result_phys in ("DECIMAL", "DECIMAL128", "TIMESTAMP64"):
                    # raw-domain foldable (int64 or int128); descriptor re-attached
                    # by the engine at the plan-known ExprProject boundary
                    slot.flags |= BC_C_NATIVE_DESC

        # Phase 1 result-wrap pattern: kernels return nanobind Vectors.
        slot.flags |= BC_RESULT_NEEDS_NB_WRAP
        # Binary ops never return BOOL, so BC_RESULT_WRAP_AS_BOOL stays false.

        # Keep type codes for debugging / introspection (not used in executor).
        slot.left_type_code = _sql_type_to_code(bin_left_type)
        slot.right_type_code = _sql_type_to_code(bin_right_type)
        # Note: slot.compare_op_str no longer needed for BC_BINARY_OP, but field stays.
        return sub_depth - 1   # pop 2, push 1

    # ------------------------------------------------------------------
    # NT_UNARY_OPERATOR — compile centre operand, store op string.
    # ------------------------------------------------------------------
    if nt == _NT_UNARY_OPERATOR:
        if node.centre == NULL:
            raise ValueError("compiled_expression: UNARY_OPERATOR missing centre operand")
        unary_op_str = <object>node.value

        # BitwiseNot (~x) → C-ABI kernel, widened INT64 dense result.
        if unary_op_str == "BitwiseNot":
            _bn_fn, _bn_unused = _resolve_kernel_and_context("draken_bitwise_not", None, None)
            if _bn_fn is not None:
                sub_depth = _linearize(node.centre, bc, depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 1
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE
                slot.kernel_fn = <void*>(<unsigned long long>_bn_fn)
                return sub_depth   # pop 1, push 1 — net 0

        # IsEmpty / IsNotEmpty (the optimizer's rewrite of `str = ''` / `str <> ''`)
        # → C-ABI bool kernel, so the whole predicate stays c-native.
        if unary_op_str in ("IsEmpty", "IsNotEmpty"):
            _se_fn, _se_ignore = _resolve_kernel_and_context(
                "draken_is_empty" if unary_op_str == "IsEmpty" else "draken_is_not_empty",
                None, None)
            if _se_fn is not None:
                sub_depth = _linearize(node.centre, bc, depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 1
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_se_fn)
                return sub_depth   # pop 1, push 1 — net 0

        sub_depth = _linearize(node.centre, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_UNARY_OP
        slot.op_code = <int>_UOP_CODE.get(unary_op_str, UOP_UNKNOWN)
        # compare_op_str not set for BC_UNARY_OP — executor uses op_code int directly
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_FUNCTION — compile each parameter, store callable and arity.
    # ------------------------------------------------------------------
    if nt == _NT_FUNCTION:
        func_val = <object>node.value

        # IF_THEN_ELSE — synthetic node from the plan compiler's CASE rewrite
        # (no function_ref, no Python fallback; the C kernel is mandatory).
        if func_val == "IF_THEN_ELSE":
            if node.parameters.size() != 3:
                raise ValueError("compiled_expression: IF_THEN_ELSE needs 3 parameters")
            _ite_fn, _ite_unused = _resolve_kernel_and_context("draken_if_then_else", None, None)
            if _ite_fn is None:
                raise ValueError("compiled_expression: draken_if_then_else kernel missing")
            sub_depth = depth
            for i in range(3):
                if node.parameters[i] == NULL:
                    raise ValueError("compiled_expression: IF_THEN_ELSE parameter NULL")
                sub_depth = _linearize(node.parameters[i], bc, sub_depth)
            slot = bc._push_instr()
            slot.opcode = BC_FUNCTION
            slot.arity = 3
            slot.bool_value = 0
            slot.flags = BC_INSTR_C_NATIVE
            # A BOOLEAN-branched CASE IS a mask, so it can be a WHERE predicate —
            # same flag, same reasoning as the IIF arm below. What earns the flag is
            # the SHAPE of the result, not the declared type: draken_if_then_else's
            # BOOL arm (function_kernels.cpp) allocates its own `(length+7)/8` bitmap
            # and returns it with data_length == length and draken_identity_sel —
            # dense by construction, never dict- or constant-shaped, which is what
            # cxx_mask_c needs. The declared type is the bind-time PROOF that that
            # arm is the one taken: the binder resolves a CASE's output type from its
            # THEN/ELSE branches and has already refused a mixed-family blend, so
            # BOOLEAN here means every branch is BOOLEAN or NULL. A NULL condition row
            # takes the ELSE branch (fk_row_valid) and a null result row lands in the
            # mask's validity, which cxx_mask_c drops — SQL's "not true, so not
            # selected".
            #
            # Only the OUTERMOST node of the folded chain carries schema_column
            # (compiler._rewrite_case sets `acc.schema_column = sc` on it alone), and
            # that is precisely the instruction bytecode_is_bool_final reads. Inner
            # links get no flag, which is correct — their result is a branch operand,
            # not the program's mask.
            #
            # _ensure_sql_types() is NOT redundant: _LogicalCategory_BOOLEAN is None
            # until it runs, and this arm returns before any other call site on the
            # FUNCTION path. Without it the identity test compares a real category
            # against None, never fires, and leaves the capability silently off.
            _ensure_sql_types()
            _ite_py_node = <object>node.source_node
            _ite_sc = getattr(_ite_py_node, "schema_column", None)
            _ite_ct = _ite_sc.column_type if _ite_sc is not None else None
            if _ite_ct is not None and _ite_ct.category is _LogicalCategory_BOOLEAN:
                slot.flags |= BC_RESULT_WRAP_AS_BOOL
            slot.kernel_fn = <void*>(<unsigned long long>_ite_fn)
            return sub_depth - 3 + 1

        n = <Py_ssize_t>node.parameters.size()
        func_ref_obj = <object>node.source_node
        func_py_node = func_ref_obj
        func_ref_meta = getattr(func_py_node, "function_ref", None)
        if func_ref_meta is None:
            raise ValueError(
                f"compiled_expression: FUNCTION '{func_val}' has no function_ref — not bound"
            )
        callable_obj = func_ref_meta.selected_overload.kernel.callable_ref

        # Every arm below keys off the function name — kernel lookup by
        # `draken_{name}`, and the _sub_/_affix_/_tr_/_extr_ lowering maps. node.value is
        # the name as parsed, so an alias (CEIL, SUBSTR, UCASE, ...) misses every one of
        # them and silently takes the Python callable_ref. The catalog canonicalised the
        # name during resolution; take it from there.
        func_val = func_ref_meta.function_definition.name

        # `constant_only` is ENFORCED, for every function that declares it.
        #
        # It was declaration-only: exported to reference/function_signatures.json,
        # read by nothing. Most functions that declare it happen to reject a
        # column anyway, because their lowering arm consumes the argument into a
        # kernel context and declines when it is not a literal. LPAD and RPAD
        # have no such arm — they take the generic C-native route, and
        # draken_lpad/draken_rpad (draken/ops/kernels/string_pad.cpp) read width
        # and fill as SCALARS from logical row 0. Handed a column they silently
        # padded every row with row 0's value: `RPAD('eta', 8, s_null)` returned
        # 153 identical rows and changed answer whenever the plan changed which
        # row was first. A silent wrong answer, which is the one outcome this
        # codebase will not take (.claude/CLAUDE.md §1). The Python
        # left_pad/right_pad backstops do the same `width[0]` / `fill[0]` read.
        #
        # Checked HERE rather than at bind time because constant folding runs
        # between the two: `LPAD(s, 4+4, '-')` is not a LITERAL node when the
        # binder sees it, and is by the time this runs. Same point HUMANIZE's
        # own mode check uses, so the two cannot disagree about what "constant"
        # means.
        _co_params = func_ref_meta.selected_overload.parameters
        for _co_i in range(min(n, <Py_ssize_t>len(_co_params))):
            if not _co_params[_co_i].constant_only:
                continue
            if node.parameters[_co_i] != NULL \
                    and node.parameters[_co_i].node_type == _NT_LITERAL:
                continue
            raise InvalidFunctionParameterError(
                f"**{func_val}** argument {_co_i + 1} (`{_co_params[_co_i].name}`) must be a "
                "constant, but a column or an expression was given. It is consumed when the "
                "query is compiled, once for the whole column, so it cannot vary per row."
            )

        # SUBSTRING(str, start[, count]) / LEFT(str, n) / RIGHT(str, n) — all lower to
        # the one draken_substring kernel (LEFT = start 1 count n; RIGHT = start -n to
        # end). start/count are LITERALS consumed into a substring_ctx; only the string
        # operand is pushed. Result is a canonical string block (VARCHAR byte /
        # NVARCHAR codepoint). NOTE the optimizer rewrites SUBSTRING(x,1,n) → LEFT(x,n).
        _sub_func = func_val.upper() if func_val else ""
        _sb_ok = False
        _sb_start = 0
        _sb_count = 0
        _sb_has_count = 0
        if _sub_func in ("SUBSTRING", "SUBSTR") and 2 <= n <= 3 \
                and node.parameters[1] != NULL \
                and node.parameters[1].node_type == _NT_LITERAL:
            _sb_start_obj = <object>node.parameters[1].value
            if isinstance(_sb_start_obj, (int, float)) and not isinstance(_sb_start_obj, bool):
                _sb_start = int(_sb_start_obj)
                if n == 2:
                    _sb_ok = True
                # n == 3: only take the literal-ctx fast path when `count` is ALSO a
                # literal. A non-literal (column) count used to fall through here with
                # _sb_ok unconditionally True, silently dropping the count operand
                # (has_count stayed 0 -> ran to end) instead of routing to the
                # column-valued dynamic kernel below. Bug fixed alongside adding that
                # kernel, since it's the same "count is a column" case.
                elif node.parameters[2] != NULL and node.parameters[2].node_type == _NT_LITERAL:
                    _sb_count_obj = <object>node.parameters[2].value
                    if _sb_count_obj is not None:
                        _sb_count = int(_sb_count_obj)
                        _sb_has_count = 1
                    _sb_ok = True
        elif _sub_func in ("LEFT", "RIGHT") and n == 2 \
                and node.parameters[1] != NULL \
                and node.parameters[1].node_type == _NT_LITERAL:
            _sb_len_obj = <object>node.parameters[1].value
            if isinstance(_sb_len_obj, (int, float)) and not isinstance(_sb_len_obj, bool):
                _sb_len = int(_sb_len_obj)
                if _sub_func == "LEFT":
                    _sb_start = 1; _sb_count = _sb_len; _sb_has_count = 1
                else:
                    _sb_start = -_sb_len; _sb_has_count = 0
                _sb_ok = True
        if _sb_ok:
            from draken.ops.kernels._kernel_registry import alloc_substring_ctx as _sb_alloc
            _sb_fn, _sb_ctx = _resolve_kernel_and_context(
                "draken_substring", _sb_alloc, (_sb_start, _sb_count, _sb_has_count))
            if _sb_fn is not None:
                sub_depth = _linearize(node.parameters[0], bc, depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 1
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE
                slot.kernel_fn = <void*>(<unsigned long long>_sb_fn)
                if _sb_ctx is not None:
                    bc._hold(_sb_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_sb_ctx.ctx_ptr)
                # No callable_ref: this is a c-native kernel — there is no Python
                # fallback, and every VM arm gates on BC_INSTR_C_NATIVE before ever
                # reading callable_ref.
                return sub_depth   # operand pushed, fn pops 1 pushes 1

        # Column-valued SUBSTRING/LEFT/RIGHT — reached only when _sb_ok above didn't
        # fire (start/count aren't both compile-time literals). draken_substring_dynamic
        # / draken_left_dynamic / draken_right_dynamic (function_kernels.cpp) read
        # start/count per-row from vector operands instead of a literal ctx; a literal
        # operand still works here too (it linearizes to a constant-shape vector), so
        # mixed literal+column args (e.g. SUBSTRING(col, 2, count_col)) are supported
        # for free via the uniform data[selection[i]] access pattern — no special-casing
        # needed. No ctx: mode is carried by which of the three entry points is resolved.
        if _sub_func in ("SUBSTRING", "SUBSTR") and 2 <= n <= 3:
            _sbd_fn, _sbd_unused = _resolve_kernel_and_context("draken_substring_dynamic", None, None)
            if _sbd_fn is not None:
                sub_depth = _linearize(node.parameters[0], bc, depth)
                sub_depth = _linearize(node.parameters[1], bc, sub_depth)
                if n == 3:
                    sub_depth = _linearize(node.parameters[2], bc, sub_depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = <int>n
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE
                slot.kernel_fn = <void*>(<unsigned long long>_sbd_fn)
                return sub_depth - n + 1
        elif _sub_func in ("LEFT", "RIGHT") and n == 2:
            _sbd_kernel_name = "draken_left_dynamic" if _sub_func == "LEFT" else "draken_right_dynamic"
            _sbd_fn, _sbd_unused = _resolve_kernel_and_context(_sbd_kernel_name, None, None)
            if _sbd_fn is not None:
                sub_depth = _linearize(node.parameters[0], bc, depth)
                sub_depth = _linearize(node.parameters[1], bc, sub_depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE
                slot.kernel_fn = <void*>(<unsigned long long>_sbd_fn)
                return sub_depth - 2 + 1

        # STARTS_WITH / ENDS_WITH (the optimizer's `LIKE 'x%'` / `LIKE '%x'` rewrite;
        # CI variants for ILIKE; negation already wrapped in a NOT node) → C-ABI bool
        # kernels. Both string args pushed; ci flag rides in binary_op_ctx.op_code.
        _affix_map = {"_STARTS_WITH": ("draken_starts_with", 0),
                      "_CI_STARTS_WITH": ("draken_starts_with", 2),
                      "_ENDS_WITH": ("draken_ends_with", 0),
                      "_CI_ENDS_WITH": ("draken_ends_with", 2)}
        if func_val in _affix_map and n == 2:
            _af_name, _af_ci = _affix_map[func_val]
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _af_alloc
            _af_fn, _af_ctx = _resolve_kernel_and_context(
                _af_name, _af_alloc, (_af_ci, 0, 0, 0, 0, 0, 0))
            if _af_fn is not None:
                sub_depth = _linearize(node.parameters[0], bc, depth)
                sub_depth = _linearize(node.parameters[1], bc, sub_depth)
                slot = bc._push_instr()
                slot.opcode = BC_FUNCTION
                slot.arity = 2
                slot.bool_value = 0
                slot.flags = BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                slot.kernel_fn = <void*>(<unsigned long long>_af_fn)
                if _af_ctx is not None:
                    bc._hold(_af_ctx)
                    slot.ctx_ptr = <void*>(<unsigned long long>_af_ctx.ctx_ptr)
                return sub_depth - 2 + 1

        # TRUNC(ts, unit) / DATE_TRUNC(unit, ts) — floor a TIMESTAMP to a unit
        # boundary. Unit literal consumed into a binary_op_ctx (op_code=part id,
        # left_unit=operand TimestampUnit); only the timestamp operand pushed.
        _tr_func = func_val.upper() if func_val else ""
        if _tr_func in ("TRUNC", "DATE_TRUNC", "DATETRUNC") and n == 2:
            _tr_unit = 0
            _tr_operand = -1
            for _ti in range(2):
                if node.parameters[_ti] != NULL \
                        and node.parameters[_ti].node_type == _NT_LITERAL:
                    _tv = <object>node.parameters[_ti].value
                    if isinstance(_tv, bytes):
                        _tv = _tv.decode("utf-8")
                    if isinstance(_tv, str):
                        _tr_unit = _TRUNC_PARTS.get(_tv.upper(), 0)
                        _tr_operand = 1 - _ti
            if _tr_unit != 0 and _tr_operand >= 0 \
                    and node.parameters[_tr_operand] != NULL \
                    and node.parameters[_tr_operand].schema_column != NULL:
                _tr_sc = <object>node.parameters[_tr_operand].schema_column
                _tr_ct = getattr(_tr_sc, "column_type", None)
                _tr_phys = getattr(getattr(_tr_ct, "physical", None), "name", "")
                if _tr_phys == "TIMESTAMP64" and _tr_ct.logical is not None:
                    _tr_uv = int(_tr_ct.logical.unit.value)
                    from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _tr_alloc
                    _tr_fn, _tr_ctx = _resolve_kernel_and_context(
                        "draken_date_trunc", _tr_alloc, (_tr_unit, 0, 0, 0, 0, _tr_uv, 0))
                    if _tr_fn is not None:
                        sub_depth = _linearize(node.parameters[_tr_operand], bc, depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 1
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE
                        slot.kernel_fn = <void*>(<unsigned long long>_tr_fn)
                        if _tr_ctx is not None:
                            bc._hold(_tr_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_tr_ctx.ctx_ptr)
                        return sub_depth

        # HUMANIZE(val, mode) — the mode literal names a scale system (bytes, time,
        # odds, ...) and is consumed HERE into binary_op_ctx.op_code, exactly as
        # DATE_TRUNC's unit is above; only the value operand is pushed, so the
        # kernel still takes one argument.
        #
        # This arm is TOTAL for the 2-argument form: every path below either emits
        # the instruction or raises. It must not be allowed to decline, because
        # falling through would reach the generic name-only kernel lookup with
        # arity=2 (the kernel would reject it as "expected 1 argument") or, past
        # that, the Python callable_ref, whose humanize() takes no mode at all.
        #
        # The single-argument form is deliberately NOT handled here — it keeps its
        # existing route through the generic lookup below, where op_code is 0,
        # which is HZ_WORDS. Same bytes out as before this parameter existed.
        if _tr_func == "HUMANIZE" and n == 2:
            _hz_mv = None
            if node.parameters[1] != NULL \
                    and node.parameters[1].node_type == _NT_LITERAL:
                _hz_mv = <object>node.parameters[1].value
                if isinstance(_hz_mv, bytes):
                    _hz_mv = _hz_mv.decode("utf-8")
            if not isinstance(_hz_mv, str):
                raise InvalidFunctionParameterError(
                    "HUMANIZE mode must be a string literal, one of: "
                    + ", ".join(sorted(m.lower() for m in _HUMANIZE_MODES))
                )
            _hz_mode = _HUMANIZE_MODES.get(_hz_mv.upper(), -1)
            if _hz_mode < 0:
                raise InvalidFunctionParameterError(
                    f"HUMANIZE: unknown mode '{_hz_mv}'. Valid modes are: "
                    + ", ".join(sorted(m.lower() for m in _HUMANIZE_MODES))
                )
            # A DECIMAL value operand still needs its bind-time scale — the kernel
            # reads an unscaled int64 otherwise. Same ctx, second field.
            _hz_scale = 0
            if node.parameters[0] != NULL and node.parameters[0].schema_column != NULL:
                _hz_ct = getattr(<object>node.parameters[0].schema_column, "column_type", None)
                if (_hz_ct is not None and _hz_ct.logical is not None
                        and getattr(_hz_ct.physical, "name", "") == "DECIMAL"):
                    _hz_scale = _binop_dec_scale(_hz_ct)
            from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _hz_alloc
            _hz_fn, _hz_ctx = _resolve_kernel_and_context(
                "draken_humanize", _hz_alloc, (_hz_mode, _hz_scale, 0, 0, 0, 0, 0))
            if _hz_fn is None:
                raise InvalidInternalStateError(
                    "HUMANIZE: the draken_humanize kernel is not registered in this build"
                )
            sub_depth = _linearize(node.parameters[0], bc, depth)
            slot = bc._push_instr()
            slot.opcode = BC_FUNCTION
            slot.arity = 1
            slot.bool_value = 0
            slot.flags = BC_INSTR_C_NATIVE
            slot.kernel_fn = <void*>(<unsigned long long>_hz_fn)
            if _hz_ctx is not None:
                bc._hold(_hz_ctx)
                slot.ctx_ptr = <void*>(<unsigned long long>_hz_ctx.ctx_ptr)
            return sub_depth

        # EXTRACT(part FROM x) / YEAR(x)-family — bind-time lowering to the C-ABI
        # draken_date_part kernel. The part literal is consumed HERE (never pushed);
        # part id + the operand's TimestampUnit travel in a binary_op_ctx
        # (op_code / left_unit — the same vehicle the temporal binops use).
        _extr_func = func_val.upper() if func_val else ""
        _dp_part = 0
        _dp_operand = -1
        if _extr_func == "EXTRACT" and n == 2 and node.parameters[0] != NULL \
                and node.parameters[0].node_type == _NT_LITERAL:
            _dp_val = <object>node.parameters[0].value
            if isinstance(_dp_val, bytes):
                _dp_val = _dp_val.decode("utf-8")
            _dp_part = _EXTRACT_PARTS.get(str(_dp_val).upper(), 0)
            _dp_operand = 1
        elif _extr_func in _EXTRACT_PARTS and n == 1:
            _dp_part = _EXTRACT_PARTS[_extr_func]
            _dp_operand = 0
        if _dp_part != 0 and node.parameters[_dp_operand] != NULL \
                and node.parameters[_dp_operand].schema_column != NULL:
            _dp_sc = <object>node.parameters[_dp_operand].schema_column
            _dp_ct = getattr(_dp_sc, "column_type", None)
            _dp_phys = getattr(getattr(_dp_ct, "physical", None), "name", "")
            if _dp_phys in ("DATE32", "TIMESTAMP64"):
                _dp_unit = 0
                if _dp_phys == "TIMESTAMP64" and _dp_ct.logical is not None:
                    _dp_unit = int(_dp_ct.logical.unit.value)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _dp_alloc
                _dp_fn, _dp_ctx = _resolve_kernel_and_context(
                    "draken_date_part", _dp_alloc,
                    (_dp_part, 0, 0, 0, 0, _dp_unit, 0))
                if _dp_fn is not None:
                    sub_depth = _linearize(node.parameters[_dp_operand], bc, depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 1
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE
                    slot.kernel_fn = <void*>(<unsigned long long>_dp_fn)
                    if _dp_ctx is not None:
                        bc._hold(_dp_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_dp_ctx.ctx_ptr)
                    # No callable_ref: c-native kernel, no Python fallback (VM arms
                    # gate on BC_INSTR_C_NATIVE before reading callable_ref).
                    return sub_depth   # operand pushed, fn pops 1 pushes 1 — net +1

        # DATEDIFF(part, start, end) / DATE_DIFF — bind-time lowering to the C-ABI
        # draken_datediff kernel. `part` is a LITERAL, consumed HERE (never
        # pushed); diff-kind id + BOTH operands' TimestampUnit travel in a
        # binary_op_ctx (op_code=diff_kind, left_unit/right_unit — the same
        # vehicle draken_date_part/draken_date_trunc use). start/end are pushed.
        _dd_func = func_val.upper() if func_val else ""
        if _dd_func == "DATEDIFF" and n == 3 \
                and node.parameters[0] != NULL \
                and node.parameters[0].node_type == _NT_LITERAL \
                and node.parameters[1] != NULL and node.parameters[2] != NULL \
                and node.parameters[1].schema_column != NULL \
                and node.parameters[2].schema_column != NULL:
            _dd_part_val = <object>node.parameters[0].value
            if isinstance(_dd_part_val, bytes):
                _dd_part_val = _dd_part_val.decode("utf-8")
            _dd_kind = _DIFF_PARTS.get(str(_dd_part_val).upper(), -1) if isinstance(_dd_part_val, str) else -1
            _dd_s_sc = <object>node.parameters[1].schema_column
            _dd_e_sc = <object>node.parameters[2].schema_column
            _dd_s_ct = getattr(_dd_s_sc, "column_type", None)
            _dd_e_ct = getattr(_dd_e_sc, "column_type", None)
            _dd_s_phys = getattr(getattr(_dd_s_ct, "physical", None), "name", "")
            _dd_e_phys = getattr(getattr(_dd_e_ct, "physical", None), "name", "")
            if _dd_kind >= 0 and _dd_s_phys in ("DATE32", "TIMESTAMP64") \
                    and _dd_e_phys in ("DATE32", "TIMESTAMP64"):
                _dd_lu = _binop_ts_unit(_dd_s_ct)
                _dd_ru = _binop_ts_unit(_dd_e_ct)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _dd_alloc
                _dd_fn, _dd_ctx = _resolve_kernel_and_context(
                    "draken_datediff", _dd_alloc, (_dd_kind, 0, 0, 0, 0, _dd_lu, _dd_ru))
                if _dd_fn is not None:
                    sub_depth = _linearize(node.parameters[1], bc, depth)
                    sub_depth = _linearize(node.parameters[2], bc, sub_depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 2
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE
                    slot.kernel_fn = <void*>(<unsigned long long>_dd_fn)
                    if _dd_ctx is not None:
                        bc._hold(_dd_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_dd_ctx.ctx_ptr)
                    return sub_depth - 2 + 1

        # TIMEDIFF(time1, time2) / TIME_DIFF — DATEDIFF with diff-kind hardcoded to
        # hours at bind time (same binary_op_ctx vehicle; see the nanobind
        # dispatch_time_diff reference this mirrors).
        _td_func = func_val.upper() if func_val else ""
        if _td_func == "TIMEDIFF" and n == 2 \
                and node.parameters[0] != NULL and node.parameters[1] != NULL \
                and node.parameters[0].schema_column != NULL \
                and node.parameters[1].schema_column != NULL:
            _td_1_sc = <object>node.parameters[0].schema_column
            _td_2_sc = <object>node.parameters[1].schema_column
            _td_1_ct = getattr(_td_1_sc, "column_type", None)
            _td_2_ct = getattr(_td_2_sc, "column_type", None)
            _td_1_phys = getattr(getattr(_td_1_ct, "physical", None), "name", "")
            _td_2_phys = getattr(getattr(_td_2_ct, "physical", None), "name", "")
            if _td_1_phys in ("DATE32", "TIMESTAMP64") and _td_2_phys in ("DATE32", "TIMESTAMP64"):
                _td_lu = _binop_ts_unit(_td_1_ct)
                _td_ru = _binop_ts_unit(_td_2_ct)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _td_alloc
                _td_fn, _td_ctx = _resolve_kernel_and_context(
                    "draken_timediff", _td_alloc, (_TIMEDIFF_DIFF_KIND, 0, 0, 0, 0, _td_lu, _td_ru))
                if _td_fn is not None:
                    sub_depth = _linearize(node.parameters[0], bc, depth)
                    sub_depth = _linearize(node.parameters[1], bc, sub_depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 2
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE
                    slot.kernel_fn = <void*>(<unsigned long long>_td_fn)
                    if _td_ctx is not None:
                        bc._hold(_td_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_td_ctx.ctx_ptr)
                    return sub_depth - 2 + 1

        # UNIXTIME(date) / TO_UNIXTIME — bind-time lowering to the C-ABI
        # draken_unixtime kernel. ctx = binary_op_ctx{left_unit=operand
        # TimestampUnit} (op_code unused) — NOT cast_timestamp_ctx, whose `unit`
        # field uses a different numbering (see function_temporal.cpp header).
        _ut_func = func_val.upper() if func_val else ""
        if _ut_func == "UNIXTIME" and n == 1 \
                and node.parameters[0] != NULL and node.parameters[0].schema_column != NULL:
            _ut_sc = <object>node.parameters[0].schema_column
            _ut_ct = getattr(_ut_sc, "column_type", None)
            _ut_phys = getattr(getattr(_ut_ct, "physical", None), "name", "")
            if _ut_phys in ("DATE32", "TIMESTAMP64"):
                _ut_unit = _binop_ts_unit(_ut_ct)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _ut_alloc
                _ut_fn, _ut_ctx = _resolve_kernel_and_context(
                    "draken_unixtime", _ut_alloc, (0, 0, 0, 0, 0, _ut_unit, 0))
                if _ut_fn is not None:
                    sub_depth = _linearize(node.parameters[0], bc, depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 1
                    slot.bool_value = 0
                    slot.flags = BC_INSTR_C_NATIVE
                    slot.kernel_fn = <void*>(<unsigned long long>_ut_fn)
                    if _ut_ctx is not None:
                        bc._hold(_ut_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_ut_ctx.ctx_ptr)
                    return sub_depth

        # TIME_BUCKET(magnitude, units, date) — bind-time lowering to the C-ABI
        # draken_time_bucket kernel. `magnitude` and `units` are effectively
        # bind-time (only lowered when magnitude is ALSO a literal — the
        # ParameterSpec allows a column, matching the SUBSTRING(start,...)
        # precedent of only lowering the literal-args case); both are consumed
        # into a time_bucket_ctx (kernel_context.h) — only `date` is pushed.
        # Accepts a TIMESTAMP64 operand (result preserves its unit) or a DATE32
        # operand (promoted to microseconds; result is TIMESTAMP64(us), matching
        # the date_trunc DATE32 convention).
        _tb_func = func_val.upper() if func_val else ""
        if _tb_func == "TIME_BUCKET" and n == 3 \
                and node.parameters[0] != NULL and node.parameters[1] != NULL \
                and node.parameters[2] != NULL \
                and node.parameters[0].node_type == _NT_LITERAL \
                and node.parameters[1].node_type == _NT_LITERAL \
                and node.parameters[2].schema_column != NULL:
            _tb_mag_val = <object>node.parameters[0].value
            _tb_units_val = <object>node.parameters[1].value
            if isinstance(_tb_units_val, bytes):
                _tb_units_val = _tb_units_val.decode("utf-8")
            # Only lower an exactly-integral magnitude — a fractional float would
            # silently truncate under int() here while the Python reference path
            # (nb::cast<int64_t>) rejects it outright; don't drift from that.
            _tb_mag_ok = (isinstance(_tb_mag_val, int) and not isinstance(_tb_mag_val, bool)) \
                or (isinstance(_tb_mag_val, float) and _tb_mag_val.is_integer())
            _tb_kind = (_BUCKET_UNITS.get(str(_tb_units_val).upper(), 0)
                        if isinstance(_tb_units_val, str) else 0)
            if _tb_mag_ok and int(_tb_mag_val) > 0 and _tb_kind != 0:
                _tb_sc = <object>node.parameters[2].schema_column
                _tb_ct = getattr(_tb_sc, "column_type", None)
                _tb_phys = getattr(getattr(_tb_ct, "physical", None), "name", "")
                # TIMESTAMP64 carries a logical unit; DATE32 has none and is
                # worked in microseconds by the kernel (ts_unit=2 is a placeholder
                # the DATE32 path ignores). Any other physical type is not lowered.
                _tb_unit = -1
                if _tb_phys == "TIMESTAMP64" and _tb_ct.logical is not None:
                    _tb_unit = int(_tb_ct.logical.unit.value)
                elif _tb_phys == "DATE32":
                    _tb_unit = 2
                if _tb_unit >= 0:
                    from draken.ops.kernels._kernel_registry import alloc_time_bucket_ctx as _tb_alloc
                    _tb_fn, _tb_ctx = _resolve_kernel_and_context(
                        "draken_time_bucket", _tb_alloc, (int(_tb_mag_val), _tb_kind, _tb_unit))
                    if _tb_fn is not None:
                        sub_depth = _linearize(node.parameters[2], bc, depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 1
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE
                        slot.kernel_fn = <void*>(<unsigned long long>_tb_fn)
                        if _tb_ctx is not None:
                            bc._hold(_tb_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_tb_ctx.ctx_ptr)
                        return sub_depth

        # FORMAT_TIMESTAMP(pattern, date) / FORMAT_DATE(pattern, date) — bind-time
        # lowering to the C-ABI draken_date_format kernel (BigQuery argument
        # order: pattern first). `pattern` is a LITERAL, consumed HERE (never
        # pushed) into a format_ctx (kernel_context.h: ts_unit + pattern bytes
        # trailing the struct) — only `date` is pushed. Reuses the SAME compiled
        # token-program formatter as the nanobind FORMAT_TIMESTAMP path
        # (draken/ops/temporal_format.h) — one formatter, not two.
        _fmt_func = func_val.upper() if func_val else ""
        if _fmt_func == "FORMAT_TIMESTAMP" and n == 2 \
                and node.parameters[0] != NULL and node.parameters[1] != NULL \
                and node.parameters[0].node_type == _NT_LITERAL \
                and node.parameters[1].schema_column != NULL:
            _fmt_pat_val = <object>node.parameters[0].value
            if isinstance(_fmt_pat_val, str):
                _fmt_pat_val = _fmt_pat_val.encode("utf-8")
            if isinstance(_fmt_pat_val, bytes):
                _fmt_sc = <object>node.parameters[1].schema_column
                _fmt_ct = getattr(_fmt_sc, "column_type", None)
                _fmt_phys = getattr(getattr(_fmt_ct, "physical", None), "name", "")
                if _fmt_phys in ("DATE32", "TIMESTAMP64"):
                    _fmt_unit = _binop_ts_unit(_fmt_ct)
                    from draken.ops.kernels._kernel_registry import alloc_format_ctx as _fmt_alloc
                    _fmt_fn, _fmt_ctx = _resolve_kernel_and_context(
                        "draken_date_format", _fmt_alloc, (_fmt_unit, _fmt_pat_val))
                    if _fmt_fn is not None:
                        sub_depth = _linearize(node.parameters[1], bc, depth)
                        slot = bc._push_instr()
                        slot.opcode = BC_FUNCTION
                        slot.arity = 1
                        slot.bool_value = 0
                        slot.flags = BC_INSTR_C_NATIVE
                        slot.kernel_fn = <void*>(<unsigned long long>_fmt_fn)
                        if _fmt_ctx is not None:
                            bc._hold(_fmt_ctx)
                            slot.ctx_ptr = <void*>(<unsigned long long>_fmt_ctx.ctx_ptr)
                        return sub_depth

        # ARRAY_CONTAINS_ANY / ARRAY_CONTAINS_ALL(arr, needles) — bind-time lowering.
        #
        # The needle set is a LITERAL, baked into an in_list_ctx blob exactly as the
        # IN-list lowering does — so it is NOT a second vector operand, and there is
        # no second ARRAY child to resolve. That matters: the array operand's child
        # element vector rides the BC_C_NATIVE_CHILD path (same as SORT / the
        # ARRAY->VARCHAR cast), whose encoding carries exactly ONE column_identity.
        # Only arity 1 is pushed (the array); the needles reach the kernel via ctx.
        #
        # Eligibility is tested BEFORE linearizing: a non-column array operand has no
        # resolvable child, and once parameters have been linearized there is no
        # clean way to un-emit them and fall through to the generic path below.
        # _NT_IDENTIFIER/_NT_EVALUATED/_NT_AGGREGATOR are exactly the node types that
        # lower to BC_LOAD_COL.
        _acm_func = func_val.upper() if func_val else ""
        if _acm_func in ("ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL") and n == 2 \
                and node.parameters[0] != NULL and node.parameters[1] != NULL \
                and node.parameters[1].node_type == _NT_LITERAL \
                and (node.parameters[0].node_type == _NT_IDENTIFIER
                     or node.parameters[0].node_type == _NT_EVALUATED
                     or node.parameters[0].node_type == _NT_AGGREGATOR):
            _acm_blob = _build_array_membership_blob(<object>node.parameters[1].value)
            if _acm_blob is not None:
                from draken.ops.kernels._kernel_registry import alloc_in_list_ctx as _acm_alloc
                _acm_fn, _acm_ctx = _resolve_kernel_and_context(
                    f"draken_{_acm_func.lower()}", _acm_alloc, _acm_blob)
                if _acm_fn is not None:
                    sub_depth = _linearize(node.parameters[0], bc, depth)
                    slot = bc._push_instr()
                    slot.opcode = BC_FUNCTION
                    slot.arity = 1
                    slot.bool_value = 0
                    slot.flags = (BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL
                                  | BC_C_NATIVE_CHILD)
                    slot.kernel_fn = <void*>(<unsigned long long>_acm_fn)
                    # The child is resolved per morsel from THIS instruction's
                    # identity; bc.count - 2 is the BC_LOAD_COL just linearized.
                    slot.column_identity = bc.instrs[bc.count - 2].column_identity
                    if _acm_ctx is not None:
                        bc._hold(_acm_ctx)
                        slot.ctx_ptr = <void*>(<unsigned long long>_acm_ctx.ctx_ptr)
                    # No callable_ref: c-native, no Python fallback.
                    return sub_depth   # operand pushed, fn pops 1 pushes 1 — net +1

        sub_depth = depth
        for i in range(n):
            if node.parameters[i] == NULL:
                raise ValueError("compiled_expression: FUNCTION parameter NULL")
            sub_depth = _linearize(node.parameters[i], bc, sub_depth)

        # Pre-compute nb_func flag at bind time — eliminates runtime
        # `type(callable).__name__ == "nb_func"` string comparison per call.
        is_nb_callable = type(callable_obj).__name__ == "nb_func"

        slot = bc._push_instr()
        slot.opcode = BC_FUNCTION
        slot.arity = <int>n
        slot.bool_value = 1 if is_nb_callable else 0

        # Set result-wrap flags based on kernel return type (resolved at bind time).
        # This eliminates runtime isinstance/type checks on the executor path.
        slot.flags = 0
        if is_nb_callable:
            slot.flags |= BC_RESULT_NEEDS_NB_WRAP
            _ensure_sql_types()
            # Phase 5: inferred_return_type is ColumnType; check .category for BOOLEAN.
            _irt = func_ref_meta.inferred_return_type
            if _irt is not None and _irt.category is _LogicalCategory_BOOLEAN:
                slot.flags |= BC_RESULT_WRAP_AS_BOOL

        # A VECTOR result's byte width is dimension*2 — per-column metadata, not a
        # function of DrakenType, so the span's fixed-width table cannot size it.
        # Record the bind-time declared width here; _dv_eval_span_cxx reads it off the
        # root instruction to materialize the result. Any VECTOR-returning kernel gets
        # this for free — nothing here is EMBED-specific.
        _fn_vec_dim = 0
        _fn_rt = func_ref_meta.inferred_return_type
        if _fn_rt is not None and _fn_rt.logical is not None \
                and getattr(_fn_rt.physical, "name", "") == "VECTOR_FP16":
            _fn_rt_dim = getattr(_fn_rt.logical, "dimension", None)
            if _fn_rt_dim is not None and int(_fn_rt_dim) > 0:
                _fn_vec_dim = int(_fn_rt_dim)
                slot.vec_dimension = <int32_t>_fn_vec_dim

        # Phase 9b: Resolve C kernel function pointer for function calls.
        # Function kernels (Phase 9a-fn) are under development; resolution is optional.
        # If a C kernel exists, use it; otherwise, fall back to Python callable_ref.
        # This is the intended behaviour while function kernels are being ported.
        func_name = func_val.upper() if func_val else None
        if func_name is not None:
            # Scalar functions over a DECIMAL operand need the operand's scale — a
            # LogicalType detail the DrakenVector cannot carry — via a bind-time ctx
            # (binary_op_ctx, the same vehicle the decimal binops use):
            #   ROUND/FLOOR/CEILING/CEIL/TRUNC — round/truncate EXACTLY in the raw
            #                              int64 domain.
            #   SQRT/POWER/LOG/HUMANIZE  — need the VALUE, not the unscaled int64.
            #   ABS                      — |unscaled| is scale-free arithmetic, but the
            #                              result is DECIMAL(p, s) and the kernel stamps
            #                              that descriptor onto the VecResult, so it has
            #                              to be told (p, s). ABS returns "same as num",
            #                              hence result == operand descriptor.
            # SIGN is absent on purpose: it returns INTEGER and sign(unscaled) ==
            # sign(value), so it is scale-invariant end to end and takes no ctx.
            # POWER/LOG take a SECOND operand (exponent/base); this ctx only ever
            # carries parameters[0]'s scale (left_scale) — a DECIMAL second operand
            # still has no scale to read and the kernel fails loud on it.
            _fn_ctx_alloc = None
            _fn_ctx_arg = None
            if func_name in ("ROUND", "FLOOR", "CEILING", "CEIL", "SQRT", "ABS",
                              "TRUNC", "POWER", "LOG", "HUMANIZE") and n >= 1 \
                    and node.parameters[0] != NULL \
                    and node.parameters[0].schema_column != NULL:
                _fn_p0_sc = <object>node.parameters[0].schema_column
                _fn_p0_ct = getattr(_fn_p0_sc, "column_type", None)
                if (_fn_p0_ct is not None and _fn_p0_ct.logical is not None
                        and getattr(_fn_p0_ct.physical, "name", "") == "DECIMAL"):
                    from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx
                    _fn_ctx_alloc = alloc_binary_op_ctx
                    _fn_dec_sc = _binop_dec_scale(_fn_p0_ct)
                    if func_name == "ABS":
                        _fn_ctx_arg = (0, _fn_dec_sc, 0,
                                       _fn_dec_sc, _binop_dec_precision(_fn_p0_ct), 0, 0)
                    else:
                        _fn_ctx_arg = (0, _fn_dec_sc, 0, 0, 0, 0, 0)
            # TRUNC is overloaded in the catalog — TRUNC_numeric (kernel id "numeric",
            # draken_trunc), TRUNC_date and TRUNC_timestamp (kernel ids "date"/
            # "timestamp", still Python-only) all share the name "TRUNC". The registry
            # lookup below resolves by NAME ("draken_trunc"), which cannot distinguish
            # overloads, so a bare name check would wrongly mark the temporal overloads
            # C-native too and defer their (currently unavoidable) failure from PLAN
            # time to RUN time. Gate on the bind-time-selected overload's kernel id so
            # only TRUNC_numeric is ever offered to the registry.
            _fn_skip_lookup = (
                func_name == "TRUNC"
                and getattr(func_ref_meta.selected_overload.kernel, "id", None) != "numeric"
            )
            # DATEDIFF/TIMEDIFF/UNIXTIME/TIME_BUCKET/FORMAT_TIMESTAMP are ONLY ever
            # correct via their dedicated lowering arms above (binary_op_ctx/
            # time_bucket_ctx/format_ctx built from the operands' TimestampUnit —
            # a days-vs-micros mixup is a wrong-answer bug, not a missing
            # optimization). Their kernel names now exist in the registry, so
            # this generic name-only lookup would otherwise ALSO match them —
            # with no ctx and arity=n (all params, including the part/pattern
            # LITERAL the dedicated arm consumes) — and either crash
            # ("expected N arguments") or read ctx==NULL. When the dedicated arm
            # didn't fire (its eligibility check failed — e.g. a NULL/untyped
            # operand), fall through to the Python callable_ref, never here.
            if func_name in ("DATEDIFF", "TIMEDIFF", "UNIXTIME", "TIME_BUCKET", "FORMAT_TIMESTAMP"):
                _fn_skip_lookup = True
            # SUBSTRING/SUBSTR: the dedicated literal-ctx arm above (_sb_ok) already
            # returned when start/count were literals. Reaching here means it wasn't
            # eligible (a non-literal from_pos/count — a column, say), so this generic
            # name-only lookup would otherwise still match "draken_substring_2"/
            # "draken_substring_3" in the registry (registered for the literal-ctx
            # kernel draken_substring) with NO ctx and arity=n — same
            # "expected N arguments" crash class as DATEDIFF/TIMEDIFF above. Fall
            # through to the Python callable_ref path (currently unsupported at PLAN
            # time — no C-native kernel handles column-valued from_pos/count yet)
            # rather than dispatch the literal-only kernel against operands it cannot
            # read.
            if func_name in ("SUBSTRING", "SUBSTR"):
                _fn_skip_lookup = True
            # Unary ARRAY reducers — SORT(arr) -> ARRAY, GREATEST/LEAST(arr) ->
            # element scalar. All three read the array's elements from the column
            # owner's child vector (draken_sort/greatest/least read it exactly like
            # draken_cast_array_to_varchar — same ARRAY-elements-unreachable-from-
            # DrakenVector* wall). Only a DIRECT column load has a child to resolve,
            # so an indirect/computed array argument (e.g. SORT(some_function(x)))
            # is NOT eligible — force it down the Python callable_ref path rather
            # than dispatch a kernel that cannot see its own input. bc.instrs[
            # bc.count - 2] is the arg's terminal instruction (this FUNCTION slot
            # was just pushed as bc.instrs[bc.count - 1]) — same indexing CAST's
            # BC_C_NATIVE_CHILD check above uses.
            _reduce_child_eligible = (
                func_name in ("SORT", "GREATEST", "LEAST") and n == 1 and bc.count >= 2
                and bc.instrs[bc.count - 2].opcode == BC_LOAD_COL
            )
            if func_name in ("SORT", "GREATEST", "LEAST") and not _reduce_child_eligible:
                _fn_skip_lookup = True
            # Reducer over ARRAY<TIMESTAMP>: the result (SORT's child, or GREATEST/
            # LEAST's scalar) is itself a TIMESTAMP64 value, and a TIMESTAMP64 with
            # no unit descriptor is a hard error (vector_owner.h). The kernel cannot
            # read the unit off a DrakenVector — it is a LogicalType detail — so
            # hand it over at bind time in binary_op_ctx.left_unit, the same vehicle
            # the temporal binops use. Element type comes from the schema
            # (ARRAY<TIMESTAMP> since the _rugo_schema container fix).
            if _reduce_child_eligible and node.parameters[0].schema_column != NULL:
                _sort_p0_ct = getattr(
                    <object>node.parameters[0].schema_column, "column_type", None)
                _sort_el = getattr(_sort_p0_ct, "element", None) if _sort_p0_ct is not None else None
                if (_sort_el is not None
                        and getattr(getattr(_sort_el, "physical", None), "name", "") == "TIMESTAMP64"
                        and _sort_el.logical is not None
                        and _sort_el.logical.unit is not None):
                    from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx
                    _fn_ctx_alloc = alloc_binary_op_ctx
                    _fn_ctx_arg = (0, 0, 0, 0, 0, int(_sort_el.logical.unit.value), 0)
            # COSINE_SIMILARITY/COSINE_DISTANCE over VECTOR operands: the kernel needs
            # the operands' VECTOR width. A vector's dimension is a LogicalType detail
            # and is NOT on the physical DrakenVector, so — exactly like the DECIMAL
            # scales above — the binder has to hand it over in a ctx. Only the _VECTOR
            # overload takes one; the _TEXT overload embeds its operands and knows its
            # own width. Mismatched widths are rejected here, at PLAN time, rather than
            # producing a garbage score per row.
            _fn_overload_id = getattr(func_ref_meta.selected_overload, "id", None)
            # Drives the bare-name fallback rule below: only an unambiguous
            # single-overload function may fall back to draken_{name}.
            _fn_overload_count = len(func_ref_meta.function_definition.overloads)
            if (func_name in ("COSINE_SIMILARITY", "COSINE_DISTANCE")
                    and _fn_overload_id is not None and _fn_overload_id.endswith("_VECTOR")
                    and n == 2):
                _cos_dims = []
                for _cos_i in range(2):
                    if node.parameters[_cos_i] == NULL \
                            or node.parameters[_cos_i].schema_column == NULL:
                        _cos_dims = []
                        break
                    _cos_ct = getattr(
                        <object>node.parameters[_cos_i].schema_column, "column_type", None)
                    _cos_lg = getattr(_cos_ct, "logical", None) if _cos_ct is not None else None
                    _cos_d = getattr(_cos_lg, "dimension", None) if _cos_lg is not None else None
                    if _cos_d is None or int(_cos_d) < 1:
                        _cos_dims = []
                        break
                    _cos_dims.append(int(_cos_d))
                if len(_cos_dims) == 2 and _cos_dims[0] == _cos_dims[1]:
                    from draken.ops.kernels._kernel_registry import alloc_vector_dim_ctx
                    _fn_ctx_alloc = alloc_vector_dim_ctx
                    _fn_ctx_arg = _cos_dims[0]
                else:
                    # No usable width (untyped/mismatched operands) — the kernel cannot
                    # run without one, and there is no Python fallback on the native
                    # engine, so let it surface as an unsupported expression.
                    _fn_skip_lookup = True
            # The _TEXT overload is "embed both operands, then compare" — the same
            # question as COSINE_SIMILARITY(EMBED(a), EMBED(b)). It must therefore use
            # the SAME embedder, so hand it the resolved draken_embed rather than let it
            # embed for itself: an embedder of its own would be duplicated logic that
            # silently diverges the moment a capability replaces the core one (observed
            # with MiniLM installed: the text overload answered 'dog'/'puppy' 0.0
            # lexically while the EMBED composition answered 0.80).
            elif (func_name in ("COSINE_SIMILARITY", "COSINE_DISTANCE")
                    and _fn_overload_id is not None and _fn_overload_id.endswith("_TEXT")):
                from draken.ops.kernels._kernel_registry import (
                    alloc_cosine_text_ctx, lookup_kernel as _lk_embed)
                from opteryx.types.vectors.embedding_capability import embedding_dimensions
                _emb_fn, _emb_unused = _lk_embed("draken_embed")
                if _emb_fn is None:
                    # draken_embed is core and always registered; its absence means the
                    # registry is broken, not that this expression is unsupported.
                    raise ValueError(
                        "compiled_expression: draken_embed is not registered — "
                        "COSINE_SIMILARITY over text cannot be lowered")
                _fn_ctx_alloc = alloc_cosine_text_ctx
                _fn_ctx_arg = (int(embedding_dimensions()), _emb_fn)
            # MATCH (col) AGAINST (str) is COSINE_SIMILARITY(col, str) >= threshold, so it
            # delegates to the SAME resolved draken_embed for exactly the reason the _TEXT
            # arm above does — a MATCH that embedded differently from COSINE_SIMILARITY
            # would be the same split-brain in a new place. The threshold comes off the
            # node, where the binder put it after reading the `match_threshold` session
            # variable (the variables are not in scope here), so a compiled plan keeps the
            # threshold it was compiled with.
            elif func_name == "_MATCH_AGAINST":
                from draken.ops.kernels._kernel_registry import (
                    alloc_match_ctx, lookup_kernel as _lk_match_embed)
                from opteryx.types.vectors.embedding_capability import embedding_dimensions
                _emb_fn, _emb_unused = _lk_match_embed("draken_embed")
                if _emb_fn is None:
                    raise ValueError(
                        "compiled_expression: draken_embed is not registered — "
                        "MATCH cannot be lowered")
                _match_thresh = getattr(func_py_node, "match_threshold", None)
                if _match_thresh is None:
                    # The binder stamps this on every bound _MATCH_AGAINST. Missing means
                    # the node reached here unbound, not that MATCH is unsupported —
                    # defaulting a threshold would silently answer a different question.
                    raise ValueError(
                        "compiled_expression: _MATCH_AGAINST has no match_threshold — "
                        "not bound")
                _fn_ctx_alloc = alloc_match_ctx
                _fn_ctx_arg = (int(embedding_dimensions()), _emb_fn, float(_match_thresh))
            # Any VECTOR-returning function (EMBED today) is TOLD the width the plan
            # declared for it, and must produce exactly that. The declaration is the
            # single source of truth: the projection boundary copies rows at the
            # declared stride, so a kernel-side width constant that drifted from it
            # would read the wrong bytes rather than fail. This is also the seam a
            # registered capability plugs into — it declares a width, the binder
            # records it, and the kernel is handed it.
            elif _fn_vec_dim > 0:
                from draken.ops.kernels._kernel_registry import alloc_vector_dim_ctx
                _fn_ctx_alloc = alloc_vector_dim_ctx
                _fn_ctx_arg = _fn_vec_dim
            if _fn_skip_lookup:
                fn_ptr, ctx_wrapper = None, None
            else:
                # Signature-aware resolution: probe the bind-time-selected OVERLOAD's
                # kernel (draken_{overload_id}) before the bare draken_{name}. A bare
                # name cannot distinguish overloads, so a name-only registry (the
                # original scheme) forced every overload of a function onto one kernel —
                # fine while each ported function had a single native overload, wrong as
                # soon as two overloads take different operand types and need different
                # kernels (COSINE_SIMILARITY over VECTOR vs over VARCHAR).
                #
                # The bare-name fallback is therefore allowed ONLY for a function with
                # exactly one overload, where it cannot be ambiguous. With more than one
                # overload the kernel must be named per overload in kernel_registry.cpp
                # (aliases are fine when the overloads genuinely share a kernel). An
                # unconditional fallback bound LENGTH(array) to the string-only
                # draken_length, which accepted the bind and then failed at RUNTIME —
                # a kernel silently chosen for operands it refuses. Leaving fn_ptr NULL
                # here instead routes to the plan-time gate, which rejects the query up
                # front (hard-cutover: there is no fallback engine to degrade into).
                fn_ptr, ctx_wrapper = None, None
                if _fn_overload_id:
                    fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                        f"draken_{_fn_overload_id.lower()}", _fn_ctx_alloc, _fn_ctx_arg)
                if fn_ptr is None and _fn_overload_count <= 1:
                    fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                        f"draken_{func_name.lower()}", _fn_ctx_alloc, _fn_ctx_arg)
            if fn_ptr is not None:
                # fn_ptr is a Python int carrying the C address — cast its VALUE,
                # not the PyObject's address (a bare <void*> on the object stored
                # the int OBJECT pointer: jump-to-garbage the first time anything
                # actually dispatched a function kernel).
                slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                if ctx_wrapper is not None:
                    bc._hold(ctx_wrapper)   # frees the ctx with the bytecode
                    slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                # Function kernels generally don't need context structs
                slot.flags |= BC_INSTR_C_NATIVE
                if func_name == "_MATCH_AGAINST":
                    # draken__match_against_2 returns a BOOL mask, and a bool-returning
                    # C-ABI kernel must say so at bind time or the filter's admission gate
                    # (bytecode_is_c_native_predicate) rejects the whole predicate. The
                    # flag-setting above only marks the nb-callable path; every other
                    # bool-returning c-native kernel (LIKE, STARTS_WITH, array membership)
                    # sets this in its own dedicated arm. Deliberately keyed to MATCH
                    # rather than generalised to "c-native and declared BOOLEAN": the other
                    # bool-returning kernels are being worked on concurrently, and widening
                    # the gate for them is their change to make, not a side effect of this.
                    slot.flags |= BC_RESULT_WRAP_AS_BOOL
                if func_name == "IIF" and _fn_rt is not None:
                    # A boolean-branched IIF IS a mask, so it can be a WHERE
                    # predicate — same flag, same reasoning as _MATCH_AGAINST above.
                    # What earns the flag is the SHAPE of the result, not the
                    # declared type: draken_iif routes two BOOL branches to
                    # nc_blend_bool (function_null_conditional.cpp), which allocates
                    # its own `length`-wide bitmap and returns it with an identity
                    # selection — dense by construction, never dict- or
                    # constant-shaped, which is what cxx_mask_c needs. The declared
                    # type is the bind-time PROOF that that arm is the one taken:
                    # _iif_return_type is BOOLEAN only when both value branches are
                    # BOOLEAN (or NULL, which nc_dispatch skips when picking the
                    # family), and _check_blend_compatible has already refused a
                    # mixed-family blend. A NULL condition row selects the false
                    # branch and a null result row lands in the mask's validity,
                    # which cxx_mask_c drops — SQL's "not true, so not selected".
                    #
                    # _ensure_sql_types() is NOT redundant: _LogicalCategory_BOOLEAN
                    # is None until it runs, and the only call on this path sits
                    # inside the `is_nb_callable` branch above, which a c-native IIF
                    # does not take. Without it the identity test compares a real
                    # category against None, never fires, and leaves the capability
                    # silently off.
                    _ensure_sql_types()
                    if _fn_rt.category is _LogicalCategory_BOOLEAN:
                        slot.flags |= BC_RESULT_WRAP_AS_BOOL
                if _reduce_child_eligible:
                    # VM resolves the child per morsel via the identity stored on
                    # THIS instruction (mirrors the ARRAY->VARCHAR cast) and
                    # appends it as a synthetic extra fargs[] entry — the reducer
                    # kernel (SORT/GREATEST/LEAST) keeps the plain
                    # func_fn_t(ctx, args[], nargs) shape, reading args[0]=parent
                    # (offsets) / args[1]=child (elements).
                    slot.flags |= BC_C_NATIVE_CHILD
                    slot.column_identity = bc.instrs[bc.count - 2].column_identity
            # else: kernel not available yet (pending Phase 9a-fn); callable_ref path remains

        bc._hold(callable_obj)
        slot.callable_ref = <PyObject*>callable_obj
        if n == 0:
            return depth + 1
        return sub_depth - n + 1   # pop n, push 1

    # ------------------------------------------------------------------
    # NT_CAST — compile source expression, resolve at bind time.
    # Phase 5: resolve_cast(source_sql, target_type, args, unit) returns a
    # pre-specialized kernel/closure; stored as callable_ref, invoked per-morsel.
    # ------------------------------------------------------------------
    if nt == _NT_CAST:
        if node.left == NULL:
            raise ValueError("compiled_expression: CAST missing source operand")
        sub_depth = _linearize(node.left, bc, depth)

        cast_py_node = <object>node.source_node
        cast_target_type = cast_py_node.value
        cast_is_try = cast_target_type.startswith("TRY_")
        if cast_is_try:
            cast_target_type = cast_target_type[4:]

        cast_unit = None
        _unit_map = {
            "_TIMESTAMP_NS": ("TIMESTAMP", "ns"),
            "_TIMESTAMP_MS": ("TIMESTAMP", "ms"),
            "_TIMESTAMP_S":  ("TIMESTAMP", "s"),
            "_TIMESTAMP_US": ("TIMESTAMP", "us"),
            "_TIMESTAMP_DAYS": ("TIMESTAMP", "days"),
        }
        if cast_target_type in _unit_map:
            cast_target_type, cast_unit = _unit_map[cast_target_type]

        cast_params = tuple(
            p.value for p in (cast_py_node.parameters or [])
        )

        # Phase 5: get the source operand's physical type from schema_column for
        # bind-time resolution. source_phys_name is the physical DrakenType name —
        # the discriminant both resolve_cast (closure) and _c_native_cast (C kernel)
        # key on. None when the binder left the source untyped (e.g. ARRAY columns).
        # source_is_ipv4 is the SECOND half of the source discriminant, and it is
        # not optional: an IPv4 column's physical type IS "UINT32", identical to a
        # plain unsigned column's, so the physical name alone cannot choose between
        # rendering '192.168.1.1' and rendering '3232235777'. The IPv4-ness lives
        # only in the bound ColumnType's LogicalKind descriptor (CLAUDE.md §14),
        # so read it here and hand it to both resolvers explicitly.
        _ensure_sql_types()
        source_phys_name = None
        source_is_ipv4 = False
        if node.left.schema_column != NULL:
            src_sc = <object>node.left.schema_column
            if src_sc is not None:
                source_sql = src_sc.column_type
                if source_sql is not None:
                    source_phys_name = getattr(source_sql.physical, "name", None)
                    _src_logical = source_sql.logical
                    source_is_ipv4 = (
                        _src_logical is not None
                        and _src_logical.kind == _LogicalKind_IPV4
                    )

        from opteryx.expression.casts import resolve_cast
        try:
            cast_kernel, _cast_needs_nb_input, _cast_returns_raw = resolve_cast(
                source_phys_name, cast_target_type, cast_params, unit=cast_unit,
                safe=cast_is_try, source_is_ipv4=source_is_ipv4
            )
        except (NotImplementedError, ValueError) as e:
            # `resolve_cast`'s refusals already name the pair they refused, so this
            # only normalises the exception type — prepending the pair a second time
            # produced "Unsupported CAST: DECIMAL → DATE: No native CAST DECIMAL →
            # DATE", which said the same thing twice and named an internal component
            # in between.
            raise ValueError(str(e))

        slot = bc._push_instr()
        slot.opcode = BC_CAST
        slot.flags = 0
        _ensure_sql_types()

        # X (thin closures): wrap / input-unwrap are driven by the resolver, not by
        # per-morsel type dispatch.
        #   returns_raw  → kernel yields a raw nanobind Vector; executor wraps it.
        #   needs_nb_input → kernel wants a raw nanobind Vector; executor unwraps the
        #                    Cython shim to ._nb before the call (slot.bool_value flag).
        cast_target_sql = getattr(cast_py_node, "inferred_type", None)
        if _cast_returns_raw:
            slot.flags |= BC_RESULT_NEEDS_NB_WRAP
        if cast_target_sql is _LogicalCategory_BOOLEAN:
            slot.flags |= BC_RESULT_WRAP_AS_BOOL
        slot.bool_value = 1 if _cast_needs_nb_input else 0

        # Y (executor flip): resolve the C-native kernel for this pair. When one
        # exists (real + registered, fixed-width result), the executor dispatches
        # it directly via BC_INSTR_C_NATIVE — no Python object per morsel. The
        # resolve_cast closure remains in callable_ref as the fallback for every
        # cast not yet C-native (strings, timestamps, DECIMAL/VECTOR/ARRAY, the
        # late-bound escape hatch). _c_native_cast is the single source of truth.
        from opteryx.expression.casts import _c_native_cast
        _cn = _c_native_cast(source_phys_name, cast_target_type, safe=cast_is_try,
                             source_is_ipv4=source_is_ipv4)

        # CAST ... FORMAT is only meaningful (and only compiles) for the kernels
        # that read a format_ctx — fail loud rather than silently drop the pattern
        # for any other (source, target) pairing. TRY_CAST/SAFE_CAST never reach
        # the C-native path at all (_c_native_cast returns None whenever
        # safe=True, by design — see its docstring), so FORMAT is not yet
        # supported combined with TRY_CAST/SAFE_CAST; call that out specifically
        # rather than reporting it as an unsupported (source, target) pairing.
        if getattr(cast_py_node, "format", None) is not None and (
            _cn is None or _cn[0] not in _CAST_FORMAT_AWARE_KERNELS
        ):
            if cast_is_try:
                raise ValueError(
                    "CAST ... FORMAT is not yet supported combined with TRY_CAST/SAFE_CAST"
                )
            raise ValueError(
                f"CAST ... FORMAT is not supported for {source_phys_name} → {cast_target_type}"
            )

        if _cn is not None:
            # TIMESTAMP-producing casts need the target unit in a cast_timestamp_ctx
            # (1 ns / 2 us / 3 ms / 4 s / 5 days); default us when the cast left it
            # implicit. Everything else is unit-less (None ctx).
            _cn_alloc = None
            _cn_arg = None
            # draken_cast_string_to_timestamp also ends with "_to_timestamp" but is
            # NOT one of the int64/date32-source unit-conversion kernels below — it
            # takes the FORMAT-pattern ctx branch further down. Must be excluded
            # here or it would be misrouted into alloc_cast_timestamp_ctx.
            if _cn[0].endswith("_to_timestamp") and _cn[0] not in _CAST_FORMAT_AWARE_KERNELS:
                # Target unit = binder-declared result unit (see the rescale branch
                # below for why the SQL suffix is NOT the target). "days" has no
                # TimestampUnit representation and keeps its special scaling code.
                _cn_unit_map = {"ns": 1, "us": 2, "ms": 3, "s": 4, "days": 5}
                from draken.ops.kernels._kernel_registry import alloc_cast_timestamp_ctx as _cn_alloc
                _cn_arg = _cn_unit_map.get(cast_unit, 2)
                if cast_unit != "days":
                    _cn_res_sc = getattr(cast_py_node, "schema_column", None)
                    _cn_res_ct = getattr(_cn_res_sc, "column_type", None) if _cn_res_sc is not None else None
                    if _cn_res_ct is not None and _cn_res_ct.logical is not None:
                        # TimestampUnit (0=s,1=ms,2=us,3=ns) -> cast_timestamp_ctx
                        # numbering (4=s,3=ms,2=us,1=ns).
                        _cn_arg = {0: 4, 1: 3, 2: 2, 3: 1}[int(_cn_res_ct.logical.unit.value)]
            elif _cn[0] == "draken_cast_timestamp_rescale":
                # TIMESTAMP64 -> TIMESTAMP64 unit rescale. THE TARGET UNIT IS THE
                # BINDER-DECLARED RESULT ColumnType's unit — NOT the SQL suffix.
                # The binder uses `::TIMESTAMP[s]` to declare how the SOURCE is
                # interpreted (it re-types the source operand's unit to `s`) and
                # ALWAYS types the cast RESULT as canonical microseconds. The
                # engine re-attaches the plan-declared descriptor at the
                # ExprProject boundary, so a kernel emitting values at any OTHER
                # unit than that descriptor displays garbage (the descriptor/value
                # mismatch behind the 1970-collapse bug — the ctx plumbing was
                # never at fault; values survived verbatim, the UNIT lied).
                # Units use TimestampUnit numbering (0=s,1=ms,2=us,3=ns), same as
                # EXTRACT/TRUNC's binary_op_ctx convention.
                # SOURCE unit: the operand's declared LogicalType when the binder
                # attached one; otherwise the SQL suffix — `x::TIMESTAMP[s]` means
                # "interpret x as SECONDS" (observed: when the same column is ALSO
                # used raw, the binder leaves the operand's logical None and only
                # the suffix carries the source interpretation).
                _cn_sfx_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
                _cn_src_unit = _cn_sfx_map.get(cast_unit, 2)
                if source_sql is not None and source_sql.logical is not None:
                    _cn_src_unit = int(source_sql.logical.unit.value)
                _cn_dst_unit = 2   # binder's canonical default
                _cn_res_sc = getattr(cast_py_node, "schema_column", None)
                _cn_res_ct = getattr(_cn_res_sc, "column_type", None) if _cn_res_sc is not None else None
                if _cn_res_ct is not None and _cn_res_ct.logical is not None:
                    _cn_dst_unit = int(_cn_res_ct.logical.unit.value)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, 0, 0, 0, 0, _cn_src_unit, _cn_dst_unit)
            elif _cn[0] == "draken_cast_timestamp_to_date32":
                # TIMESTAMP64 -> DATE32: truncates the time component. Only the
                # SOURCE unit matters (DATE32 carries no descriptor) — same
                # source-unit determination as the rescale branch above: the
                # operand's declared LogicalType when the binder attached one,
                # otherwise the SQL suffix (`x::DATE` interprets x per its own
                # column type, so this is the defensive fallback, not the
                # common path).
                _cn_sfx_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
                _cn_src_unit = _cn_sfx_map.get(cast_unit, 2)
                if source_sql is not None and source_sql.logical is not None:
                    _cn_src_unit = int(source_sql.logical.unit.value)
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, 0, 0, 0, 0, _cn_src_unit, 0)
            elif _cn[0].endswith("_to_decimal"):
                # DECIMAL(p, s): params are the cast's (precision, scale) literals.
                # left_scale carries the SOURCE scale — zero for the float source
                # (which has none), the real thing for a DECIMAL source, where the
                # rescale kernel cannot know how far to shift without it.
                if len(cast_params) != 2:
                    raise ValueError("CAST to DECIMAL requires (precision, scale)")
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, _binop_dec_scale(source_sql), 0,
                           int(cast_params[1]), int(cast_params[0]), 0, 0,
                           1 if cast_is_try else 0)
            elif _cn[0] == "draken_cast_to_array":
                # CAST(json AS ARRAY<T>): the kernel needs the ELEMENT type, which
                # a DrakenVector cannot carry (the parent's tag is just ARRAY), and
                # the TRY_CAST disposition. Both ride a bind-time cast_array_ctx.
                #
                # The element type is read off the BOUND result ColumnType, not
                # re-parsed from the cast's parameter literal: the binder already
                # resolved `ARRAY<VARCHAR>` to ColumnType(ARRAY, element=VARCHAR),
                # and re-parsing would be a second, driftable resolution of the
                # same thing.
                _cn_res_sc = getattr(cast_py_node, "schema_column", None)
                _cn_res_ct = getattr(_cn_res_sc, "column_type", None) if _cn_res_sc is not None else None
                _cn_elem = _cn_res_ct.element if _cn_res_ct is not None else None
                if _cn_elem is None:
                    raise ValueError(
                        "CAST to ARRAY requires an element type, e.g. "
                        "CAST(x AS ARRAY<VARCHAR>) — the binder resolved no element type"
                    )
                from draken.ops.kernels._kernel_registry import alloc_cast_array_ctx as _cn_alloc
                _cn_arg = (int(_cn_elem.physical.value), 1 if cast_is_try else 0)
            elif _cn[0].startswith("draken_cast_decimal"):
                # EVERY decimal-SOURCE kernel — → VARCHAR/BLOB, → INT64, → FLOAT64,
                # → UINT8/16/32/64 — reads exactly one ctx field and the same one:
                # the source scale. Matched by prefix rather than an enumerated
                # list so adding a decimal-source kernel cannot silently arrive
                # here with a null ctx (which would mean "scale 0" — every value
                # off by a power of ten). The one decimal-source kernel that needs
                # MORE than this, decimal→decimal, is claimed by the `_to_decimal`
                # branch above and never reaches here.
                #
                # DECIMAL → VARCHAR/BLOB: the SOURCE scale (LogicalType, absent from
                # the runtime vector) rides in binary_op_ctx.left_scale. Read it off
                # the bound source ColumnType (source_sql is set for any typed decimal
                # source — the only way _cn selected this kernel). The _to_blob twin
                # forwards this same ctx straight into decimal_to_string_core before
                # retagging the result (see cast_numeric.cpp), so it needs the
                # identical ctx shape as its _to_string sibling.
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, _binop_dec_scale(source_sql), 0, 0, 0, 0, 0,
                           1 if cast_is_try else 0)
            elif _cn[0] in _CAST_FORMAT_AWARE_KERNELS:
                # CAST ... FORMAT '<pattern>' (or the ISO-8601 default when absent) —
                # the pattern must be a compile-time literal (enforced in
                # logical_planner_builders.cast()); baked into a format_ctx exactly
                # like FORMAT_TIMESTAMP's pattern (same struct, same allocator — see
                # draken/ops/kernels/kernel_context.h). Always allocated (even with
                # an empty pattern) for the two TIMESTAMP64 kernels: the ctx also
                # carries the source's TimestampUnit, which the raw int64 payload's
                # scale depends on — a null ctx previously meant "always treat as
                # microseconds", silently wrong for non-us TIMESTAMP64 columns.
                _fc_fmt_node = getattr(cast_py_node, "format", None)
                _fc_fmt_bytes = b""
                if _fc_fmt_node is not None:
                    _fc_fmt_val = _fc_fmt_node.value
                    if isinstance(_fc_fmt_val, str):
                        _fc_fmt_bytes = _fc_fmt_val.encode("utf-8")
                    elif isinstance(_fc_fmt_val, bytes):
                        _fc_fmt_bytes = _fc_fmt_val
                _fc_ts_unit = 2
                if _cn[0] in ("draken_cast_timestamp_to_string", "draken_cast_timestamp_to_blob"):
                    if source_sql is not None and source_sql.logical is not None:
                        _fc_ts_unit = int(source_sql.logical.unit.value)
                from draken.ops.kernels._kernel_registry import alloc_format_ctx as _cn_alloc
                _cn_arg = (_fc_ts_unit, _fc_fmt_bytes, 1 if cast_is_try else 0)
            if cast_is_try and _cn_alloc is None:
                # TRY_CAST over a kernel that needs no other parameter: allocate a
                # binary_op_ctx whose ONLY meaningful field is `safe`. Without
                # this the kernel sees a null ctx, reads the disposition as
                # "raise", and TRY_CAST silently becomes CAST — which is why this
                # is not conditional on the kernel being one that can fail. A
                # kernel that cannot fail ignores the flag; one that can must
                # never be handed a null ctx under TRY_CAST.
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, 0, 0, 0, 0, 0, 0, 1)
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(_cn[0], _cn_alloc, _cn_arg)
            if fn_ptr is not None:
                slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                slot.flags |= BC_INSTR_C_NATIVE
                if ctx_wrapper is not None:
                    bc._hold(ctx_wrapper)
                    slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                # S2: string-result cast kernels are named `..._to_string`
                # (VARCHAR target), `..._to_varchar`/`..._to_blob` (the
                # string-family retag, VARCHAR/BLOB target), or `..._to_nvarchar`
                # (validate+retag, NVARCHAR target); their canonical-block
                # results fold nogil too (S6). TIMESTAMP results are DESC (unit
                # re-attached at the ExprProject boundary); everything else is
                # plain fixed-width.
                if (_cn[0].endswith("_to_string") or _cn[0].endswith("_to_varchar")
                        or _cn[0].endswith("_to_blob") or _cn[0].endswith("_to_nvarchar")):
                    slot.flags |= BC_C_NATIVE_STRING
                elif (_cn[0].endswith("_to_timestamp") or _cn[0].endswith("_to_decimal")
                        or _cn[0] == "draken_cast_timestamp_rescale"):
                    slot.flags |= BC_C_NATIVE_DESC
                elif _cn[0] == "draken_cast_to_array":
                    # ARRAY result: elements ride VecResult.child, adoptable only
                    # on the engine path. Must NOT be marked FIXED — that would
                    # admit it to is_all_c_native, whose VM drops the child.
                    slot.flags |= BC_C_NATIVE_ARRAY
                else:
                    slot.flags |= BC_C_NATIVE_FIXED
        elif (not cast_is_try and cast_target_type == "VARCHAR"
                and (source_phys_name is None or source_phys_name == "ARRAY")
                and bc.count >= 2
                and bc.instrs[bc.count - 2].opcode == BC_LOAD_COL):
            # ARRAY -> VARCHAR: the elements live in the column owner's child
            # vector, unreachable from the parent DrakenVector*. Only a DIRECT
            # column load has a child to resolve, hence the preceding-LOAD
            # requirement. The binder leaves ARRAY columns UNTYPED
            # (schema_column.column_type is None), so None is accepted here and
            # the kernel fails loud at run time on any non-ARRAY parent. The VM
            # resolves the child per morsel via the identity stored on THIS
            # instruction (BC_C_NATIVE_CHILD) and dispatches the two-vector
            # kernel; the result is a canonical string block (STRING).
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                "draken_cast_array_to_varchar", None, None)
            if fn_ptr is not None:
                slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                slot.flags |= (BC_INSTR_C_NATIVE | BC_C_NATIVE_STRING
                               | BC_C_NATIVE_CHILD)
                slot.column_identity = bc.instrs[bc.count - 2].column_identity
        elif (not cast_is_try and cast_target_type == "VECTOR"
                and (source_phys_name is None or source_phys_name == "ARRAY")
                and bc.count >= 2
                and bc.instrs[bc.count - 2].opcode == BC_LOAD_COL):
            # CAST(array_column AS VECTOR(n)) — the mirror of the ARRAY->VARCHAR arm
            # above: the elements hang off the column owner's child vector, reachable
            # only from a DIRECT column load (hence the preceding-LOAD requirement), so
            # an indirect/computed array is not eligible and falls out of the c-native
            # set rather than dispatching a kernel that cannot see its own input.
            # ARRAY columns are left UNTYPED by the binder, so source None is accepted
            # and the kernel fails loud on a non-ARRAY parent.
            #
            # The width is a plan-time constant the physical vector cannot carry, so it
            # goes down in a vector_dim_ctx AND onto slot.vec_dimension (which the span
            # boundary reads off the root instruction to size the result copy). The
            # binder guarantees it: a bare VECTOR never reaches here.
            _cv_dim = 0
            _cv_sc = getattr(cast_py_node, "schema_column", None)
            _cv_ct = getattr(_cv_sc, "column_type", None) if _cv_sc is not None else None
            _cv_lg = getattr(_cv_ct, "logical", None) if _cv_ct is not None else None
            if _cv_lg is not None and getattr(_cv_lg, "dimension", None):
                _cv_dim = int(_cv_lg.dimension)
            if _cv_dim > 0:
                from draken.ops.kernels._kernel_registry import alloc_vector_dim_ctx as _cv_alloc
                fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                    "draken_cast_array_to_vector", _cv_alloc, _cv_dim)
                if fn_ptr is not None:
                    slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                    if ctx_wrapper is not None:
                        bc._hold(ctx_wrapper)
                        slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                    # DESC: VECTOR carries a descriptor (its width) that the arena DV*
                    # cannot hold; the plan re-attaches it at the projection boundary,
                    # exactly as DECIMAL/TIMESTAMP results do.
                    slot.flags |= (BC_INSTR_C_NATIVE | BC_C_NATIVE_DESC
                                   | BC_C_NATIVE_CHILD)
                    slot.column_identity = bc.instrs[bc.count - 2].column_identity
                    slot.vec_dimension = <int32_t>_cv_dim

        bc._hold(cast_kernel)
        slot.callable_ref = <PyObject*>cast_kernel
        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_EXTRACTION_OPERATOR — Phase 3: bind-time resolution
    #
    # Resolve (op_str, operand_type) → kernel + sub-op flag at bind time.
    # No Python wrappers, no runtime type dispatch. The executor calls the
    # resolved native kernel directly via the sub-op code in slot.op_code.
    #
    # op_code stores the sub-op flag (BC_EXTR_MAP_STRING, etc.)
    # literal_obj stores either:
    #   - raw key bytes for JSON extraction (Arrow / LongArrow)
    #   - length-1 INT64 key Vector for string map access
    # bool_value stores:
    #   - scalar int64 key for ARRAY map access (option B: store int directly)
    # ------------------------------------------------------------------
    if nt == _NT_EXTRACTION_OPERATOR:
        if node.left == NULL:
            raise ValueError("compiled_expression: EXTRACTION_OPERATOR missing left operand")

        _ensure_sql_types()
        extr_op_str = <object>node.value
        extr_key = <object>node.right.value if node.right != NULL else None

        # Resolve operand type from schema_column on the left operand node.
        left_sc = <object>node.left.schema_column if node.left.schema_column != NULL else None
        if left_sc is None:
            raise ValueError("compiled_expression: EXTRACTION_OPERATOR left operand missing schema_column")
        _left_ct = left_sc.column_type
        left_sql = _left_ct.category if _left_ct is not None else None

        # Sub-op + kernel selection: resolve at bind time.
        sub_op = BC_EXTR_UNKNOWN
        extr_literal = None
        slot_bool_val = 0

        if extr_op_str == "MapAccess":
            if left_sql == _LogicalCategory_ARRAY:
                # MapAccess on ARRAY: store scalar int64 key in bool_value (Option B).
                # The scalar is extracted from the constant key at bind time.
                sub_op = BC_EXTR_MAP_ARRAY
                # Store the int64 key directly in bool_value.
                slot_bool_val = int(extr_key)
            elif left_sql == _LogicalCategory_VARIANT:
                # VARIANT is in _STRING_FAMILY, but subscripting one is ambiguous - the
                # value may be an array, a string, or neither, and it differs per row.
                # Taking the string path here would byte-index the raw JSON text, which
                # is nearly never what was meant. The binder rejects this first; this is
                # the backstop so the string path cannot be reached by accident.
                raise IncorrectTypeError(
                    "MapAccess: subscripting a VARIANT is ambiguous - it may hold an array, "
                    "a string, or neither. Cast it to the type you mean first, for example "
                    "`(value::VARCHAR)[0]` to index the JSON text."
                )
            elif left_sql in _STRING_FAMILY:
                # MapAccess on string: store length-1 INT64 key Vector.
                sub_op = BC_EXTR_MAP_STRING
                extr_literal = _draken_native.vector_from_constant(int(extr_key), 1)
            else:
                raise IncorrectTypeError(
                    f"MapAccess: operand must be ARRAY or string family; got {left_sql!r}"
                )
        elif extr_op_str == "Arrow":
            if left_sql not in _STRING_FAMILY:
                raise IncorrectTypeError(
                    f"-> requires a string/JSON operand; got {left_sql!r}"
                )
            sub_op = BC_EXTR_JSON_PTR
            # Store raw key bytes.
            extr_literal = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
        elif extr_op_str == "LongArrow":
            if left_sql not in _STRING_FAMILY:
                raise IncorrectTypeError(
                    f"->> requires a string/JSON operand; got {left_sql!r}"
                )
            sub_op = BC_EXTR_JSON_KEY
            # Store raw key bytes.
            extr_literal = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
        else:
            raise ValueError(f"unknown EXTRACTION_OPERATOR: {extr_op_str!r}")

        sub_depth = _linearize(node.left, bc, depth)
        slot = bc._push_instr()
        slot.opcode = BC_EXTRACTION
        slot.op_code = sub_op
        slot.flags = BC_RESULT_NEEDS_NB_WRAP

        # Resolve the C-ABI kernel.
        #
        # BC_EXTR_MAP_ARRAY (arr[i]) is admitted ONLY when its array operand lowers to
        # BC_LOAD_COL. The elements hang off the VectorOwner, not off DrakenVector, so
        # the kernel cannot reach them from the parent operand: the VM resolves the
        # child per morsel from the column owner and hands it over in the ABI's unused
        # `key` slot (BC_C_NATIVE_CHILD — the mechanism the ARRAY→VARCHAR cast and SORT
        # use). That resolution is keyed on a column identity, which only a column
        # operand has. A computed array is projected into a column first
        # (compiler.py _hoist_array_operands), so this stays satisfiable rather than
        # rejecting the query.
        # _NT_IDENTIFIER/_NT_EVALUATED/_NT_AGGREGATOR are exactly the node types that
        # lower to BC_LOAD_COL.
        _extr_child_eligible = (
            sub_op == BC_EXTR_MAP_ARRAY
            and (node.left.node_type == _NT_IDENTIFIER
                 or node.left.node_type == _NT_EVALUATED
                 or node.left.node_type == _NT_AGGREGATOR)
        )
        _extr_kernel_names = {
            BC_EXTR_MAP_STRING: "draken_map_access_string",
            BC_EXTR_JSON_PTR: "draken_json_extract",
            BC_EXTR_JSON_KEY: "draken_json_extract",
        }
        if _extr_child_eligible:
            _extr_kernel_names[BC_EXTR_MAP_ARRAY] = "draken_array_map_access"
        if sub_op in _extr_kernel_names:
            from draken.ops.kernels._kernel_registry import alloc_extraction_ctx
            # The path/key and the subscript index are bind-time constants: they go
            # into the ctx, so BC_EXTRACTION pops exactly one operand at run time.
            # JSON paths are normalized to RFC 6901 inside the allocator.
            ctx_nav = extr_literal if sub_op in (BC_EXTR_JSON_PTR, BC_EXTR_JSON_KEY) else None
            ctx_index = 0
            if sub_op == BC_EXTR_MAP_STRING:
                ctx_index = int(extr_key)
            elif sub_op == BC_EXTR_MAP_ARRAY:
                ctx_index = int(slot_bool_val)
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                _extr_kernel_names[sub_op],
                alloc_extraction_ctx,
                (sub_op, ctx_nav, ctx_index),
            )
            if fn_ptr is None:
                raise ValueError(
                    f"Extraction kernel '{_extr_kernel_names[sub_op]}' not found in registry. "
                    f"This is a supported extraction operation but kernel is missing."
                )

            slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
            if ctx_wrapper is not None:
                bc._hold(ctx_wrapper)
                slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
            slot.flags |= BC_INSTR_C_NATIVE
            if _extr_child_eligible:
                # The VM resolves the child per morsel via the identity stored on THIS
                # instruction and passes it as the second ABI operand.
                slot.flags |= BC_C_NATIVE_CHILD
                # bc.count-1 is THIS slot (just pushed); bc.count-2 is the operand's
                # own BC_LOAD_COL, which carries the identity to resolve the child by.
                slot.column_identity = bc.instrs[bc.count - 2].column_identity

        # Store the extracted literal (bytes or Vector).
        if extr_literal is not None:
            bc._hold(extr_literal)
            slot.literal_obj = <PyObject*>extr_literal
        # For MapAccess ARRAY, the bool_value was set above.
        if sub_op == BC_EXTR_MAP_ARRAY:
            slot.bool_value = slot_bool_val

        return sub_depth   # pop 1, push 1 — net 0

    # ------------------------------------------------------------------
    # NT_CASE — compile all WHEN conditions and THEN/ELSE results to
    # bytecode at bind time; resolve output type and select assembly kernel
    # at bind time; store a pre-built closure in callable_ref.
    # At execution time BC_CASE simply calls callable_ref(morsel) — no
    # runtime type dispatch.
    # ------------------------------------------------------------------
    if nt == _NT_CASE:
        src = <object>node.source_node

        cond_bcs = [build_bytecode(lower(c)) for c in src.conditions]
        result_bcs = [build_bytecode(lower(r)) for r in src.results]
        else_bc = build_bytecode(lower(src.else_result)) if src.else_result is not None else None

        # Phase 7: resolve output type at bind time.
        #
        # The binder resolves the CASE output type and records it on
        # src.schema_column.column_type (the first non-NULL THEN/ELSE branch
        # type — see binder.py NodeType.CASE). That is the authoritative
        # output type and the source of truth for kernel selection.
        #
        # `src.inferred_type` is never populated on a CASE node (Node.__getattr__
        # returns None for unset attributes), so relying on it left kernel_type
        # at the -1 runtime-dispatch sentinel for EVERY case. Runtime dispatch
        # picks the kernel from the first non-None branch result vector, which is
        # wrong when the first branch is a typed-NULL: a string CASE whose first
        # THEN is NULL (e.g. `CASE WHEN .. THEN NULL ELSE str_col END`) was
        # dispatched to the FIXED kernel, producing a fixed-width vector mislabelled
        # as the string output column — a heap-corrupting type confusion downstream.
        _ensure_sql_types()
        case_inferred_type = getattr(src, "inferred_type", None)
        if case_inferred_type is None:
            _case_sc = getattr(src, "schema_column", None)
            _case_ct = _case_sc.column_type if _case_sc is not None else None
            case_inferred_type = _case_ct.category if _case_ct is not None else None

        # Select the assembly kernel based on the inferred result type.
        # All THEN/ELSE branches must agree on type (enforced by binder).
        from opteryx.expression.evaluator.case_eval import build_case_fn

        # Kernel type constants match the DEF values in case_eval.pyx
        _ASSEMBLE_BOOL = 0
        _ASSEMBLE_FIXED = 1
        _ASSEMBLE_STRING = 2

        # Determine kernel type from inferred type
        if case_inferred_type is _LogicalCategory_BOOLEAN:
            kernel_type = _ASSEMBLE_BOOL
        elif case_inferred_type in _STRING_FAMILY:
            kernel_type = _ASSEMBLE_STRING
        elif case_inferred_type is None:
            # Output type unresolved (e.g. every branch is NULL). Defer to runtime
            # type dispatch via the -1 sentinel in build_case_fn.
            kernel_type = -1
        else:
            # Fixed-width (numeric, date, timestamp, etc.)
            kernel_type = _ASSEMBLE_FIXED

        # A DECIMAL CASE also needs its declared (precision, scale) carried into the
        # assembler: the branch parts come straight off the expression VM, which
        # leaves DECIMAL results descriptor-less (the descriptor is re-attached at
        # the ExprProject boundary, downstream of this path). Bind time is the only
        # place that still knows the scale.
        _case_dec_p = -1
        _case_dec_s = -1
        _case_sc = getattr(src, "schema_column", None)
        _case_ct = _case_sc.column_type if _case_sc is not None else None
        if _case_ct is not None and _case_ct.logical is not None and \
                getattr(_case_ct.physical, "name", "") in ("DECIMAL", "DECIMAL128"):
            _case_dec_p = int(_case_ct.logical.precision)
            _case_dec_s = int(_case_ct.logical.scale)

        case_callable = build_case_fn(cond_bcs, result_bcs, else_bc, kernel_type,
                                      _case_dec_p, _case_dec_s)

        slot = bc._push_instr()
        slot.opcode = BC_CASE
        # CASE closure returns a nanobind Vector; set NEEDS_NB_WRAP.
        slot.flags = BC_RESULT_NEEDS_NB_WRAP
        if case_inferred_type is _LogicalCategory_BOOLEAN:
            slot.flags |= BC_RESULT_WRAP_AS_BOOL
        bc._hold(case_callable)
        slot.callable_ref = <PyObject*>case_callable
        depth += 1
        if depth > bc.max_stack_depth:
            bc.max_stack_depth = depth
        return depth

    # ------------------------------------------------------------------
    # Bind-time invariant: every supported node type has an explicit branch
    # above. Reaching here is a planner/compiler bug.
    # ------------------------------------------------------------------
    raise NotImplementedError(f"compiled_expression: unsupported node type {nt}")


cdef _validate_temporal_at_bind(
    int left_nt, left_type, int right_nt, right_type, op
):
    """Raise IncompatibleTypesError at bind time if a temporal comparison
    has an un-cast literal on one side. Runs once per COMPARISON node.
    """
    _ensure_sql_types()
    cdef object _lcat = left_type.category if left_type is not None else None
    cdef object _rcat = right_type.category if right_type is not None else None
    cdef bint left_is_temporal = (_lcat is _LogicalCategory_DATE) or (_lcat is _LogicalCategory_TIMESTAMP)
    cdef bint right_is_temporal = (_rcat is _LogicalCategory_DATE) or (_rcat is _LogicalCategory_TIMESTAMP)

    if not (left_is_temporal or right_is_temporal):
        return
    if left_is_temporal and right_is_temporal:
        return

    cdef int non_temporal_nt = right_nt if left_is_temporal else left_nt
    non_temporal_side = "right" if left_is_temporal else "left"

    if non_temporal_nt != _NT_IDENTIFIER:
        from opteryx.exceptions import IncompatibleTypesError
        raise IncompatibleTypesError(
            message=(
                f"Temporal comparison requires literals to be explicitly cast to temporal types.\n"
                f"The {non_temporal_side} side is missing an explicit CAST or :: operator.\n\n"
                f"Examples of valid syntax:\n"
                f"  - col {op} literal::DATE\n"
                f"  - col {op} literal::TIMESTAMP[ms]\n\n"
                f"Supported temporal types: DATE, TIMESTAMP[ms], TIMESTAMP[us], TIMESTAMP[s], TIMESTAMP[ns], TIMESTAMP[d]"
            )
        )


# ---------------------------------------------------------------------------
# CompiledExpressionHandle (unchanged from previous wedge)
# ---------------------------------------------------------------------------

cdef class CompiledExpressionHandle:
    """Owns one CompiledExpressionArena and the root pointer into it."""

    def __cinit__(self):
        self._arena = new CompiledExpressionArena()
        self._root = NULL

    def __dealloc__(self):
        if self._arena != NULL:
            del self._arena
            self._arena = NULL

    @property
    def node_count(self):
        return self._arena.node_count()

    def node_type_walk(self):
        if self._root == NULL:
            return []
        return self._arena.node_type_walk(self._root)

    cdef CompiledExpression* root(self) noexcept:
        return self._root


def expand_between(node):
    """PLAN-TIME tree rewrite: BETWEEN(operand; lower, upper) becomes
    AND(operand >= lower, operand <= upper), honouring each bound's inclusivity.

    BETWEEN is pure sugar. A dedicated opcode had to carry both bounds as raw
    PyObject scalars and compare them RAW against the column, which silently
    mis-compared a DATE32 column (days) against a TIMESTAMP64 bound (unit-scaled
    ticks) — `date_col BETWEEN <ts> AND <ts>` returned zero rows. Expanding to
    two compares routes each bound through BC_COMPARE, which resolves the
    unit-aware `draken_temporal_cmp` for mixed-domain temporal operands.

    Called from `lower()` so NO caller can produce a domain-blind range check,
    and from the plan-time rewrite chain (`_rewrite_decimal_compares` must see
    the expanded compares to rescale decimal bounds). Idempotent: a tree with no
    BETWEEN is returned unchanged. Non-mutating — parents of a changed child are
    rebuilt via Node.copy()."""
    from opteryx.compiled.structures.node import Node
    from opteryx.expression import NodeType

    if not isinstance(node, Node):
        return node
    if node.node_type == NodeType.BETWEEN:
        lower_incl, upper_incl = node.value
        operand = expand_between(node.left)
        lo = Node(NodeType.COMPARISON_OPERATOR,
                  value=("GtEq" if lower_incl else "Gt"))
        lo.left = operand
        lo.right = node.right      # lower-bound literal node, reused as-is
        hi = Node(NodeType.COMPARISON_OPERATOR,
                  value=("LtEq" if upper_incl else "Lt"))
        hi.left = operand
        hi.right = node.centre     # upper-bound literal node
        both = Node(NodeType.AND)
        both.left = lo
        both.right = hi
        return both

    rebuilt = None
    for attr in ("left", "right", "centre"):
        child = getattr(node, attr)
        if isinstance(child, Node):
            new_child = expand_between(child)
            if new_child is not child:
                if rebuilt is None:
                    rebuilt = node.copy()
                setattr(rebuilt, attr, new_child)
    params = node.parameters
    if isinstance(params, list):
        new_params = [expand_between(c) if isinstance(c, Node) else c for c in params]
        if any(a is not b for a, b in zip(new_params, params)):
            if rebuilt is None:
                rebuilt = node.copy()
            rebuilt.parameters = new_params
    return rebuilt if rebuilt is not None else node


def lower(node):
    """Lower an opteryx Node tree into a CompiledExpressionHandle.

    BETWEEN is expanded to a pair of compares FIRST — see `expand_between`. This
    is the single entry point that builds the C node tree, so no consumer can
    reach the executor with a domain-blind range check."""
    cdef CompiledExpressionHandle handle = CompiledExpressionHandle()
    handle._root = handle._arena.lower(expand_between(node))
    return handle


_PURE_BITMAP_OPCODES = frozenset({
    BC_LOAD_COL, BC_LOAD_LIT_BOOL,
    BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF,
})

# S2: opcodes that run on the nogil DV* operand stack. Loads place operands;
# compute ops produce an arena-owned result (so a bytecode ending in a compute
# op has anchor-free result → the nogil path needs no Python anchor tracking).
_C_NATIVE_LOAD_OPCODES = frozenset({BC_LOAD_COL, BC_LOAD_LIT_CONST, BC_LOAD_LIT_BOOL})
_C_NATIVE_BOOL_OPCODES = frozenset({BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF})
_C_NATIVE_COMPUTE_OPCODES = frozenset(
    _C_NATIVE_BOOL_OPCODES | {BC_COMPARE, BC_BINARY_OP, BC_CAST, BC_UNARY_OP,
                              BC_FUNCTION, BC_EXTRACTION})

def build_bytecode(CompiledExpressionHandle handle):
    """Linearise the lowered tree into a typed CompiledBytecode container."""
    if handle._root == NULL:
        raise ValueError("build_bytecode: handle has no lowered root")
    cdef CompiledBytecode bc = CompiledBytecode()
    _linearize(handle._root, bc, 0)

    # Scan opcodes: is_pure_bitmap is True when every instruction is GIL-free.
    # BC_LOAD_COL is included; the runtime pre-pass verifies the column type.
    cdef Py_ssize_t k
    bc.is_pure_bitmap = True
    for k in range(bc.count):
        if bc.instrs[k].opcode not in _PURE_BITMAP_OPCODES:
            bc.is_pure_bitmap = False
            break

    # S2: is_all_c_native — every op runs nogil on the DV* stack with a fixed-width
    # result, and the LAST op is a compute op (arena result → no anchor tracking).
    # Enables evaluate_c_native (whole-bytecode single GIL release). Excludes:
    # inline-IN-list / non-ordinal compares (LIKE/IN — Python kernels), string-
    # result binop/cast (need a Vector owner), unresolved FUNCTION/EXTRACTION
    # (Python callables), and CASE/LIT_SET/LIT_SCALAR (GIL).
    cdef int op, fl, opc
    bc.is_all_c_native = bc.count > 0
    for k in range(bc.count):
        op = bc.instrs[k].opcode
        fl = bc.instrs[k].flags
        if op in _C_NATIVE_LOAD_OPCODES or op in _C_NATIVE_BOOL_OPCODES:
            continue
        if op == BC_COMPARE:
            opc = bc.instrs[k].op_code
            if (fl & BC_CMP_INLIST_INLINE) == 0 and 1 <= opc <= 6:
                continue
            bc.is_all_c_native = False
            break
        if op == BC_UNARY_OP:
            # IS [NOT] NULL is a pure validity read, and IS [NOT] TRUE/FALSE is a
            # never-null truth test — both have nogil DV* arms (evaluation.pyx
            # _dv_unary_null_c / _dv_unary_bool_test_c). Other unaries stay GIL.
            opc = bc.instrs[k].op_code
            if (opc == UOP_IS_NULL or opc == UOP_IS_NOT_NULL
                    or opc == UOP_IS_TRUE or opc == UOP_IS_FALSE
                    or opc == UOP_IS_NOT_TRUE or opc == UOP_IS_NOT_FALSE):
                continue
            bc.is_all_c_native = False
            break
        if op == BC_BINARY_OP or op == BC_CAST:
            # BC_C_NATIVE_CHILD is engine-only: this path evaluates over Python
            # Morsels, which cannot resolve the owner-held ARRAY child vector.
            if (fl & BC_INSTR_C_NATIVE) != 0 and (fl & BC_C_NATIVE_CHILD) == 0 and \
                    (fl & (BC_C_NATIVE_FIXED | BC_C_NATIVE_STRING)) != 0:
                continue
            bc.is_all_c_native = False
            break
        if op == BC_FUNCTION:
            # Phase 9a-fn: a resolved C-ABI kernel runs on the nogil DV* stack
            # (fixed or canonical-block string result); Python callables do not.
            if (fl & BC_INSTR_C_NATIVE) != 0:
                continue
            bc.is_all_c_native = False
            break
        if op == BC_EXTRACTION:
            # `->`, `->>` and str[i] resolve C-ABI kernels whose path/index is bound
            # into extraction_ctx, so they pop one vector and produce a canonical-block
            # string. BC_EXTR_MAP_ARRAY never carries the flag (no reachable child).
            if (fl & BC_INSTR_C_NATIVE) != 0:
                continue
            bc.is_all_c_native = False
            break
        bc.is_all_c_native = False
        break
    if bc.is_all_c_native and bc.instrs[bc.count - 1].opcode not in _C_NATIVE_COMPUTE_OPCODES:
        bc.is_all_c_native = False

    return bc
