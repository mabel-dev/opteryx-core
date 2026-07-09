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
from libc.string cimport memset

import draken.draken_native as _draken_native
from opteryx.exceptions import IncorrectTypeError

import datetime as _datetime
import decimal as _decimal
import struct as _struct

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY, DRAKEN_INTERVAL, DRAKEN_DATE32, DRAKEN_TIMESTAMP64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_FLOAT32, DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_UINT8, DRAKEN_UINT16, DRAKEN_UINT32, DRAKEN_UINT64

# Epoch anchor for DATE literal → days-since-1970 conversion (bind time only).
cdef object _EPOCH_DATE = _datetime.date(1970, 1, 1)


cdef Vector _materialise_constant_literal(object value, int physical_type):
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
    """
    cdef long long ordinal
    cdef bytes raw
    cdef object int_vec
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
        return Vector(_draken_native.vector_null_from_length(1))
    if isinstance(value, bool):
        # Bools are handled upstream by BC_LOAD_LIT_BOOL; reaching here is a bug.
        raise IncorrectTypeError(
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
        sign, digits, exponent = value.as_tuple()
        scale = max(0, -int(exponent))
        precision = max(len(digits), scale + 1)
        if precision > 18:
            return Vector(_draken_native.vector_decimal128_from_constant(value, 1, precision, scale))
        return Vector(_draken_native.vector_decimal_from_constant(value, 1, precision, scale))
    if physical_type == <int>DRAKEN_INTERVAL:
        # INTERVAL literals are (months, microseconds) tuples (see
        # logical_planner_builders.literal_interval). Materialise a constant-shape
        # INTERVAL Vector so the temporal arithmetic kernels receive a real vector
        # rather than a raw Python tuple.
        return Vector(_draken_native.vector_interval_from_constant(value, 1))
    if isinstance(value, _datetime.date) and not isinstance(value, _datetime.datetime):
        ordinal = (value - _EPOCH_DATE).days
        int_vec = _draken_native.vector_from_constant(ordinal, 1)
        return Vector(_draken_native.vector_reinterpret_as_date32(int_vec))
    if isinstance(value, _datetime.datetime):
        return Vector(_draken_native.vector_timestamp_from_constant(value, 1))
    if isinstance(value, _datetime.time):
        return Vector(_draken_native.vector_time64_from_constant(value, 1))
    raise IncorrectTypeError(
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
        if self.ctx_ptr != 0:
            from draken.ops.kernels._kernel_registry import free_context
            free_context(self.ctx_ptr)

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
cdef tuple _STRING_FAMILY = ()

# draken_date_part bind-time part ids (kernel contract — function_kernels.cpp).
cdef dict _EXTRACT_PARTS = {"YEAR": 1, "MONTH": 2, "DAY": 3, "QUARTER": 4,
                            "HOUR": 5, "MINUTE": 6, "SECOND": 7}

# draken_like bind-time modes (bit0 = negate, bit1 = case-insensitive).
cdef dict _LIKE_MODES = {"Like": 0, "NotLike": 1, "ILike": 2, "NotILike": 3}

# draken_contains modes — the optimizer's `LIKE '%x%'` → InStr rewrite family.
cdef dict _CONTAINS_MODES = {"InStr": 0, "NotInStr": 1, "IInStr": 2, "NotIInStr": 3}

# draken_date_trunc unit ids (kernel contract — function_kernels.cpp FK_TR_*).
cdef dict _TRUNC_PARTS = {"SECOND": 1, "MINUTE": 2, "HOUR": 3, "DAY": 4,
                          "WEEK": 5, "MONTH": 6, "QUARTER": 7, "YEAR": 8}


def _build_in_list_blob(values, left_type, int negate):
    """Serialize a literal IN-list into the in_list_ctx blob (kernel contract —
    kernel_context.h). Returns None when the list shape is outside the kernel's
    contract (NULL entries, mixed/unsupported types) — caller falls through."""
    import struct as _struct

    if isinstance(values, (list, tuple, set, frozenset)):
        vals = list(values)
    else:
        return None   # CarcharSet / PerfectHashSet wrappers stay on the Python path
    if len(vals) == 0 or any(v is None for v in vals):
        return None
    phys = ""
    if left_type is not None and left_type.physical is not None:
        phys = getattr(left_type.physical, "name", "")
    if phys in ("INT8", "INT16", "INT32", "INT64") and all(
            isinstance(v, int) and not isinstance(v, bool) for v in vals):
        items = sorted(set(int(v) for v in vals))
        blob = _struct.pack("<IBBH", len(items), 0, negate, 0)
        return blob + b"".join(_struct.pack("<q", v) for v in items)
    if phys in ("VARCHAR", "NVARCHAR") and all(
            isinstance(v, (str, bytes)) for v in vals):
        entries = sorted(set(
            v.encode("utf-8") if isinstance(v, str) else bytes(v) for v in vals))
        blob = _struct.pack("<IBBH", len(entries), 1, negate, 0)
        for e in entries:
            blob += _struct.pack("<I", len(e)) + e
        return blob
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

    result_phys (the bound result physical type) guards the same-kind decimal case:
    the dec_*/dec128_* kernels output the SAME kind as their operands, so if the
    binder promotes the result to a different kind (e.g. DECIMAL × DECIMAL whose
    precision exceeds 18 → DECIMAL128), the kernel's physical output disagrees with
    the bound type and a downstream op reads the wrong width. Stay on the closure
    for those. When result_phys is None (introspection) the guard is skipped."""
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
    # decimal×float and decimal×INT64 are now c-native — incl. promotion to DECIMAL128
    # (draken_binop widens int64 operands to int128). Only narrower ints (INT8/16/32)
    # stay on the closure. Results carry their precision/scale descriptor across the
    # c-native wrap (binder stamps it via ctx; executor reattaches it). MUST precede
    # the numeric range check (decimal operands are not l_num/r_num → would short-circuit).
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
        # DECIMAL(int64) × INT64 → DECIMAL or DECIMAL128 (S-A.3). The INT64 side is a
        # scale-0 decimal; draken_binop runs dec_* (int64-tier result, ≤18) or widens
        # both operands to int128 and runs dec128_* (result DECIMAL128, >18). INT64 only
        # (dec_* read int64 stride); narrower ints (INT8/16/32) stay on the closure,
        # which widens them itself.
        if ((left_phys == "DECIMAL" and right_phys == "INT64") or
                (left_phys == "INT64" and right_phys == "DECIMAL")) and \
                (result_phys is None or result_phys == "DECIMAL"
                 or result_phys == "DECIMAL128"):
            return True
        # DECIMAL128 promotion (S-A.3 completion): DECIMAL128 × INT64 (either order) and
        # cross-kind DECIMAL × DECIMAL128 (either order). draken_binop widens the
        # int64-backed operand to int128 and runs dec128_*; the result is always
        # DECIMAL128 (the rc-5 wrap reattaches precision/scale). INT64 only — narrow ints
        # (INT8/16/32) stay on the closure, which widens them itself.
        l128 = left_phys == "DECIMAL128"
        r128 = right_phys == "DECIMAL128"
        l_i64dec = left_phys == "DECIMAL" or left_phys == "INT64"
        r_i64dec = right_phys == "DECIMAL" or right_phys == "INT64"
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
    # IP-in-CIDR: BitwiseOr over string operands (left = IP column, right = CIDR
    # scalar) → BOOL. Distinct from integer bitwise-OR; the kernel reads both
    # operands as strings. Mixed string/non-string stays on the closure.
    if op_code == BOP_BITWISE_OR and left_phys in _BINOP_NATIVE_STRING \
            and right_phys in _BINOP_NATIVE_STRING:
        return True
    # Bitwise OR/AND/XOR/SHIFT over SAME-type integers (int_bitwise requires it;
    # mismatch would return a loud error sentinel, so require equality up front).
    if BOP_BITWISE_OR <= op_code <= BOP_SHIFT_RIGHT:
        return l_int and r_int and left_phys == right_phys
    # String concat over SAME-type string columns (VARCHAR/NVARCHAR/VARBINARY).
    # Mixed/non-string operands stay on the closure (which coerces) — the kernel
    # only sees string||string of one type, result type = that type.
    if op_code == BOP_STRING_CONCAT:
        return left_phys in _BINOP_NATIVE_STRING and left_phys == right_phys
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
    if _LogicalCategory_DATE is None:
        from opteryx.types.logical_type import LogicalCategory
        _LogicalCategory_DATE = LogicalCategory.DATE
        _LogicalCategory_TIMESTAMP = LogicalCategory.TIMESTAMP
        _LogicalCategory_BOOLEAN = LogicalCategory.BOOLEAN
        _LogicalCategory_VARCHAR = LogicalCategory.VARCHAR
        _LogicalCategory_ARRAY = LogicalCategory.ARRAY
        _LogicalCategory_BLOB = LogicalCategory.VARBINARY
        # Types valid as the LEFT operand of an extraction operator (-> ->> [i]).
        # Includes VARIANT so JSON access chains (a -> b ->> c) with no user cast.
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
            elif node.physical_type == <int>DRAKEN_INTERVAL:
                # INTERVAL literal — its value is a (months, microseconds) tuple,
                # but it is a genuine scalar, not an in-list collection. Materialise
                # a constant INTERVAL Vector (must precede the tuple/in-list branch
                # below, which would otherwise mis-handle it as a membership set).
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

        sub_depth = _linearize(node.left, bc, depth)
        sub_depth = _linearize(node.right, bc, sub_depth)

        slot = bc._push_instr()
        slot.opcode = BC_BINARY_OP
        slot.op_code = <int>_BOP_CODE.get(bin_op_str, BOP_UNKNOWN)
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
    # _PASSTHRU is transparent: just compile the single parameter.
    # ------------------------------------------------------------------
    if nt == _NT_FUNCTION:
        func_val = <object>node.value
        if func_val == "_PASSTHRU":
            if node.parameters.size() == 0:
                raise ValueError("compiled_expression: _PASSTHRU FUNCTION has no parameters")
            return _linearize(node.parameters[0], bc, depth)

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
                if n == 3 and node.parameters[2] != NULL \
                        and node.parameters[2].node_type == _NT_LITERAL:
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

        # Phase 9b: Resolve C kernel function pointer for function calls.
        # Function kernels (Phase 9a-fn) are under development; resolution is optional.
        # If a C kernel exists, use it; otherwise, fall back to Python callable_ref.
        # This is the intended behaviour while function kernels are being ported.
        func_name = func_val.upper() if func_val else None
        if func_name is not None:
            # ROUND-family over a DECIMAL operand: the kernel rounds EXACTLY in the
            # raw int64 domain, which needs the operand's scale — a LogicalType
            # detail the DrakenVector cannot carry — passed via a bind-time ctx
            # (binary_op_ctx.left_scale, the same vehicle the decimal binops use).
            _fn_ctx_alloc = None
            _fn_ctx_arg = None
            if func_name in ("ROUND", "FLOOR", "CEILING", "CEIL") and n >= 1 \
                    and node.parameters[0] != NULL \
                    and node.parameters[0].schema_column != NULL:
                _fn_p0_sc = <object>node.parameters[0].schema_column
                _fn_p0_ct = getattr(_fn_p0_sc, "column_type", None)
                if (_fn_p0_ct is not None and _fn_p0_ct.logical is not None
                        and getattr(_fn_p0_ct.physical, "name", "") == "DECIMAL"):
                    from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx
                    _fn_ctx_alloc = alloc_binary_op_ctx
                    _fn_ctx_arg = (0, int(_fn_p0_ct.logical.scale), 0, 0, 0, 0, 0)
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
        source_phys_name = None
        if node.left.schema_column != NULL:
            src_sc = <object>node.left.schema_column
            if src_sc is not None:
                source_sql = src_sc.column_type
                if source_sql is not None:
                    source_phys_name = getattr(source_sql.physical, "name", None)

        from opteryx.expression.casts import resolve_cast
        try:
            cast_kernel, _cast_needs_nb_input, _cast_returns_raw = resolve_cast(
                source_phys_name, cast_target_type, cast_params, unit=cast_unit, safe=cast_is_try
            )
        except (NotImplementedError, ValueError) as e:
            raise ValueError(f"Unsupported CAST: {source_phys_name} → {cast_target_type}: {e}")

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
        _cn = _c_native_cast(source_phys_name, cast_target_type, safe=cast_is_try)
        if _cn is not None:
            # TIMESTAMP-producing casts need the target unit in a cast_timestamp_ctx
            # (1 ns / 2 us / 3 ms / 4 s / 5 days); default us when the cast left it
            # implicit. Everything else is unit-less (None ctx).
            _cn_alloc = None
            _cn_arg = None
            if _cn[0].endswith("_to_timestamp"):
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
            elif _cn[0].endswith("_to_decimal"):
                # DECIMAL(p, s): params are the cast's (precision, scale) literals.
                if len(cast_params) != 2:
                    raise ValueError("CAST to DECIMAL requires (precision, scale)")
                from draken.ops.kernels._kernel_registry import alloc_binary_op_ctx as _cn_alloc
                _cn_arg = (0, 0, 0, int(cast_params[1]), int(cast_params[0]), 0, 0)
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(_cn[0], _cn_alloc, _cn_arg)
            if fn_ptr is not None:
                slot.kernel_fn = <void*>(<unsigned long long>fn_ptr)
                slot.flags |= BC_INSTR_C_NATIVE
                if ctx_wrapper is not None:
                    bc._hold(ctx_wrapper)
                    slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
                # S2: string-result cast kernels are named `..._to_string`;
                # their canonical-block results fold nogil too (S6). TIMESTAMP
                # results are DESC (unit re-attached at the ExprProject boundary);
                # everything else is plain fixed-width.
                if _cn[0].endswith("_to_string"):
                    slot.flags |= BC_C_NATIVE_STRING
                elif (_cn[0].endswith("_to_timestamp") or _cn[0].endswith("_to_decimal")
                        or _cn[0] == "draken_cast_timestamp_rescale"):
                    slot.flags |= BC_C_NATIVE_DESC
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

        # Phase 9b: Resolve C kernel function pointer for extraction operations.
        _extr_kernel_names = {
            BC_EXTR_MAP_STRING: "draken_map_access_string",
            BC_EXTR_MAP_ARRAY: "draken_array_map_access",
            BC_EXTR_JSON_PTR: "draken_json_extract",
            BC_EXTR_JSON_KEY: "draken_json_extract",
        }
        if sub_op in _extr_kernel_names:
            from draken.ops.kernels._kernel_registry import alloc_extraction_ctx
            context_allocator = alloc_extraction_ctx
            fn_ptr, ctx_wrapper = _resolve_kernel_and_context(
                _extr_kernel_names[sub_op],
                context_allocator,
                sub_op
            )
            if fn_ptr is None:
                raise ValueError(
                    f"Extraction kernel '{_extr_kernel_names[sub_op]}' not found in registry. "
                    f"This is a supported extraction operation but kernel is missing."
                )

            slot.kernel_fn = <void*>fn_ptr
            if ctx_wrapper is not None:
                bc._hold(ctx_wrapper)
                slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
            slot.flags |= BC_INSTR_C_NATIVE

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

        case_callable = build_case_fn(cond_bcs, result_bcs, else_bc, kernel_type)

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
                              BC_FUNCTION})

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
    # result binop/cast (need a Vector owner), and FUNCTION/EXTRACTION/UNARY/
    # CASE/LIT_SET/LIT_SCALAR (GIL).
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
            # IS [NOT] NULL is a pure validity read — nogil DV* arm exists
            # (evaluation.pyx _dv_unary_null_c). Other unaries stay GIL.
            opc = bc.instrs[k].op_code
            if opc == UOP_IS_NULL or opc == UOP_IS_NOT_NULL:
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
        bc.is_all_c_native = False
        break
    if bc.is_all_c_native and bc.instrs[bc.count - 1].opcode not in _C_NATIVE_COMPUTE_OPCODES:
        bc.is_all_c_native = False

    return bc
