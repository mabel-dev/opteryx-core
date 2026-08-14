"""CASE WHEN evaluator — compiled bytecode path only.

Three-phase model:

    Decide   — walk conditions in order, assigning each row to the first
               branch whose predicate is true (or -1 for unmatched).
    Compute  — evaluate each branch's result expression on only the rows
               that landed in that branch (and ELSE on the rest).
    Assemble — scatter the branch outputs into the final vector via the
               native Cython kernels in opteryx/compiled/vector_ops/case_helpers.

Entry point:
    build_case_fn(cond_bcs, result_bcs, else_bc) — bind-time factory; returns a
                                                    closure that calls execute_bytecode
                                                    per branch.
"""

from opteryx.compiled.vector_ops import (
    assemble_bool,
    assemble_fixed,
    assemble_flat_string,
    decide_one_branch,
    group_indices_and_perm,
)
from opteryx.compiled.vector_ops.vector_ops import _make_const_int16, _make_range_int32

# Note: NodeType is intentionally not imported here. The only use case in
# this file is the LITERAL test, which is compared against the DEF
# NT_LITERAL integer constant below. Importing NodeType at module level
# creates a circular import when this leaf is included into the evaluator
# package __init__ (which is itself imported during expression/__init__).

from draken.morsels.morsel cimport Morsel
from draken.vectors.bool_vector import BoolVector
import draken.draken_native as _draken_native


# Compile-time NodeType.LITERAL value — kept in sync with the runtime enum by
# the verification check in opteryx.expression.evaluator.__init__.
DEF NT_LITERAL = 42


# ---------------------------------------------------------------------------
# Sub-morsel construction
# ---------------------------------------------------------------------------


cdef inline Morsel _sub_morsel(Morsel morsel, indices):
    """Return a new Morsel containing only the rows at `indices`.

    Delegates to Morsel.take() for zero-overhead row filtering.
    `indices` must be an int32_t-typed buffer (e.g. array('i')).
    """
    return morsel.take(indices)


# ---------------------------------------------------------------------------
# Phase 2 — Compute (compiled)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Phase 3 — Assemble dispatch (compiled)
# ---------------------------------------------------------------------------


# Assembly kernel type constants for bind-time dispatch
DEF _ASSEMBLE_BOOL = 0
DEF _ASSEMBLE_FIXED = 1
DEF _ASSEMBLE_STRING = 2


# ---------------------------------------------------------------------------
# Compiled path — conditions and results are pre-compiled CompiledBytecode
# objects; execute_bytecode is called instead of evaluate_draken/_eval_value.
# ---------------------------------------------------------------------------


cdef _decide_compiled(list cond_bcs, morsel):
    """Compiled-bytecode version: walk conditions and assign branch_id for each row.

    cond_bcs: list of CompiledBytecode, one per WHEN condition.
    Returns (branch_id, rows_per_branch, unmatched, pos_in_branch).
    """
    from opteryx.expression.evaluator.evaluation import execute_bytecode

    cdef Py_ssize_t n = morsel.num_rows
    cdef Py_ssize_t i
    cdef Py_ssize_t num_conditions = len(cond_bcs)
    cdef object _null_type = _draken_native.DrakenType.NULL

    branch_id = _make_const_int16(n, -1)
    live = _make_range_int32(n)

    for i in range(num_conditions):
        if len(live) == 0:
            break
        sub = _sub_morsel(morsel, live)
        c = execute_bytecode(cond_bcs[i], sub)   # BoolVector for every typed condition
        # The condition-side counterpart of the THEN-side normalisation in
        # _compute_compiled below. `decide_one_branch` is declared `BoolVector bv`
        # and reads the bit-packed layout through it, so anything else arrived as
        # the raw Cython `TypeError: Argument 'bv' has incorrect type` — which is
        # what `CASE WHEN NULL`, `CASE WHEN 1` and `CASE WHEN 'x'` all raised, while
        # the same conditions written over a COLUMN are refused with a sentence that
        # names the type ("expected BOOLEAN, got INTEGER").
        if not isinstance(c, BoolVector):
            if getattr(c, "type", None) is _null_type:
                # An untyped NULL condition is UNKNOWN for every row, and UNKNOWN
                # never matches: this WHEN wins nothing and every row stays live for
                # the next one (or for ELSE). Same answer the typed BOOL null gives
                # through decide_one_branch, which is the point — `CASE WHEN NULL`
                # and `CASE WHEN CAST(NULL AS BOOLEAN)` are the same question.
                continue
            from opteryx.exceptions import IncorrectTypeError

            _seen = getattr(c, "type", None)
            raise IncorrectTypeError(
                f"**CASE WHEN** requires a BOOLEAN condition, not "
                f"`{getattr(_seen, 'name', _seen)}`."
            )
        live = decide_one_branch(c, live, branch_id, i)

    rows_per_branch, unmatched, pos_in_branch = group_indices_and_perm(
        branch_id, num_conditions
    )
    return branch_id, rows_per_branch, unmatched, pos_in_branch


cdef _compute_compiled(list result_bcs, else_bc, morsel, rows_per_branch, unmatched):
    """Compiled-bytecode version: evaluate result expressions on matched rows only.

    result_bcs: list of CompiledBytecode, one per THEN result.
    else_bc:    CompiledBytecode for ELSE, or None.
    """
    from opteryx.expression.evaluator.evaluation import execute_bytecode

    cdef list parts = []
    cdef Py_ssize_t i
    cdef Py_ssize_t num_results = len(result_bcs)
    cdef object _null_type = _draken_native.DrakenType.NULL

    for i in range(num_results):
        rows_i = rows_per_branch[i]
        if len(rows_i) == 0:
            parts.append(None)
            continue
        sub = _sub_morsel(morsel, rows_i)
        part = execute_bytecode(result_bcs[i], sub)
        # A bare `THEN NULL` yields an untyped DRAKEN_NULL vector (all rows null,
        # data == NULL). The typed assemble kernels (bool/fixed) C-cast the part to
        # the output type and would mis-read it — a heap-corrupting type confusion.
        # Normalise it to Python None: every assemble kernel already skips None
        # parts, leaving those rows null, which is the correct CASE semantics.
        if part is not None and getattr(part, "type", None) is _null_type:
            part = None
        parts.append(part)

    else_part = None
    if else_bc is not None and len(unmatched) > 0:
        sub_else = _sub_morsel(morsel, unmatched)
        else_part = execute_bytecode(else_bc, sub_else)
        if else_part is not None and getattr(else_part, "type", None) is _null_type:
            else_part = None

    return parts, else_part


def build_case_fn(list cond_bcs, list result_bcs, else_bc, int kernel_type,
                  int dec_precision=-1, int dec_scale=-1):
    """Bind-time factory: compile CASE WHEN into a morsel→vector callable.

    Returns a Python closure suitable for storage in BC_CASE slot.callable_ref.
    At execution time the closure is called as fn(morsel) and returns the
    assembled result vector.

    cond_bcs:       list of CompiledBytecode, one per WHEN condition (len >= 1).
    result_bcs:     list of CompiledBytecode, one per THEN result (same length).
    else_bc:        CompiledBytecode for ELSE, or None.
    kernel_type:    Pre-resolved kernel type (_ASSEMBLE_BOOL, _ASSEMBLE_FIXED,
                    or _ASSEMBLE_STRING). -1 indicates runtime type dispatch.
    dec_precision/dec_scale: the CASE's declared DECIMAL descriptor (-1 when the
                    result is not DECIMAL, or its type is unresolved). Branch
                    parts come straight off the expression VM, which does not
                    carry the scale on the vector — bind time is the only place
                    that knows it. See assemble_fixed.
    """
    if not cond_bcs:
        raise ValueError("build_case_fn: cond_bcs must be non-empty")

    # Capture kernel_type at bind time. It's captured in the closure as a Python int.
    kt = kernel_type
    dec_p = dec_precision
    dec_s = dec_scale

    def _case_fn(morsel):
        n = morsel.num_rows

        branch_id, rows_per_branch, unmatched, pos_in_branch = \
            _decide_compiled(cond_bcs, morsel)
        parts, else_part = \
            _compute_compiled(result_bcs, else_bc, morsel, rows_per_branch, unmatched)
        # Call the pre-resolved kernel directly. No runtime type dispatch.
        first = None
        for p in parts:
            if p is not None:
                first = p
                break
        if first is None:
            first = else_part
        if first is None:
            return _draken_native.vector_null_from_length(n)

        # Dispatch to the pre-resolved kernel, or fall back to runtime dispatch if kernel_type is -1.
        if kt == _ASSEMBLE_BOOL:
            return assemble_bool(parts, else_part, branch_id, rows_per_branch, unmatched)
        elif kt == _ASSEMBLE_STRING:
            return assemble_flat_string(parts, else_part, branch_id, pos_in_branch, n)
        elif kt == _ASSEMBLE_FIXED:
            return assemble_fixed(parts, else_part, branch_id, rows_per_branch, unmatched,
                                  dec_p, dec_s)
        else:
            # Runtime dispatch fallback (kt == -1, inferred_type was None)
            if isinstance(first, BoolVector):
                return assemble_bool(parts, else_part, branch_id, rows_per_branch, unmatched)
            first_type = getattr(first, "type", None)
            if first_type in (_draken_native.VARCHAR, _draken_native.NVARCHAR):
                return assemble_flat_string(parts, else_part, branch_id, pos_in_branch, n)
            return assemble_fixed(parts, else_part, branch_id, rows_per_branch, unmatched,
                                  dec_p, dec_s)

    return _case_fn
