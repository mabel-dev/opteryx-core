"""Lazy CASE WHEN evaluator.

Three-phase model:

    Decide   — walk conditions in order, assigning each row to the first
               branch whose predicate is true (or -1 for unmatched).
    Compute  — evaluate each branch's result expression on only the rows
               that landed in that branch (and ELSE on the rest).
    Assemble — scatter the branch outputs into the final vector via the
               native Cython kernels in opteryx/compiled/vector_ops/case_helpers.

Wired into production via NT_CASE dispatch in
opteryx.expression.evaluator.evaluation (_eval_value and
evaluate_and_append_draken).
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

from draken.morsels.morsel import Morsel
from draken.vectors.bool_vector import BoolVector
import draken.draken_native as _draken_native


# Compile-time NodeType.LITERAL value — kept in sync with the runtime enum by
# the verification check in opteryx.expression.evaluator.__init__.
DEF NT_LITERAL = 42


# ---------------------------------------------------------------------------
# Sub-morsel construction
# ---------------------------------------------------------------------------


cdef _sub_morsel(morsel, indices):
    """Return a new Morsel containing only the rows at `indices`.

    Uses per-vector .take() so no full morsel copy is made.
    `indices` must be an int32_t-typed buffer (e.g. array('i')).
    """
    cdef list names = list(morsel.column_names)
    cdef list vecs = []
    for n in names:
        key = n if isinstance(n, bytes) else n.encode()
        vecs.append(morsel.column(key).take(indices))
    return Morsel.from_vectors(names, vecs)


# ---------------------------------------------------------------------------
# Phase 1 — Decide
# ---------------------------------------------------------------------------


cdef _decide(node, morsel):
    """Evaluate conditions lazily; assign branch_id[r] for each row.

    Returns (branch_id, rows_per_branch, unmatched, pos_in_branch).
    """
    # Lazy because evaluation.pyx imports this module's evaluate_case; the
    # import cycle is broken by deferring to first call. By the time _decide
    # is reached the evaluation module has finished initialising.
    from opteryx.expression.evaluator.evaluation import evaluate_draken

    cdef Py_ssize_t n = morsel.num_rows
    cdef Py_ssize_t i
    cdef Py_ssize_t num_conditions = len(node.conditions)
    cdef int cond_node_type

    branch_id = _make_const_int16(n, -1)   # int16 array, all -1
    live = _make_range_int32(n)            # int32 array [0..n-1]

    for i in range(num_conditions):
        if len(live) == 0:
            break

        cond_node = node.conditions[i]
        cond_node_type = <int>cond_node.node_type

        # Constant-condition shortcuts — mirror _case_collect_conditions.
        if cond_node_type == NT_LITERAL:
            if cond_node.value is True:
                # All live rows go to branch i; later branches are unreachable.
                const_true = BoolVector.from_constant(True, len(live))
                live = decide_one_branch(const_true, live, branch_id, i)
                break
            # False or None: branch is dead — skip.
            continue

        sub = _sub_morsel(morsel, live)
        c = evaluate_draken(cond_node, sub)  # BoolVector
        live = decide_one_branch(c, live, branch_id, i)

    rows_per_branch, unmatched, pos_in_branch = group_indices_and_perm(
        branch_id, num_conditions
    )
    return branch_id, rows_per_branch, unmatched, pos_in_branch


# ---------------------------------------------------------------------------
# Phase 2 — Compute
# ---------------------------------------------------------------------------


cdef _compute(node, morsel, rows_per_branch, unmatched):
    """Evaluate result expressions only on rows that matched each branch.

    Returns (parts, else_part):
        parts[i]   Draken vector for branch i results, or None if branch had
                   zero matching rows.
        else_part  Draken vector for ELSE results (len = unmatched rows), or
                   None if no ELSE or no unmatched rows.
    """
    from opteryx.expression.evaluator.evaluation import _eval_value

    cdef list parts = []
    cdef Py_ssize_t i
    cdef Py_ssize_t num_results = len(node.results)

    for i in range(num_results):
        rows_i = rows_per_branch[i]
        if len(rows_i) == 0:
            parts.append(None)
            continue
        sub = _sub_morsel(morsel, rows_i)
        parts.append(_eval_value(node.results[i], sub))

    else_part = None
    if node.else_result is not None and len(unmatched) > 0:
        sub_else = _sub_morsel(morsel, unmatched)
        else_part = _eval_value(node.else_result, sub_else)

    return parts, else_part


# ---------------------------------------------------------------------------
# Phase 3 — Assemble dispatch
# ---------------------------------------------------------------------------


cdef _assemble(
    node,
    list parts,
    else_part,
    branch_id,
    rows_per_branch,
    unmatched,
    pos_in_branch,
    Py_ssize_t n,
):
    """Dispatch to the appropriate assembly helper based on output type."""

    # First non-None part determines the output family. Use isinstance rather
    # than get_vector_type: constant-encoded vectors are still concrete vector
    # instances (BoolVector / StringVector / Integer64Vector / …) even when
    # encoding == _ENC_CONSTANT.
    first = None
    for p in parts:
        if p is not None:
            first = p
            break
    if first is None:
        first = else_part
    if first is None:
        return _draken_native.vector_null_from_length(n)

    if isinstance(first, BoolVector):
        return assemble_bool(parts, else_part, branch_id, rows_per_branch, unmatched)

    first_type = getattr(first, "type", None)
    if first_type in (_draken_native.VARCHAR, _draken_native.NVARCHAR):
        return assemble_flat_string(parts, else_part, branch_id, pos_in_branch, n)

    # Fixed-width (numeric, date, timestamp, …)
    return assemble_fixed(parts, else_part, branch_id, rows_per_branch, unmatched)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def evaluate_case(node, morsel):
    """Evaluate a CASE WHEN node lazily against `morsel`.

    `node` must have:
        .conditions   list[Node]
        .results      list[Node]
        .else_result  Node | None
    """
    if not node.conditions:
        raise ValueError("evaluate_case: node.conditions must be non-empty")

    cdef Py_ssize_t n = morsel.num_rows

    branch_id, rows_per_branch, unmatched, pos_in_branch = _decide(node, morsel)
    parts, else_part = _compute(node, morsel, rows_per_branch, unmatched)
    return _assemble(
        node, parts, else_part,
        branch_id, rows_per_branch, unmatched, pos_in_branch, n,
    )
