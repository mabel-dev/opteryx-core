"""Lazy CASE WHEN evaluator.

PR 1 — pure addition.  Not yet dispatched in production (no NodeType.CASE,
no entry point from _eval_value).  Validated via direct unit tests.

Node contract (matches the forthcoming NodeType.CASE shape):
    node.conditions   : list[Node]     # length k >= 1
    node.results      : list[Node]     # length k, parallel to conditions
    node.else_result  : Node | None    # explicit; no LITERAL(True) injection

Phase 1 (Decide):   evaluate conditions lazily, assign branch_id per row.
Phase 2 (Compute):  evaluate result expressions only on rows that matched.
Phase 3 (Assemble): scatter/build the output vector.
"""

from opteryx.compiled.vector_ops import (
    assemble_bool,
    assemble_dict_string,
    assemble_fixed,
    assemble_flat_string,
    decide_one_branch,
    group_indices_and_perm,
)
from opteryx.compiled.vector_ops.vector_ops import _make_const_int16, _make_range_int32


# ---------------------------------------------------------------------------
# Sub-morsel construction
# ---------------------------------------------------------------------------

def _sub_morsel(morsel, indices):
    """Return a new Morsel containing only the rows at `indices`.

    Uses per-vector .take() so no full morsel copy is made.
    `indices` must be an int32_t-typed buffer (e.g. array('i')).
    """
    from draken.morsels.morsel import Morsel

    names = list(morsel.column_names)
    vecs = [
        morsel.column(n if isinstance(n, bytes) else n.encode()).take(indices)
        for n in names
    ]
    return Morsel.from_vectors(names, vecs)


# ---------------------------------------------------------------------------
# Phase 1 — Decide
# ---------------------------------------------------------------------------

def _decide(node, morsel):
    """Evaluate conditions lazily; assign branch_id[r] for each row.

    Returns (branch_id, rows_per_branch, unmatched, pos_in_branch).
    """
    from opteryx.expression import NodeType
    from opteryx.expression.evaluator.evaluation import evaluate_draken
    from draken.vectors.bool_vector import BoolVector

    n = morsel.num_rows
    branch_id = _make_const_int16(n, -1)   # int16 array, all -1
    live = _make_range_int32(n)             # int32 array [0..n-1]

    for i, cond_node in enumerate(node.conditions):
        if len(live) == 0:
            break

        # Constant-condition shortcuts — mirror _case_collect_conditions behaviour
        if cond_node.node_type == NodeType.LITERAL:
            if cond_node.value is True:
                # All live rows go to branch i; remaining branches unreachable
                const_true = BoolVector.from_constant(True, len(live))
                live = decide_one_branch(const_true, live, branch_id, i)
                # live is now empty
                break
            # False or None: branch is dead — skip
            continue

        sub = _sub_morsel(morsel, live)
        c = evaluate_draken(cond_node, sub)  # BoolVector
        live = decide_one_branch(c, live, branch_id, i)

    rows_per_branch, unmatched, pos_in_branch = group_indices_and_perm(
        branch_id, len(node.conditions)
    )
    return branch_id, rows_per_branch, unmatched, pos_in_branch


# ---------------------------------------------------------------------------
# Phase 2 — Compute
# ---------------------------------------------------------------------------

def _compute(node, morsel, rows_per_branch, unmatched):
    """Evaluate result expressions only on rows that matched each branch.

    Returns (parts, else_part):
        parts[i]   Draken vector for branch i results, or None if branch had
                   zero matching rows.
        else_part  Draken vector for ELSE results (len = unmatched rows), or
                   None if no ELSE or no unmatched rows.
    """
    from opteryx.expression.evaluator.evaluation import _eval_value

    parts = []
    for i, result_node in enumerate(node.results):
        rows_i = rows_per_branch[i]
        if len(rows_i) == 0:
            parts.append(None)
            continue
        sub = _sub_morsel(morsel, rows_i)
        parts.append(_eval_value(result_node, sub))

    else_part = None
    if node.else_result is not None and len(unmatched) > 0:
        sub_else = _sub_morsel(morsel, unmatched)
        else_part = _eval_value(node.else_result, sub_else)

    return parts, else_part


# ---------------------------------------------------------------------------
# Phase 3 — Assemble dispatch
# ---------------------------------------------------------------------------

def _is_dict_path(parts, else_part):
    """True iff every non-None part (and else_part) is a StringVector in either
    dict or constant encoding, AND at least one is dict-encoded.

    Constants are admissible because they collapse to a single dict entry in
    the unified output.  Pure constants alone don't justify the unified-dict
    overhead — the flat path is simpler.
    """
    from draken.vectors.string_vector import StringVector

    all_parts = [p for p in parts if p is not None]
    if else_part is not None:
        all_parts.append(else_part)
    if not all_parts:
        return False
    has_dict = False
    for p in all_parts:
        if not isinstance(p, StringVector):
            return False
        enc = p.encoding
        if enc == 1:           # DRAKEN_ENCODING_DICTIONARY
            has_dict = True
        elif enc == 3:         # DRAKEN_ENCODING_CONSTANT
            pass
        else:
            return False
    return has_dict


def _normalize_constants_for_dict_path(parts, else_part):
    """Convert constant-encoded StringVector parts to single-entry dict-encoded
    vectors so assemble_dict_string can process them uniformly.

    Null constants are materialized — the dict kernel reads the per-row null
    bitmap from the dict accessor, and a synthesized 1-entry dict with all-null
    codes is the simplest representation.
    """
    from array import array
    from draken.vectors.string_vector import StringVector

    def _to_dict(sv):
        if sv is None:
            return None
        if sv.encoding != 3:        # not constant — already dict
            return sv
        n = len(sv)
        values = sv.to_pylist()
        val = values[0] if values else None
        # Materialize constants that can't form a valid dict entry
        if val is None or val == b'' or val == '':
            return sv.materialize()
        codes = array("i", [0] * n)
        return StringVector.from_dict(codes, [val])

    new_parts = [_to_dict(p) for p in parts]
    new_else = _to_dict(else_part)
    return new_parts, new_else


def _assemble(node, parts, else_part, branch_id, rows_per_branch, unmatched, pos_in_branch, n):
    """Dispatch to the appropriate assembly helper based on output type."""
    from draken.vectors.bool_vector import BoolVector
    from draken.vectors.string_vector import StringVector

    # Find the first non-None part to determine the output family.
    # Use isinstance rather than get_vector_type: constant-encoded vectors are
    # still BoolVector/StringVector/Int64Vector instances even when encoding==3.
    first = next((p for p in parts if p is not None), None)
    if first is None:
        first = else_part
    if first is None:
        from draken.vectors.null_vector import NullVector
        return NullVector(n)

    if isinstance(first, BoolVector):
        return assemble_bool(parts, else_part, branch_id, rows_per_branch, unmatched)

    if isinstance(first, StringVector):
        if _is_dict_path(parts, else_part):
            d_parts, d_else = _normalize_constants_for_dict_path(parts, else_part)
            # Normalization may have materialized some parts (e.g. empty string
            # constants) to flat encoding (enc=0).  assemble_dict_string requires
            # all inputs to be dict-encoded (enc=1); fall back to flat if not.
            all_dict = all(
                p is None or p.encoding == 1 for p in d_parts
            ) and (d_else is None or d_else.encoding == 1)
            if all_dict:
                return assemble_dict_string(
                    d_parts, d_else, branch_id, pos_in_branch, n
                )
        return assemble_flat_string(
            parts, else_part, branch_id, pos_in_branch, n
        )

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

    n = morsel.num_rows

    branch_id, rows_per_branch, unmatched, pos_in_branch = _decide(node, morsel)
    parts, else_part = _compute(node, morsel, rows_per_branch, unmatched)
    return _assemble(
        node, parts, else_part,
        branch_id, rows_per_branch, unmatched, pos_in_branch, n,
    )
