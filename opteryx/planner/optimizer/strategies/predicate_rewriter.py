# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate rewriter

Type: Heuristic
Goal: Chose more efficient predicate evaluations

We rewrite conditions to a more optimal form based on two objectives:
1) the execution of the condition is faster
2) the condition is more likely to be able to be pushed to the storage layer (where its faster)

Rewrites Implemented:

x IN (single_value)                         → x = single_value
x NOT IN (single_value)                     → x != single_value
x LIKE 'pattern'                            → x = 'pattern' (when no wildcards)
x NOT LIKE 'pattern'                        → x != 'pattern' (when no wildcards)
x LIKE '%pattern%'                          → x INSTR 'pattern' (for contains without underscores)
x NOT LIKE '%pattern%'                      → x NOT INSTR 'pattern' (for contains without underscores)
x ILIKE '%pattern%'                         → x IINSTR 'pattern' (case-insensitive version)
x NOT ILIKE '%pattern%'                     → x NOT IINSTR 'pattern' (case-insensitive version)
x LIKE '%%%pattern%%'                       → x LIKE '%pattern%' (removing adjacent wildcards)
x ANY_OP = value                            → x IN (value) (when right side is a literal)
end - start > interval                      → start + interval < end (for date comparisons)
CASE WHEN x IS NULL THEN y ELSE x END       → IFNULL(x, y)
CASE WHEN x THEN y ELSE z END               → IIF(x, y, z)
COALESCE(x, y)                              → IFNULL(x, y) (when only two parameters)
SUBSTRING(x, 1, n)                          → LEFT(x, n) (when starting at position 1)
x LIKE 'pattern1%' OR x LIKE '%pattern2'    → x REGEX '^pattern1.*|.*pattern2$' (for ORed LIKE conditions)
expr = v1 OR expr = v2 OR ... OR expr = vN  → expr IN (v1, ..., vN) (CNF branches with same LHS)
CONCAT(x, y, z)                             → x || y || z (CONCAT to operators)
CONCAT_WS(x, y, z)                          → y || x || z (CONCAT_WS to operators)
x = 'a' OR x = 'b' OR x = 'c'               → x IN ('a', 'b', 'c') (for ORed Equals conditions)
a = ANY(z) OR b = ANY(z) OR c = ANY(z)      → (a, b, c) @> z
addr <<= '10.0.0.0/8'                       → addr BETWEEN base AND broadcast (so it prunes)
addr <<= '1.2.3.4/32'                       → addr = 16909060 (a /32 is one host)

#### IN THE PREDICATE ORDERING STRATEGY
a = ANY(z) AND b = ANY(z) AND c = ANY(z)    → z @>> (a, b, c)
"""

import datetime
import math
import re
from typing import Callable, Dict

from draken.draken_native import DrakenType as _DrakenType

from opteryx.exceptions import NotSupportedError
from opteryx.expression import ExpressionColumn, NodeType, format_expression
from opteryx.models import Node, QueryTelemetry
from opteryx.planner import build_literal_node
from opteryx.planner.binder.operator_map import determine_type, _STRING_CATEGORIES
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory, ColumnType
from opteryx.types import logical_type as _lt
from opteryx.types.schema import ConstantColumn
from opteryx.utils.dates import add_single_unit, parse_iso, truncate_single

from .optimization_strategy import OptimizationStrategy, OptimizerContext

# fmt: off
IN_REWRITES = {"InList": "Eq", "NotInList": "NotEq"}
LIKE_REWRITES = {"Like": "Eq", "NotLike": "NotEq"}
LITERALS_TO_THE_RIGHT = {"Plus": "Minus", "Minus": "Plus"}
INSTR_REWRITES = {"Like": "InStr", "NotLike": "NotInStr", "ILike": "IInStr", "NotILike": "NotIInStr"}
# fmt: on


def rewrite_in_to_eq(predicate):
    """
    Rewrite IN conditions with a single value to equality conditions.

    If the IN condition contains only one value, it is equivalent to an equality check.
    This optimization replaces the IN condition with a faster equality check.
    """
    predicate.value = IN_REWRITES[predicate.value]
    predicate.right.value = tuple(predicate.right.value)[0]
    # Phase 2: element is embedded in ARRAY ColumnType; sidecar element_type is gone.
    _arr_ct = predicate.right.type
    if isinstance(_arr_ct, ColumnType) and _arr_ct.element is not None:
        predicate.right.type = _arr_ct.element
    else:
        predicate.right.type = _lt.VARCHAR
    # schema_column is the single source of truth downstream (e.g. the bind-time
    # temporal-cast validator in compiled_expression.pyx reads
    # schema_column.column_type, not .type). Left describing the ORIGINAL ARRAY
    # literal, `col IN (cast(lit AS DATE))` folded to `col = cast(lit AS DATE)`
    # here but the stale ARRAY-typed schema_column still read as a DATE-vs-ARRAY
    # mismatch one stage later.
    _right_sc = predicate.right.schema_column
    if _right_sc is not None:
        _right_sc.column_type = predicate.right.type
        _right_sc.value = predicate.right.value
    return predicate


def reorder_interval_calc(predicate):
    """
    rewrite:
        end - start > interval => start + interval > end

    This is because comparing a Date with a Date is faster than
    comparing in Interval with an Interval.
    """
    date_start = predicate.left.right
    date_end = predicate.left.left
    interval = predicate.right

    # Check if the operation is date - date
    if predicate.left.value == "Minus":
        # Create a new binary operator node for date + interval
        new_binary_op = Node(
            node_type=NodeType.BINARY_OPERATOR,
            value="Plus",
            left=date_start,
            right=interval,
        )
        binary_op_column_name = format_expression(new_binary_op, True)
        new_binary_op.schema_column = ExpressionColumn(
            name=binary_op_column_name, column_type=_lt.TIMESTAMP()
        )

        # Create a new comparison operator node for date > date
        predicate.node_type = NodeType.COMPARISON_OPERATOR
        predicate.right = new_binary_op
        predicate.left = date_end

        predicate_column_name = format_expression(predicate, True)
        predicate.schema_column = ExpressionColumn(
            name=predicate_column_name, column_type=_lt.BOOLEAN
        )

        return predicate


def _rewrite_rlike_to_dfa(predicate, telemetry):
    """RLike/NotRLike with a literal pattern: compile the pattern into a byte
    DFA at plan time (RE2's parser only — see vector_dfa_compile.pyx's module
    docstring) and replace the pattern operand with the compiled blob.

    A non-literal pattern, or a literal pattern outside the compiler's
    supported scope (non-ASCII content, case-fold, nested anchors, or a
    state-count blowup), raises NotSupportedError here rather than reaching
    vector_rlike at runtime with a pattern it can no longer interpret —
    vector_rlike expects a pre-compiled blob unconditionally now that RE2's
    matcher has been removed from it, so there is no runtime fallback to
    silently degrade to.
    """
    if predicate.value not in ("RLike", "NotRLike"):
        return predicate

    if predicate.right.node_type != NodeType.LITERAL:
        raise NotSupportedError(
            "**RLIKE**/REGEXP_LIKE requires a constant pattern — "
            f"got a non-literal expression for {predicate.value}."
        )

    pattern_value = predicate.right.value
    if isinstance(pattern_value, str):
        pattern_value = pattern_value.encode("utf8")
    elif not isinstance(pattern_value, bytes):
        raise NotSupportedError(
            f"**RLIKE**/REGEXP_LIKE pattern must be a string constant, got {type(pattern_value)!r}."
        )

    from opteryx.compiled import vector_ops as compiled_vector_ops

    # Prefer the SIMD op-program (blob version 2) when the pattern decomposes to
    # ASCII literals joined by `.*`/`.+` with optional `^`/`$` anchors — it beats
    # the transition-table DFA (blob version 1) on short and long columns alike.
    # The blob's version byte tells compiled_expression which kernel to dispatch
    # (draken_like_program vs draken_rlike). Non-decomposable patterns fall
    # through to the DFA, which stays the correct, fully-general fallback.
    compiled_blob = compiled_vector_ops.compile_rlike_program(pattern_value)
    if compiled_blob is not None:
        telemetry.optimization_predicate_rewriter_rlike_to_dfa += 1
        predicate.right = build_literal_node(
            compiled_blob,
            root=predicate.right,
            suggested_type=_lt.VARBINARY,
        )
        return predicate

    compiled_blob = compiled_vector_ops.compile_rlike_dfa(pattern_value)
    if compiled_blob is None:
        raise NotSupportedError(
            "**RLIKE**/REGEXP_LIKE pattern is outside the supported regex dialect "
            "(no lookaround/backreferences/case-fold, ASCII pattern content only, "
            "anchors only at the outermost start/end, and the compiled automaton "
            f"must stay within the state-count cap): {pattern_value!r}"
        )

    telemetry.optimization_predicate_rewriter_rlike_to_dfa += 1
    predicate.right = build_literal_node(
        compiled_blob,
        root=predicate.right,
        suggested_type=_lt.VARBINARY,
    )
    return predicate


def rewrite_ored_like_to_any(predicate, telemetry):
    """
    Rewrite OR'd LIKE/ILIKE conditions on the same column into a single
    ``LIKE ANY`` node (AnyOpLike / AnyOpILike), routed to the native
    ``draken_like_any`` (Aho-Corasick) kernel — NOT a regex/DFA.

    Example:
        col LIKE 'a%' OR col LIKE '%b' OR col LIKE '%c%'
        -->  col LIKE ANY ('a%', '%b', '%c%')

    The raw glob patterns pass straight through — no ``sql_like_to_regex``
    middle translation — because ``draken_like_any`` buckets them itself
    (exact / prefix / suffix / contains-Aho-Corasick / residual glob) and stays
    O(1) in pattern count, whereas OR'd LIKEs execute as N independent passes
    (measured ~16× slower at N=50). Grouped by (column, case-sensitivity) so a
    single ANY node is uniformly case-sensitive or -insensitive; a column mixing
    LIKE and ILIKE yields one AnyOpLike and one AnyOpILike. Only positive
    LIKE/ILIKE over an identifier with a string-literal pattern are fused; NOT
    LIKE and everything else is left untouched.
    """
    groups: dict = {}  # (col_id, is_ci) -> {"patterns": [...], "nodes": [...]}

    def collect(node):
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Like", "ILike"}:
            if (
                node.right.node_type == NodeType.LITERAL
                and isinstance(node.right.value, (str, bytes))
                and node.left.node_type == NodeType.IDENTIFIER
                and node.left.schema_column is not None
            ):
                key = (node.left.schema_column.identity, node.value == "ILike")
                group = groups.setdefault(key, {"patterns": [], "nodes": []})
                group["patterns"].append(node.right.value)
                group["nodes"].append(node)
            return
        if node.node_type == NodeType.OR:
            collect(node.left)
            collect(node.right)

    collect(predicate)

    for (col_id, is_ci), group in groups.items():
        if len(group["nodes"]) > 1:
            telemetry.optimization_predicate_rewriter_like_to_any += 1
            _fuse_like_group(group["nodes"][0], group["patterns"], is_ci)
            # Disable the now-redundant OR branches (X OR False OR False == X).
            for node in group["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = _lt.BOOLEAN

    return predicate


def _fuse_like_group(first_node, patterns, is_ci):
    """Turn a group's first LIKE/ILIKE node into a LIKE ANY node in place:
    the raw glob patterns become an ARRAY<VARCHAR> literal on the right, and the
    op becomes AnyOpLike / AnyOpILike (native draken_like_any). The node's
    BOOLEAN schema_column is preserved."""
    first_node.value = "AnyOpILike" if is_ci else "AnyOpLike"
    first_node.right.value = list(patterns)
    first_node.right.type = _lt.ARRAY(_lt.VARCHAR)


def rewrite_cnf_like_to_any(condition, telemetry):
    """CNF counterpart of rewrite_ored_like_to_any: 3+ OR'd LIKE/ILIKE on one
    column are normalised to a CNF node whose `parameters` are the OR-terms, so
    the binary-OR walker never sees them. Fuse same-column same-case LIKE terms
    into one `LIKE ANY` (AnyOpLike/AnyOpILike) here too — this is the case that
    matters most (large N), where OR'd LIKEs otherwise run as N separate passes."""
    if condition.node_type != NodeType.CNF:
        return condition

    groups: dict = {}  # (col_id, is_ci) -> {"patterns": [...], "nodes": [...]}
    others = []
    for branch in condition.parameters:
        if (
            branch.node_type == NodeType.COMPARISON_OPERATOR
            and branch.value in {"Like", "ILike"}
            and branch.right.node_type == NodeType.LITERAL
            and isinstance(branch.right.value, (str, bytes))
            and branch.left.node_type == NodeType.IDENTIFIER
            and branch.left.schema_column is not None
        ):
            key = (branch.left.schema_column.identity, branch.value == "ILike")
            group = groups.setdefault(key, {"patterns": [], "nodes": []})
            group["patterns"].append(branch.right.value)
            group["nodes"].append(branch)
        else:
            others.append(branch)

    new_params = list(others)
    rewrote = False
    for (col_id, is_ci), group in groups.items():
        if len(group["nodes"]) > 1:
            telemetry.optimization_predicate_rewriter_like_to_any += 1
            _fuse_like_group(group["nodes"][0], group["patterns"], is_ci)
            new_params.append(group["nodes"][0])
            rewrote = True
        else:
            new_params.extend(group["nodes"])

    if not rewrote:
        return condition
    if len(new_params) == 1:
        return new_params[0]
    result = Node(node_type=NodeType.CNF)
    result.parameters = new_params
    return result


def rewrite_ored_any_eq_to_contains(predicate, telemetry):
    """
    Rewrite multiple OR'ed ANYOPEQ conditions on the same column to a single @> condition.

    Example:
    'a' = ANY(z) OR 'b' = ANY(z) OR 'c' = ANY(z)
    -->
    ('a', 'b', 'c') @> z

    This rewrite reduces many repeated ANY checks to a single containment operation.
    """
    anyeq_conditions = {}

    def collect_any_eq(node, grouped):
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value == "AnyOpEq":
            # Match only: literal = ANY(identifier)
            if (
                node.left.node_type == NodeType.LITERAL
                and node.right.node_type == NodeType.IDENTIFIER
            ):
                col_id = node.right.schema_column.identity
                if col_id not in grouped:
                    grouped[col_id] = {"values": [], "nodes": [], "column_node": node.right}
                grouped[col_id]["values"].append(node.left.value)
                grouped[col_id]["nodes"].append(node)
            return

        if node.node_type == NodeType.OR:
            collect_any_eq(node.left, grouped)
            collect_any_eq(node.right, grouped)

    collect_any_eq(predicate, anyeq_conditions)

    for data in anyeq_conditions.values():
        if len(data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_anyeq_to_contains += 1

            # Build new comparison node: ('a', 'b', 'c') @> z
            new_node = data["nodes"][0]

            # Sorted, not `list(set(...))`: set iteration order is not stable across
            # runs, so the same query compiled twice rendered its literals in
            # different orders. Matches the CNF counterpart
            # (rewrite_cnf_any_eq_to_contains) — sort by string repr for a
            # deterministic order across mixed literal types while keeping the
            # actual typed values (VARCHAR literals are bytes by this point).
            new_node.left.value = sorted(set(data["values"]), key=str)
            # Phase 2: build ARRAY ColumnType directly from old element type.
            _old_elem_ct = new_node.left.type  # ColumnType of element
            _arr_ct_1 = _lt.ARRAY(_old_elem_ct if isinstance(_old_elem_ct, ColumnType) else _lt.VARIANT)
            new_node.left.type = _arr_ct_1
            # Phase 2: use the already-computed _arr_ct_1 for schema_column.
            new_node.left.schema_column = ConstantColumn(
                name=new_node.left.name,
                column_type=_arr_ct_1,
                value=new_node.left.value,
            )

            new_node.value = "AtArrow"
            new_node.node_type = NodeType.COMPARISON_OPERATOR
            new_node.right = data["column_node"]

            new_node.left, new_node.right = new_node.right, new_node.left  # Swap sides

            # Disable the remaining OR nodes
            for node in data["nodes"][1:]:
                node.node_type = NodeType.LITERAL
                node.type = _lt.BOOLEAN
                node.value = False

    return predicate


def rewrite_cnf_any_eq_to_contains(condition, telemetry):
    """CNF (n-ary OR) counterpart of `rewrite_ored_any_eq_to_contains`.

    'a' = ANY(z) OR 'b' = ANY(z) OR 'c' = ANY(z)  →  z @> ('a', 'b', 'c')

    Needed because DisjunctionSimplificationStrategy normalises a chain of THREE OR
    MORE OR-branches into a single n-ary CNF node, while two branches stay a binary
    OR. The OR-shaped rewrite above is reached only from the `node_type == OR` arm of
    `_rewrite_predicate`, so without this the fusion fired at two terms and silently
    stopped at three — losing the optimization exactly where more terms make it pay
    most (each surviving branch is another full ANY scan per row). Its Eq and LIKE
    siblings already had CNF counterparts (`rewrite_cnf_eq_to_inlist`,
    `rewrite_cnf_like_to_any`); this was the missing third.
    """
    if condition.node_type != NodeType.CNF:
        return condition

    groups: Dict[str, dict] = {}
    others = []

    for branch in condition.parameters:
        if (
            branch.node_type == NodeType.COMPARISON_OPERATOR
            and branch.value == "AnyOpEq"
            and branch.left.node_type == NodeType.LITERAL
            and branch.right.node_type == NodeType.IDENTIFIER
            and branch.right.schema_column is not None
        ):
            col_id = branch.right.schema_column.identity
            if col_id not in groups:
                groups[col_id] = {"values": [], "nodes": [], "column_node": branch.right}
            groups[col_id]["values"].append(branch.left.value)
            groups[col_id]["nodes"].append(branch)
        else:
            others.append(branch)

    new_params = list(others)
    rewrote = False

    for data in groups.values():
        if len(data["values"]) <= 1:
            new_params.extend(data["nodes"])
            continue

        telemetry.optimization_predicate_rewriter_anyeq_to_contains += 1
        rewrote = True

        node = data["nodes"][0]
        # Sorted, not `list(set(...))`: set iteration order is not stable across runs,
        # and an unstable literal order makes the rendered plan differ between two
        # compilations of the same query. Values are already typed by the binder
        # (VARCHAR literals are bytes) — sort by string repr for a deterministic order
        # across mixed literal types while keeping the actual typed values.
        values = sorted(set(data["values"]), key=str)
        node.left.value = values
        _old_elem_ct = node.left.type
        _arr_ct = _lt.ARRAY(_old_elem_ct if isinstance(_old_elem_ct, ColumnType) else _lt.VARIANT)
        node.left.type = _arr_ct
        node.left.schema_column = ConstantColumn(
            name=node.left.name,
            column_type=_arr_ct,
            value=node.left.value,
        )
        node.value = "AtArrow"
        node.node_type = NodeType.COMPARISON_OPERATOR
        node.right = data["column_node"]
        # AtArrow reads container-on-the-left: `alma_mater @> ['MIT', ...]`.
        node.left, node.right = node.right, node.left
        new_params.append(node)
        # The other branches are DROPPED rather than turned into LITERAL False (what
        # the OR-shaped twin must do, since it cannot restructure a binary tree in
        # place) — a CNF node owns a parameter list, so the absorbed branches simply
        # do not come along.

    if not rewrote:
        return condition

    if len(new_params) == 1:
        return new_params[0]

    result = Node(node_type=NodeType.CNF)
    result.parameters = new_params
    return result


def rewrite_ored_eq_to_inlist(predicate, telemetry):
    """
    Rewrite multiple OR'ed Equals conditions on the same column to a single regex pattern.

    Example:
    name = 'Earth' OR name = 'Mars' OR name = 'Venus'
    -->
    name IN ('Earth', 'Mars', 'Venus')
    """
    # Collect Equals conditions that can be combined
    eq_conditions = {}

    def collect_eqs(node, eqs_dict):
        # Base case: LIKE/ILIKE condition
        if node.node_type == NodeType.COMPARISON_OPERATOR and node.value in {"Eq"}:
            # Only proceed if the right side is a literal
            if node.right.node_type == NodeType.LITERAL:
                # `col = NULL` cannot be folded into the same-column IN-list: it has
                # three-valued (UNKNOWN) semantics distinct from a membership test,
                # and a NULL-mixed literal array is rejected outright at parse time
                # (ArrayWithMixedTypesError, logical_planner_builders.in_list) — the
                # optimizer must not manufacture what the front door forbids. Leave
                # `col = NULL` branches untouched; each compiles on its own via the
                # native NULL-comparison short-circuit.
                if node.right.value is None:
                    return
                # Get column identifier for grouping
                col_id = None
                if node.left.node_type == NodeType.IDENTIFIER:
                    col_id = node.left.schema_column.identity

                if col_id:
                    if col_id not in eqs_dict:
                        eqs_dict[col_id] = {
                            "values": [],
                            "nodes": [],
                        }

                    eqs_dict[col_id]["values"].append(node.right.value)
                    eqs_dict[col_id]["nodes"].append(node)
            return

        # Recursive cases
        if node.node_type == NodeType.OR:
            collect_eqs(node.left, eqs_dict)
            collect_eqs(node.right, eqs_dict)

    collect_eqs(predicate, eq_conditions)

    for col_id, eq_data in eq_conditions.items():
        if len(eq_data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_eqs_to_list += 1
            # Create a new regex pattern
            new_node = eq_data["nodes"][0]
            new_node.value = "InList"
            # Sorted, not `list(set(...))`: set iteration order is not stable across
            # runs, so the same query compiled twice rendered its IN-list literals in
            # different orders. Matches the CNF counterpart (rewrite_cnf_eq_to_inlist)
            # — sort by string repr for a deterministic order across mixed literal
            # types while keeping the actual typed values. `col = NULL` branches are
            # already excluded by collect_eqs, so there is no None to compare.
            new_node.right.value = sorted(set(eq_data["values"]), key=str)
            # Phase 2: build ARRAY ColumnType from old element type.
            _old_elem_ct_2 = new_node.right.type
            _arr_ct_2 = _lt.ARRAY(_old_elem_ct_2) if isinstance(_old_elem_ct_2, ColumnType) else _lt.ARRAY(_lt.VARIANT)
            new_node.right.type = _arr_ct_2
            new_node.right.schema_column = ConstantColumn(
                name=new_node.right.name,
                column_type=_arr_ct_2,
                value=new_node.right.value,
            )
            for node in eq_data["nodes"][1:]:
                node.value = False
                node.node_type = NodeType.LITERAL
                node.type = _lt.BOOLEAN

    return predicate


def rewrite_cnf_eq_to_inlist(condition, telemetry):
    """
    For a CNF (n-ary OR) node, group Eq branches that share the same left-hand
    expression and collect their literal values into a single InList node.

    expr = v1 OR expr = v2 OR ... OR expr = vN  →  expr IN (v1, v2, ..., vN)

    Unlike rewrite_ored_eq_to_inlist this works on any left-hand expression
    (identifiers, function calls, etc.) using the canonical string as the key.
    """
    if condition.node_type != NodeType.CNF:
        return condition

    groups: Dict[str, dict] = {}
    non_eq = []

    for branch in condition.parameters:
        if (
            branch.node_type == NodeType.COMPARISON_OPERATOR
            and branch.value == "Eq"
            and branch.right.node_type == NodeType.LITERAL
            # `col = NULL` cannot be folded into the same-column IN-list — see the
            # matching guard in rewrite_ored_eq_to_inlist.collect_eqs above.
            and branch.right.value is not None
        ):
            key = format_expression(branch.left)
            if key not in groups:
                groups[key] = {"values": [], "node": branch}
            groups[key]["values"].append(branch.right.value)
        else:
            non_eq.append(branch)

    new_params = list(non_eq)
    rewrote = False

    for data in groups.values():
        if len(data["values"]) > 1:
            node = data["node"]
            # Values arrive already typed/coerced by the binder (e.g. VARCHAR/
            # VARBINARY literals are bytes, not str) — sort by string repr for a
            # deterministic order across mixed literal types, but keep the actual
            # typed values. A prior version stringified and re-coerced through
            # str(v), which corrupted bytes literals into their Python repr
            # (b'x' -> "b'x'") instead of round-tripping them.
            values = sorted(set(data["values"]), key=str)
            node.value = "InList"
            node.right.value = values
            # Phase 2: build ARRAY ColumnType from old element type.
            _old_elem_ct_3 = node.right.type
            _arr_ct_3 = _lt.ARRAY(_old_elem_ct_3 if isinstance(_old_elem_ct_3, ColumnType) else _lt.VARIANT)
            node.right.type = _arr_ct_3
            node.right.schema_column = ConstantColumn(
                name=node.right.name,
                column_type=_arr_ct_3,
                value=node.right.value,
            )
            new_params.append(node)
            telemetry.optimization_predicate_rewriter_eqs_to_list = (
                getattr(telemetry, "optimization_predicate_rewriter_eqs_to_list", 0) + 1
            )
            rewrote = True
        else:
            new_params.append(data["node"])

    if not rewrote:
        return condition

    if len(new_params) == 1:
        return new_params[0]

    result = Node(node_type=NodeType.CNF)
    result.parameters = new_params
    return result


_LENGTH_FN_NAMES = frozenset({"LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH"})

# Map (operator, integer-literal-value) → unary-operator name.
# Each entry holds, for the form `LENGTH(c) <op> <lit>`, the rewrite that is
# logically equivalent given LENGTH never returns a negative value and NULLs
# propagate (NULL <op> N → NULL in both forms).
_LENGTH_COMPARE_REWRITES = {
    ("Eq", 0): "IsEmpty",
    ("NotEq", 0): "IsNotEmpty",
    ("Lt", 1): "IsEmpty",       # LENGTH(c) < 1  → empty (LENGTH >= 0 always)
    ("LtEq", 0): "IsEmpty",     # LENGTH(c) <= 0 → empty
    ("Gt", 0): "IsNotEmpty",    # LENGTH(c) > 0  → non-empty
    ("GtEq", 1): "IsNotEmpty",  # LENGTH(c) >= 1 → non-empty
}

# Operator flips for when the literal is on the left.
_FLIP_OP = {
    "Eq": "Eq",
    "NotEq": "NotEq",
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
}


def _build_emptiness_node(ident, op_name):
    new_node = Node(
        node_type=NodeType.UNARY_OPERATOR,
        value=op_name,
        centre=ident,
    )
    new_node.schema_column = ExpressionColumn(
        name=format_expression(new_node, True),
        column_type=_lt.BOOLEAN,
    )
    return new_node


def rewrite_string_empty_compare(predicate, telemetry):
    """
    Rewrite empty-string comparisons to `IsEmpty` / `IsNotEmpty` UNARY_OPERATOR
    nodes. Modelling these as unary operators (same shape as `IsNull`) lets
    later optimisations (e.g. metadata-only push-down to the IO layer) treat
    all metadata-answerable predicates uniformly.

    Forms recognised (sides may appear in either order):
      col = ''                         → IsEmpty(col)
      col != ''                        → IsNotEmpty(col)
      LENGTH(col) = 0                  → IsEmpty(col)
      LENGTH(col) <> 0                 → IsNotEmpty(col)
      LENGTH(col) < 1 / <= 0           → IsEmpty(col)
      LENGTH(col) > 0 / >= 1           → IsNotEmpty(col)

    Also matches the CHAR_LENGTH and CHARACTER_LENGTH aliases of LENGTH.
    LENGTH never returns a negative value and NULL-propagates, so the rewrites
    preserve SQL 3VL semantics.
    """
    if predicate.node_type != NodeType.COMPARISON_OPERATOR:
        return predicate

    # ------------------------------------------------------------------
    # Form 1: col {=, !=} ''
    # ------------------------------------------------------------------
    if predicate.value in {"Eq", "NotEq"}:
        if (
            predicate.left.node_type == NodeType.IDENTIFIER
            and predicate.right.node_type == NodeType.LITERAL
        ):
            ident, literal = predicate.left, predicate.right
        elif (
            predicate.right.node_type == NodeType.IDENTIFIER
            and predicate.left.node_type == NodeType.LITERAL
        ):
            ident, literal = predicate.right, predicate.left
        else:
            ident = literal = None

        if ident is not None:
            col_type = getattr(getattr(ident, "schema_column", None), "category", None)
            val = literal.value
            if (
                col_type in {LogicalCategory.VARCHAR, LogicalCategory.VARBINARY}
                and val is not None
                and val in ("", b"")
            ):
                op_name = "IsEmpty" if predicate.value == "Eq" else "IsNotEmpty"
                telemetry.optimization_predicate_rewriter_string_empty_compare += 1
                return _build_emptiness_node(ident, op_name)

    # ------------------------------------------------------------------
    # Form 2: LENGTH(col) <op> <int_literal>
    # ------------------------------------------------------------------
    if (
        predicate.left.node_type == NodeType.FUNCTION
        and predicate.left.value in _LENGTH_FN_NAMES
        and predicate.right.node_type == NodeType.LITERAL
    ):
        func_node, literal, operator = predicate.left, predicate.right, predicate.value
    elif (
        predicate.right.node_type == NodeType.FUNCTION
        and predicate.right.value in _LENGTH_FN_NAMES
        and predicate.left.node_type == NodeType.LITERAL
    ):
        func_node, literal = predicate.right, predicate.left
        operator = _FLIP_OP.get(predicate.value)
        if operator is None:
            return predicate
    else:
        return predicate

    if not func_node.parameters or len(func_node.parameters) != 1:
        return predicate
    inner = func_node.parameters[0]
    if inner.node_type != NodeType.IDENTIFIER:
        return predicate
    col_type = getattr(getattr(inner, "schema_column", None), "category", None)
    if col_type not in {LogicalCategory.VARCHAR, LogicalCategory.VARBINARY}:
        return predicate

    val = literal.value
    if val is None or not isinstance(val, int) or isinstance(val, bool):
        return predicate

    op_name = _LENGTH_COMPARE_REWRITES.get((operator, val))
    if op_name is None:
        return predicate

    telemetry.optimization_predicate_rewriter_string_empty_compare += 1
    return _build_emptiness_node(inner, op_name)


_EPOCH = datetime.datetime(1970, 1, 1)
#: Ticks per second for each TimestampUnit, indexed by the unit's own enum value.
_TICKS_PER_SECOND = (1, 10**3, 10**6, 10**9)


def _canonical_temporal_literal_value(dt: datetime.datetime, column_ct: ColumnType):
    """A temporal literal's value in the representation the engine stores it in.

    A TIMESTAMP literal is an epoch INTEGER in the column's own unit and a DATE
    literal is days since the epoch — the form every other producer of one emits,
    and the form this same function's own parsing step (above) goes out of its way
    to read. Handing back a `datetime` instead is a value/type-tag divergence: the
    node claims TIMESTAMP but carries something no consumer of a TIMESTAMP literal
    expects. It reached the cost model as
    `min(datetime.datetime(...), 946693884000000)` and killed
    `WHERE TRUNC(ts, 'hour') <= <literal>` with a bare Python TypeError, while the
    un-rewritten `WHERE ts <= <literal>` — whose literal is an int — was fine.

    Anything that is not TIMESTAMP or DATE is returned unchanged: the caller falls
    back to VARCHAR when the column has no schema_column, and inventing an integer
    for a type that does not store one would be a second divergence, not a fix.
    """
    category = column_ct.category
    if category == LogicalCategory.TIMESTAMP:
        unit = column_ct.logical.unit.value if column_ct.logical is not None else 2
        delta = dt - _EPOCH
        return (delta.days * 86400 + delta.seconds) * _TICKS_PER_SECOND[unit] + (
            delta.microseconds * _TICKS_PER_SECOND[unit] // 10**6
        )
    if category == LogicalCategory.DATE:
        return (dt - _EPOCH).days
    return dt


def rewrite_date_trunc_to_range(predicate, telemetry: QueryTelemetry):
    """
    Rewrite temporal TRUNC comparisons to range comparisons for better pushdown eligibility.

    Examples:
    TRUNC(col, 'year') = '1970-01-01'  → col >= '1970-01-01' AND col < '1971-01-01'
    TRUNC(col, 'month') <= '2021-02-01' → col < '2021-03-01'
    TRUNC(col, 'day') > '2021-01-15'    → col >= '2021-01-16'
    """

    # Extract the TRUNC function and the comparison value
    # Determine which side is the function and which is the literal
    if predicate.left.node_type == NodeType.FUNCTION and predicate.left.value == "TRUNC":
        func_node = predicate.left
        literal_node = predicate.right
        operator = predicate.value
        is_left_func = True
    elif predicate.right.node_type == NodeType.FUNCTION and predicate.right.value == "TRUNC":
        func_node = predicate.right
        literal_node = predicate.left
        operator = predicate.value
        # Flip the operator if the function is on the right
        flip_ops = {
            "Lt": "Gt",
            "Gt": "Lt",
            "LtEq": "GtEq",
            "GtEq": "LtEq",
            "Eq": "Eq",
            "NotEq": "NotEq",
        }
        operator = flip_ops.get(operator, operator)
        is_left_func = False
    else:
        return predicate

    # Ensure the function has the right structure
    if len(func_node.parameters) != 2:
        return predicate

    column_node = func_node.parameters[0]
    unit_node = func_node.parameters[1]

    # Unit must be a literal string
    if unit_node.node_type != NodeType.LITERAL or not isinstance(unit_node.value, str):
        return predicate

    unit = unit_node.value.lower()

    # Literal must be a string or timestamp
    if literal_node.node_type != NodeType.LITERAL:
        return predicate

    # Parse the literal value. Once ConstantFolding has evaluated a
    # CAST('...' AS TIMESTAMP/DATE), the literal arrives as an INTEGER in the
    # column's native unit — TIMESTAMP: microseconds since epoch; DATE: days
    # since epoch. parse_iso would misread such an integer as epoch SECONDS
    # (e.g. a 2026 timestamp of ~1.78e15 µs read as seconds lands ~56 million
    # years out and overflows datetime), silently returning None and aborting
    # the whole rewrite. Convert an integer temporal literal by its own type
    # first, falling back to parse_iso for strings / datetimes.
    literal_value = literal_node.value
    literal_cat = getattr(getattr(literal_node, "type", None), "category", None)
    if (
        isinstance(literal_value, int)
        and not isinstance(literal_value, bool)
        and literal_cat in (LogicalCategory.TIMESTAMP, LogicalCategory.DATE)
    ):
        import datetime as _datetime

        if literal_cat == LogicalCategory.TIMESTAMP:
            parsed_literal = _datetime.datetime(1970, 1, 1) + _datetime.timedelta(
                microseconds=literal_value
            )
        else:  # DATE — integer days since epoch
            parsed_literal = _datetime.datetime(1970, 1, 1) + _datetime.timedelta(
                days=literal_value
            )
    else:
        parsed_literal = parse_iso(literal_value)
    if parsed_literal is None:
        return predicate

    # Compute floor and next boundary
    try:
        floor_val = truncate_single(parsed_literal, unit)
        next_floor = add_single_unit(floor_val, unit, 1)
    except ValueError:
        # Unsupported unit
        return predicate

    # Determine if the literal is aligned (already at the boundary)
    is_aligned = parsed_literal == floor_val

    telemetry.optimization_predicate_rewriter_date_trunc_to_range += 1

    # Get the column's actual type to match the literal type
    column_ct = _lt.VARCHAR  # ColumnType for the schema_column
    if column_node.schema_column:
        column_ct = column_node.schema_column.column_type or _lt.VARCHAR

    # Helper function to create a literal timestamp node with the column's type
    def make_timestamp_literal(dt):
        lit = Node(
            node_type=NodeType.LITERAL,
            value=_canonical_temporal_literal_value(dt, column_ct),
            type=column_ct,
        )
        lit.schema_column = ExpressionColumn(name="", column_type=column_ct)
        return lit

    # Rewrite based on operator and alignment
    if operator == "Eq":
        if not is_aligned:
            # Non-aligned equality is always false
            predicate.node_type = NodeType.LITERAL
            predicate.type = _lt.BOOLEAN
            predicate.value = False
            return predicate

        # Aligned equality: col >= floor AND col < next
        floor_literal = make_timestamp_literal(floor_val)
        next_literal = make_timestamp_literal(next_floor)

        gte_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=column_node,
            right=floor_literal,
            schema_column=ExpressionColumn(name="", column_type=_lt.BOOLEAN),
        )

        lt_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=column_node,
            right=next_literal,
            schema_column=ExpressionColumn(name="", column_type=_lt.BOOLEAN),
        )

        # Create AND node
        predicate.node_type = NodeType.AND
        predicate.value = "And"
        predicate.left = gte_pred
        predicate.right = lt_pred

    elif operator == "Lt":
        # col < floor
        predicate.left = column_node
        predicate.right = make_timestamp_literal(floor_val)
        predicate.value = "Lt"

    elif operator == "LtEq":
        # col < next_floor
        predicate.left = column_node
        predicate.value = "Lt"
        predicate.right = make_timestamp_literal(next_floor)

    elif operator == "Gt":
        # col >= next_floor
        predicate.left = column_node
        predicate.value = "GtEq"
        predicate.right = make_timestamp_literal(next_floor)

    elif operator == "GtEq":
        # col >= floor (aligned) or col >= next_floor (non-aligned)
        bound = floor_val if is_aligned else next_floor
        predicate.left = column_node
        predicate.value = "GtEq"
        predicate.right = make_timestamp_literal(bound)

    elif operator == "NotEq":
        # col < floor OR col >= next_floor - create an OR node
        lt_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Lt",
            left=column_node,
            right=make_timestamp_literal(floor_val),
            schema_column=ExpressionColumn(name="", column_type=_lt.BOOLEAN),
        )

        gte_pred = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="GtEq",
            left=column_node,
            right=make_timestamp_literal(next_floor),
            schema_column=ExpressionColumn(name="", column_type=_lt.BOOLEAN),
        )

        predicate.node_type = NodeType.OR
        predicate.value = "Or"
        predicate.left = lt_pred
        predicate.right = gte_pred

    return predicate


# Operator flip for when the integer side appears on the right of the literal.
_INT_FRAC_FLIP = {
    "Eq": "Eq",
    "NotEq": "NotEq",
    "Lt": "Gt",
    "LtEq": "GtEq",
    "Gt": "Lt",
    "GtEq": "LtEq",
}
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _unwrap_ipv4_retag(addr_node):
    """Return the operand of a pure ``UINT32 -> IPV4`` CAST, or ``addr_node`` itself.

    IPv4 is ``DrakenType.UINT32`` refined by a ``LogicalKind.IPV4`` descriptor — the
    physical buffer is the same four bytes either way, every uint32 is a valid
    address, and the two orderings are the same ordering. So in the VALUE context of
    a comparison the cast is a pure retag: ``CAST(col, IPV4) >= n`` and ``col >= n``
    select identical rows, and neither can fail where the other did not.

    RedundantCastElimination does not fold it, correctly — ``ColumnType`` equality is
    physical AND descriptor, and dropping the descriptor in an IDENTITY context would
    render addresses as bare integers. Here nothing downstream reads the cast's
    identity: the node is being replaced by a range on the raw column.

    Stripping it is what lets the bounds reach the reader. Everything downstream of
    this rewrite — PredicateCompaction's range merge, PredicatePushdown's
    ``_normalize_col_op_lit`` (which refuses ANY condition containing a CAST) and the
    connector's own ``can_push`` — keys on a bare IDENTIFIER. With the cast left on,
    a CIDR predicate reached none of them and every row was read and materialised.

    Deliberately narrow: only UINT32, only IPV4. An INT64-stored address is NOT
    covered — that cast RAISES on a value outside [0, 2**32) rather than wrapping, so
    stripping it would turn a query that errors into one that quietly returns rows.
    """
    if addr_node.node_type != NodeType.CAST:
        return addr_node
    if addr_node.value != "IPV4":
        return addr_node

    operand = addr_node.left
    if operand is None:
        return addr_node

    operand_type = getattr(getattr(operand, "schema_column", None), "column_type", None)
    if operand_type is None or operand_type.physical != _DrakenType.UINT32:
        return addr_node

    return operand


def rewrite_cidr_to_range(predicate, telemetry: QueryTelemetry):
    """
    Rewrite IPv4 CIDR containment against a LITERAL network into a range.

        addr <<= '10.0.0.0/8'   → addr BETWEEN 167772160 AND 184549375
        '10.0.0.0/8' >>= addr   → the same (the operands are order-agnostic)
        addr <<= '1.2.3.4/32'   → addr = 16909060

    Because the 32 bits ARE the address (draken/core/ipv4.h), a network is
    exactly the closed unsigned interval [base, broadcast]: `(ip & mask) == base`
    and `base <= ip <= broadcast` select precisely the same rows.

    This is the difference between a full scan and a pruned one. Containment is
    otherwise an opaque native kernel call, so it can only run AFTER every row
    has been read and materialised: it is not in `_SIMPLE_COMPARISON_OPS`, so it
    never reaches the connector, never prunes a manifest or a row group, and has
    no selectivity estimate beyond the flat default. A range on the underlying
    UINT32 gets all of that for free, which for a selective network over a large
    partitioned table decides how much data is read at all — the dominant cost.

    The bounds come from draken's own parser via `ipv4_parse_cidr`, never from a
    parser written here: the planner and the kernel disagreeing about which
    addresses '10.0.0.0/8' contains would be a silently-wrong ACL. An
    unparseable CIDR is left ALONE rather than raised on — this is an
    optimisation, and declining leaves the kernel to raise the error it always
    raised, at the point it always raised it.

    NULL: the kernel yields FALSE for a NULL address where the range yields NULL.
    WHERE discards both, and this strategy only ever visits Filter conditions
    (see PredicateRewriteStrategy.visit), so the distinction is unobservable. If
    this is ever reached from a projection, that stops being true.

    The result is a BETWEEN, not an `AND` of two comparisons, and that is
    load-bearing. This rewrite runs AFTER SplitConjunctivePredicates, so nothing
    re-splits what it emits, and PredicatePushdown only ever COLLECTS a Filter
    whose condition root is a comparison, a BETWEEN or a unary operator — an `AND`
    root is left exactly where it sits, which for this query meant stranded above
    a join, filtering a materialised 500k-row join result. An `AND` only ever
    reached a scan because PredicateCompaction happened to fold it back into a
    BETWEEN, and compaction requires a bare IDENTIFIER on the left — so the moment
    the address was a CAST (the normal shape, see below) the whole chain silently
    went away. Emitting the compacted form directly makes this rewrite carry its
    own pushability instead of borrowing another strategy's.
    """
    if predicate.node_type != NodeType.COMPARISON_OPERATOR:
        return predicate
    if predicate.value not in ("IPContainedBy", "IPContains"):
        return predicate

    # Identify the literal (network) side by NODE TYPE rather than by which
    # operator spelling was used — `<<=` and `>>=` are the same predicate with
    # the operands swapped, exactly as the kernel treats them.
    left, right = predicate.left, predicate.right
    if right.node_type == NodeType.LITERAL and left.node_type != NodeType.LITERAL:
        addr_node, cidr_node = left, right
    elif left.node_type == NodeType.LITERAL and right.node_type != NodeType.LITERAL:
        addr_node, cidr_node = right, left
    else:
        # Both literals (constant folding's job) or neither (a column of CIDRs):
        # the kernel remains the only thing that can evaluate this.
        return predicate

    if not isinstance(cidr_node.value, (str, bytes)):
        return predicate

    # An IPv4 column's category IS INTEGER (the descriptor is not visible to
    # LogicalCategory), so this is what "the address side" means here. The binder
    # runs first and only admits that pairing, but the range is only equivalent
    # to the mask-and-compare for an integer, so it is checked rather than
    # assumed — the cost is one comparison at plan time.
    addr_cat = getattr(getattr(addr_node, "schema_column", None), "category", None)
    if addr_cat != LogicalCategory.INTEGER:
        return predicate

    from draken.draken_native import ipv4_parse_cidr

    try:
        base, upper, prefix = ipv4_parse_cidr(cidr_node.value)
    except ValueError:
        # Not a CIDR we can bound. Leave the predicate for the kernel to reject
        # at runtime with its own message — see the docstring.
        return predicate

    telemetry.optimization_predicate_rewriter_cidr_to_range += 1

    # Only now that the CIDR has parsed and the rewrite is certain to happen:
    # unwrap a pure UINT32->IPV4 retag off the address, so the bounds land on the
    # raw stored column. Done HERE and not earlier because the decline paths above
    # must hand the kernel back byte-for-byte what it was given — an optimisation
    # does not get to change which queries fail.
    addr_node = _unwrap_ipv4_retag(addr_node)

    # A /32 is a single host: one equality prunes better than two bounds.
    if prefix == 32:
        predicate.value = "Eq"
        predicate.left = addr_node
        predicate.right = build_literal_node(int(base))
        return predicate

    # A network is a CLOSED interval, so both bounds are inclusive. This is the
    # same node PredicateCompaction builds for a merged range (_build_range_node),
    # which is what makes it pushable — see the docstring.
    predicate.node_type = NodeType.BETWEEN
    predicate.value = (True, True)
    predicate.left = addr_node
    predicate.right = build_literal_node(int(base))
    predicate.centre = build_literal_node(int(upper))
    predicate.schema_column = ExpressionColumn(name="", column_type=_lt.BOOLEAN)
    return predicate


def rewrite_int_vs_fractional_const(predicate, telemetry: QueryTelemetry):
    """
    Rewrite `integer_expr <op> float_literal` to an equivalent integer comparison.

    An integer can never lie strictly between two consecutive integers, so a
    comparison against a float literal collapses to an exact integer bound:

        id >  4.5  → id >= 5        id <  4.5  → id <= 4
        id >= 4.5  → id >= 5        id <= 4.5  → id <= 4
        id =  4.5  → FALSE          id != 4.5  → TRUE
        id =  4.0  → id =  4        id >  4.0  → id >  4   (whole float: exact)

    This keeps the comparison on the native integer fast path (no FLOAT64
    promotion of the column) and lets integer predicates push to storage for
    row-group pruning. The runtime float-promotion path remains the general
    fallback for the cases this rewrite declines (non-finite literals, or
    floor/ceil outside the INT64 range).
    """
    if predicate.node_type != NodeType.COMPARISON_OPERATOR:
        return predicate
    op = predicate.value
    if op not in _INT_FRAC_FLIP:
        return predicate

    # Identify the integer-expression side and the float-literal side. The
    # literal may sit on either side; normalise to `col <op> lit` order.
    left, right = predicate.left, predicate.right
    if right.node_type == NodeType.LITERAL and left.node_type != NodeType.LITERAL:
        col_node, lit_node = left, right
    elif left.node_type == NodeType.LITERAL and right.node_type != NodeType.LITERAL:
        col_node, lit_node = right, left
        op = _INT_FRAC_FLIP[op]
    else:
        return predicate

    col_cat = getattr(getattr(col_node, "schema_column", None), "category", None)
    if col_cat != LogicalCategory.INTEGER:
        return predicate

    val = lit_node.value
    if not isinstance(val, float) or isinstance(val, bool):
        return predicate
    if not math.isfinite(val):
        return predicate

    lo = math.floor(val)
    hi = math.ceil(val)
    if lo < _INT64_MIN or hi > _INT64_MAX:
        return predicate

    telemetry.optimization_predicate_rewriter_int_fractional_const += 1

    # Fractional literal where equality can never hold: collapse to a constant.
    if lo != hi and op in ("Eq", "NotEq"):
        predicate.node_type = NodeType.LITERAL
        predicate.type = _lt.BOOLEAN
        predicate.value = op == "NotEq"
        predicate.left = None
        predicate.right = None
        if predicate.schema_column is not None:
            predicate.schema_column.column_type = _lt.BOOLEAN
        return predicate

    if lo == hi:
        # Whole-valued float (e.g. 4.0): exact integer comparison, same operator.
        final_op, bound = op, lo
    elif op in ("Gt", "GtEq"):
        final_op, bound = "GtEq", hi
    else:  # Lt, LtEq
        final_op, bound = "LtEq", lo

    predicate.left = col_node
    predicate.right = build_literal_node(int(bound))
    predicate.value = final_op
    return predicate


# Define dispatcher conditions and actions
dispatcher: Dict[str, Callable] = {
    "rewrite_in_to_eq": rewrite_in_to_eq,
    "reorder_interval_calc": reorder_interval_calc,
    "rewrite_date_trunc_to_range": rewrite_date_trunc_to_range,
}


# Dispatcher conditions
def _rebind_function_node(function_node, origin: str = None):
    """Rebind a newly-created function node to its catalog entry.

    `origin` names the SQL construct this node was rewritten from (e.g.
    "CASE"). A branch-type mismatch (IncompatibleTypesError) only becomes
    visible once the catalog resolves this synthetic node — by which point
    the user's original syntax is gone, so the error would otherwise name a
    function (e.g. IFNULL) they never wrote. Re-raising with `origin` and the
    target column attached points back at what's actually in their query.
    """
    from opteryx.exceptions import IncompatibleTypesError
    from opteryx.expression.functions import get_catalog

    try:
        resolved = get_catalog().resolve(function_node.value, list(function_node.parameters))
    except IncompatibleTypesError as err:
        if origin is None:
            raise
        column = function_node.alias or getattr(function_node.schema_column, "name", None)
        where = f" (column '{column}')" if column else ""
        raise IncompatibleTypesError(
            message=f"{err} [the optimizer rewrote a {origin} expression{where} into {function_node.value}]"
        ) from err
    if resolved is None:
        raise ValueError(f"Unable to resolve function '{function_node.value}'")
    function_node.function_ref = resolved


def _is_safe(node) -> bool:
    """Return True iff node is safe to evaluate eagerly (no exceptions, no side effects)."""
    if node is None:
        return True
    nt = node.node_type
    if nt in (NodeType.LITERAL, NodeType.IDENTIFIER):
        return True
    if nt == NodeType.CAST:
        return False
    if nt == NodeType.BINARY_OPERATOR:
        if node.value in {"Divide", "Modulo"}:
            return False
        return _is_safe(node.left) and _is_safe(node.right)
    if nt == NodeType.FUNCTION:
        return all(_is_safe(p) for p in (node.parameters or []))
    return False


# Physical casts that can NEVER map a non-NULL input to NULL: identity, and
# value-preserving numeric widenings. Deliberately narrow — this is the exact family
# a CASE branch type-blend inserts, and nothing else earns the benefit of the doubt.
# Excluded on purpose: every narrowing (can overflow), string<->numeric (can fail to
# parse), DECIMAL/temporal/VARIANT, and every TRY_ cast (whose entire contract is to
# yield NULL on failure).
_NULL_PRESERVING_WIDENING: Dict[str, frozenset] = {
    "INT8":    frozenset({"INT8", "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "INT16":   frozenset({"INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "INT32":   frozenset({"INT32", "INT64", "FLOAT64"}),  # not FLOAT32: 2^31 > 2^24
    "INT64":   frozenset({"INT64"}),
    "UINT8":   frozenset({"UINT8", "UINT16", "UINT32", "UINT64",
                          "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "UINT16":  frozenset({"UINT16", "UINT32", "UINT64",
                          "INT32", "INT64", "FLOAT32", "FLOAT64"}),
    "UINT32":  frozenset({"UINT32", "UINT64", "INT64", "FLOAT64"}),
    "UINT64":  frozenset({"UINT64"}),
    "FLOAT32": frozenset({"FLOAT32", "FLOAT64"}),
    "FLOAT64": frozenset({"FLOAT64"}),
}


def _physical_name(expression) -> str:
    """Physical DrakenType name bound to an expression, or '' when untyped."""
    schema_column = getattr(expression, "schema_column", None)
    column_type = getattr(schema_column, "column_type", None)
    physical = getattr(column_type, "physical", None)
    return getattr(physical, "name", "") or ""


def _is_null_preserving_cast(cast_node) -> bool:
    """True iff this CAST cannot turn a non-NULL value into NULL."""
    target_name = (getattr(cast_node, "value", "") or "").upper()
    if target_name.startswith("TRY_"):
        return False  # TRY_ exists precisely to yield NULL on failure
    if getattr(cast_node, "format", None) is not None:
        return False  # a FORMAT-driven parse can fail on a non-NULL input
    source = getattr(cast_node, "left", None)
    if source is None:
        return False
    source_physical = _physical_name(source)
    target_physical = _physical_name(cast_node)
    if not source_physical or not target_physical:
        return False
    return target_physical in _NULL_PRESERVING_WIDENING.get(source_physical, frozenset())


def _identity_through_transparent(expression):
    """`schema_column.identity` of an expression, seeing through wrappers that change
    neither which value it denotes nor which rows are NULL.

    The IFNULL rewrite below matches `CASE WHEN x IS NULL THEN y ELSE x END` by
    comparing the identity of both mentions of `x`. Branch type-blending routinely
    wraps the ELSE mention in a CAST the user never wrote — `surface_pressure`
    (FLOAT32) against a FLOAT64 literal binds as `surface_pressure::FLOAT64` — and the
    wrapper carries its OWN identity, so the two stopped matching and the rewrite
    silently declined for every blended CASE. Looking through the wrapper restores it.

    The look-through is NOT unconditional. The rewrite re-points the null test at the
    wrapped expression (`IFNULL(CAST(x), y)` tests `CAST(x) IS NULL`, not `x IS NULL`),
    so an unwrapped cast that can produce NULL from a non-NULL input would change the
    answer: `CASE WHEN x IS NULL THEN y ELSE CAST(x AS INTEGER) END` over an
    unparseable string yields NULL, while `IFNULL(CAST(x AS INTEGER), y)` yields y.
    Only casts that provably cannot do that are crossed. NESTED is the sanctioned
    transparent wrapper (a parenthesis) and is always safe.
    """
    while expression is not None:
        node_type = expression.node_type
        if node_type == NodeType.NESTED:
            expression = expression.centre
            continue
        if node_type == NodeType.CAST and _is_null_preserving_cast(expression):
            expression = expression.left
            continue
        break
    return getattr(getattr(expression, "schema_column", None), "identity", None)


def _rewrite_case_node(node, telemetry: QueryTelemetry):
    """Rewrite a NodeType.CASE node to IFNULL or IIF when safe."""
    if len(node.conditions) != 1 or node.else_result is None:
        return node
    cond = node.conditions[0]
    then_ = node.results[0]
    else_ = node.else_result

    # CASE WHEN x IS NULL THEN y ELSE x END → IFNULL(x, y)
    if (
        cond.node_type == NodeType.UNARY_OPERATOR
        and cond.value == "IsNull"
        and _is_safe(then_)
    ):
        # `else_` itself (wrapper and all) stays the IFNULL argument — only the
        # identity MATCH looks through it. Unwrapping the argument would drop the
        # blend cast and hand back the narrower branch type.
        cond_identity = _identity_through_transparent(cond.centre)
        else_identity = _identity_through_transparent(else_)
        if cond_identity is not None and cond_identity == else_identity:
            telemetry.optimization_predicate_rewriter_case_to_ifnull += 1
            new_node = Node(
                NodeType.FUNCTION,
                value="IFNULL",
                parameters=[else_, then_],
                alias=node.alias,
                schema_column=node.schema_column,
            )
            _rebind_function_node(new_node, origin="CASE")
            return new_node

    # CASE WHEN c THEN y ELSE z END → IIF(c, y, z) when condition and both branches are safe
    if _is_safe(cond) and _is_safe(then_) and _is_safe(else_):
        telemetry.optimization_predicate_rewriter_case_to_iif += 1
        new_node = Node(
            NodeType.FUNCTION,
            value="IIF",
            parameters=[cond, then_, else_],
            alias=node.alias,
            schema_column=node.schema_column,
        )
        _rebind_function_node(new_node, origin="CASE")
        return new_node

    return node


def _rewrite_predicate(predicate, telemetry: QueryTelemetry):
    if predicate.node_type == NodeType.CASE:
        return _rewrite_case_node(predicate, telemetry)
    if predicate.node_type == NodeType.FUNCTION:
        return _rewrite_function(predicate, telemetry)

    if predicate.node_type == NodeType.COMPARISON_OPERATOR and predicate.value in (
        "RLike",
        "NotRLike",
    ):
        return _rewrite_rlike_to_dfa(predicate, telemetry)

    # Fuse OR'd LIKE/ILIKE on one column into a single native LIKE ANY.
    if predicate.node_type == NodeType.OR:
        rewritten = rewrite_ored_like_to_any(predicate, telemetry)
        rewritten = rewrite_ored_eq_to_inlist(rewritten, telemetry)
        rewritten = rewrite_ored_any_eq_to_contains(rewritten, telemetry)
        if rewritten != predicate:
            return rewritten

    if predicate.node_type == NodeType.CNF:
        predicate = rewrite_cnf_eq_to_inlist(predicate, telemetry)
        predicate = rewrite_cnf_like_to_any(predicate, telemetry)
        predicate = rewrite_cnf_any_eq_to_contains(predicate, telemetry)

    # if predicate.node_type in {NodeType.AND, NodeType.OR, NodeType.XOR}:
    if predicate.left:
        predicate.left = _rewrite_predicate(predicate.left, telemetry)
    if predicate.right:
        predicate.right = _rewrite_predicate(predicate.right, telemetry)
    if predicate.centre:
        predicate.centre = _rewrite_predicate(predicate.centre, telemetry)

    if predicate.node_type not in {NodeType.BINARY_OPERATOR, NodeType.COMPARISON_OPERATOR}:
        # after rewrites, some filters aren't actually predicates
        return predicate

    # Rewrite temporal TRUNC comparisons to range comparisons
    if predicate.node_type == NodeType.COMPARISON_OPERATOR:
        if (predicate.left.node_type == NodeType.FUNCTION and predicate.left.value == "TRUNC") or (
            predicate.right.node_type == NodeType.FUNCTION and predicate.right.value == "TRUNC"
        ):
            predicate = rewrite_date_trunc_to_range(predicate, telemetry)
            # After rewrite, return early if it's no longer a comparison (e.g., became a literal or AND node)
            if predicate.node_type != NodeType.COMPARISON_OPERATOR:
                return predicate

    # Rewrite `col = ''` / `col != ''` to `IsEmpty(col)` / `IsNotEmpty(col)`.
    if predicate.node_type == NodeType.COMPARISON_OPERATOR:
        rewritten = rewrite_string_empty_compare(predicate, telemetry)
        if rewritten is not predicate:
            return rewritten

    # Rewrite `integer_expr <op> fractional_literal` to an exact integer bound.
    # May collapse the comparison to a boolean literal (e.g. `id = 4.5` → FALSE),
    # so return early when it is no longer a comparison.
    if predicate.node_type == NodeType.COMPARISON_OPERATOR:
        predicate = rewrite_int_vs_fractional_const(predicate, telemetry)
        if predicate.node_type != NodeType.COMPARISON_OPERATOR:
            return predicate

    # Rewrite `addr <<= '10.0.0.0/8'` to the equivalent range on the underlying
    # UINT32, so the predicate can prune at the scan instead of being an opaque
    # kernel call over every materialised row. Becomes an AND of two bounds
    # (or an Eq for a /32), so return early once it is no longer a comparison.
    if predicate.node_type == NodeType.COMPARISON_OPERATOR:
        predicate = rewrite_cidr_to_range(predicate, telemetry)
        if predicate.node_type != NodeType.COMPARISON_OPERATOR:
            return predicate

    if predicate.right.type == _lt.VARCHAR:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if "%%" in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(r"%+", "%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if "%" not in predicate.right.value and "_" not in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                "_" not in predicate.right.value
                and predicate.right.value.endswith("%")
                and predicate.right.value.startswith("%")
                and "%" not in predicate.right.value[1:-1]
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if (
                predicate.right.value.endswith("%")
                and "%" not in predicate.right.value[:-1]
                and "_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_starts_with += 1
                pattern_bytes = predicate.right.value[:-1].encode()
                negated = predicate.value in {"NotLike", "NotILike"}
                func_name = "_CI_STARTS_WITH" if predicate.value in {"ILike", "NotILike"} else "_STARTS_WITH"
                fn_node = Node(node_type=NodeType.FUNCTION, value=func_name, parameters=[predicate.left, build_literal_node(pattern_bytes)])
                _rebind_function_node(fn_node)
                if negated:
                    predicate.node_type = NodeType.NOT
                    predicate.centre = fn_node
                    predicate.value = None
                    predicate.left = None
                    predicate.right = None
                else:
                    predicate.node_type = NodeType.FUNCTION
                    predicate.value = func_name
                    predicate.parameters = fn_node.parameters
                    predicate.function_ref = fn_node.function_ref
                    predicate.left = None
                    predicate.right = None
            elif (
                predicate.right.value.startswith("%")
                and "%" not in predicate.right.value[1:]
                and "_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_ends_with += 1
                pattern_bytes = predicate.right.value[1:].encode()
                negated = predicate.value in {"NotLike", "NotILike"}
                func_name = "_CI_ENDS_WITH" if predicate.value in {"ILike", "NotILike"} else "_ENDS_WITH"
                fn_node = Node(node_type=NodeType.FUNCTION, value=func_name, parameters=[predicate.left, build_literal_node(pattern_bytes)])
                _rebind_function_node(fn_node)
                if negated:
                    predicate.node_type = NodeType.NOT
                    predicate.centre = fn_node
                    predicate.value = None
                    predicate.left = None
                    predicate.right = None
                else:
                    predicate.node_type = NodeType.FUNCTION
                    predicate.value = func_name
                    predicate.parameters = fn_node.parameters
                    predicate.function_ref = fn_node.function_ref
                    predicate.left = None
                    predicate.right = None

    # If the predicate was transformed to a FUNCTION or NOT node, return early
    if predicate.node_type in {NodeType.FUNCTION, NodeType.NOT}:
        return predicate

    if predicate.right.type == _lt.VARBINARY:
        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if b"%%" in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_adjacent_wildcards += 1
                predicate.right.value = re.sub(b"%+", b"%", predicate.right.value)

        if predicate.value in LIKE_REWRITES:
            if b"%" not in predicate.right.value and b"_" not in predicate.right.value:
                telemetry.optimization_predicate_rewriter_remove_redundant_like += 1
                predicate.value = LIKE_REWRITES[predicate.value]

        if predicate.value in INSTR_REWRITES:
            if (
                b"_" not in predicate.right.value
                and predicate.right.value.endswith(b"%")
                and predicate.right.value.startswith(b"%")
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_in_string += 1
                predicate.right.value = predicate.right.value[1:-1]
                predicate.value = INSTR_REWRITES[predicate.value]

        if predicate.value in {"Like", "ILike", "NotLike", "NotILike"}:
            if (
                predicate.right.value.endswith(b"%")
                and b"%" not in predicate.right.value[:-1]
                and b"_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_starts_with += 1
                pattern_bytes = predicate.right.value[:-1]
                negated = predicate.value in {"NotLike", "NotILike"}
                func_name = "_CI_STARTS_WITH" if predicate.value in {"ILike", "NotILike"} else "_STARTS_WITH"
                fn_node = Node(node_type=NodeType.FUNCTION, value=func_name, parameters=[predicate.left, build_literal_node(pattern_bytes)])
                _rebind_function_node(fn_node)
                if negated:
                    predicate.node_type = NodeType.NOT
                    predicate.centre = fn_node
                    predicate.value = None
                    predicate.left = None
                    predicate.right = None
                else:
                    predicate.node_type = NodeType.FUNCTION
                    predicate.value = func_name
                    predicate.parameters = fn_node.parameters
                    predicate.function_ref = fn_node.function_ref
                    predicate.left = None
                    predicate.right = None
            elif (
                predicate.right.value.startswith(b"%")
                and b"%" not in predicate.right.value[1:]
                and b"_" not in predicate.right.value
            ):
                telemetry.optimization_predicate_rewriter_replace_like_with_ends_with += 1
                pattern_bytes = predicate.right.value[1:]
                negated = predicate.value in {"NotLike", "NotILike"}
                func_name = "_CI_ENDS_WITH" if predicate.value in {"ILike", "NotILike"} else "_ENDS_WITH"
                fn_node = Node(node_type=NodeType.FUNCTION, value=func_name, parameters=[predicate.left, build_literal_node(pattern_bytes)])
                _rebind_function_node(fn_node)
                if negated:
                    predicate.node_type = NodeType.NOT
                    predicate.centre = fn_node
                    predicate.value = None
                    predicate.left = None
                    predicate.right = None
                else:
                    predicate.node_type = NodeType.FUNCTION
                    predicate.value = func_name
                    predicate.parameters = fn_node.parameters
                    predicate.function_ref = fn_node.function_ref
                    predicate.left = None
                    predicate.right = None

    if predicate.value == "AnyOpEq":
        if predicate.right.node_type == NodeType.LITERAL:
            telemetry.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "InList"

    if predicate.value == "AnyOpNotEq":
        if predicate.right.node_type == NodeType.LITERAL:
            telemetry.optimization_predicate_rewriter_any_to_inlist += 1
            predicate.value = "NotInList"

    if predicate.value in IN_REWRITES:
        # The binder may have pre-baked the IN-list into a CarcharSetWrapper
        # (hash-only, not iterable, original values discarded). Skip the
        # IN→Eq rewrite in that case — the evaluator hash-set path handles it.
        if (
            predicate.right.node_type == NodeType.LITERAL
            and isinstance(predicate.right.value, (list, tuple, set, frozenset))
            and len(predicate.right.value) == 1
        ):
            telemetry.optimization_predicate_rewriter_in_to_equals += 1
            predicate = dispatcher["rewrite_in_to_eq"](predicate)
            # The rewrite may hand back a plain numeric-vs-numeric Eq (e.g. an
            # INTEGER column against a FLOAT literal) that still needs the
            # cross-numeric-family handling (rewrite_int_vs_fractional_const)
            # a directly-written `col = 1.5` would have gone through above —
            # re-enter so it gets the same treatment instead of reaching the
            # native kernel as a raw, unnormalised Eq.
            return _rewrite_predicate(predicate, telemetry)

    if (
        predicate.node_type == NodeType.COMPARISON_OPERATOR
        and predicate.left.node_type == NodeType.BINARY_OPERATOR
    ):
        _dt_left = determine_type(predicate.left)
        _dt_right = determine_type(predicate.right)
        if (
            _dt_left is not None and _dt_left.category == LogicalCategory.INTERVAL
            and _dt_right is not None and _dt_right.category == LogicalCategory.INTERVAL
        ):
            telemetry.optimization_predicate_rewriter_date_ += 1
            predicate = dispatcher["reorder_interval_calc"](predicate)

    return predicate


def _stringify_for_concat(node):
    """Coerce a CONCAT/CONCAT_WS operand to VARCHAR, unless it is already
    string-family (VARCHAR/NVARCHAR/VARBINARY) or NULL-typed.

    CONCAT/CONCAT_WS are rewritten to `||` (StringConcat) chains, and StringConcat
    is string-only natively (binop_string_concat, function_string_extra.cpp
    header), so an operand with no string type cannot be handed to it.

    The cast is now a BACKSTOP, not the main path. CONCAT/CONCAT_WS declare one
    overload per string type, so a genuinely non-string operand (`CONCAT(id,
    name)`) is refused by overload resolution at BIND and never reaches here —
    that coercion was deliberately removed (architect, 2026-08-09; see
    RATIFIED/string-concatenation-requires-homogeneous-string-types). What still
    arrives untyped is a node the binder could not type at all (determine_type
    returns None), and VARCHAR is the only thing to do with one.

    A NULL-typed operand is passed through unwrapped: StringConcat already
    short-circuits `x || NULL` to NULL via the dedicated NULL-operand rule in
    operator_map.determine_type, so no cast is needed to make it type-check.

    Built directly (Node(NodeType.CAST, ...) with a synthesized
    schema_column) rather than through the binder — this rewrite runs POST-bind,
    so there is no second binder pass to fill one in. This is the same pattern
    the StringConcat wrapper nodes in this function already use for their own
    schema_column. An operand whose target CAST has no native kernel still
    fails — but loudly, naming the exact unsupported CAST, in place of the
    previous blanket refusal.
    """
    ct = determine_type(node)
    if ct is not None and (ct.category in _STRING_CATEGORIES or ct.category == LogicalCategory.NULL):
        return node
    return Node(
        node_type=NodeType.CAST,
        left=node,
        value="VARCHAR",
        parameters=(),
        alias=None,
        schema_column=ExpressionColumn(name="", column_type=_lt.VARCHAR),
    )


def _concat_chain_type(function):
    """Validate CONCAT/CONCAT_WS operand homogeneity; return the chain's ColumnType.

    CONCAT/CONCAT_WS require HOMOGENEOUS string operands.

    Architect ruling 2026-08-09: string concatenation takes one string type, and
    mixing VARCHAR/NVARCHAR/VARBINARY is an IncorrectTypeError the caller resolves
    with an explicit cast. `||` gets this from OPERATOR_MAP, which holds only the
    three matching pairs (see operator_map._STRING_CATEGORIES). CONCAT cannot: it
    binds through the catalog with `any` parameters and is desugared to a
    StringConcat chain HERE, post-bind, so the operator map never sees its operands
    until the chain is already built. Checking up front also lets the error name
    `CONCAT(...)` — the thing the caller actually wrote — rather than a `||` that
    never appeared in the query.

    This is the SECOND line of defence, and it is worth keeping. CONCAT/CONCAT_WS
    declare one overload per string type, so overload resolution already rejects a
    mixed call at bind with a message naming the offending argument — that is the
    error a caller normally sees. Resolution scores an argument the binder could
    not type as compatible with everything, though, so an untyped node can still
    reach the desugaring; this catches it there rather than letting it build a
    mixed `||` chain the plan compiler would refuse with "outside the c-native
    kernel set" — the opaque message this ruling exists to remove.

    NULL-typed operands carry no string type to disagree with and are skipped —
    `CONCAT(name, NULL)` stays legal, matching the StringConcat NULL-operand rule.

    The returned ColumnType types EVERY node of the desugared chain. The
    intermediate StringConcat nodes used to be hardcoded VARCHAR, which was
    invisible for a VARCHAR chain and wrong for any other: a three-operand
    VARBINARY concat declared `b'a' || b'b'` VARCHAR, so the next link read VARCHAR
    against VARBINARY and the plan compiler refused a chain whose operands were
    perfectly homogeneous. One resolved type for the whole chain removes that.
    """
    categories = set()
    for param in function.parameters:
        ct = determine_type(param)
        if ct is None or ct.category == LogicalCategory.NULL:
            continue
        categories.add(ct.category if ct.category in _STRING_CATEGORIES else LogicalCategory.VARCHAR)
    if len(categories) > 1:
        from opteryx.exceptions import IncorrectTypeError, compose, md_code

        names = sorted(category.name for category in categories)
        raise IncorrectTypeError(
            compose(
                f"{md_code(format_expression(function))} cannot be evaluated, because "
                f"{md_code(function.value)} concatenates one string type at a time and "
                f"{', '.join(md_code(name) for name in names)} were provided",
                f"Casting the operands to match with {md_code('CAST(column AS type)')} "
                f"usually resolves it",
            )
        )
    # All-NULL operands leave `categories` empty — VARCHAR is the type the
    # StringConcat NULL-operand rule already gives that shape.
    if categories == {LogicalCategory.VARBINARY}:
        return _lt.VARBINARY
    if categories == {LogicalCategory.NVARCHAR}:
        return _lt.NVARCHAR
    return _lt.VARCHAR


def _rewrite_function(function, telemetry: QueryTelemetry):
    def _rebind_function_ref():
        # Rebind the function reference when the function name or parameters have been rewritten.
        # The binder runs before the optimizer, so we must update node.function_ref here.
        from opteryx.expression.functions import get_catalog

        resolved = get_catalog().resolve(function.value, list(function.parameters))
        if resolved is None:
            raise ValueError(f"Unable to resolve rewritten function '{function.value}'")
        function.function_ref = resolved
        if getattr(function, "schema_column", None) is not None and resolved.inferred_return_type:
            # Phase 5: inferred_return_type is ColumnType — use directly.
            function.schema_column.column_type = resolved.inferred_return_type

    def _normalise_dfa_replacement(value):
        if isinstance(value, bytes):
            return re.sub(rb"\\\\([0-9])", rb"\\\1", value)
        if isinstance(value, str):
            return re.sub(r"\\\\([0-9])", r"\\\1", value)
        return value

    def _compile_dfa_program_blob(pattern_value, replacement_value):
        from opteryx.compiled import vector_ops as compiled_vector_ops

        if isinstance(pattern_value, str):
            pattern_value = pattern_value.encode("utf8")
        elif not isinstance(pattern_value, bytes):
            return None

        if isinstance(replacement_value, str):
            replacement_value = replacement_value.encode("utf8")
        elif not isinstance(replacement_value, bytes):
            return None

        return compiled_vector_ops.compile_dfa_program(pattern_value, replacement_value)

    def _rewrite_regexp_replace_to_dfa():
        if function.value != "REGEXP_REPLACE" or len(function.parameters) != 3:
            return None

        pattern_node = function.parameters[1]
        replacement_node = function.parameters[2]

        if (
            pattern_node.node_type != NodeType.LITERAL
            or replacement_node.node_type != NodeType.LITERAL
        ):
            return None

        pattern_value = pattern_node.value
        replacement_value = _normalise_dfa_replacement(replacement_node.value)
        compiled_program = _compile_dfa_program_blob(pattern_value, replacement_value)

        if compiled_program is None:
            return None

        telemetry.optimization_predicate_rewriter_regex_replace_to_dfa += 1
        function.value = "_DFA_EXTRACT"
        function.parameters = [
            function.parameters[0],
            build_literal_node(
                compiled_program,
                root=function.parameters[1],
                suggested_type=_lt.VARBINARY,
            ),
        ]
        _rebind_function_ref()
        return function

    rewritten = _rewrite_regexp_replace_to_dfa()
    if rewritten is not None:
        return rewritten

    # COALESCE(x, y) → IFNULL(x, y)
    if function.value == "COALESCE":
        if len(function.parameters) == 2:
            telemetry.optimization_predicate_rewriter_coalesce_to_ifnull += 1
            function.value = "IFNULL"
            _rebind_function_ref()
            return function
    # SUBSTRING(x, 1, n) → LEFT(x, n). Arity-guarded: the two-argument form
    # SUBSTRING(x, 1) has no length to hand LEFT (it means "the whole string"),
    # and reading parameters[2] there is an IndexError.
    if (
        function.value == "SUBSTRING"
        and len(function.parameters) == 3
        and function.parameters[1].value == 1
    ):
        telemetry.optimization_predicate_rewriter_substring_to_left += 1
        function.value = "LEFT"
        function.parameters = [function.parameters[0], function.parameters[2]]
        _rebind_function_ref()
        return function
    # CONCAT(x, y, z) → x || y || z. Each operand is stringified first
    # (_stringify_for_concat) — StringConcat is string-only natively, so a
    # non-string operand (CONCAT(id, name)) was refused family-wide before this.
    if function.value == "CONCAT" and len(function.parameters) > 1:
        telemetry.optimization_predicate_rewriter_concat_to_double_pipe += 1
        _chain_ct = _concat_chain_type(function)
        left_node = _stringify_for_concat(function.parameters[0])
        for param in function.parameters[1:]:
            left_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=_stringify_for_concat(param),
                schema_column=ExpressionColumn(name="", column_type=_chain_ct),
            )
        left_node.alias = function.alias
        left_node.schema_column = function.schema_column
        function = left_node
    # CONCAT_WS(x, y, z) → y || x || z. Same stringify-every-operand treatment
    # as CONCAT above, applied to the separator and every value.
    if function.value == "CONCAT_WS" and len(function.parameters) > 2:
        telemetry.optimization_predicate_rewriter_concatws_to_double_pipe += 1
        _chain_ct = _concat_chain_type(function)
        separator = _stringify_for_concat(function.parameters[0])
        left_node = _stringify_for_concat(function.parameters[1])
        for param in function.parameters[2:]:
            separator_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=left_node,
                right=separator,
                schema_column=ExpressionColumn(name="", column_type=_chain_ct),
            )
            left_node = Node(
                node_type=NodeType.BINARY_OPERATOR,
                value="StringConcat",
                left=separator_node,
                right=_stringify_for_concat(param),
                schema_column=ExpressionColumn(name="", column_type=_chain_ct),
            )
        left_node.alias = function.alias
        left_node.schema_column = function.schema_column
        function = left_node
    # CONCAT_WS(sep, x) → x || '' (the single-value degenerate form). With one
    # value the separator never appears, so the result is just x rendered as a
    # string. The `|| ''` is not cosmetic: it routes x through the SAME
    # StringConcat + _stringify_for_concat coercion the >2 path applies to
    # parameters[1], so a non-string arg is stringified and NULL propagates
    # identically — parity with the multi-arg form, not a second semantics.
    # Without this the 2-arg form is refused (the >2 guard skipped it, and there
    # is no draken_concat_ws kernel). NOTE: CONCAT/CONCAT_WS correctness
    # depending on this optimizer pass at all is a known smell — under
    # DISABLE_OPTIMIZER=1 every arity fails. Widening this guard keeps that
    # dependency (the architect's choice) rather than removing it with a kernel.
    if function.value == "CONCAT_WS" and len(function.parameters) == 2:
        telemetry.optimization_predicate_rewriter_concatws_to_double_pipe += 1
        _chain_ct = _concat_chain_type(function)
        value_node = _stringify_for_concat(function.parameters[1])
        # The empty literal must carry the CHAIN's string type, not a hardcoded
        # VARCHAR. StringConcat is homogeneous-only, so pairing a VARBINARY value
        # with a VARCHAR '' would build the very mixed node this ruling forbids —
        # `CONCAT_WS(b'-', b'a')` would be refused by its own desugaring.
        if _chain_ct is _lt.VARBINARY:
            _empty = build_literal_node(b"", suggested_type=_lt.VARBINARY)
        elif _chain_ct is _lt.NVARCHAR:
            _empty = build_literal_node("", suggested_type=_lt.NVARCHAR)
        else:
            _empty = build_literal_node("", suggested_type=_lt.VARCHAR)
        left_node = Node(
            node_type=NodeType.BINARY_OPERATOR,
            value="StringConcat",
            left=value_node,
            right=_empty,
            schema_column=ExpressionColumn(name="", column_type=_chain_ct),
        )
        left_node.alias = function.alias
        left_node.schema_column = function.schema_column
        function = left_node

    return function


class PredicateRewriteStrategy(OptimizationStrategy):
    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.condition = _rewrite_predicate(node.condition, self.telemetry)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
