# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import decimal
from typing import Any, Dict, Optional, Tuple

from opteryx.compiled.structures.expressions import Expression
from opteryx.exceptions import (
    AmbiguousIdentifierError,
    ColumnNotFoundError,
    IncompatibleTypesError,
    InvalidInternalStateError,
    UnexpectedDatasetReferenceError,
    UnsupportedSyntaxError,
)
from opteryx.expression import ExpressionColumn
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.functions import get_catalog as _get_function_catalog
from opteryx.expression.functions.registrar import fixed_value_function
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.join_helpers import get_mismatched_condition_column_types
from opteryx.planner.binder.operator_map import determine_type
from opteryx.planner.binder.pattern_compile import compile_rlike_pattern
from opteryx.types import logical_type as _lt
from opteryx.types.logical_type import (
    BOOLEAN as _CT_BOOLEAN,
)
from opteryx.types.logical_type import (
    DATE as _CT_DATE,
)
from opteryx.types.logical_type import (
    FLOAT64 as _CT_FLOAT64,
)
from opteryx.types.logical_type import (
    INT64 as _CT_INT64,
)
from opteryx.types.logical_type import (
    INTERVAL as _CT_INTERVAL,
)
from opteryx.types.logical_type import (
    NULL as _CT_NULL,
)
from opteryx.types.logical_type import (
    TIMESTAMP as _CT_TIMESTAMP,
)
from opteryx.types.logical_type import (
    VARCHAR as _CT_VARCHAR,
)
from opteryx.types.logical_type import (
    ColumnType as _ColumnType,
)
from opteryx.types.logical_type import (
    LogicalCategory,
    parse_column_type,
)
from opteryx.types.scalars.value_parsing import parse_value
from opteryx.types.schema import ConstantColumn, FunctionColumn, RelationSchema, SchemaColumn
from opteryx.types.type_unification import NOT_LITERAL, compute_selection_result_type
from opteryx.types.literal_values import native_literal_value
from opteryx.compiled.structures.expressions import Cast
from opteryx.compiled.structures.expressions import Literal
# The binder's per-node core is compiled (architect plan P2, 2026-10-25): the names
# below live there and are re-exported here, where every visitor imports them from.
from opteryx.compiled.planner.binder_core import _append_derived  # noqa: F401
from opteryx.compiled.planner.binder_core import _rebind  # noqa: F401
from opteryx.compiled.planner.binder_core import inner_binder  # noqa: F401
from opteryx.compiled.planner.binder_core import locate_identifier  # noqa: F401
from opteryx.compiled.planner.binder_core import locate_identifier_in_loaded_schemas  # noqa: F401
from opteryx.compiled.planner.binder_core import merge_schemas  # noqa: F401
from opteryx.compiled.planner.binder_core import traversive_recursive_bind  # noqa: F401

# Aggregate return-type inference for the binder. Aggregates are dispatched by
# the physical aggregate operators (not the function catalog), but the binder
# still needs to know the result type so expressions like `0.2 * AVG(col)` and
# downstream comparisons type-check correctly.
_AGGREGATE_RESULT_INTEGER = frozenset(
    {"COUNT", "COUNT_DISTINCT", "DISTINCT", "APPROX_COUNT_DISTINCT", "GROUPING"}
)
_AGGREGATE_RESULT_PASSTHROUGH = frozenset({"SUM", "MIN", "MAX", "ANY_VALUE"})
# MEDIAN and PERCENTILE_CONT always return DOUBLE — the native exact-quantile sink
# emits FLOAT64 and non-numeric inputs are rejected outright. Typing them as DOUBLE
# (not input-passthrough) keeps the binder honest with the runtime, the same way
# AVG is forced to DOUBLE below.
# STDDEV/STDDEV_POP/STDDEV_SAMP/VAR_POP/VAR_SAMP: always DOUBLE regardless of
# input numeric type — the native sink rejects DECIMAL input outright
# (native_group_sinks.hpp), so there's no AVG-style decimal-passthrough case to
# handle. STDDEV_POP is a pure alias for STDDEV (population stddev, N
# denominator); STDDEV_SAMP/VAR_POP/VAR_SAMP are new finalizations over the
# same accumulated Σx/Σx²/count.
# CORR: Pearson correlation over two numeric operand columns — always DOUBLE
# (NULL when undefined), same DECIMAL rejection posture as STDDEV/MEDIAN.
_AGGREGATE_RESULT_DOUBLE = frozenset(
    {
        "APPROX_PERCENTILE",
        "CORR",
        "MEDIAN",
        "PERCENTILE_CONT",
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "VAR_POP",
        "VAR_SAMP",
    }
)

# ---------------------------------------------------------------------------
# CASE THEN/ELSE branch-type validation.
#
# This is deliberately its own family model, not the COALESCE/IFNULL/IIF one
# (opteryx.expression.functions.registrar._check_blend_compatible): CASE may
# lower to the native draken_if_then_else kernel (draken/ops/kernels/
# function_kernels.cpp), whose supported-type surface differs from nc_dispatch
# — notably it DOES support DECIMAL/DECIMAL128 (with a widening exception),
# which nc_dispatch explicitly rejects. draken_if_then_else also does no
# promotion of its own beyond that one exception ("Non-NULL branch types must
# already match"), so a compatible-but-differently-typed pair (e.g. INT8 vs
# INT64) is CAST-aligned below rather than rejected.
_CASE_BLEND_FAMILIES = (
    frozenset((LogicalCategory.BOOLEAN,)),
    frozenset((LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR, LogicalCategory.VARBINARY)),
    frozenset((LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL)),
    frozenset((LogicalCategory.DATE, LogicalCategory.TIME, LogicalCategory.TIMESTAMP)),
    # UUID blends only with UUID: 16 fixed bytes (draken_type_fixed_itemsize), so
    # draken_if_then_else carries it like any fixed-width scalar.
    frozenset((LogicalCategory.UUID,)),
)

# Categories draken_if_then_else can blend AT ALL, independent of what it is
# paired with. This is a strictly WIDER question than _CASE_BLEND_FAMILIES above,
# which only answers "can these two share one type" — INTERVAL is blendable by the
# kernel (draken_type_fixed_itemsize == 16) yet belongs to no pair-family, so it
# must be listed here or `CASE WHEN c THEN NULL ELSE <interval> END`, which works
# today, would start being refused.
#
# The kernel's real surface is: BOOL, fk_is_string (VARCHAR/NVARCHAR/VARBINARY),
# and every type with a non-zero draken_type_fixed_itemsize. What is left out —
# ARRAY, VARIANT, VECTOR — hits `fk_fixed_elem_size(out_t) == 0` and dies with a
# bare "unsupported branch type" mid-execution.
_CASE_BLENDABLE_CATEGORIES = frozenset(
    cat for family in _CASE_BLEND_FAMILIES for cat in family
) | frozenset((LogicalCategory.INTERVAL,))


def _describe_case_branch(node) -> str:
    if node.node_type == 42:  # NodeType.LITERAL
        return f"literal {node.value!r}"
    if node.node_type == 38:  # NodeType.IDENTIFIER
        name = node.query_column or node.source_column or node.value
        return f"column '{name}'"
    return "expression"


def _check_case_branches_compatible(typed_branches, case_column) -> None:
    """Reject a CASE whose THEN/ELSE branches can never share one type — e.g. a
    scalar branch against ARRAY/VECTOR/VARIANT. draken_if_then_else has no
    family for these at all (fk_fixed_elem_size returns 0), so no CAST could
    ever align them; every other combination within one family in
    _CASE_BLEND_FAMILIES is CAST-aligned by the caller, not rejected here.
    """
    # Blendability is a per-branch property, so it is checked BEFORE the pairwise
    # family test and regardless of how many branches survived NULL-filtering.
    # `typed_branches` is what is left after the caller drops typed-NULL literals,
    # so `CASE WHEN c THEN NULL ELSE <array> END` — and the implicit-NULL-ELSE form
    # `CASE WHEN c THEN <array> END` — arrive here with ONE entry. Returning early
    # on len < 2 sent both straight to the kernel, which failed mid-execution with
    # a bare "draken_if_then_else: unsupported branch type" and no SQL to name.
    for node_i, ct_i in typed_branches:
        if ct_i.category not in _CASE_BLENDABLE_CATEGORIES:
            raise IncompatibleTypesError(
                message=(
                    f"CASE: {_describe_case_branch(node_i)} is {ct_i}, which CASE "
                    "cannot blend — only BOOLEAN, string, and numeric/temporal "
                    f"scalar branches are supported. (column '{case_column}')"
                )
            )

    if len(typed_branches) < 2:
        return
    node0, ct0 = typed_branches[0]
    family0 = next((f for f in _CASE_BLEND_FAMILIES if ct0.category in f), None)
    for node_i, ct_i in typed_branches[1:]:
        family_i = next((f for f in _CASE_BLEND_FAMILIES if ct_i.category in f), None)
        if family0 is None or family_i is None or family0 is not family_i:
            raise IncompatibleTypesError(
                message=(
                    f"CASE: {_describe_case_branch(node0)} is {ct0} but "
                    f"{_describe_case_branch(node_i)} is {ct_i} — CASE branches must "
                    "share a compatible type (all BOOLEAN, all string, all "
                    "numeric/DECIMAL, or all DATE/TIME/TIMESTAMP). Use CAST to align "
                    f"the branches. (column '{case_column}')"
                )
            )


# The null/conditional family returns one ARGUMENT VERBATIM — a selection, exactly like
# CASE — so every argument that can supply the result must carry the result's own
# descriptor. Value = the index of the first such argument: IIF's arg 0 is the CONDITION
# and never a value. IFNOTNULL includes arg 0 because its declared type is derived over
# BOTH args (see draken_ifnotnull in function_null_conditional.cpp); leaving arg 0
# unaligned would trip the kernel's own branch-type check.
_NULL_CONDITIONAL_VALUE_ARG = {"COALESCE": 0, "IFNULL": 0, "IFNOTNULL": 0, "IIF": 1}


def _descriptor_carries_meaning(target) -> bool:
    """True when matching physical/category tags do NOT make two types the same
    value domain, so a coercion must compare the FULL ColumnType.

    DECIMAL's scale and TIMESTAMP/TIME's unit live on the LogicalType, never on the
    physical tag (§11/§14). `DECIMAL(10,2)` and `DECIMAL(10,4)` are both
    DRAKEN_DECIMAL; `timestamp[ms]` and `timestamp[us]` are both DRAKEN_TIMESTAMP64.
    Blending either pair on tag equality alone reinterprets one side's raw payload
    at the other's scale/unit — 100x off for decimals, and 2024-01-02 read as
    1970-01-20 for timestamps. Both were live silent-wrong-answer bugs.

    Unparameterized types cannot hide a difference like that, and keep the cheap
    tag comparison.
    """
    return target.logical is not None


def _bound_cast_node(source, target, *, plan_context):
    """CAST-wrap an already-bound expression to `target`, itself fully bound.

    The CAST node contract is the one the parser emits (logical_planner_builders.
    cast): `value` is the BARE type name and any parameters are LITERAL nodes in
    `parameters`. That is what the lowering reads — `compiled_expression._linearize`
    passes `value` to `resolve_cast` and `p.value for p in parameters` to the ctx
    allocator. Writing the parametrized DISPLAY form (`str(ColumnType)` →
    "DECIMAL(24, 4)") into `value` instead builds a CAST no resolver can match, and
    the query dies at compile time with "CAST DECIMAL → DECIMAL(24, 4) is not
    supported".
    """
    parameters = []
    if target.category == LogicalCategory.DECIMAL:
        value = "DECIMAL"
        parameters = [
            Literal(value=int(target.logical.precision), type=_lt.INT64, arena=plan_context.expressions),
            Literal(value=int(target.logical.scale), type=_lt.INT64, arena=plan_context.expressions),
        ]
    elif target.category in (LogicalCategory.TIMESTAMP, LogicalCategory.TIME):
        # Same rule as DECIMAL above, for the same reason. `str(ColumnType)` now
        # carries the unit ("TIMESTAMP[ms]") so that a PERSISTED type does not
        # silently read back as the default — but that display form matches no
        # resolver arm either. The unit does not need to travel in the name here:
        # the rescale kernel reads the target unit off the binder-declared result
        # ColumnType (schema_column below), never off the type string.
        value = "TIMESTAMP" if target.category == LogicalCategory.TIMESTAMP else "TIME"
    else:
        value = str(target)
    return Cast(
        left=source,
        value=value,
        parameters=parameters,
        alias=source.alias,
        schema_column=plan_context.columns.computed(ExpressionColumn, "", column_type=target),
        arena=plan_context.expressions,
    )


def _operand_column_type(operand):
    """Build a ColumnType for a binary-op operand, or None if it can't be expressed.

    Used by D-2 (`compute_result_logical_type`) at the binder. Returns None when the
    operand isn't a numeric type the result-derivation rules know how to handle —
    the caller then skips parameter derivation (the LogicalCategory-only result stands).

    D-4 Phase 2: the operand's bound schema_column `column_type` is the
    authoritative unified type — use it directly (it carries the LogicalType
    descriptor for DECIMAL, etc.). Operands reach here already bound, literals
    included.
    """
    sc = operand.schema_column
    if sc is not None and sc.column_type is not None:
        ct = sc.column_type
        cat = ct.category
        if cat == LogicalCategory.DECIMAL:
            return ct
        if cat == LogicalCategory.INTEGER:
            # INTEGER operands are treated as scale-0 decimals by the runtime's
            # decimal_operand_scale_prec — represent as DRAKEN_INT64 ColumnType.
            return ct
        # Other categories aren't handled by _decimal_result; return None.
    return None


def _aggregate_operand_type(node: Expression) -> Optional[_ColumnType]:
    """The bound ColumnType of an aggregate's first operand, or None when it is
    unknown (no operand, an unbound expression, or a NULL-typed operand)."""
    if not node.parameters:
        return None
    param = node.parameters[0]
    sc = param.schema_column
    if sc is not None:
        param_type = sc.column_type  # ColumnType or None
    elif param.node_type == NodeType.LITERAL:
        param_type = param.type  # ColumnType from Phase 2
    else:
        param_type = None
    if param_type is None or param_type.category in (None, LogicalCategory.NULL):
        return None
    return param_type


def _aggregate_return_type(node: Expression) -> Optional[_ColumnType]:
    """Best-effort result-type inference for aggregate functions.

    Returns ColumnType or None (never LogicalCategory — callers use the result
    directly as a ColumnType carrier).
    """
    name = node.value
    if name in _AGGREGATE_RESULT_INTEGER:
        return _CT_INT64
    if name in _AGGREGATE_RESULT_DOUBLE:
        return _CT_FLOAT64
    if name == "ARRAY_AGG":
        # The array's elements ARE the operand's values, so the element type is the
        # operand's (the runtime child carries that same physical tag — DATE32 stays
        # DATE32, DECIMAL stays DECIMAL). Consumers that coerce against the element
        # at bind time (`item = ANY(arr)`'s DECIMAL rescale, the temporal-literal
        # check) need it. VARIANT only when the operand's type is itself unknown.
        operand_type = _aggregate_operand_type(node)
        return _lt.ARRAY(operand_type if operand_type is not None else _lt.VARIANT)
    if name == "CIDR_AGG":
        # Unlike ARRAY_AGG, the element type is FIXED and known here: CIDR_AGG
        # always renders blocks as text regardless of its operand, so there is
        # nothing to infer and no reason to fall back to VARIANT.
        return _lt.ARRAY(_lt.VARCHAR)
    if name in _AGGREGATE_RESULT_PASSTHROUGH or name == "AVG":
        # SUM/MIN/MAX/ANY_VALUE pass through the input column's type. AVG is a ratio,
        # not a value drawn from the data, so it returns DOUBLE for both INTEGER and
        # DECIMAL inputs (matches DuckDB and the runtime: the AVG collector divides as
        # double). Typing AVG(DECIMAL) as DOUBLE keeps the binder honest with the
        # runtime — an earlier DECIMAL passthrough was a latent lie.
        param_type = _aggregate_operand_type(node)
        if param_type is not None:
            if name == "AVG" and param_type.category in (
                LogicalCategory.INTEGER,
                LogicalCategory.DECIMAL,
            ):
                return _CT_FLOAT64
            if name == "SUM" and param_type.category == LogicalCategory.INTEGER:
                # SUM over integer-family operands always accumulates and emits
                # INT64 at runtime (native_group_sinks.hpp: exact int64 sum),
                # regardless of the input column's width (INT8/16/32). Binding
                # the result to the narrower input type is a lie that later
                # breaks callers (e.g. UNION type-unification) which pick a
                # cast kernel from the bound type but receive INT64 data.
                return _CT_INT64
            return param_type  # ColumnType
    return None


def _bind_function_reference(node: Expression, context: Any):
    """Resolve a FUNCTION node against the catalog and attach what execution needs.

    The catalog owns type reasoning, so this is also where the overload — and with
    it the kernel the expression compiler will call — is chosen. Both binding paths
    for a FUNCTION go through here so they cannot drift: the one that mints a new
    derived column, and the one that adopts a derived column an earlier occurrence
    of the same expression already registered. Returns the resolution (None when
    the catalog does not know the name, e.g. an AGGREGATOR).
    """
    try:
        resolved = _get_function_catalog().resolve(node.value, list(node.parameters))
    except TypeError as type_err:
        raise IncompatibleTypesError(message=str(type_err)) from type_err
    if resolved is None:
        return None
    node.function_ref = resolved
    # MATCH is `cosine_similarity(a, b) >= threshold`. The threshold is a
    # session variable, but the kernel is handed it in a ctx at BIND time
    # (the same channel EMBED's width uses): a compiled plan must keep
    # answering the question it was compiled for, so a later SET cannot
    # reach back and change it. Resolved here because this is where the
    # session's variables are in scope.
    if resolved.function_definition.name == "_MATCH_AGAINST":
        node.match_threshold = context.execution_context.variables["match_threshold"]
    return resolved


def bind_correlated_subquery(node: Expression, context: Any) -> Tuple[Expression, Dict]:
    """
    Bind the plan of a subquery that appears inside an expression.

    `node.value` is a whole LogicalPlan. It is bound with a child scope
    (`open_correlated_scope`) so that:

      * names the subquery's own FROM provides resolve locally, and
      * names it does NOT provide resolve against the enclosing query and are
        tagged `is_outer_reference` — i.e. the correlation is identified by
        real name resolution rather than inferred from qualifiers.

    The subquery is left in place as a SUBQUERY node carrying a bound plan;
    removing it is decorrelation's job, which now runs post-bind with the
    orientation already settled.

    The returned context is the CALLER's, unchanged — a subquery's scope must
    not leak outwards.
    """
    from opteryx.planner.binder.common import BinderVisitor
    from opteryx.planner.logical_planner import LogicalPlanStepType

    # Plan steps whose output columns are exactly their input's: the value a
    # scalar subquery yields is declared further down (see below).
    output_pass_through = (
        LogicalPlanStepType.Limit,
        LogicalPlanStepType.Order,
        LogicalPlanStepType.Distinct,
    )

    subplan = node.value
    exit_point = subplan.exit_point()

    bound_subplan, _ = BinderVisitor().traverse(
        subplan, exit_point, context=context.open_correlated_scope()
    )
    node.value = bound_subplan

    # A CORRELATED reference is read by the subquery, not by the enclosing SELECT /
    # GROUP BY, so nothing in the outer query NAMES it — and the bind-time narrowing
    # in aggregate.py / project.py keeps only what is named. `SUM(CASE WHEN EXISTS
    # (SELECT 1 FROM q WHERE q.id = p.id) THEN 1 ELSE 0 END) ... GROUP BY g` therefore
    # dropped `p.id` from the outer scan's schema, and decorrelation later built a
    # join whose probe key no longer existed in the stream. This is exactly the
    # "read structurally rather than by name" case `retained_columns` exists for —
    # the same one CROSS JOIN UNNEST's source column has. Registered on the CALLER's
    # context, which is the scope that has to keep the column alive.
    for _nid, subnode in bound_subplan.nodes(True):
        conditions = [subnode.condition] if subnode.node_type == LogicalPlanStepType.Filter else []
        for expression in list(subnode.columns or []) + conditions:
            for identifier in get_all_nodes_of_type(expression, (NodeType.IDENTIFIER,)):
                if identifier.is_outer_reference and identifier.schema_column is not None:
                    context.retained_columns.add(identifier.schema_column.identity)

    # A scalar subquery IS a value, so it has a type: that of the single column its
    # plan emits. Publishing it lets the ENCLOSING expression bind normally —
    # without it, `x < (SELECT AVG(y)) * 2` binds the Multiply while its operand is
    # still typeless, leaving the arithmetic with no result type and the engine
    # unable to select a kernel. Decorrelation later swaps this node for a
    # reference to the very same column, so the types agree by construction.
    # The column-declaring node is not always the head: LIMIT / ORDER BY /
    # DISTINCT bind as pass-through steps carrying no columns of their own, so
    # `(SELECT x FROM t LIMIT 1)` heads with a column-less Limit node. Descend
    # through single-input, column-less steps to the node that declares the
    # output (the Project). Publishing nothing here is not a safe fallback —
    # the SELECT-list binder requires every top-level column to carry a
    # schema_column, so a silently unpublished one crashed project.py with an
    # AttributeError instead of binding the LIMIT/DISTINCT shapes the runtime
    # cardinality guard exists to admit.
    #
    # "Column-less" is not the test on its own: an ORDER BY step lists its SORT
    # expressions in `columns`, which are not its output. `(SELECT o_totalprice ...
    # ORDER BY o_orderdate DESC LIMIT 1)` stopped at the Order step and published
    # o_orderdate's DATE as the subquery's type, so `(...) > 100000` was refused as
    # DATE vs INTEGER. LIMIT, ORDER BY and DISTINCT pass their input's columns
    # through unchanged, so they are descended whatever `columns` they carry.
    top_nid = bound_subplan.exit_point()
    top = bound_subplan[top_nid]
    while not top.columns or top.node_type in output_pass_through:
        feeders = bound_subplan.ingoing_edges(top_nid)
        if len(feeders) != 1:
            break
        top_nid = feeders[0][0]
        top = bound_subplan[top_nid]
    columns = list(top.columns or [])
    if len(columns) == 1 and columns[0].schema_column is not None:
        node.schema_column = columns[0].schema_column

    return node, context



def _bind_reused(node, context, schema_name, schema, found_column, column_name, format_cache, relations_memo):
    """Bind `node` as a repeat of an expression this scope already computed:
    `found_column` is the column `schema` (`schema_name` in scope) holds under the
    node's rendered name. Called by the compiled core (binder_core.inner_binder)."""
    # A column found in `$project` was MATERIALISED by the Project below this
    # step: the node is a reference to that output, and its children are not
    # in scope — the logical planner deliberately drops `id` from the
    # Project's passthrough when `id + 1` is projected, so `SELECT id + 1 ...
    # ORDER BY id + 1` has no `id` above the Project. Binding the children
    # there raised ColumnNotFoundError on `id`. (This bind used to sit under
    # `suppress(Exception)`, which hid exactly that failure; it was removed.)
    # Anywhere else — `$derived` (a repeat within the computing step) or a
    # source relation — the children ARE in scope and are bound.
    if schema_name != "$project":
        node, _ = traversive_recursive_bind(node, context, format_cache, relations_memo)

    node.schema_column = found_column
    node.query_column = node.alias or column_name

    # A reuse still NAMES an output, so its alias is published onto the shared
    # column exactly as the identifier path publishes one (below, where an
    # identifier binds) and as the mint path does when it creates the column.
    # This was the one bind path that dropped it: `SELECT id + 1 AS a,
    # id + 1 AS b ... ORDER BY b` minted the column as `a`, reused it for `b`
    # without recording `b`, and visit_project re-publishes only the FIRST
    # node's alias per identity — so nothing above the Project could resolve
    # `b` and ORDER BY raised ColumnNotFoundError.
    # (An alias is a new ALIAS slot swapped into the scope - see
    # locate_identifier.)
    if node.alias and node.alias not in found_column.all_names:
        renamed = context.plan_context.columns.alias(
            found_column,
            found_column.name,
            aliases=[*(found_column.aliases or []), node.alias],
            origin=found_column.origin,
        )
        _rebind(
            context,
            context.schemas,
            schema,
            schema.with_columns(
                renamed if candidate is found_column else candidate
                for candidate in schema.columns
            ),
        )
        found_column = renamed
        node.schema_column = found_column

    if isinstance(found_column, ConstantColumn):
        # A repeat of a constant (a nullary constant function — PI(), E() —
        # folded to its value) IS that constant: a new LITERAL in its place.
        node = Literal(
            value=native_literal_value(found_column.value, found_column.column_type),
            type=found_column.column_type,
            origin=node.origin_id,
            alias=node.alias,
            query_column=node.query_column,
            schema_column=found_column,
            relations=node.relations,
            arena=context.plan_context.expressions,
        )
    context.reused_expressions[id(node)] = node

    # Sharing the schema column is not the same as not needing to be bound.
    # This node keeps its FUNCTION shape, and the expression compiler reads
    # `function_ref` off the NODE (compiled_expression's NT_FUNCTION arm) to
    # find the kernel — a second occurrence that only inherited an identity
    # reached the compiler with function_ref None and raised
    # "FUNCTION 'UPPER' has no function_ref - not bound". Visible wherever
    # two occurrences of one call are compiled as separate expressions
    # rather than one being an identifier reference to the other, which is
    # exactly what the CASE -> IF_THEN_ELSE rewrite does with
    # `CASE WHEN ... THEN UPPER(name) ELSE UPPER(name) END`.
    #
    # Read the node's CURRENT type, not the `node_type` local captured
    # before this block ran. The ConstantColumn arm above REPLACES the node
    # with a LITERAL (a nullary constant function — PI(), E() — folds to its
    # value), which leaves the local stale and node.value holding a float.
    # Binding a function reference off that called the catalog's
    # `resolve(name: str)` with 3.14159..., and Cython's argument coercion
    # raised `Argument 'name' has incorrect type (expected str, got float)`
    # from inside _bind_function_reference — surfacing as an
    # IncompatibleTypesError that named neither the function nor the query.
    # `SELECT PI() AS a, PI() AS b` was unrunnable, and so was any second
    # mention of one nullary constant function in a projection, while
    # `SELECT PI() AS a` alone was fine: it takes the mint-a-new-column path
    # and never reaches here.
    #
    # Not for a `$project` reference (above): it is never compiled as an
    # expression, and its parameters are deliberately left unbound, so there
    # is no overload to choose.
    if (
        schema_name != "$project"
        and node.node_type == NodeType.FUNCTION
        and node.function_ref is None
    ):
        _bind_function_reference(node, context)

    return node, context


def _bind_new_column(node, node_type, column_name, context, schemas):
    """Mint the column a FUNCTION / AGGREGATOR / CASE / CAST / operator node
    computes, its children already bound, and add it to the scope's `$derived`
    (`schemas`, the scope as it was before the children bound). Returns the node,
    which a fixed-value function replaces by its LITERAL. Called by the compiled
    core (binder_core.inner_binder)."""
    if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
        # we need to add this new column to the schema
        aliases = [node.alias] if node.alias else []
        result_type = None
        fixed_function_result = None
        if len(node.parameters) == 0:
            result_type, fixed_function_result = fixed_value_function(node.value, context)
        if result_type:
            # fixed_value_function returns (ColumnType, value) directly — no lookup needed.
            schema_column = context.plan_context.columns.constant(
                column_name,
                column_type=result_type,
                aliases=aliases,
                value=fixed_function_result,
                nullable=False,
            )
            # A fixed-value function IS its value: a new LITERAL in its place.
            node = Literal(
                type=result_type,
                value=native_literal_value(fixed_function_result, result_type),
                origin=node.origin_id,
                alias=node.alias,
                query_column=node.query_column,
                relations=node.relations,
                arena=context.plan_context.expressions,
            )
        else:
            element_type = None  # for types with elements (ARRAYs)
            precision = 38  # Maximum precision for Decimal128
            scale = 21  # A reasonable scale that's less than precision

            # DECIMAL carries precision/scale as parameters — extract before catalog lookup
            if node.value == "DECIMAL" and len(node.parameters) > 1:
                precision = node.parameters[1].value
                scale = node.parameters[2].value if len(node.parameters) > 2 else scale

            # Ask the catalog for the return type (catalog owns type reasoning)
            _resolved = _bind_function_reference(node, context)
            if _resolved is not None:
                result_type = _resolved.inferred_return_type
                element_type = _resolved.inferred_element_type
            elif node_type == NodeType.AGGREGATOR:
                # Aggregates are not in the function catalog (dispatched at
                # runtime by the aggregate operators). The binder still needs
                # a result type so expressions like `0.2 * AVG(col)` and
                # downstream comparisons type-check correctly.
                result_type = _aggregate_return_type(node)
                if result_type is None:
                    result_type = _CT_NULL
            else:
                result_type = _CT_NULL  # unknown function; type resolved at runtime

            # Phase 5: result_type is always ColumnType (catalog + aggregate + fallback).
            _result_type_lc = result_type.category if result_type is not None else None

            # Literal coercion: binder's job — mutate AST nodes to match the resolved type.
            # This is NOT type inference; it's making literals consistent with the
            # surrounding expression's type after the catalog has declared the return
            # type. Runs over VALUE args only (_value_arg_start below excludes IIF's
            # condition) so it now covers IIF's literal branches too, not just
            # COALESCE/IFNULL/IFNOTNULL — those previously fell through to the
            # descriptor-coercion pass below, which retypes but (for the non-DECIMAL
            # case it was written for) never needed to re-quantize a value's digits.
            #
            # Builds a FRESH Node + ConstantColumn per coerced literal rather than
            # mutating `param`/`param.schema_column` in place: a bare literal like
            # `0` is commonly INTERNED — the same ConstantColumn object shared by
            # every textually-identical "0" elsewhere in the query (found_column
            # adoption above, and again downstream when other passes re-resolve a
            # copied subtree by name). Retyping that SHARED object to this one
            # call's result type silently retyped every OTHER "0" in the query too:
            # `COALESCE(ws_qty, 0)` (INTEGER) picked up `COALESCE(ws_wc, 0)`'s
            # DECIMAL(7,2) literal type from a later-bound copy, and
            # `IFNULL(ws_qty,0) + IFNULL(cs_qty,0)` died with "not supported" for
            # an INTEGER+DECIMAL mix that was never really there.
            _fn_name = (
                _resolved.function_definition.name if _resolved is not None else node.value
            )
            _value_arg_start = _NULL_CONDITIONAL_VALUE_ARG.get(_fn_name)
            if _value_arg_start is not None and _result_type_lc not in (
                None,
                LogicalCategory.NULL,
            ):
                _params = list(node.parameters)
                for _i in range(_value_arg_start, len(_params)):
                    param = _params[_i]
                    if not (
                        param.node_type == NodeType.LITERAL
                        and param.value is not None
                        and param.value != set()
                    ):
                        continue
                    _new_value = native_literal_value(
                        parse_value(_result_type_lc, param.value), result_type
                    )
                    if _result_type_lc == LogicalCategory.DECIMAL and result_type.logical is not None:
                        # A written literal's digits are not re-quantized to the
                        # declared scale by parse_value/_parse_decimal (it keeps
                        # whatever str(value) produced) — an inexact value would
                        # silently misread at the wrong scale downstream:
                        # _materialise_constant_literal derives its physical tier
                        # from precision alone and never re-rounds. Quantize here
                        # and fail loud instead of letting a wrong value through.
                        # An explicit widened context: opteryx/__init__.py pins
                        # getcontext().prec = 28 globally, but DECIMAL128 holds up
                        # to 38 significant digits — quantizing under the ambient
                        # (28) context would silently round/reject a legitimate
                        # wide value before it ever reaches the engine.
                        _quantum = decimal.Decimal(1).scaleb(-int(result_type.logical.scale))
                        _rescaled = _new_value.quantize(_quantum, context=decimal.Context(prec=38))
                        if _rescaled != _new_value:
                            raise IncompatibleTypesError(
                                message=(
                                    f"{node.value}: literal {param.value!r} cannot be "
                                    f"represented exactly as {result_type} — CAST to "
                                    "align the branches."
                                )
                            )
                        _new_value = _rescaled
                    if _new_value == param.value and result_type == param.type:
                        continue
                    _new_param = param.copy()
                    _new_param.value = _new_value
                    _new_param.type = result_type
                    _old_sc = param.schema_column
                    _new_param.schema_column = context.plan_context.columns.constant(
                        _old_sc.name if _old_sc is not None else str(_new_value),
                        column_type=result_type,
                        aliases=list(_old_sc.aliases) if _old_sc is not None and _old_sc.aliases else [],
                        value=_new_value,
                        nullable=False,
                    )
                    _params[_i] = _new_param
                node.parameters = _params

            # Descriptor coercion for the same family (plus IIF). These
            # functions hand back one argument VERBATIM, so an argument whose
            # descriptor differs from the declared result reinterprets its own
            # payload at the wrong scale/unit — COALESCE(ts_ms, ts_us) returned
            # 1970-01-20 for a 2024 timestamp. The kernel cannot catch it:
            # nc_dispatch types on the physical tag, and the DrakenVector
            # carries neither scale nor unit (§11/§14), so both branches look
            # identical to it. Alignment therefore has to happen HERE, at bind
            # time — the same move as the CASE branch coercion below and the
            # UNION-leg coercion in set_ops.
            #
            # Two different rules, by family:
            #   - DECIMAL/DECIMAL128 (either side): ALWAYS CAST on any mismatch,
            #     regardless of whether the physical tag matches. DECIMAL cannot
            #     self-promote — scale is out-of-band on the vector — so unlike
            #     the plain-fixed family below, there is no kernel-side promotion
            #     to defer to. This is what lets COALESCE(decimal_col, 0.0) reach
            #     nc_dispatch with the decimal_col branch already rescaled/
            #     retagged to the resolved result type (FLOAT64 once FLOAT wins,
            #     find_compatible_type; or an exact DECIMAL(p,s) otherwise).
            #   - everything else: limited to same-physical descriptor mismatches
            #     (TIMESTAMP/TIME unit). A CROSS-physical pair (narrow int vs
            #     INT64, int vs float) is the kernel's own promotion to make
            #     (nc_promote_fixed), and inserting casts there would take work
            #     off a path that is already correct.
            if _value_arg_start is not None and result_type is not None:
                _params = list(node.parameters)
                for _i in range(_value_arg_start, len(_params)):
                    _arg = _params[_i]
                    _arg_sc = _arg.schema_column
                    if _arg_sc is None or _arg_sc.column_type is None:
                        continue
                    _arg_ct = _arg_sc.column_type
                    # A typed NULL has no value to convert (mirrors the NULL-literal
                    # case in set_ops._cast_leg_columns_to).
                    if _arg_ct == result_type or _arg_ct == _CT_NULL:
                        continue
                    if (
                        _arg_ct.category == LogicalCategory.DECIMAL
                        or result_type.category == LogicalCategory.DECIMAL
                    ):
                        _params[_i] = _bound_cast_node(
                            _arg, result_type, plan_context=context.plan_context
                        )
                        continue
                    if _arg_ct.physical != result_type.physical:
                        continue
                    if not _descriptor_carries_meaning(result_type):
                        continue
                    _params[_i] = _bound_cast_node(
                        _arg, result_type, plan_context=context.plan_context
                    )
                node.parameters = _params

            # Phase 5: result_type is ColumnType — use directly.
            _ct = result_type
            schema_column = context.plan_context.columns.computed(
                FunctionColumn,
                column_name,
                column_type=_ct,
                aliases=aliases,
            )
        _append_derived(context, schemas, schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif node_type == NodeType.CASE:
        aliases = [node.alias] if node.alias else []
        branch_nodes = list(node.results or [])
        if node.else_result is not None:
            branch_nodes.append(node.else_result)
        typed_branches = [
            (branch, branch.schema_column.column_type)
            for branch in branch_nodes
            if branch.schema_column is not None
            and branch.schema_column.column_type is not None
            and branch.schema_column.column_type != _CT_NULL
        ]
        _check_case_branches_compatible(typed_branches, node.alias or column_name)

        # The promoted common type across every branch (mirrors COALESCE/
        # IFNULL and the UNION-leg coercion in set_ops.py) rather than
        # "whichever branch came first" — D-4 Phase 2 carries the full
        # ColumnType (precision/scale/element_type), not just a category.
        # Short-circuit when every branch already carries the identical
        # ColumnType: find_compatible_type's DECIMAL path has no "already
        # equal" fast case and always recomputes a precision/scale via
        # Plus-style promotion (or a wide DECIMAL(38,18) default for a
        # single input) — widening a same-scale-everywhere CASE for no
        # reason.
        #
        # Otherwise, a DECIMAL mix uses the SELECTION rule
        # (compute_selection_result_type), not find_compatible_type's
        # additive one: CASE returns one branch verbatim, so the result
        # only has to REPRESENT each branch. Plus-style promotion sized
        # `THEN decimal_col ELSE 0` for `decimal_col + 0` and pushed an
        # ordinary DECIMAL(15,2) to DECIMAL(22,2), across the int64/int128
        # tier. Everything else (strings, temporals, the FLOAT mix) keeps
        # find_compatible_type.
        _branch_cts = [ct for _, ct in typed_branches]
        if _branch_cts and all(ct == _branch_cts[0] for ct in _branch_cts):
            result_ct = _branch_cts[0]
        elif _branch_cts:
            result_ct = compute_selection_result_type(
                [
                    (
                        ct,
                        branch.value
                        if branch.node_type == NodeType.LITERAL
                        else NOT_LITERAL,
                    )
                    for branch, ct in typed_branches
                ]
            )
            if result_ct is None:
                result_ct = _lt.find_compatible_type(_branch_cts)
        else:
            result_ct = None

        # Coerce LITERAL branches to the resolved result type in place;
        # CAST-wrap every other (non-literal) branch whose physical type
        # doesn't already match. draken_if_then_else — the native kernel
        # this CASE may lower to — blends identical-physical-type branches
        # only (plus a DECIMAL/DECIMAL128 widening exception) and performs
        # no promotion of its own, so every branch must carry the same
        # physical type by the time it reaches execution.
        if result_ct is not None:
            _result_cat = result_ct.category

            def _coerce_case_branch(branch):
                if branch is None:
                    return branch
                sc = branch.schema_column
                if sc is None or sc.column_type is None or sc.column_type == _CT_NULL:
                    return branch
                if branch.node_type == NodeType.LITERAL and branch.value is not None:
                    branch.value = native_literal_value(
                        parse_value(_result_cat, branch.value), result_ct
                    )
                    branch.type = result_ct  # ColumnType
                    branch.schema_column = context.plan_context.columns.retype(
                        sc, result_ct
                    )
                    return branch
                if sc.column_type == result_ct:
                    return branch
                # A matching physical tag is enough only for types whose tag
                # tells the whole story — see _descriptor_carries_meaning.
                # A decimal at another scale, or a timestamp at another unit,
                # goes through a real rescale cast instead of blending raw.
                if (sc.column_type.physical == result_ct.physical
                        and not _descriptor_carries_meaning(result_ct)):
                    return branch
                # Inserted fully bound (own schema_column) — this runs
                # inside the CASE's own bind step, after its branches are
                # already bound, so nothing will traverse into a freshly-
                # inserted CAST to bind it a second time.
                return _bound_cast_node(branch, result_ct, plan_context=context.plan_context)

            node.results = [_coerce_case_branch(b) for b in (node.results or [])]
            if node.else_result is not None:
                node.else_result = _coerce_case_branch(node.else_result)
        schema_column = context.plan_context.columns.computed(
            FunctionColumn,
            column_name,
            column_type=result_ct,
            aliases=aliases,
        )
        _append_derived(context, schemas, schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif node_type == NodeType.CAST:
        # Handle CAST operations (CAST(expr AS type))
        # The source expression is already bound via recursive traversal above
        # node.value contains the target type name (e.g., "VARCHAR", "INTEGER", "DOUBLE", "BLOB")

        # Define aliases for the schema column
        aliases = [node.alias] if node.alias else []

        # Map type name to LogicalCategory
        target_type_name = node.value.upper()
        # Strip TRY_ prefix for safe casts — the prefix is kept in node.value for the evaluator
        if target_type_name.startswith("TRY_"):
            target_type_name = target_type_name[4:]

        # Extract unit from internal temporal type forms (e.g., "_TIMESTAMP_MS" → unit="ms")
        unit = None
        if target_type_name.startswith("_TIMESTAMP_"):
            # Internal form detected - extract unit
            unit_map = {
                "_TIMESTAMP_NS": "ns",
                "_TIMESTAMP_MS": "ms",
                "_TIMESTAMP_S": "s",
                "_TIMESTAMP_US": "us",
                "_TIMESTAMP_DAYS": "days",
            }
            if target_type_name in unit_map:
                unit = unit_map[target_type_name]
                target_type_name = "TIMESTAMP"
            else:
                raise IncompatibleTypesError(
                    message=f"Unknown temporal unit form: {target_type_name}. "
                    "Use the public form: `CAST(expr AS TIMESTAMP[ns])`, "
                    "`CAST(expr AS TIMESTAMP[ms])`, `CAST(expr AS TIMESTAMP[s])`, `CAST(expr AS TIMESTAMP[us])`, or `CAST(expr AS TIMESTAMP[d])`."
                )

        # Validate TIMESTAMP casts from INTEGER — require explicit unit
        if target_type_name == "TIMESTAMP" and node.left:
            source_type = determine_type(node.left)
            if source_type is not None and source_type.category == LogicalCategory.INTEGER:
                if unit is None:
                    raise IncompatibleTypesError(
                        message="Ambiguous cast: INTEGER → TIMESTAMP requires a unit. "
                        "Use `expr::TIMESTAMP[ms]`, `expr::TIMESTAMP[s]`, or `expr::TIMESTAMP[us]`."
                    )

        element_type = None
        precision = 38
        scale = 21

        # Handle type-specific parameters.
        # CAST(expr AS DECIMAL(precision, scale)): `node.parameters` carries the
        # TYPE's parenthesized arguments only — parameters[0]=precision,
        # parameters[1]=scale (the cast expression is in node.left/centre, NOT
        # parameters[0]). A prior off-by-one read parameters[1]/[2], so
        # `DECIMAL(32, 2)` resolved to a corrupt (precision=2, scale=21) — which
        # was silently dropped (column_type=None) until the D-4 invariant made it
        # fail loud. The runtime cast reads the params independently, so this only
        # corrects the schema-column metadata.
        if target_type_name == "DECIMAL":
            # Both parameters are always present here: a parsed cast without
            # them is refused by the cast builder (logical_planner_builders,
            # _decimal_needs_precision_and_scale), the only place that knows how
            # the user spelled it, and every synthesized DECIMAL cast carries
            # the pair from its target ColumnType.
            precision = int(node.parameters[0].value)
            scale = int(node.parameters[1].value)

        if (
            target_type_name == "ARRAY"
            and node.parameters is not None
            and len(node.parameters) > 0
        ):
            # CAST(expr AS ARRAY(element_type)) - extract the element type
            element_param = node.parameters[0]
            if element_param.node_type == NodeType.LITERAL and element_param.value is not None:
                element_type = parse_column_type(element_param.text().upper())
            else:
                element_type = _lt.VARIANT

        if target_type_name == "DECIMAL":
            _ct = _lt.DECIMAL(precision, scale)
        elif target_type_name == "ARRAY":
            _ct = _lt.ARRAY(element_type if element_type is not None else _lt.VARIANT)
        else:
            _ct = parse_column_type(target_type_name)
        schema_column = context.plan_context.columns.computed(
            FunctionColumn,
            column_name,
            column_type=_ct,
            aliases=aliases,
        )
        # NO identity override here. Identity is the engine's per-column handle
        # and MUST be unique; FunctionColumn.__post_init__ mints `$derived_<rand>`
        # for exactly that reason. This arm used to stamp the RENDERED EXPRESSION
        # TEXT over that minted id, so every CAST that rendered the same string
        # became the SAME column — across scopes, across relations, across join
        # legs. Two derived tables each computing `CAST(ts AS TIMESTAMP)` over
        # their own `ts` both got identity `ts::TIMESTAMP`, and the join then
        # emitted one leg's values for both columns: a SILENT wrong answer.
        # Expression REUSE is matched by NAME (the `schema.find_column(column_name)`
        # lookup above), never by identity, so minting is safe here.
        _append_derived(context, schemas, schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif node_type == NodeType.COMPARISON_OPERATOR and node.value.startswith(
        (
            "AnyOp",
            "AllOp",
        )
    ):
        if node.right.node_type == NodeType.LITERAL:
            if type(node.right.value) is not tuple:
                try:
                    node.right.value = tuple(node.right.value)
                except TypeError as e:
                    raise IncompatibleTypesError(
                        message=f"Cannot construct ARRAY from incompatible types."
                    ) from e
            # LIKE/ILIKE patterns must all be strings.
            # ⛔ AnyOpNotLike / AnyOpNotILike are RETIRED, not live operators:
            # nothing constructs them any more. `NOT LIKE ANY` is refused in the
            # planner (its two readings differ, and the engine used to silently
            # answer the ALL one), and the AND'd-NOT-LIKE fusion now emits the
            # correctly named AllOpNotLike. They are listed here only so this
            # guard stays exhaustive over the family; do not read their presence
            # as evidence the spelling works.
            if node.value in (
                "AnyOpLike",
                "AnyOpNotLike",
                "AnyOpILike",
                "AnyOpNotILike",
                "AllOpLike",
                "AllOpNotLike",
                "AllOpILike",
                "AllOpNotILike",
            ):
                for pat in node.right.value:
                    if pat is not None and not isinstance(pat, (str, bytes)):
                        raise IncompatibleTypesError(
                            message=f"LIKE patterns must be strings, got {type(pat).__name__}."
                        )
        schema_column = context.plan_context.columns.computed(
            ExpressionColumn, column_name, column_type=_lt.BOOLEAN
        )
        node.schema_column = schema_column
        _append_derived(context, schemas, schema_column)
    else:
        # VARCHAR/NVARCHAR/VARBINARY literals must be coerced to bytes at bind
        # time, on EVERY comparison operator — not just InList/NotInList. The
        # execution engine's canonical string representation is bytes (VARCHAR
        # and VARBINARY are byte-identical past the type tag), and downstream
        # optimizer passes (e.g. statistics_refresh's range-constraint merge)
        # compare literal bounds from different operators against each other.
        # Coercing only the InList side left scalar comparisons (Eq/Lt/Gt/...)
        # holding `str` while InList held `bytes`, so a query mixing both against
        # the same column crashed with `'>' not supported between bytes and str`.
        #
        # This coercion is CONFINED TO THE STRING FAMILY, where str -> bytes is a
        # change of representation and nothing else: the literal's ColumnType tag
        # stays true after it. It must never cross a type category. It used to
        # also map FLOAT->float / INTEGER->int / BOOLEAN->bool, which rewrote the
        # VALUE while leaving `.type` and `schema_column.column_type` describing
        # the ORIGINAL literal — a vector whose payload and type tag disagree.
        # Two live wrong answers came out of that: `int_col = 4.5` silently
        # truncated the literal to 4 and matched row 4, and a DECIMAL(18,6)
        # literal against an INT64 column kept its DECIMAL tag over an int
        # payload, so `4` was read as 0.000004 and matched nothing. A numeric
        # literal is now left exactly as written, and the comparison kernels
        # resolve the mixed-type compare (or the engine refuses it loudly).
        if node_type == NodeType.COMPARISON_OPERATOR and node.value in (
            "Eq",
            "NotEq",
            "Lt",
            "LtEq",
            "Gt",
            "GtEq",
            "InList",
            "NotInList",
        ):
            from opteryx.types.logical_type import LogicalCategory as _OT

            def _literal_text(v):
                return v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v

            # UUID against a string LITERAL (docs/UUID_TYPE_DESIGN.md, ruling U7):
            # the literal is parsed strictly NOW and becomes a UUID literal —
            # value, type and schema column together, so payload and type tag
            # never disagree. A non-UUID string fails here, at bind. Only a
            # literal: a VARCHAR column against a UUID stays a type mismatch.
            def _retype_uuid_literal(literal_node, other_node):
                if other_node is None or other_node.schema_column is None:
                    return
                if other_node.schema_column.category != _OT.UUID:
                    return
                if literal_node.schema_column is None or literal_node.schema_column.category not in (
                    _OT.VARCHAR, _OT.NVARCHAR, _OT.ARRAY
                ):
                    return
                from opteryx.types.logical_type import ARRAY as _ARRAY
                from opteryx.types.logical_type import UUID as _UUID
                from opteryx.types.scalars.value_parsing import _parse_uuid
                from opteryx.types.scalars.value_parsing import is_uuid_text

                value = literal_node.value
                items = value if isinstance(value, (list, tuple)) else (value,)
                if not all(isinstance(v, (str, bytes)) or v is None for v in items):
                    return
                for v in items:
                    if v is not None and not is_uuid_text(v):
                        raise IncompatibleTypesError(
                            message=(
                                f"{_literal_text(v)!r} is compared with a UUID column but "
                                "is not a UUID - a UUID literal is 8-4-4-4-12 hex digits, "
                                "for example '550e8400-e29b-41d4-a716-446655440000'"
                            )
                        )
                if isinstance(value, (list, tuple)):
                    new_value = type(value)(None if v is None else _parse_uuid(v) for v in value)
                    new_type = _ARRAY(_UUID)
                else:
                    new_value = _parse_uuid(value)
                    new_type = _UUID
                _old_sc = literal_node.schema_column
                literal_node.value = new_value
                literal_node.type = new_type
                literal_node.schema_column = context.plan_context.columns.constant(
                    _old_sc.name,
                    column_type=new_type,
                    aliases=list(_old_sc.aliases) if _old_sc.aliases else [],
                    value=new_value,
                    nullable=False,
                )

            if node.right is not None and node.right.node_type == NodeType.LITERAL:
                _retype_uuid_literal(node.right, node.left)
            elif node.left is not None and node.left.node_type == NodeType.LITERAL:
                _retype_uuid_literal(node.left, node.right)

            _COERCE = {
                _OT.VARCHAR: lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                _OT.NVARCHAR: lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                _OT.VARBINARY: lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
            }

            def _coerce_literal(literal_node, other_node):
                other_type = (
                    other_node.schema_column.category
                    if other_node.schema_column is not None
                    else None
                )
                coerce = _COERCE.get(other_type)
                if coerce is None:
                    return
                # An IN-list literal is a collection. `build_literal_node` maps
                # BOTH list and tuple to ARRAY, and the two spellings reach here
                # from different front doors — SQL's `IN (...)` builds a list,
                # while a visibility filter's list is tuple-ised by
                # `dnf.simplify_dnf` (it needs hashable clauses to dedup). Handling
                # only `list` sent the whole tuple through the scalar branch.
                # Rebuild in the container we were given: the ARRAY tag does not
                # distinguish them, but nothing downstream should see its literal
                # silently change shape either.
                value = literal_node.value
                if isinstance(value, (list, tuple)):
                    literal_node.value = type(value)(
                        None if v is None else coerce(v) for v in value
                    )
                elif value is not None:
                    literal_node.value = coerce(value)

            if node.right is not None and node.right.node_type == NodeType.LITERAL:
                _coerce_literal(node.right, node.left)
            elif node.left is not None and node.left.node_type == NodeType.LITERAL:
                _coerce_literal(node.left, node.right)

        # Capture like_selectivity_decay at BIND time, before predicate_rewriter
        # later mutates node.value from Like/ILike/NotLike/NotILike to
        # InStr/IInStr/NotInStr/NotIInStr IN PLACE ON THIS SAME NODE (see
        # predicate_rewriter.INSTR_REWRITES) — the char-class LIKE selectivity
        # estimator (the native instr estimator, src/cpp/planner/selectivity.hpp)
        # reads it via plain attribute access. Same reasoning as match_threshold's
        # bind-time capture above: a compiled plan must keep answering the
        # selectivity question it was compiled for, so a later SET cannot reach
        # back and change an already-bound plan's estimate.
        if node_type == NodeType.COMPARISON_OPERATOR and node.value in (
            "Like",
            "ILike",
            "NotLike",
            "NotILike",
        ):
            node.like_selectivity_decay = context.execution_context.variables[
                "like_selectivity_decay"
            ]

        # The type-mismatch check recurses the whole subtree for AND/OR/XOR
        # nodes, but inner_binder already visits every comparison leaf
        # bottom-up (each gets its own check before its parent connective is
        # reached), so re-checking at each connective is pure O(n^2)
        # re-walking on deep predicate chains. Only check at the nodes that do
        # the actual per-comparison work; connectives just OR their children's
        # results, which are already covered.
        if node_type not in (NodeType.AND, NodeType.OR, NodeType.XOR):
            mismatches = get_mismatched_condition_column_types(node, relaxed=True)
            if mismatches:
                raise IncompatibleTypesError(**mismatches)

        # RLIKE's kernel takes only a compiled pattern, so compiling it is a
        # lowering every query needs, done here rather than in a strategy that
        # can be switched off. After the type check, so a wrongly-typed pattern
        # is reported as that.
        if node_type == NodeType.COMPARISON_OPERATOR and node.value in ("RLike", "NotRLike"):
            compile_rlike_pattern(node, plan_context=context.plan_context)

        result_type = determine_type(node)  # ColumnType | None (Phase 2)
        # D-2: when the result is DECIMAL, also derive (precision, scale).
        # determine_type() now returns ColumnType; use .category for LogicalCategory
        # comparisons. Only attempt parameter derivation for binary arithmetic on
        # numeric operands — other results (BOOLEAN comparisons, etc.) don't land here.
        _result_cat = result_type.category if result_type is not None else None
        result_ct_final = None
        if (
            _result_cat == LogicalCategory.DECIMAL
            and node_type == NodeType.BINARY_OPERATOR
            and node.value in ("Plus", "Minus", "Multiply", "Divide")
            and node.left is not None
            and node.right is not None
        ):
            left_ct = _operand_column_type(node.left)
            right_ct = _operand_column_type(node.right)
            if left_ct is not None and right_ct is not None:
                from opteryx.types.type_unification import compute_result_logical_type

                result_ct_final = compute_result_logical_type(
                    left_ct, right_ct, node.value, LogicalCategory.DECIMAL
                )
        elif _result_cat == LogicalCategory.DECIMAL and node_type == NodeType.NESTED:
            # NESTED is a parenthesised expression wrapping `node.centre`.
            # Inherit the centre's column_type directly (single source of truth).
            centre = node.centre
            centre_sc = centre.schema_column if centre else None
            if centre_sc is not None and centre_sc.column_type is not None:
                result_ct_final = centre_sc.column_type

        _schema_ct = result_ct_final if result_ct_final is not None else result_type
        schema_column = context.plan_context.columns.computed(
            ExpressionColumn,
            column_name,
            column_type=_schema_ct,
            aliases=[node.alias] if node.alias else [],
        )
        _append_derived(context, schemas, schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name


    return node
