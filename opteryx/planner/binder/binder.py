# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import copy
from contextlib import suppress
from typing import Any, Dict, Optional, Tuple

from opteryx.exceptions import (
    AmbiguousIdentifierError,
    ColumnNotFoundError,
    IncompatibleTypesError,
    InvalidInternalStateError,
    UnexpectedDatasetReferenceError,
    UnsupportedSyntaxError,
)
from opteryx.expression import NodeType
from opteryx.expression.functions import get_catalog as _get_function_catalog
from opteryx.expression.functions.registrar import fixed_value_function
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.join_helpers import get_mismatched_condition_column_types
from opteryx.planner.binder.operator_map import determine_type
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

# Aggregate return-type inference for the binder. Aggregates are dispatched by
# the physical aggregate operators (not the function catalog), but the binder
# still needs to know the result type so expressions like `0.2 * AVG(col)` and
# downstream comparisons type-check correctly.
_AGGREGATE_RESULT_INTEGER = frozenset(
    {"COUNT", "COUNT_DISTINCT", "DISTINCT", "APPROX_COUNT_DISTINCT"}
)
_AGGREGATE_RESULT_PASSTHROUGH = frozenset({"SUM", "MIN", "MAX", "ANY_VALUE"})
# MEDIAN always returns DOUBLE — the runtime is MedianFloat64Aggregate (column) and
# float(value) (literal), and non-numeric inputs are rejected outright. Typing it as
# DOUBLE (not input-passthrough) keeps the binder honest with the runtime, the same
# way AVG is forced to DOUBLE below.
_AGGREGATE_RESULT_DOUBLE = frozenset({"APPROX_PERCENTILE", "MEDIAN"})


def _operand_column_type(operand):
    """Build a ColumnType for a binary-op operand, or None if it can't be expressed.

    Used by D-2 (`compute_result_logical_type`) at the binder. Returns None when the
    operand isn't a numeric type the result-derivation rules know how to handle —
    the caller then skips parameter derivation (the LogicalCategory-only result stands).

    D-4 Phase 2: when the operand has a bound schema_column, its `column_type` is
    the authoritative unified type — use it directly (it carries the LogicalType
    descriptor for DECIMAL, etc.). LITERAL operands without a bound schema_column
    fall through to the LogicalCategory path.
    """
    from draken.draken_native import DrakenType

    from opteryx.types import logical_type as lt
    from opteryx.types.logical_type import ColumnType

    # Prefer the bound schema_column's column_type (single source of truth).
    sc = getattr(operand, "schema_column", None)
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

    # LITERAL fallback: operand carries .type/.precision/.scale directly.
    if operand.node_type == NodeType.LITERAL:
        op_type = getattr(operand, "type", None)
        if op_type == LogicalCategory.DECIMAL:
            op_p = getattr(operand, "precision", None)
            op_s = getattr(operand, "scale", None)
            p = op_p if op_p is not None else 18
            s = op_s if op_s is not None else 6
            if s > p:
                s = p
            return lt.DECIMAL(p, s)
        if op_type == LogicalCategory.INTEGER:
            return ColumnType(DrakenType.INT64)
    return None


def _aggregate_return_type(node: Node) -> Optional[_ColumnType]:
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
        # Element type is unknown at bind time; VARIANT is a safe placeholder.
        return _lt.ARRAY(_lt.VARIANT)
    if name in _AGGREGATE_RESULT_PASSTHROUGH or name == "AVG":
        # SUM/MIN/MAX/ANY_VALUE pass through the input column's type. AVG is a ratio,
        # not a value drawn from the data, so it returns DOUBLE for both INTEGER and
        # DECIMAL inputs (matches DuckDB and the runtime: the AVG collector divides as
        # double). Typing AVG(DECIMAL) as DOUBLE keeps the binder honest with the
        # runtime — an earlier DECIMAL passthrough was a latent lie.
        if node.parameters:
            param = node.parameters[0]
            sc = getattr(param, "schema_column", None)
            if sc is not None:
                param_type = sc.column_type  # ColumnType or None
            elif param.node_type == NodeType.LITERAL:
                param_type = getattr(param, "type", None)  # ColumnType from Phase 2
            else:
                param_type = None
            if param_type is None or param_type.category in (None, LogicalCategory.NULL):
                return None
            if name == "AVG" and param_type.category in (
                LogicalCategory.INTEGER,
                LogicalCategory.DECIMAL,
            ):
                return _CT_FLOAT64
            return param_type  # ColumnType
    return None


def _copy_relation_schema(schema: RelationSchema) -> RelationSchema:
    """Copy a RelationSchema for branch isolation during expression binding.

    ``merge_schemas`` previously ``deepcopy``'d each schema, which recursed into
    every column's ``column_type`` (ColumnType + LogicalType) — by far the most
    expensive part of binding a wide relation. That deep recursion is
    unnecessary: ``column_type`` is only ever *replaced* on a column during
    binding (binder.py rebinds the attribute), never mutated in place, so the
    type carrier can be shared by reference.

    What *is* mutated in place during binding is the column's own metadata —
    ``identity``, ``aliases`` (appended to), and ``origin`` (assigned). Each
    column therefore gets its own ``SchemaColumn`` with detached mutable lists,
    while the immutable ``column_type`` is shared. Nested STRUCT ``fields`` are
    rare and may themselves be rebound, so they are deep-copied when present.
    """
    new_schema = copy.copy(schema)  # shallow: shares the columns/aliases lists we overwrite below
    new_columns = []
    for col in schema.columns:
        c = copy.copy(col)  # new column object; shares column_type, aliases, origin, fields refs
        if c.aliases is not None:
            c.aliases = list(c.aliases)
        if c.origin is not None:
            c.origin = list(c.origin)
        if c.fields is not None:
            c.fields = copy.deepcopy(c.fields)
        new_columns.append(c)
    new_schema.columns = new_columns
    if schema.aliases is not None:
        new_schema.aliases = list(schema.aliases)
    return new_schema


def merge_schemas(*schemas: Dict[str, RelationSchema]) -> Dict[str, RelationSchema]:
    """
    Handles the merging of relations, requiring a custom merge function.

    Parameters:
        dicts: Tuple[Dict[str, RelationSchema]]
            Dictionaries to be merged.

    Returns:
        A merged dictionary containing RelationSchemas.
    """
    merged_dict: Dict[str, RelationSchema] = {}
    for dic in schemas:
        # DEBUG: if type(dic) is not dict:
        # DEBUG:    raise InvalidInternalStateError("Internal Error - merge_schemas expected dicts")
        for key, value in dic.items():
            if key in merged_dict:
                if type(value) is RelationSchema:
                    merged_dict[key] += value
                else:
                    raise InvalidInternalStateError(
                        "Internal Error - merge_schemas expects schemas"
                    )
            else:
                merged_dict[key] = _copy_relation_schema(value)
    return merged_dict


def locate_identifier_in_loaded_schemas(
    value: str, schemas: Dict[str, RelationSchema]
) -> Tuple[Optional[SchemaColumn], Optional[RelationSchema]]:
    """
    Locate a given identifier in a set of loaded schemas.

    Parameters:
        value: str
            The identifier to locate.
        schemas: Dict[str, Schema]
            The loaded schemas to search within.

    Returns:
        A tuple containing the column and its source schema, if found.
    """
    found_source_relation = None
    column = None

    for schema in schemas.values():
        found = schema.find_column(value, case_insensitive=True)
        if found:
            if column and found_source_relation:
                # test for duplicates
                raise AmbiguousIdentifierError(identifier=value)
            found_source_relation = schema
            column = found  # don't exit here, so we can test for duplicates

    return column, found_source_relation


def locate_identifier(node: Node, context: Any) -> Tuple[Node, Dict]:
    """
    Locate which schema the identifier is defined in. We return a populated node
    and the context.

    Parameters:
        node: Node
            The node representing the identifier
        context: BindingContext
            The current query context.

    Returns:
        Tuple[Node, Dict]: The updated node and the current context.

    Raises:
        UnexpectedDatasetReferenceError: If the source dataset is not found.
        ColumnNotFoundError: If the column is not found in the schema.
    """
    from opteryx.planner.binder import BindingContext

    def create_variable_node(node: Node, context: BindingContext) -> Node:
        """Populates a Node object for a variable."""
        schema_column = context.execution_context.variables.as_column(node.value)
        new_node = Node(
            node_type=NodeType.LITERAL,
            schema_column=schema_column,
            type=schema_column.column_type,
            value=schema_column.value,
            relations={},
        )
        return new_node

    # get the list of candidate schemas
    if node.source:
        # A reference like `partsupp.ps_suppkey` from `FROM testdata.tpch.partsupp`
        # carries `node.source = "partsupp"` while the schema is keyed by the
        # full path `testdata.tpch.partsupp`. Match the bare qualifier against
        # both the exact key and the trailing dotted segment.
        suffix = f".{node.source}"
        candidate_schemas = {
            name: schema
            for name, schema in context.schemas.items()
            if name.startswith("$shared") or name == node.source or name.endswith(suffix)
        }
    else:
        candidate_schemas = context.schemas

    # if there are no candidates, we probably don't know the relation
    if not candidate_schemas:
        if node.source in context.relations:
            raise UnexpectedDatasetReferenceError(
                dataset=node.source,
                message=f"Dataset `{node.source}` is not available after being used on the right side of a ANTI or SEMI JOIN",
            )
        else:
            raise UnexpectedDatasetReferenceError(dataset=node.source)

    # look up the column in the candidate schemas
    column, found_source_relation = locate_identifier_in_loaded_schemas(
        node.source_column, candidate_schemas
    )

    # if we didn't find the column, suggest alternatives
    if not column:
        # Check if the identifier is a variable
        if node.current_name[0] == "@":
            node = create_variable_node(node, context)
            context.schemas["$derived"].columns.append(node.schema_column)
            return node, context

        from opteryx.utils import suggest_alternative

        suggestion = suggest_alternative(
            node.source_column,
            [
                column_name
                for _, schema in candidate_schemas.items()
                for column_name in schema.all_column_names
                if column_name is not None
            ],
        )
        raise ColumnNotFoundError(column=node.value, suggestion=suggestion)
    elif node.current_name[0] == "@":
        new_node = Node(
            node_type=NodeType.LITERAL,
            schema_column=column,
            type=column.column_type,
            value=column.value,
        )
        return new_node, context

    # Update node.source to the found relation name
    if not node.source:
        node.source = found_source_relation.name

    # if we have an alias for a column not known about in the schema, add it
    if node.alias and node.alias not in column.all_names:
        if column.aliases:
            column.aliases.append(node.alias)
        else:
            column.aliases = [node.alias]

    # Update node.schema_column with the found column
    node.schema_column = column
    node.source_connector = {context.relations.get(a) for a in found_source_relation.aliases} - {
        None
    }
    # if may need to map source aliases to the columns if they weren't able to be
    # mapped before now
    if column.origin and len(column.origin) == 1:
        node.source = column.origin[0]
    return node, context


def traversive_recursive_bind(
    node: Node, context: Any, format_cache: Optional[dict] = None
) -> Tuple[Node, Any]:
    # First recurse and do this for all the sub parts of the evaluation plan
    for attr in ("left", "right", "centre"):
        if getattr(node, attr) is not None:
            value, context = inner_binder(getattr(node, attr), context, format_cache)
            setattr(node, attr, value)
    if node.parameters:
        node.parameters, new_contexts = zip(
            *(inner_binder(parm, context, format_cache) for parm in node.parameters)
        )
        merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
        context.schemas = merged_schemas
    if node.node_type == NodeType.CASE:
        # NodeType.CASE uses conditions/results/else_result instead of parameters
        if node.conditions:
            bound, new_contexts = zip(
                *(inner_binder(c, context, format_cache) for c in node.conditions)
            )
            node.conditions = list(bound)
            merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
            context.schemas = merged_schemas
        if node.results:
            bound, new_contexts = zip(
                *(inner_binder(r, context, format_cache) for r in node.results)
            )
            node.results = list(bound)
            merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
            context.schemas = merged_schemas
        if node.else_result is not None:
            node.else_result, context = inner_binder(node.else_result, context, format_cache)
    return node, context


def inner_binder(
    node: Node, context: BindingContext, format_cache: Optional[dict] = None
) -> Tuple[Node, Any]:
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).

    ``format_cache`` memoizes format_expression renders for the lifetime of one
    root bind (callers omit it; the root call creates it). Keys are computed
    top-down over pristine subtrees, so within a root bind the cached render of
    every node is exactly what an uncached render would produce — this collapses
    the O(n²) per-node memo-key rendering of deep expressions to O(n).
    """
    # Import relevant classes and functions
    from opteryx.expression import ExpressionColumn, format_expression, get_all_nodes_of_type

    # Retrieve the node type for further processing.
    node_type = node.node_type

    # Early exit for columns that are already bound.
    # If the node has a 'schema_column' already set, it doesn't need to be processed again.
    # This is an optimization to avoid unnecessary work.
    if node.schema_column is not None:
        return node, context

    # Early exit for nodes representing IDENTIFIER types.
    # If the node is of type IDENTIFIER, it's just a simple look up to bind the node.
    if node_type in (NodeType.IDENTIFIER, NodeType.EVALUATED):
        return locate_identifier(node, context)

    # Early exit for nodes representing calculated columns.
    # If the node represents a calculated column, if we're seeing it again it's because it
    # has appeared earlier in the plan and in that case we don't need to recalcuate, we just
    # need to treat the result like an IDENTIFIER
    # We discard columns not referenced, so this sometimes holds the only reference to
    # child columns, e.g. MAX(id), we may not have 'id' next time we see it, only MAX(id)
    if format_cache is None:
        format_cache = {}
    column_name = node.query_column or format_expression(node, True, format_cache)
    for schema in context.schemas.values():
        found_column = schema.find_column(column_name, case_insensitive=True)
        # A literal's column_name is its own textual form, so the case-insensitive
        # lookup above (correct for identifiers) would otherwise collapse two
        # case-distinct string literals ('ss' and 'SS', REPLACE's needle and
        # replacement) onto a single interned constant and hand the executor
        # value == value. Reuse a constant for a literal only when the stored
        # value is byte-identical; otherwise keep it distinct.
        if (
            found_column
            and node_type == NodeType.LITERAL
            and not (
                isinstance(found_column, ConstantColumn) and found_column.value == node.value
            )
        ):
            found_column = None
        # If the column exists in the schema, update node and context accordingly.
        if found_column:
            # found_identity = found_column.identity
            with suppress(Exception):
                node, _ = traversive_recursive_bind(node, context, format_cache)

            node.schema_column = found_column
            node.query_column = node.alias or column_name
            node.fully_bound = False

            if isinstance(found_column, ConstantColumn):
                node.node_type = NodeType.LITERAL
                node.value = found_column.value
                node.type = found_column.column_type

            return node, context

    schemas = context.schemas

    # do the sub trees off this node
    node, context = traversive_recursive_bind(node, context, format_cache)

    # Now do the node we're at
    if node_type == NodeType.LITERAL:
        schema_column = ConstantColumn(
            name=column_name,
            column_type=node.type,
            aliases=[node.alias] if node.alias else [],
            value=node.value,
            nullable=False,
        )
        schemas["$derived"].columns.append(schema_column)
        node.schema_column = schema_column
        node.query_column = node.alias or column_name

    elif node_type != NodeType.SUBQUERY and not node.do_not_create_column:
        if node_type in (NodeType.FUNCTION, NodeType.AGGREGATOR):
            # we need to add this new column to the schema
            aliases = [node.alias] if node.alias else []
            result_type = None
            fixed_function_result = None
            if len(node.parameters) == 0:
                result_type, fixed_function_result = fixed_value_function(node.value, context)
            if result_type:
                # fixed_value_function returns (ColumnType, value) directly — no lookup needed.
                schema_column = ConstantColumn(
                    name=column_name,
                    column_type=result_type,
                    aliases=aliases,
                    value=fixed_function_result,
                    nullable=False,
                )
                node.node_type = NodeType.LITERAL
                node.type = result_type
                node.value = fixed_function_result
            else:
                element_type = None  # for types with elements (ARRAYs)
                precision = 38  # Maximum precision for Decimal128
                scale = 21  # A reasonable scale that's less than precision

                # DECIMAL carries precision/scale as parameters — extract before catalog lookup
                if node.value == "DECIMAL" and len(node.parameters) > 1:
                    precision = node.parameters[1].value
                    scale = node.parameters[2].value if len(node.parameters) > 2 else scale

                # Ask the catalog for the return type (catalog owns type reasoning)
                try:
                    _resolved = _get_function_catalog().resolve(node.value, list(node.parameters))
                except TypeError as _type_err:
                    raise IncompatibleTypesError(message=str(_type_err)) from _type_err
                if _resolved is not None:
                    result_type = _resolved.inferred_return_type
                    element_type = _resolved.inferred_element_type
                    node.function_ref = _resolved
                    # MATCH is `cosine_similarity(a, b) >= threshold`. The threshold is a
                    # session variable, but the kernel is handed it in a ctx at BIND time
                    # (the same channel EMBED's width uses): a compiled plan must keep
                    # answering the question it was compiled for, so a later SET cannot
                    # reach back and change it. Resolved here because this is where the
                    # session's variables are in scope.
                    if _resolved.function_definition.name == "_MATCH_AGAINST":
                        node.match_threshold = context.execution_context.variables[
                            "match_threshold"
                        ]
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
                # surrounding expression's type after the catalog has declared the return type.
                if node.value in ("COALESCE", "IFNULL", "IFNOTNULL") and _result_type_lc not in (
                    None,
                    LogicalCategory.NULL,
                ):
                    parameters = []
                    for param in node.parameters:
                        if (
                            param.node_type == NodeType.LITERAL
                            and param.value is not None
                            and param.value != set()
                        ):
                            param.value = parse_value(_result_type_lc, param.value)
                            param.type = result_type
                            if param.schema_column is not None:
                                param.schema_column.column_type = result_type
                        parameters.append(param)
                    node.parameters = parameters

                # Phase 5: result_type is ColumnType — use directly.
                _ct = result_type
                schema_column = FunctionColumn(
                    name=column_name,
                    column_type=_ct,
                    aliases=aliases,
                )
            schemas["$derived"].columns.append(schema_column)
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        elif node_type == NodeType.CASE:
            aliases = [node.alias] if node.alias else []
            # Resolve result type: first non-NULL type from results + else_result.
            # D-4 Phase 2: inherit the full ColumnType from the first matching
            # branch — carries precision/scale/element_type uniformly instead of
            # unpacking sidecars.
            result_ct = None
            branch_nodes = list(node.results or [])
            if node.else_result is not None:
                branch_nodes.append(node.else_result)
            for branch in branch_nodes:
                sc = getattr(branch, "schema_column", None)
                if sc is not None and sc.column_type is not None and sc.column_type != _CT_NULL:
                    result_ct = sc.column_type
                    break
            # Coerce LITERAL branches to the resolved result type
            if result_ct is not None:
                _result_cat = result_ct.category
                for branch in branch_nodes:
                    if branch.node_type == NodeType.LITERAL and branch.value is not None:
                        branch.value = parse_value(_result_cat, branch.value)
                        branch.type = result_ct  # ColumnType
                        if branch.schema_column is not None:
                            branch.schema_column.column_type = result_ct
            schema_column = FunctionColumn(
                name=column_name,
                column_type=result_ct,
                aliases=aliases,
            )
            schemas["$derived"].columns.append(schema_column)
            node.derived_from = []
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
            if target_type_name == "DECIMAL" and len(node.parameters) >= 1:
                if node.parameters[0].node_type == NodeType.LITERAL:
                    precision = int(node.parameters[0].value)
                scale = 0
                if len(node.parameters) > 1 and node.parameters[1].node_type == NodeType.LITERAL:
                    scale = int(node.parameters[1].value)

            if (
                target_type_name == "ARRAY"
                and node.parameters is not None
                and len(node.parameters) > 0
            ):
                # CAST(expr AS ARRAY(element_type)) - extract the element type
                element_param = node.parameters[0]
                if element_param.node_type == NodeType.LITERAL and element_param.value is not None:
                    element_type = parse_column_type(str(element_param.value).upper())
                else:
                    element_type = _lt.VARIANT

            if target_type_name == "DECIMAL":
                _ct = _lt.DECIMAL(precision, scale)
            elif target_type_name == "ARRAY":
                _ct = _lt.ARRAY(element_type if element_type is not None else _lt.VARIANT)
            elif target_type_name == "VECTOR":
                # CAST(expr AS VECTOR(n)): the width comes from the TYPE's parenthesized
                # argument, carried in node.parameters (same channel as DECIMAL's
                # precision/scale). It cannot be inferred: an ARRAY column's row lengths
                # vary per row and are unknown at bind time, while the plan must fix the
                # result type — and the projection boundary copies rows at exactly that
                # stride. So a bare VECTOR is rejected here rather than guessed.
                _vec_dim = None
                if node.parameters and node.parameters[0].node_type == NodeType.LITERAL:
                    _vec_dim = node.parameters[0].value
                if _vec_dim is None:
                    raise UnsupportedSyntaxError(
                        "CAST to VECTOR requires a dimension, e.g. CAST(x AS VECTOR(384)) — "
                        "a vector's width cannot be inferred from an array column."
                    )
                _ct = _lt.VECTOR(int(_vec_dim))
            else:
                _ct = parse_column_type(target_type_name)
            schema_column = FunctionColumn(
                name=column_name,
                column_type=_ct,
                aliases=aliases,
            )
            schema_column.identity = (
                column_name.encode("utf-8") if isinstance(column_name, str) else column_name
            )
            schemas["$derived"].columns.append(schema_column)
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        elif node.value and node.value.startswith(
            (
                "AnyOp",
                "AllOp",
            )
        ):
            if node.right.node_type == NodeType.LITERAL:
                if not isinstance(node.right.value, list):
                    try:
                        node.right.value = list(node.right.value)
                    except TypeError as e:
                        raise IncompatibleTypesError(
                            message=f"Cannot construct ARRAY from incompatible types."
                        ) from e
                # LIKE/ILIKE patterns must all be strings
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
            schema_column = ExpressionColumn(name=column_name, column_type=_lt.BOOLEAN)
            node.schema_column = schema_column
            schemas["$derived"].columns.append(schema_column)
        else:
            if node.value in ("InList", "NotInList") and isinstance(
                getattr(node.right, "value", None), list
            ):
                from opteryx.types.logical_type import LogicalCategory as _OT

                left_type = getattr(getattr(node.left, "schema_column", None), "category", None)
                _COERCE = {
                    _OT.FLOAT: float,
                    _OT.INTEGER: int,
                    _OT.BOOLEAN: bool,
                    _OT.VARCHAR: lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    _OT.NVARCHAR: lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    _OT.VARBINARY: lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
                }
                coerce = _COERCE.get(left_type, lambda v: v)
                node.right.value = [None if v is None else coerce(v) for v in node.right.value]

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

            result_type = determine_type(node)  # ColumnType | None (Phase 2)
            # D-2: when the result is DECIMAL, also derive (precision, scale).
            # determine_type() now returns ColumnType; use .category for LogicalCategory
            # comparisons. Only attempt parameter derivation for binary arithmetic on
            # numeric operands — other results (BOOLEAN comparisons, etc.) don't land here.
            _result_cat = result_type.category if result_type is not None else None
            result_ct_final = None
            if (
                _result_cat == LogicalCategory.DECIMAL
                and node.value in ("Plus", "Minus", "Multiply", "Divide")
                and getattr(node, "left", None) is not None
                and getattr(node, "right", None) is not None
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
                centre = getattr(node, "centre", None)
                centre_sc = getattr(centre, "schema_column", None) if centre else None
                if centre_sc is not None and centre_sc.column_type is not None:
                    result_ct_final = centre_sc.column_type

            _schema_ct = result_ct_final if result_ct_final is not None else result_type
            schema_column = ExpressionColumn(
                name=column_name,
                column_type=_schema_ct,
                aliases=[node.alias] if node.alias else [],
                expression=node.value,
            )
            schemas["$derived"].columns.append(schema_column)
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

    identifiers = get_all_nodes_of_type(node, (NodeType.IDENTIFIER,))
    sources = []
    for col in identifiers:
        if col.source is not None:
            sources.append(col.source)
        if col.schema_column is not None:
            sources.extend(col.schema_column.origin or [])
    node.relations = set(sources)

    context.schemas = schemas
    return node, context
