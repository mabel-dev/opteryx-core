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
)
from opteryx.expression import NodeType
from opteryx.expression.functions import get_catalog as _get_function_catalog
from opteryx.expression.functions.registrar import fixed_value_function
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.join_helpers import get_mismatched_condition_column_types
from opteryx.planner.binder.operator_map import determine_type
from opteryx.types import OrsoTypes
from opteryx.types.schema import ConstantColumn, FlatColumn, FunctionColumn, RelationSchema


# Aggregate return-type inference for the binder. Aggregates are dispatched by
# the physical aggregate operators (not the function catalog), but the binder
# still needs to know the result type so expressions like `0.2 * AVG(col)` and
# downstream comparisons type-check correctly.
_AGGREGATE_RESULT_INTEGER = frozenset(
    {"COUNT", "COUNT_DISTINCT", "DISTINCT", "APPROX_COUNT_DISTINCT"}
)
_AGGREGATE_RESULT_PASSTHROUGH = frozenset({"SUM", "MIN", "MAX", "ANY_VALUE"})
_AGGREGATE_RESULT_DOUBLE = frozenset({"APPROX_PERCENTILE"})


def _aggregate_return_type(node: Node) -> Optional[OrsoTypes]:
    """Best-effort result-type inference for aggregate functions."""
    name = node.value
    if name in _AGGREGATE_RESULT_INTEGER:
        return OrsoTypes.INTEGER
    if name in _AGGREGATE_RESULT_DOUBLE:
        return OrsoTypes.DOUBLE
    if name == "ARRAY_AGG":
        return OrsoTypes.ARRAY
    if name in _AGGREGATE_RESULT_PASSTHROUGH or name == "AVG":
        # Pass through the input column's type. AVG of a DECIMAL stays DECIMAL;
        # AVG of an INTEGER becomes DOUBLE (standard SQL behaviour).
        if node.parameters:
            param = node.parameters[0]
            param_type = None
            if param.node_type == NodeType.LITERAL:
                param_type = getattr(param, "type", None)
            elif getattr(param, "schema_column", None) is not None:
                param_type = param.schema_column.type
            if param_type in (None, 0, OrsoTypes._MISSING_TYPE, OrsoTypes.NULL):
                return None
            if name == "AVG" and param_type == OrsoTypes.INTEGER:
                return OrsoTypes.DOUBLE
            return param_type
    return None


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
                merged_dict[key] = copy.deepcopy(value)
    return merged_dict


def locate_identifier_in_loaded_schemas(
    value: str, schemas: Dict[str, RelationSchema]
) -> Tuple[Optional[FlatColumn], Optional[RelationSchema]]:
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
            type=schema_column.type,
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
            if name.startswith("$shared")
            or name == node.source
            or name.endswith(suffix)
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
            type=column.type,
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


def traversive_recursive_bind(node: Node, context: Any) -> Tuple[Node, Any]:
    # First recurse and do this for all the sub parts of the evaluation plan
    for attr in ("left", "right", "centre"):
        if hasattr(node, attr) and getattr(node, attr) is not None:
            value, context = inner_binder(getattr(node, attr), context)
            setattr(node, attr, value)
    if node.parameters:
        node.parameters, new_contexts = zip(
            *(inner_binder(parm, context) for parm in node.parameters)
        )
        merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
        context.schemas = merged_schemas
    if node.node_type == NodeType.CASE:
        # NodeType.CASE uses conditions/results/else_result instead of parameters
        if node.conditions:
            bound, new_contexts = zip(*(inner_binder(c, context) for c in node.conditions))
            node.conditions = list(bound)
            merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
            context.schemas = merged_schemas
        if node.results:
            bound, new_contexts = zip(*(inner_binder(r, context) for r in node.results))
            node.results = list(bound)
            merged_schemas = merge_schemas(*[ctx.schemas for ctx in new_contexts])
            context.schemas = merged_schemas
        if node.else_result is not None:
            node.else_result, context = inner_binder(node.else_result, context)
    return node, context


def inner_binder(node: Node, context: BindingContext) -> Tuple[Node, Any]:
    """
    Note, this is a tree within a tree. This function represents a single step in the execution
    plan (associated with the relational algebra) which may itself be an evaluation plan
    (executing comparisons).
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
    column_name = node.query_column or format_expression(node, True)
    for schema in context.schemas.values():
        found_column = schema.find_column(column_name, case_insensitive=True)
        # If the column exists in the schema, update node and context accordingly.
        if found_column:
            # found_identity = found_column.identity
            with suppress(Exception):
                node, _ = traversive_recursive_bind(node, context)

            node.schema_column = found_column
            node.query_column = node.alias or column_name
            node.fully_bound = False

            if isinstance(found_column, ConstantColumn):
                node.node_type = NodeType.LITERAL
                node.value = found_column.value
                node.type = found_column.type

            return node, context

    schemas = context.schemas

    # do the sub trees off this node
    node, context = traversive_recursive_bind(node, context)

    # Now do the node we're at
    if node_type == NodeType.LITERAL:
        schema_column = ConstantColumn(
            name=column_name,
            aliases=[node.alias] if node.alias else [],
            type=node.type,
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
                # Some functions return constants, so return the constant
                schema_column = ConstantColumn(
                    name=column_name,
                    aliases=aliases,
                    type=result_type,
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
                elif node_type == NodeType.AGGREGATOR:
                    # Aggregates are not in the function catalog (dispatched at
                    # runtime by the aggregate operators). The binder still needs
                    # a result type so expressions like `0.2 * AVG(col)` and
                    # downstream comparisons type-check correctly.
                    result_type = _aggregate_return_type(node)
                    if result_type is None:
                        result_type = OrsoTypes.NULL
                else:
                    result_type = OrsoTypes.NULL  # unknown function; type resolved at runtime

                # Literal coercion: binder's job — mutate AST nodes to match the resolved type.
                # This is NOT type inference; it's making literals consistent with the
                # surrounding expression's type after the catalog has declared the return type.
                if node.value in ("COALESCE", "IFNULL", "IFNOTNULL") and result_type not in (
                    OrsoTypes._MISSING_TYPE,
                    OrsoTypes.NULL,
                    0,
                ):
                    parameters = []
                    for param in node.parameters:
                        if (
                            param.node_type == NodeType.LITERAL
                            and param.value is not None
                            and param.value != set()
                        ):
                            param.value = result_type.parse(param.value)
                            param.type = result_type
                            param.schema_column.type = result_type
                        parameters.append(param)
                    node.parameters = parameters

                schema_column = FunctionColumn(
                    name=column_name,
                    type=result_type,
                    element_type=element_type,
                    aliases=aliases,
                    precision=precision,
                    scale=scale,
                )
            schemas["$derived"].columns.append(schema_column)
            node.derived_from = []
            node.schema_column = schema_column
            node.query_column = node.alias or column_name

        elif node_type == NodeType.CASE:
            aliases = [node.alias] if node.alias else []
            # Resolve result type: first non-NULL type from results + else_result
            result_type = OrsoTypes.NULL
            branch_nodes = list(node.results or [])
            if node.else_result is not None:
                branch_nodes.append(node.else_result)
            for branch in branch_nodes:
                sc = getattr(branch, "schema_column", None)
                if sc is not None and sc.type not in (OrsoTypes.NULL, 0, OrsoTypes._MISSING_TYPE):
                    result_type = sc.type
                    break
            # Coerce LITERAL branches to the resolved result type
            if result_type not in (OrsoTypes._MISSING_TYPE, OrsoTypes.NULL, 0):
                for branch in branch_nodes:
                    if branch.node_type == NodeType.LITERAL and branch.value is not None:
                        branch.value = result_type.parse(branch.value)
                        branch.type = result_type
                        if branch.schema_column is not None:
                            branch.schema_column.type = result_type
            schema_column = FunctionColumn(
                name=column_name,
                type=result_type,
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

            # Map type name to OrsoType
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

            # VARBINARY is not a canonical OrsoType — map to BLOB
            if target_type_name == "VARBINARY":
                target_type_name = "BLOB"
            result_type = OrsoTypes[target_type_name]

            # Validate TIMESTAMP casts from INTEGER — require explicit unit
            if target_type_name == "TIMESTAMP" and node.left:
                source_type = determine_type(node.left)
                if source_type == OrsoTypes.INTEGER:
                    if unit is None:
                        raise IncompatibleTypesError(
                            message="Ambiguous cast: INTEGER → TIMESTAMP requires a unit. "
                            "Use `expr::TIMESTAMP[ms]`, `expr::TIMESTAMP[s]`, or `expr::TIMESTAMP[us]`."
                        )

            element_type = None
            precision = 38
            scale = 21

            # Handle type-specific parameters
            if target_type_name == "DECIMAL" and len(node.parameters) > 1:
                # CAST(expr AS DECIMAL(precision, scale))
                precision = (
                    int(node.parameters[1].value)
                    if node.parameters[1].node_type == NodeType.LITERAL
                    else 38
                )
                scale = (
                    int(node.parameters[2].value)
                    if len(node.parameters) > 2 and node.parameters[2].node_type == NodeType.LITERAL
                    else 21
                )

            if (
                target_type_name == "ARRAY"
                and node.parameters is not None
                and len(node.parameters) > 0
            ):
                # CAST(expr AS ARRAY(element_type)) - extract the element type
                element_param = node.parameters[0]
                if element_param.node_type == NodeType.LITERAL and element_param.value is not None:
                    element_type = OrsoTypes[str(element_param.value).upper()]
                else:
                    element_type = OrsoTypes.VARIANT

            schema_column = FunctionColumn(
                name=column_name,
                type=result_type,
                element_type=element_type,
                aliases=aliases,
                precision=precision,
                scale=scale,
            )
            schema_column.identity = column_name.encode("utf-8") if isinstance(column_name, str) else column_name
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
            schema_column = ExpressionColumn(name=column_name, type=OrsoTypes.BOOLEAN)
            node.schema_column = schema_column
            schemas["$derived"].columns.append(schema_column)
        else:
            if node.value in ("InList", "NotInList") and isinstance(
                getattr(node.right, "value", None), list
            ):
                from opteryx.types import OrsoTypes as _OT

                left_type = getattr(getattr(node.left, "schema_column", None), "type", None)
                _COERCE = {
                    _OT.DOUBLE: float,
                    _OT.INTEGER: int,
                    _OT.BOOLEAN: bool,
                    _OT.VARCHAR: lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    _OT.BLOB: lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
                }
                coerce = _COERCE.get(left_type, lambda v: v)
                node.right.value = [
                    None if v is None else coerce(v) for v in node.right.value
                ]

            mismatches = get_mismatched_condition_column_types(node, relaxed=True)
            if mismatches:
                raise IncompatibleTypesError(**mismatches)

            schema_column = ExpressionColumn(
                name=column_name,
                aliases=[node.alias] if node.alias else [],
                type=determine_type(node),
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
