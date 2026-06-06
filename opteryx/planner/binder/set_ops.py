# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List, Tuple

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.logical_type import LogicalCategory, find_compatible_type
from opteryx.types.schema import ConstantColumn, SchemaColumn, RelationSchema


def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.variables = context.execution_context.variables
    node.columns = []
    return node, context


def _columns_for_side(
    self,
    node: Node,
    relation_names: List[str],
    context: BindingContext,
):
    """Resolve the schema columns produced by one side of a set operation.

    Normally each side's relation names are registered in `context.schemas`.
    When a branch has no FROM clause (e.g. `SELECT 1`), the project step pops
    the synthetic `$no_table` source because none of its columns are projected,
    and the projected literals end up under a shared `$project` key that gets
    merged across branches. In that case fall back to walking the plan to find
    the branch's direct Project child of the set-op node and use its columns.
    """
    columns = []
    missing = False
    for rel_name in relation_names:
        schema = context.schemas.get(rel_name)
        if schema is not None:
            columns.extend(schema.columns)
        else:
            missing = True
    if not missing:
        return columns

    graph = getattr(self, "graph", None)
    if graph is None:
        raise KeyError(relation_names)

    set_op_nid = None
    for nid, n in graph.nodes(True):
        if n is node:
            set_op_nid = nid
            break
    if set_op_nid is None:
        raise KeyError(relation_names)

    rel_set = set(relation_names)
    for child_nid, _, _ in graph.ingoing_edges(set_op_nid):
        stack = [child_nid]
        seen = set()
        matched = False
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            cur_node = graph[cur]
            if getattr(cur_node, "alias", None) in rel_set:
                matched = True
                break
            for upstream_nid, _, _ in graph.ingoing_edges(cur):
                stack.append(upstream_nid)
        if matched:
            child_node = graph[child_nid]
            branch_columns = []
            for col in (child_node.columns or []):
                schema_column = getattr(col, "schema_column", None)
                if schema_column is not None:
                    branch_columns.append(schema_column)
            return branch_columns

    raise KeyError(relation_names)


def _validate_set_operation_types(
    self,
    node: Node,
    context: BindingContext,
    operation_name: str = "SET OPERATION",
) -> List[LogicalCategory]:
    """Validate and find compatible types for columns in set operations.

    For each column position across left and right relations, find a compatible type.
    Returns list of coerced types in column order.
    """
    left_columns = _columns_for_side(self, node, node.left_relation_names, context)
    right_columns = _columns_for_side(self, node, node.right_relation_names, context)

    if len(left_columns) != len(right_columns):
        raise ValueError(
            f"{operation_name}: column count mismatch — left has {len(left_columns)}, right has {len(right_columns)}"
        )

    coerced_types = []
    for left_col, right_col in zip(left_columns, right_columns):
        coerced_type = find_compatible_type([left_col.type, right_col.type])
        coerced_types.append(coerced_type)

    return coerced_types


def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Validate and determine coerced types for UNION/INTERSECT/EXCEPT
    coerced_types = _validate_set_operation_types(self, node, context, "UNION")
    node.coerced_types = coerced_types

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "union" for n in node.left_relation_names}

    if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,  # column type
                        source_column=schema_column.name,  # the source column
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


def visit_intersect(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Validate and determine coerced types for INTERSECT
    coerced_types = _validate_set_operation_types(self, node, context, "INTERSECT")
    node.coerced_types = coerced_types

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "intersect" for n in node.left_relation_names}

    if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=schema_column.name,
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


def visit_except(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Validate and determine coerced types for EXCEPT
    coerced_types = _validate_set_operation_types(self, node, context, "EXCEPT")
    node.coerced_types = coerced_types

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "except" for n in node.left_relation_names}

    if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=schema_column.name,
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


def visit_unnest(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.columns = []

    # we create a new schema for the unnested column
    unnest_schema = node.alias

    # this is the column which is being unnested
    if node.unnest_column.node_type == NodeType.LITERAL:
        # D-4 Phase 2: construct the ConstantColumn with its type up front (the
        # side-cars are gone; the array element rides on column_type). The prior
        # `ConstantColumn(name=...)`-then-mutate pattern relied on side-car writes.
        lit_type = node.unnest_column.type
        lit_elem = node.unnest_column.element_type
        from opteryx.types.logical_type import sql_to_column_type
        try:
            _ct = sql_to_column_type(lit_type, element_type=lit_elem)
            schema_column = ConstantColumn.from_column_type(
                name=node.unnest_alias,
                column_type=_ct,
                value=node.unnest_column.value,
            )
        except Exception:
            schema_column = ConstantColumn(
                name=node.unnest_alias,
                type=lit_type,
                value=node.unnest_column.value,
            )
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )
        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])
        # reference the new column in the node
        node.columns.append(node.unnest_target)
    else:
        from opteryx.planner.binder.binder import inner_binder

        node.unnest_column, context = inner_binder(node.unnest_column, context)
        node.columns += [node.unnest_column]

        # we can only UNNEST an ARRAY type column, we need to find it before we know its type
        if node.unnest_column.schema_column.type not in (
            0,
            LogicalCategory.ARRAY,
            LogicalCategory.VECTOR,
            LogicalCategory.NULL,
        ):
            from opteryx.exceptions import IncorrectTypeError

            raise IncorrectTypeError(
                f"CROSS JOIN UNNEST requires an ARRAY or VECTOR type column, not {node.unnest_column.schema_column.type}."
            )

        # this is the column that is being created
        # D-4 Phase 2: resolve the UNNEST element type from the unified column_type
        # (carries the ARRAY child as `column_type.element`). VECTOR unnests to
        # DOUBLE. Falls back to the legacy sidecar when the bridge couldn't map it.
        element_type = LogicalCategory.VARCHAR
        unnest_sc = node.unnest_column.schema_column
        if unnest_sc and unnest_sc.type == LogicalCategory.VECTOR:
            element_type = LogicalCategory.DOUBLE
        elif unnest_sc is not None and unnest_sc.column_type is not None and unnest_sc.column_type.element is not None:
            from opteryx.types.logical_type import column_type_to_sql
            element_type = column_type_to_sql(unnest_sc.column_type.element).get("type") or element_type

        schema_column = SchemaColumn(name=node.unnest_alias, type=element_type)
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )

        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])

        # reference the new column in the node
        node.columns.append(node.unnest_target)

    return node, context
