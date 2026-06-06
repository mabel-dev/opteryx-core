# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Converts the AST to a logical query plan.

The plan does not try to be efficient or clever, at this point it is only trying to be correct.
"""

import time
from enum import Enum, auto
from typing import List, Optional, Tuple

from opteryx.exceptions import UnnamedColumnError, UnsupportedSyntaxError
from opteryx.expression import NodeType, format_expression, get_all_nodes_of_type
from opteryx.models import LogicalColumn, Node
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import logical_planner_builders
from opteryx.planner.logical_planner.logical_planner_rewriter import decompose_aggregates
from opteryx.third_party.travers import Graph
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils import dnf, random_string
from opteryx.vectors.vector_types import (
    get_vector_source_identifier,
    node_is_vector_query_expression,
)


class LogicalPlanStepType(int, Enum):
    Project = auto()  # field selection
    Filter = auto()  # tuple filtering
    Union = auto()  # appending relations (UNION/UNION ALL)
    Intersect = auto()  # set intersection (INTERSECT/INTERSECT ALL)
    Except = auto()  # set difference (EXCEPT/EXCEPT ALL)
    Explain = auto()  # EXPLAIN
    Difference = auto()  # relation interection
    Join = auto()  # all joins
    Unnest = auto()  # UNNEST
    #    Containment = auto() # IN (maybe also EXISTS?)
    AggregateAndGroup = auto()  # group by
    Aggregate = auto()
    Scan = auto()  # read a dataset
    Show = auto()  # show a variable
    ShowColumns = auto()  # SHOW COLUMNS
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()
    Exit = auto()
    HeapSort = auto()

    CTE = auto()
    Subquery = auto()
    Window = auto()  # OVER (PARTITION BY ...) — rewritten to join by plan rewriter
    FunctionDataset = auto()  # Unnest, GenerateSeries, values + Fake
    DependentJoin = auto()  # Correlated subquery awaiting decorrelation

    CreateView = auto()
    AlterView = auto()
    DropView = auto()
    Analyze = auto()
    Comment = auto()  # COMMENT ON VIEW/TABLE

    CreateRelation = auto()
    DropRelation = auto()
    TruncateRelation = auto()
    Insert = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def copy(self) -> "Node":
        parent_copy = super().copy()
        new_node = LogicalPlanNode(**parent_copy.properties)
        new_node.uuid = parent_copy.uuid
        return new_node

    def __str__(self):  # pragma: no cover
        try:
            from opteryx.planner.logical_planner.logical_planner_renderers import _render_registry

            render_fn = _render_registry.get(self.node_type)
            if render_fn:
                return render_fn(self)
        except Exception as err:
            import warnings

            warnings.warn(f"Problem drawing logical plan - {err}")
        return self.node_type.name


def get_subplan_schemas(sub_plan: Graph) -> List[str]:
    """
    Collects all schema aliases used within a given sub-plan.

    This function traverses the sub-plan graph to collect aliases, including those from subqueries.
    Aliases define the schemas used at exit and entry points of the sub-plan.

    Parameters:
        sub_plan: Graph
            The sub-plan object representing a branch of the logical plan.

    Returns:
        List[str]:
            A sorted list of unique schema aliases found within the sub-plan.
    """

    def collect_aliases(node: dict) -> List[str]:
        """
        Recursively traverse the graph to collect schema aliases.

        Parameters:
            node: dict
                The current node in the graph.

        Returns:
            List[str]:
                A list of unique schema aliases collected from the current node and its children.
        """
        current_node = sub_plan[node["name"]]

        # Start with the alias of the current node, if it exists
        aliases = [current_node.alias] if current_node.alias else []

        # If this node is a subquery, stop traversal here
        if current_node.node_type == LogicalPlanStepType.Subquery:
            return aliases

        # Recursively collect aliases from children
        for child in node.get("children", []):
            aliases.extend(collect_aliases(child))

        return aliases

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    aliases = collect_aliases(root_node)

    # Return sorted list of unique aliases
    return sorted(set(aliases))


def get_subplan_reads(sub_plan: Graph) -> List[str]:
    def collect_reads(node: dict) -> List[str]:
        current_node = sub_plan[node["name"]]

        # If this node is a subquery, stop traversal here
        if current_node.node_type in (
            LogicalPlanStepType.Scan,
            LogicalPlanStepType.FunctionDataset,
        ):
            return [current_node.uuid]

        readers = []
        # Recursively collect aliases from children
        for child in node.get("children", []):
            readers.extend(collect_reads(child))

        return readers

    # Start the traversal from the root node
    root_node = sub_plan.depth_first_search()
    readers = collect_reads(root_node)

    # Return sorted list of unique aliases
    return sorted(set(readers))


"""
CLAUSE PLANNERS
"""


def extract_ctes(branch, planner):
    ctes = {}
    if branch.get("Query", branch).get("with"):
        for _ast in branch.get("Query", branch)["with"]["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            logical_plan = planner(_ast["query"]["body"])
            # CTEs don't have an exit node
            plan_head = logical_plan.get_exit_points()[0]
            logical_plan.remove_node(plan_head, True)
            ctes[alias] = logical_plan
    return ctes


def extract_value(clause):
    if len(clause) == 1:
        return logical_planner_builders.build(clause[0])
    return [logical_planner_builders.build(token) for token in clause]


def extract_variable(clause):
    if len(clause) == 1:
        return clause[0]["Identifier"]["value"]
    return [token["Identifier"]["value"] for token in clause]


def extract_simple_filter(filters, identifier: str = "Name"):
    if "Like" in filters:
        left = Node(NodeType.IDENTIFIER, value=identifier)
        right = Node(NodeType.LITERAL, type=LogicalCategory.VARCHAR, value=filters["Like"])
        root = Node(
            NodeType.COMPARISON_OPERATOR,
            value="ILike",  # we're case insensitive for SHOW filters
            left=left,
            right=right,
        )
        return root
    if "Where" in filters:
        root = logical_planner_builders.build(filters["Where"])
        return root


def _is_vector_order_expression(node: Node) -> bool:
    source_identifier = (
        _get_vector_order_source_identifier(node.parameters[0])
        if (node.node_type == NodeType.FUNCTION and len(node.parameters) == 2)
        else None
    )
    return (
        node.node_type == NodeType.FUNCTION
        and node.value in ("COSINE_SIMILARITY", "COSINE_DISTANCE")
        and len(node.parameters) == 2
        and source_identifier is not None
        and node_is_vector_query_expression(node.parameters[1])
    )


def _get_vector_order_source_identifier(node: Node):
    source_identifier = get_vector_source_identifier(node)
    if source_identifier is not None:
        return source_identifier
    if node.node_type == NodeType.IDENTIFIER:
        return node
    if (
        node.node_type == NodeType.CAST
        and getattr(node, "value", None) in {"VECTOR", "TRY_VECTOR"}
        and getattr(node, "left", None) is not None
        and node.left.node_type == NodeType.IDENTIFIER
    ):
        return node.left
    return None


def _table_name(branch):
    keys = ("Table", "Derived")
    for key in keys:
        if key in branch["relation"]:
            break
    if branch["relation"][key]["alias"]:
        return branch["relation"][key]["alias"]["name"]["value"]
    return ".".join(part["Identifier"]["value"] for part in branch["relation"][key]["name"])


def _validate_where_clause_expression(node: Node) -> None:
    """Validate that a WHERE clause contains a valid boolean expression.

    WHERE clauses must contain explicit comparisons or boolean operators, not bare literals
    or identifiers. This prevents ambiguity and silent incorrect results.

    Allowed expressions:
    - COMPARISON_OPERATOR (=, <>, <, >, etc.)
    - IS TRUE / IS FALSE / IS NULL
    - AND, OR, XOR, NOT (applied to valid expressions)
    - Function calls that return boolean
    - Binary/unary operators that return boolean

    Disallowed:
    - Bare LITERAL (TRUE, FALSE, or numeric constants)
    - Bare IDENTIFIER (column names without comparison)
    - NOT applied to non-boolean expressions
    """
    if node is None:
        return

    node_type = node.node_type

    # Allowed: comparison operators and boolean functions
    if node_type == NodeType.COMPARISON_OPERATOR:
        return
    if node_type == NodeType.FUNCTION:
        # Function result validity checked at evaluation time
        return

    # Allowed: IS TRUE/FALSE/NULL
    if node_type == NodeType.UNARY_OPERATOR and node.value in (
        "IsTrue",
        "IsNotTrue",
        "IsFalse",
        "IsNotFalse",
        "IsNull",
        "IsNotNull",
    ):
        return

    # Allowed: logical operators applied to valid expressions
    if node_type in (NodeType.AND, NodeType.OR, NodeType.XOR):
        _validate_where_clause_expression(node.left)
        _validate_where_clause_expression(node.right)
        return

    # NOT is allowed if applied to a valid boolean expression
    if node_type == NodeType.NOT:
        _validate_where_clause_expression(node.centre)
        return

    # Allowed: nested expressions
    if node_type == NodeType.NESTED:
        _validate_where_clause_expression(node.centre)
        return

    # Binary/unary operators that might return boolean (like LIKE)
    if node_type == NodeType.BINARY_OPERATOR:
        # These are validated at evaluation time
        return
    if node_type == NodeType.UNARY_OPERATOR:
        # Unary operators like NOT on columns (validated at evaluation)
        return

    # Disallowed: bare literals
    if node_type == NodeType.LITERAL:
        raise UnsupportedSyntaxError(
            f"WHERE clause cannot be a bare literal ({node.value!r}). "
            "Use a comparison (e.g., 'WHERE column = {value}') or IS operator (e.g., 'WHERE column IS TRUE')."
        )

    # Disallowed: bare identifiers (column names without comparison)
    if node_type == NodeType.IDENTIFIER:
        raise UnsupportedSyntaxError(
            f"WHERE clause cannot be a bare column name ({node.value!r}). "
            "Use a comparison (e.g., 'WHERE {node.value} = value') or IS operator (e.g., 'WHERE {node.value} IS TRUE')."
        )

    # Any other node type in WHERE is unsupported
    raise UnsupportedSyntaxError(
        f"WHERE clause contains unsupported expression type: {node_type}. "
        "WHERE requires a boolean comparison or function."
    )


def _find_base_scan(plan: LogicalPlan) -> "LogicalPlanNode":
    """Return the sole Scan node from plan, for use as the CTE source in a window rewrite."""
    scans = [node for _, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Scan]
    if not scans:
        raise UnsupportedSyntaxError(
            "Window functions require a base table — cannot be used without a FROM clause."
        )
    if len(scans) > 1:
        raise UnsupportedSyntaxError(
            "Window functions over multiple joined tables are not yet supported."
        )
    return scans[0]


def inner_query_planner(ast_branch: dict) -> LogicalPlan:
    if "Query" in ast_branch:
        # Sometimes we get a full query plan here (e.g. when queries in set
        # functions are in parenthesis)
        return plan_query(ast_branch)

    # Handle nested SetOperations (chained UNION/INTERSECT/EXCEPT)
    if "SetOperation" in ast_branch:
        # Recursively call plan_query to handle the nested set operation
        return plan_query({"Query": {"body": ast_branch}})

    inner_plan = LogicalPlan()
    step_id = None

    # TOP used?
    if ast_branch["Select"].get("top") is not None:
        raise UnsupportedSyntaxError(
            "SELECT TOP to limit number of returned records not supported, use LIMIT instead."
        )

    # from
    _relations = ast_branch["Select"].get("from", [])

    # Process first relation if any
    if len(_relations) > 0:
        step_id, sub_plan = create_node_relation(_relations[0])
        inner_plan += sub_plan

        # If there are multiple relations, build sequential binary implicit cross joins
        # This converts FROM A, B, C into A CROSS JOIN B CROSS JOIN C
        if len(_relations) > 1:
            for i in range(1, len(_relations)):
                # Process the next relation
                right_step_id, right_sub_plan = create_node_relation(_relations[i])

                # Get relation names BEFORE adding right_sub_plan to inner_plan
                left_relation_names = get_subplan_schemas(inner_plan)
                left_readers = get_subplan_reads(inner_plan)
                right_relation_names = get_subplan_schemas(right_sub_plan)
                right_readers = get_subplan_reads(right_sub_plan)

                # Create binary cross join node
                join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
                join_step.type = "cross join"
                join_step.implied_join = True
                join_step.left_relation_names = left_relation_names
                join_step.left_readers = left_readers
                join_step.right_relation_names = right_relation_names
                join_step.right_readers = right_readers

                # For compatibility with binder's fallback extraction (if left_relation_names not set)
                # Don't set readers to avoid triggering the >2 relations check in the binder
                join_step.relation_names = [left_relation_names, right_relation_names]

                # Add the right sub_plan to inner_plan
                inner_plan += right_sub_plan

                # Add join node and wire it
                join_step_id = random_string()
                inner_plan.add_node(join_step_id, join_step)
                inner_plan.add_edge(step_id, join_step_id)
                inner_plan.add_edge(right_step_id, join_step_id)

                # Update step_id for next iteration
                step_id = join_step_id

    # If there's no relations, use $no_table
    if len(_relations) == 0:
        step_id, sub_plan = create_node_relation(
            {
                "relation": {
                    "Table": {
                        "name": [{"Identifier": {"value": "$no_table"}}],
                        "args": None,
                        "alias": None,
                        "with_hints": [],
                    }
                }
            }
        )
        inner_plan += sub_plan

    # selection
    _selection = logical_planner_builders.build(ast_branch["Select"].get("selection"))
    if _selection:
        if len(_relations) == 0:
            raise UnsupportedSyntaxError("Statement has a WHERE clause but no FROM clause.")
        _validate_where_clause_expression(_selection)
        selection_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        selection_step.condition = _selection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, selection_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # groups
    _projection = logical_planner_builders.build(ast_branch["Select"].get("projection")) or []
    if len(_projection) > 1 and any(
        p.node_type == NodeType.WILDCARD for p in _projection if p.value is None
    ):
        from opteryx.exceptions import SqlError

        raise SqlError("SELECT * cannot coexist with additional columns.")

    if len(_projection) > 1 and any(p.node_type == NodeType.WILDCARD for p in _projection[1:]):
        from opteryx.exceptions import SqlError

        raise SqlError(
            "Qualified wild cards (`table.*`) must be the first column when used with additional columns."
        )

    # Detect window functions (AGGREGATOR nodes with an OVER clause) before aggregate extraction.
    # Replace each window function in _projection with a plain column reference to its output alias,
    # so the regular aggregate path does not see them. Window logical nodes are inserted here so
    # they sit between the scan/filter chain and the project, ready for the plan rewriter.
    _window_specs: list = []  # (index, agg_node, partition_by_nodes)
    for _i, proj_col in enumerate(_projection):
        _over = getattr(proj_col, "over", None)
        if _over is not None and proj_col.node_type == NodeType.AGGREGATOR:
            if _over.get("order_by"):
                raise UnsupportedSyntaxError(
                    "Window functions with ORDER BY are not supported. Use PARTITION BY only."
                )
            if _over.get("window_frame") is not None:
                raise UnsupportedSyntaxError(
                    "Window functions with frame specifications (ROWS/RANGE BETWEEN) are not supported."
                )
            _partition_by = [
                logical_planner_builders.build(pb) for pb in _over.get("partition_by", [])
            ]
            _win_alias = proj_col.alias or f"$win_{random_string(6)}"
            proj_col.alias = _win_alias
            proj_col.query_column = _win_alias
            proj_col.over = None  # clear so it acts as a plain aggregate inside the CTE
            _window_specs.append((_i, proj_col, _partition_by))
            _ref = LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=_win_alias,
                alias=_win_alias,
            )
            _ref.query_column = _win_alias
            _projection[_i] = _ref

    # Collect aggregates in projection (SELECT) order. get_all_nodes_of_type uses a
    # LIFO stack, so passing the whole projection list scrambles cross-column order;
    # for the ungrouped Aggregate operator that order leaks straight to the output
    # columns. Walking each projection column in turn preserves left-to-right order
    # (the binder dedups by identity afterwards).
    _aggregates = []
    for _proj_col in _projection:
        _aggregates.extend(get_all_nodes_of_type(_proj_col, select_nodes=(NodeType.AGGREGATOR,)))
    _aggregates, _projection = decompose_aggregates(_aggregates, _projection)
    _groups = logical_planner_builders.build(ast_branch["Select"].get("group_by"))[0]

    if _window_specs:
        if _groups is not None and _groups != []:
            raise UnsupportedSyntaxError("Window functions cannot be combined with GROUP BY.")
        _source_scan = _find_base_scan(inner_plan)
        # Group by distinct partition spec; same partition → one Window node (shared CTE).
        _by_partition: dict = {}
        for _i, _agg_node, _partition_by in _window_specs:
            _key = tuple(
                getattr(pb, "source_column", None) or getattr(pb, "value", None) or format_expression(pb)
                for pb in _partition_by
            )
            if _key not in _by_partition:
                _by_partition[_key] = (_partition_by, [])
            _by_partition[_key][1].append(_agg_node)
        for _key, (_partition_by, _agg_nodes) in _by_partition.items():
            _window_step = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
            _window_step.aggregates = _agg_nodes
            _window_step.partition_by = _partition_by
            _window_step.source_scan = _source_scan.copy()
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, _window_step)
            inner_plan.add_edge(previous_step_id, step_id)

    if _groups is not None and _groups != []:
        if any(p.node_type == NodeType.WILDCARD for p in _projection):
            raise UnsupportedSyntaxError(
                "SELECT * cannot be used with GROUP BY — did you mean `GROUP BY ALL`?"
            )
        # WILDCARD is used to represent GROUP BY ALL, we group by all columns in the projection
        # which aren't aggregates
        if _groups == NodeType.WILDCARD:
            _groups = [
                p
                for p in _projection
                if len(get_all_nodes_of_type(p, select_nodes=(NodeType.AGGREGATOR,))) == 0
            ]

        group_step = LogicalPlanNode(node_type=LogicalPlanStepType.AggregateAndGroup)
        group_step.groups = _groups
        group_step.aggregates = _aggregates
        group_step.projection = _projection
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, group_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)
    # aggregates
    elif len(_aggregates) > 0:
        aggregate_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
        aggregate_step.groups = _groups
        aggregate_step.aggregates = _aggregates

        known_columns = {
            hash(n) for n in get_all_nodes_of_type(_groups + _aggregates, (NodeType.IDENTIFIER,))
        }
        project_columns = [
            n
            for n in get_all_nodes_of_type(_projection, (NodeType.IDENTIFIER,))
            if hash(n) not in known_columns
        ]

        if len(project_columns) > 0:
            from opteryx.exceptions import SqlError

            column = project_columns.pop().source_column
            error = f"Column '{column}' must appear in the `GROUP BY` clause or must be part of an aggregate function. Either add it to the `GROUP BY` list, or add an aggregation such as `MIN({column})`."
            raise SqlError(error)

        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, aggregate_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # pre-process part of the order by before the projection
    _order_by = ast_branch.get("order_by")
    _order_by_columns_not_in_projection = []
    _order_by_columns = []
    if _order_by and _order_by.get("kind") and _order_by["kind"].get("Expressions"):
        _order_by = [
            (
                logical_planner_builders.build(item["expr"]),
                True if item["options"]["asc"] is None else item["options"]["asc"],
            )
            for item in _order_by["kind"]["Expressions"]
        ]
        # Resolve positional ORDER BY (SQL-92): an integer literal refers to the
        # 1-based position in the SELECT list. Replace it with the projection
        # expression so downstream stages see a normal column reference.
        # Any other literal (string, float, NULL, ...) is rejected.
        rewritten = []
        for expr, ascending in _order_by:
            if expr.node_type == NodeType.LITERAL:
                if expr.type != LogicalCategory.INTEGER:
                    raise UnsupportedSyntaxError("Cannot ORDER BY constant values")
                position = int(expr.value)
                if position < 1 or position > len(_projection):
                    raise UnsupportedSyntaxError(
                        f"ORDER BY position {position} is out of range — SELECT has {len(_projection)} column(s)."
                    )
                expr = _projection[position - 1]
            rewritten.append((expr, ascending))
        _order_by = rewritten
        _order_by_columns = [exp[0] for exp in _order_by]

    # projection
    project_step = None
    if not (
        len(_projection) == 1
        and _projection[0].node_type == NodeType.WILDCARD
        and _projection[0].except_columns is None
        and _projection[0].value is None
    ):
        for column in _projection:
            if column.node_type == NodeType.LITERAL and column.type in (
                LogicalCategory.ARRAY,
                LogicalCategory.VECTOR,
            ):
                if ast_branch["Select"].get("distinct"):
                    raise UnsupportedSyntaxError(
                        "Values cannot be parenthesised in the SELECT clause — did you mean DISTINCT ON(cols) cols FROM ?"
                    )
                raise UnsupportedSyntaxError("Values cannot be parenthesised in the SELECT clause.")

        # ORDER BY needing to be able to order by columns not in the projection
        # whilst being able to order by aliases created by the projection means
        # we need to do specific checks
        if _order_by_columns:
            # Collect qualified names and aliases from projection columns
            projection_qualified_names = {
                proj_col.qualified_name for proj_col in _projection if proj_col.qualified_name
            }.union({f".{proj_col.alias}" for proj_col in _projection if proj_col.alias})

            # Compare projection and ORDER BY identifiers case-insensitively
            projection_qualified_names_lower = {n.lower() for n in projection_qualified_names}

            # Collect expression columns from projection (lowercased)
            projection_expressions_lower = {
                format_expression(proj_col).lower()
                for proj_col in _projection
                if proj_col.node_type != NodeType.IDENTIFIER
            }

            # Collect source column names from projection (lowercased)
            projection_source_columns_lower = {
                f".{proj_col.source_column}".lower()
                for proj_col in _projection
                if getattr(proj_col, "source_column", None)
            }

            # Remove columns from ORDER BY that are directly in the projection, aliased, or have the same expression
            _order_by_columns_not_in_projection = [
                ord_col
                for ord_col in _order_by_columns
                if (
                    (ord_col.qualified_name or "").lower() not in projection_qualified_names_lower
                    and f".{(ord_col.source_column or '')}".lower()
                    not in projection_qualified_names_lower
                    and f".{(ord_col.source_column or '')}".lower()
                    not in projection_source_columns_lower
                    and format_expression(ord_col).lower() not in projection_expressions_lower
                )
            ]

            # Remove columns from ORDER BY that match the source of a wildcard in the projection
            if _projection[0].except_columns is None:
                for proj_col in [pc for pc in _projection if pc.node_type == NodeType.WILDCARD]:
                    _order_by_columns_not_in_projection = [
                        ord_col
                        for ord_col in _order_by_columns_not_in_projection
                        if (ord_col.source or "").lower() != (proj_col.value[0] or "").lower()
                    ]

            for ord_col in _order_by_columns:
                if not _is_vector_order_expression(ord_col):
                    continue
                _order_by_columns_not_in_projection = [
                    candidate
                    for candidate in _order_by_columns_not_in_projection
                    if candidate is not ord_col
                ]
                source_column = _get_vector_order_source_identifier(ord_col.parameters[0])
                if source_column is None:
                    continue
                source_identity = getattr(source_column.schema_column, "identity", None)
                existing_projection_identities = {
                    getattr(col.schema_column, "identity", None)
                    for col in list(_projection) + list(_order_by_columns_not_in_projection)
                    if getattr(col, "schema_column", None) is not None
                }
                if source_identity in existing_projection_identities:
                    continue
                _order_by_columns_not_in_projection.append(source_column)

        project_step = LogicalPlanNode(node_type=LogicalPlanStepType.Project)
        project_step.columns = _projection
        project_step.order_by_columns = _order_by_columns_not_in_projection
        project_step.except_columns = _projection[0].except_columns
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, project_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # EXCEPT with ORDER BY creates complex situations
    if project_step and project_step.except_columns and _order_by_columns_not_in_projection:
        if any(
            col.source_column in {c.source_column for c in project_step.except_columns}
            for col in project_step.order_by_columns
        ):
            raise UnsupportedSyntaxError(
                "Cannot ORDER BY columns excluded by the EXCEPT clause in the projection."
            )
        project_step.order_by_columns = []

    # having
    _having = logical_planner_builders.build(ast_branch["Select"].get("having"))
    if _having:
        having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        having_step.condition = _having
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, having_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # distinct
    if ast_branch["Select"].get("distinct"):
        distinct_step = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
        if isinstance(ast_branch["Select"]["distinct"], dict):
            distinct_step.on = logical_planner_builders.build(
                ast_branch["Select"]["distinct"]["On"]
            )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, distinct_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # order
    if _order_by:
        order_step = LogicalPlanNode(node_type=LogicalPlanStepType.Order)
        order_step.order_by = _order_by
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, order_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # limit/offset
    _limit = ast_branch.get("limit")
    _offset = ast_branch.get("offset")
    if _limit or _offset:
        limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
        limit_step.limit = None if _limit is None else logical_planner_builders.build(_limit).value
        limit_step.offset = (
            None if _offset is None else logical_planner_builders.build(_offset).value
        )
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, limit_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # add the exit node
    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    exit_node.columns = _projection
    previous_step_id, step_id = step_id, random_string()
    inner_plan.add_node(step_id, exit_node)
    if previous_step_id is not None:
        inner_plan.add_edge(previous_step_id, step_id)

    return inner_plan


"""
STATEMENT PLANNERS
"""


def process_join_tree(join: dict) -> LogicalPlanNode:
    """
    Processes a join tree from the AST and returns a LogicalPlanNode representing the join.
    """

    def extract_join_type(join: dict) -> str:
        """
        Extracts the type of the join from the AST node representing the join.
        """
        join_operator = join["join_operator"]

        if join_operator == {"Join": "Natural"}:
            return "natural join"
        elif join_operator == "CrossJoin":
            return "cross join"
        elif join_operator == {"Join": "None"}:
            return "cross join"

        join_operator = next(iter(join["join_operator"]))

        return {
            "Anti": "left anti",  # ANTI JOIN is a LEFT ANTI JOIN
            "AsOf": "asof",
            "FullOuter": "full outer",
            "Join": "inner",
            "Inner": "inner",
            "LeftAnti": "left anti",
            "Left": "left outer",
            "LeftOuter": "left outer",
            "LeftSemi": "left semi",
            "RightAnti": "right anti",  # not supported
            "Right": "right outer",
            "RightOuter": "right outer",
            "RightSemi": "right semi",  # not supported
            "Semi": "left semi",  # SEMI JOIN is a LEFT SEMI JOIN
            "CrossJoin": "cross join",  # should never match, here for completeness
            "Natural": "natural join",  # should never match, here for completeness
        }.get(join_operator)

    def extract_join_condition(join: dict) -> Tuple[Optional[str], Optional[List[str]]]:
        """
        Extracts the join's limiting condition from the AST node representing the join.
        """
        join_operator = join["join_operator"]
        if not isinstance(join_operator, dict):
            return None, None

        join_on = None
        join_using = None

        join_operator = next(iter(join_operator))
        join_condition = next(iter(join["join_operator"][join_operator]))
        if join_condition == "On":
            join_on = logical_planner_builders.build(
                join["join_operator"][join_operator][join_condition]
            )
        if join_condition == "Using":
            join_using = [
                logical_planner_builders.build(identifier[0])
                for identifier in join["join_operator"][join_operator][join_condition]
            ]

        return join_on, join_using

    def create_unnest_node(join: dict, join_step: Node) -> Node:
        """
        Extracts information for an UNNEST dataset from the AST node representing the join.
        """
        if join_step.type != "cross join":
            raise UnsupportedSyntaxError("JOIN on UNNEST only supported for CROSS joins.")
        unnest_column = logical_planner_builders.build(join["relation"]["Table"]["args"]["args"][0])
        if join["relation"]["Table"].get("alias") is None:
            raise UnnamedColumnError(
                "Column created by UNNEST has no name, use AS to name the column."
            )
        unnest_alias = join["relation"]["Table"]["alias"]["name"]["value"]

        # if we're a UNNEST JOIN, we're a different node type
        join_step.node_type = LogicalPlanStepType.Unnest
        join_step.unnest_column = unnest_column
        join_step.unnest_alias = unnest_alias
        join_step.alias = f"$unnest-{random_string(6)}"

        # return the updated node
        return join_step

    join_step = LogicalPlanNode(node_type=LogicalPlanStepType.Join)

    join_step.type = extract_join_type(join)

    if join_step.type in ("right semi", "right anti"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} JOIN not supported, use LEFT variations only."
        )

    if join_step.type == "asof":
        asof_payload = join["join_operator"]["AsOf"]
        join_step.asof_condition = logical_planner_builders.build(asof_payload["match_condition"])
        constraint = asof_payload.get("constraint", "None")
        if isinstance(constraint, dict) and "On" in constraint:
            join_step.on = logical_planner_builders.build(constraint["On"])
        elif isinstance(constraint, dict) and "Using" in constraint:
            join_step.using = [
                logical_planner_builders.build(i[0]) for i in constraint["Using"]
            ]
    else:
        join_step.on, join_step.using = extract_join_condition(join)

    if not join_step.on and not join_step.using and join_step.type in ("left outer", "right outer"):
        raise UnsupportedSyntaxError(
            f"{join_step.type.upper()} JOIN must have an ON or USING clause."
        )

    # JOIN UNNEST needs to be handled differently
    if "Table" in join.get("relation", {}):
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in join["relation"]["Table"]["name"]
        )
        if relation_name.upper() == "UNNEST":
            join_step = create_unnest_node(join, join_step)

    return join_step


def create_node_relation(relation: dict):
    sub_plan = LogicalPlan()
    root_node = None

    relation_name = None
    if "Table" in relation["relation"]:
        relation_name = ".".join(
            logical_planner_builders.build(p).value for p in relation["relation"]["Table"]["name"]
        )

    if "Derived" in relation["relation"]:
        if relation["relation"]["Derived"]["subquery"]:
            subquery = relation["relation"]["Derived"]
            if "Values" not in subquery["subquery"]["body"]:
                # SUBQUERY nodes wrap other queries and the result is available as a relation in
                # the parent query.
                subquery_step = LogicalPlanNode(node_type=LogicalPlanStepType.Subquery)
                if subquery["alias"] is None:
                    subquery_step.alias = f"$subquery-{random_string(6)}"
                else:
                    subquery_step.alias = subquery["alias"]["name"]["value"]
                step_id = random_string()
                sub_plan.add_node(step_id, subquery_step)

                subquery_plan = plan_query(subquery["subquery"])
                exit_node = subquery_plan.get_exit_points()[0]
                subquery_step.columns = subquery_plan[exit_node].columns
                subquery_plan.remove_node(exit_node, heal=True)

                sub_plan += subquery_plan
                subquery_entry_id = subquery_plan.get_exit_points()[0]
                sub_plan.add_edge(subquery_entry_id, step_id)

                root_node = step_id
                relation["step_id"] = step_id
            else:
                # VALUES nodes are where the relation is defined within the SQL statement.
                # e.g. SELECT * FROM (VALUES(1),(2)) AS numbers (number)
                #
                # We have the name of the relation (alias), the column names (columns) and the
                # values in each row (values)
                values_step = LogicalPlanNode(
                    node_type=LogicalPlanStepType.FunctionDataset, function="VALUES"
                )
                values_step.alias = subquery["alias"]["name"]["value"]
                values_step.columns = tuple(
                    col["name"]["value"] for col in subquery["alias"]["columns"]
                )
                values_step.values = [
                    tuple(logical_planner_builders.build(value) for value in row["content"])
                    for row in subquery["subquery"]["body"]["Values"]["rows"]
                ]
                step_id = random_string()
                sub_plan.add_node(step_id, values_step)
                root_node = step_id
        else:  # pragma: no cover
            raise NotImplementedError(relation["relation"]["Derived"])

    elif relation["relation"]["Table"]["args"]:
        # If we have args, we're a function dataset (like FAKE or UNNEST)
        function = relation["relation"]["Table"]
        function_name = relation_name.upper()

        if function["alias"] is None:
            from opteryx.exceptions import UnnamedColumnError

            raise UnnamedColumnError(
                f"Column or Relation created by {function_name} has no name, use AS to give it a name."
            )

        function_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function=function_name
        )
        if function_name == "UNNEST":
            function_step.alias = f"$unnest-{random_string(6)}"
            function_step.relation = function_step.alias
            function_step.unnest_target = function["alias"]["name"]["value"]
        else:
            function_step.alias = function["alias"]["name"]["value"]

        function_step.args = [
            logical_planner_builders.build(arg) for arg in function["args"]["args"]
        ]
        function_step.columns = tuple(col["name"]["value"] for col in function["alias"]["columns"])

        step_id = random_string()
        sub_plan.add_node(step_id, function_step)
        root_node = step_id
        relation["step_id"] = step_id
    else:
        # SCAN nodes are where we read relations; these can be from memory, disk or a remote
        # system. This has many physical implementations but at this point all we have is the
        # name/location of the relation (relation), what the relation is called inside the
        # query (alias) and if there are any hints (hints)
        from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
        table = relation["relation"]["Table"]
        from_step.relation = relation_name
        from_step.alias = (
            from_step.relation if table["alias"] is None else table["alias"]["name"]["value"]
        )
        from_step.hints = [hint["Identifier"]["value"] for hint in table["with_hints"]]

        # Extract and validate AT clause if present
        version_clause = table.get("version")
        if version_clause is not None:
            from_step.at_date = logical_planner_builders.extract_timetravel_timestamp(
                version_clause
            )

        step_id = random_string()
        sub_plan.add_node(step_id, from_step)

        root_node = step_id
        relation["step_id"] = step_id

    # joins
    _joins = relation.get("joins", [])
    for join in _joins:
        # this is the convention: select * from LEFT join RIGHT

        join_step = process_join_tree(join)

        if join_step.node_type == LogicalPlanStepType.Unnest:
            # UNNEST joins don't have a LEFT and RIGHT side
            join_step_id = random_string()
            sub_plan.add_node(join_step_id, join_step)
            sub_plan.add_edge(root_node, join_step_id, "left")
            root_node = join_step_id
            continue

        right_node_id, right_plan = create_node_relation(join)

        # add the left and right relation names - we sometimes need these later
        join_step.left_relation_names = get_subplan_schemas(sub_plan)
        join_step.left_readers = get_subplan_reads(sub_plan)
        join_step.right_relation_names = get_subplan_schemas(right_plan)
        join_step.right_readers = get_subplan_reads(right_plan)

        # add the right side of the join
        sub_plan += right_plan

        join_step_id = random_string()
        sub_plan.add_node(join_step_id, join_step)

        # add the from table as the left side of the join
        sub_plan.add_edge(root_node, join_step_id, "left")
        sub_plan.add_edge(right_node_id, join_step_id, "right")

        root_node = join_step_id

    return root_node, sub_plan


def plan_explain(statement, **kwargs) -> LogicalPlan:
    plan = LogicalPlan()
    explain_node = LogicalPlanNode(node_type=LogicalPlanStepType.Explain)
    explain_node.analyze = statement["Explain"]["analyze"]
    explain_format = statement["Explain"].get("format")

    if explain_format is None:
        explain_node.format = "TEXT"
    else:
        explain_node.format = explain_format.get("Keyword", "TEXT").upper()
    if explain_node.format == "GRAPHVIZ":
        explain_node.format = "MERMAID"

    explain_id = random_string()
    plan.add_node(explain_id, explain_node)

    sub_plan = plan_query(statement=statement["Explain"]["statement"])
    sub_plan_id = sub_plan.get_exit_points()[0]
    plan += sub_plan
    plan.add_edge(sub_plan_id, explain_id)

    return plan


def plan_query(statement: dict) -> LogicalPlan:
    """ """

    root_node = statement
    if "Query" in root_node:
        root_node = root_node["Query"]

    # set operations (UNION, INTERSECT, EXCEPT)
    if "SetOperation" in root_node["body"]:
        set_operation = root_node["body"]["SetOperation"]

        op_type = set_operation["op"]
        if op_type == "Union":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Union)
        elif op_type == "Intersect":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Intersect)
        elif op_type == "Except":
            set_op_node = LogicalPlanNode(node_type=LogicalPlanStepType.Except)
        else:
            raise UnsupportedSyntaxError(f"Unsupported SET operator '{op_type}'")

        set_op_node.modifier = (
            None if set_operation["set_quantifier"] == "None" else set_operation["set_quantifier"]
        )
        step_id = random_string()
        plan = LogicalPlan()
        plan.add_node(step_id, set_op_node)
        head_nid = step_id

        left_plan = inner_query_planner(set_operation["left"])
        from opteryx.planner.binder import rename_relations
        left_plan = rename_relations(left_plan, prefix="$union-")
        plan += left_plan
        subquery_entry_id = left_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        right_plan = inner_query_planner(set_operation["right"])
        right_plan = rename_relations(right_plan, prefix="$union-")
        plan += right_plan
        subquery_entry_id = right_plan.get_exit_points()[0]
        plan.add_edge(subquery_entry_id, step_id)
        # remove the exit node
        plan.remove_node(subquery_entry_id, heal=True)

        # UNION ALL
        if set_op_node.modifier != "All":
            distinct = LogicalPlanNode(node_type=LogicalPlanStepType.Distinct)
            head_nid, step_id = step_id, random_string()
            plan.add_node(step_id, distinct)
            plan.add_edge(head_nid, step_id)

        # limit/offset
        if root_node.get("limit_clause"):
            _limit = root_node["limit_clause"].get("LimitOffset", {}).get("limit")
            _offset = root_node["limit_clause"].get("LimitOffset", {}).get("offset")

            if _offset:
                _offset = _offset.get("value")
            if _limit or _offset:
                limit_step = LogicalPlanNode(node_type=LogicalPlanStepType.Limit)
                limit_step.limit = (
                    None if _limit is None else logical_planner_builders.build(_limit).value
                )
                limit_step.offset = (
                    None if _offset is None else logical_planner_builders.build(_offset).value
                )
                head_nid, step_id = step_id, random_string()
                plan.add_node(step_id, limit_step)
                if head_nid is not None:
                    plan.add_edge(head_nid, step_id)

        # add the exit node
        exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
        _projection_nodes = [
            left_plan[nid]
            for nid in left_plan.nodes()
            if left_plan[nid].node_type in (LogicalPlanStepType.Project,)
        ]
        columns = [LogicalPlanNode(NodeType.WILDCARD, value=(None,))]
        if _projection_nodes:
            columns = _projection_nodes[0].columns
        exit_node.columns = columns
        head_nid, step_id = step_id, random_string()
        plan.add_node(step_id, exit_node)
        if head_nid is not None:
            plan.add_edge(head_nid, step_id)

        set_op_node.columns = columns
        set_op_node.left_relation_names = get_subplan_schemas(left_plan)
        set_op_node.right_relation_names = get_subplan_schemas(right_plan)

        return plan

    # we do some minor AST rewriting
    if root_node.get("limit_clause"):
        root_node["body"]["limit"] = root_node["limit_clause"].get("LimitOffset", {}).get("limit")
        root_node["body"]["offset"] = root_node["limit_clause"].get("LimitOffset", {}).get("offset")
    root_node["body"]["order_by"] = root_node.get("order_by", None)

    planned_query = inner_query_planner(root_node["body"])

    # DEBUG: print("LOGICAL PLAN")
    # DEBUG: print(planned_query.draw())

    return planned_query


def plan_set_variable(statement, **kwargs):
    root_node = "SingleAssignment"
    statement = statement["Set"]
    plan = LogicalPlan()
    set_step = LogicalPlanNode(
        node_type=LogicalPlanStepType.Set,
        variable=extract_variable(statement[root_node]["variable"]),
        value=extract_value(statement[root_node]["values"]),
    )
    plan.add_node(random_string(), set_step)
    return plan


def plan_show_columns(statement, **kwargs):
    root_node = "ShowColumns"
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    table = statement[root_node]["show_options"]["show_in"]["parent_name"]
    from_step.relation = ".".join(part["Identifier"]["value"] for part in table)
    from_step.alias = from_step.relation
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowColumns)
    show_step.extended = statement[root_node]["extended"]
    show_step.full = statement[root_node]["full"]
    show_step.relation = from_step.relation
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    _filter = statement[root_node]["show_options"].get("filter_position")
    if _filter:
        _filter = _filter["Suffix"]
        filter_node = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
        filter_node.condition = extract_simple_filter(_filter, "name")
        previous_step_id, step_id = step_id, random_string()
        plan.add_node(step_id, filter_node)
        plan.add_edge(previous_step_id, step_id)
        raise UnsupportedSyntaxError("Unable to filter colmns in SHOW COLUMNS")

    return plan


def plan_show_create_query(statement, **kwargs):
    root_node = "ShowCreate"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.Show)
    show_step.object_type = statement[root_node]["obj_type"].upper()
    show_step.object_name = extract_variable(statement[root_node]["obj_name"])
    if isinstance(show_step.object_name, list):
        show_step.object_name = ".".join(show_step.object_name)
    plan.add_node(random_string(), show_step)
    return plan


def plan_create_view(statement, **kwargs):
    """
    Create a logical plan for CREATE VIEW statement.

    CREATE VIEW view_name AS query

    Note: The query is stored as text, not planned. It will be planned
    when the view is referenced in a query.
    """
    root_node = "CreateView"
    plan = LogicalPlan()

    create_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateView)

    # Extract view name
    view_name_parts = statement[root_node]["name"]
    create_view_node.view_name = extract_variable(view_name_parts)
    if isinstance(create_view_node.view_name, list):
        create_view_node.view_name = ".".join(create_view_node.view_name)

    # Extract OR REPLACE flag
    create_view_node.or_replace = statement[root_node].get("or_replace", False)

    # Extract MATERIALIZED flag (if supported)
    create_view_node.materialized = statement[root_node].get("materialized", False)

    # Extract columns (if specified)
    columns = statement[root_node].get("columns")
    if columns:
        cols = []
        for col in columns:
            # Accept several AST shapes for an identifier
            try:
                if isinstance(col, dict) and "Identifier" in col:
                    cols.append(col["Identifier"]["value"])
                elif isinstance(col, dict) and "name" in col:
                    name = col["name"]
                    if isinstance(name, dict) and "Identifier" in name:
                        cols.append(name["Identifier"]["value"])
                    elif isinstance(name, str):
                        cols.append(name)
                    else:
                        # fallback to the first string value in the dict
                        for v in col.values():
                            if isinstance(v, str):
                                cols.append(v)
                                break
                elif isinstance(col, str):
                    cols.append(col)
                else:
                    # generic fallback
                    cols.append(str(col))
            except Exception:
                raise KeyError("Unexpected column AST format in CREATE VIEW")
        create_view_node.columns = cols
    else:
        create_view_node.columns = None

    # Store the query as the AST - we'll need it to reconstruct the SQL or re-parse later
    create_view_node.query = statement[root_node]["query"]

    # Add the CreateView node
    plan.add_node(random_string(), create_view_node)

    return plan


def plan_alter_view(statement, **kwargs):
    """
    Create a logical plan for ALTER VIEW statement (UpdateView).

    ALTER VIEW view_name AS query

    Note: The query is stored as text, not planned. It will be planned
    when the view is referenced in a query.
    """
    root_node = "AlterView"
    plan = LogicalPlan()

    alter_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterView)

    # Extract view name
    view_name_parts = statement[root_node]["name"]
    alter_view_node.view_name = extract_variable(view_name_parts)
    if isinstance(alter_view_node.view_name, list):
        alter_view_node.view_name = ".".join(alter_view_node.view_name)

    # Extract columns (if specified)
    columns = statement[root_node].get("columns")
    if columns:
        cols = []
        for col in columns:
            try:
                if isinstance(col, dict) and "Identifier" in col:
                    cols.append(col["Identifier"]["value"])
                elif isinstance(col, dict) and "name" in col:
                    name = col["name"]
                    if isinstance(name, dict) and "Identifier" in name:
                        cols.append(name["Identifier"]["value"])
                    elif isinstance(name, str):
                        cols.append(name)
                    else:
                        for v in col.values():
                            if isinstance(v, str):
                                cols.append(v)
                                break
                elif isinstance(col, str):
                    cols.append(col)
                else:
                    cols.append(str(col))
            except Exception:
                raise KeyError("Unexpected column AST format in ALTER VIEW")
        alter_view_node.columns = cols
    else:
        alter_view_node.columns = None

    # Store the query as the AST - we'll need it to reconstruct the SQL or re-parse later
    alter_view_node.query = statement[root_node]["query"]

    # Add the AlterView node
    plan.add_node(random_string(), alter_view_node)

    return plan


def plan_drop(statement, **kwargs):
    """
    Create a logical plan for DROP statement (VIEW or TABLE).

    DROP VIEW [IF EXISTS] view_name
    DROP TABLE [IF EXISTS] table_name
    """
    root_node = "Drop"
    plan = LogicalPlan()

    drop_statement = statement[root_node]
    object_type = drop_statement.get("object_type")

    if object_type == "View":
        # DROP VIEW path (unchanged)
        drop_view_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropView)

        # Extract view names (can drop multiple views)
        names = drop_statement["names"]
        view_names = []
        for name_parts in names:
            view_name = extract_variable(name_parts)
            if isinstance(view_name, list):
                view_name = ".".join(view_name)
            view_names.append(view_name)

        drop_view_node.view_names = view_names

        # Extract IF EXISTS flag
        drop_view_node.if_exists = drop_statement.get("if_exists", False)

        # Extract CASCADE/RESTRICT flag
        drop_view_node.cascade = drop_statement.get("cascade", False)

        plan.add_node(random_string(), drop_view_node)
        return plan

    elif object_type == "Table":
        # DROP TABLE path (new)
        drop_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropRelation)

        # Extract table names (can drop multiple tables)
        names = drop_statement["names"]
        relation_names = []
        for name_parts in names:
            relation_name = extract_variable(name_parts)
            if isinstance(relation_name, list):
                relation_name = ".".join(relation_name)
            relation_names.append(relation_name)

        drop_relation_node.relation_names = relation_names

        # Extract IF EXISTS flag
        drop_relation_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_relation_node)
        return plan

    else:
        raise UnsupportedSyntaxError(f"DROP {object_type} is not supported")


def _plan_ctas(relation_name, if_not_exists, query_ast):
    """Plan CREATE TABLE ... AS SELECT.

    Builds: SELECT subtree (Exit-stripped) → InsertNode(create_target=True).
    Target schema is derived at bind time from the SELECT's exit columns.
    """
    plan = LogicalPlan()

    source_plan = plan_query(query_ast)
    exit_node_id = source_plan.get_exit_points()[0]
    source_plan.remove_node(exit_node_id, heal=True)
    plan += source_plan
    source_tail_id = source_plan.get_exit_points()[0]

    insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
    insert_step.relation_name = relation_name
    insert_step.values_feeder = None
    insert_step.source_tail_id = source_tail_id
    insert_step.explicit_columns = None
    insert_step.create_target = True
    insert_step.if_not_exists = if_not_exists

    insert_id = random_string()
    plan.add_node(insert_id, insert_step)
    plan.add_edge(source_tail_id, insert_id)
    return plan


def plan_create_table(statement, **kwargs):
    """
    Create a logical plan for CREATE TABLE statement.

    CREATE TABLE [IF NOT EXISTS] table_name (
        column_name column_type [NOT NULL],
        ...
    )

    Maps sqloxide column types to LogicalCategory and constructs a RelationSchema.
    """
    from opteryx.types.schema import SchemaColumn, RelationSchema

    root_node = "CreateTable"
    plan = LogicalPlan()

    create_table_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateRelation)

    # Extract table name
    table_name_parts = statement[root_node]["name"]
    create_table_node.relation_name = extract_variable(table_name_parts)
    if isinstance(create_table_node.relation_name, list):
        create_table_node.relation_name = ".".join(create_table_node.relation_name)

    # Extract IF NOT EXISTS flag
    create_table_node.if_not_exists = statement[root_node].get("if_not_exists", False)

    # Check for unsupported options
    for option in ["or_replace", "external", "temporary", "transient", "volatile", "iceberg"]:
        if statement[root_node].get(option):
            raise UnsupportedSyntaxError(f"CREATE TABLE option not supported: {option}")

    # CTAS path
    query_ast = statement[root_node].get("query")
    if query_ast is not None:
        column_defs = statement[root_node].get("columns", [])
        if column_defs:
            raise UnsupportedSyntaxError(
                "CREATE TABLE AS SELECT cannot specify column definitions"
            )
        return _plan_ctas(
            relation_name=create_table_node.relation_name,
            if_not_exists=create_table_node.if_not_exists,
            query_ast=query_ast,
        )

    # Parse columns
    columns = []
    column_defs = statement[root_node].get("columns", [])
    if not column_defs:
        raise UnsupportedSyntaxError("CREATE TABLE requires at least one column")

    # Type mapping from sqloxide to LogicalCategory
    type_mapping = {
        "BigInt": "INTEGER",
        "Int": "INTEGER",
        "Integer": "INTEGER",
        "SmallInt": "INTEGER",
        "TinyInt": "INTEGER",
        "Varchar": "VARCHAR",
        "Text": "VARCHAR",
        "String": "VARCHAR",
        "Char": "VARCHAR",
        "Double": "DOUBLE",
        "Float": "DOUBLE",
        "Real": "DOUBLE",
        "Boolean": "BOOLEAN",
        "Bool": "BOOLEAN",
        "Date": "DATE",
        "Timestamp": "TIMESTAMP",
        "Blob": "BLOB",
        "Bytea": "BLOB",
        "Bytes": "BLOB",
    }

    for col_def in column_defs:
        col_name = col_def["name"]["value"]
        col_type_data = col_def["data_type"]

        # Extract the type key. sqloxide returns either:
        # - A dict like {"BigInt": None}
        # - A string like "Boolean" or "Date"
        if isinstance(col_type_data, str):
            type_key = col_type_data
        elif isinstance(col_type_data, dict):
            type_key = next(iter(col_type_data.keys()))
        else:
            raise UnsupportedSyntaxError(
                f"unsupported column type in CREATE TABLE: {col_type_data}"
            )

        if type_key not in type_mapping:
            raise UnsupportedSyntaxError(
                f"unsupported column type in CREATE TABLE: {type_key}"
            )

        # Map to LogicalCategory
        sql_type_str = type_mapping[type_key]
        sql_type = LogicalCategory[sql_type_str]

        # Check for NOT NULL constraint
        col_nullable = True
        col_options = col_def.get("options", [])
        if col_options:
            for opt in col_options:
                if isinstance(opt, dict) and opt.get("option") == "NotNull":
                    col_nullable = False
                    break

        # Create SchemaColumn
        flat_col = SchemaColumn(name=col_name, type=sql_type, nullable=col_nullable)
        columns.append(flat_col)

    create_table_node.columns = columns

    # Build RelationSchema
    schema = RelationSchema(name=create_table_node.relation_name, columns=columns)
    create_table_node.schema = schema

    plan.add_node(random_string(), create_table_node)
    return plan


def plan_truncate(statement, **kwargs):
    """
    Create a logical plan for TRUNCATE TABLE statement.

    TRUNCATE [TABLE] table_name [IF EXISTS]
    """
    root = "Truncate"
    truncate_stmt = statement[root]

    if not truncate_stmt.get("table"):
        raise UnsupportedSyntaxError("TRUNCATE without TABLE keyword is not supported")

    table_names = truncate_stmt.get("table_names", [])
    if len(table_names) != 1:
        raise UnsupportedSyntaxError("TRUNCATE supports a single table name")

    # Extract table name
    name_parts = table_names[0].get("name", [])
    relation_name = ".".join(p["Identifier"]["value"] for p in name_parts)

    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.TruncateRelation)
    node.relation_name = relation_name
    node.if_exists = truncate_stmt.get("if_exists", False)

    plan.add_node(random_string(), node)
    return plan


def plan_insert(statement, **kwargs):
    """
    Create a logical plan for INSERT statement.

    Supports:
      INSERT INTO table_name [(c1, c2, ...)] VALUES (v1, v2), ...
      INSERT INTO table_name [(c1, c2, ...)] SELECT ...
    """
    root = "Insert"
    insert_stmt = statement[root]

    if insert_stmt.get("overwrite"):
        raise UnsupportedSyntaxError("INSERT OVERWRITE is not supported")

    body = insert_stmt["source"]["body"]

    # Target relation name
    table_name_parts = insert_stmt["table"]["TableName"]
    relation_name = ".".join(
        logical_planner_builders.build(p).value for p in table_name_parts
    )

    # Explicit column list (may be empty/None)
    explicit_columns = []
    for col in insert_stmt.get("columns") or []:
        if isinstance(col, dict) and "value" in col:
            explicit_columns.append(col["value"])
        else:
            raise UnsupportedSyntaxError(
                f"Unsupported column reference in INSERT column list: {col}"
            )
    explicit_columns_tuple = tuple(explicit_columns) if explicit_columns else None

    plan = LogicalPlan()

    if "Values" in body:
        # VALUES source — mirror the existing FromClause VALUES path.
        values_step = LogicalPlanNode(
            node_type=LogicalPlanStepType.FunctionDataset, function="VALUES"
        )
        values_step.alias = f"$insert_values-{random_string(6)}"
        values_step.values = [
            tuple(logical_planner_builders.build(value) for value in row["content"])
            for row in body["Values"]["rows"]
        ]
        # Generate placeholder column names. These will be replaced by visit_insert
        # with the actual column names from the target relation's schema.
        if values_step.values:
            num_cols = len(values_step.values[0])
            values_step.columns = tuple(f"$col{i}" for i in range(num_cols))
        else:
            values_step.columns = ()

        values_id = random_string()
        plan.add_node(values_id, values_step)

        insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
        insert_step.relation_name = relation_name
        insert_step.values_feeder = values_step
        insert_step.source_tail_id = None
        insert_step.explicit_columns = explicit_columns_tuple
        insert_id = random_string()
        plan.add_node(insert_id, insert_step)
        plan.add_edge(values_id, insert_id)
    else:
        # SELECT source — plan the sub-query, strip its Exit node, then attach
        # the Insert sink in the Exit's place.
        source_plan = plan_query(insert_stmt["source"])
        exit_node_id = source_plan.get_exit_points()[0]
        source_plan.remove_node(exit_node_id, heal=True)

        plan += source_plan
        source_tail_id = source_plan.get_exit_points()[0]

        insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
        insert_step.relation_name = relation_name
        insert_step.values_feeder = None
        insert_step.source_tail_id = source_tail_id
        insert_step.explicit_columns = explicit_columns_tuple
        insert_id = random_string()
        plan.add_node(insert_id, insert_step)
        plan.add_edge(source_tail_id, insert_id)

    return plan


def plan_analyze_query(statement, **kwargs) -> LogicalPlan:
    root = "Analyze"

    if not statement[root]["has_table_keyword"]:
        raise UnsupportedSyntaxError("ANALYZE without TABLE keyword is not supported")

    plan = LogicalPlan()
    analyze_node = LogicalPlanNode(node_type=LogicalPlanStepType.Analyze)
    analyze_node.table_name = ".".join(
        part["Identifier"]["value"] for part in statement[root]["table_name"]
    )

    analyze_id = random_string()
    plan.add_node(analyze_id, analyze_node)

    return plan


def build_expression_tree(relation, dnf_list):
    """
    Recursively build an expression tree from a DNF-like list structure.
    The structure can include:
      - Flat clauses (list of tuples) -> AND of predicates
      - OR groups (list of clauses) -> OR of them
      - Mixed (some tuples, some nested lists) -> AND between flat predicates and subgroup
      - Factored form [common, [or_clauses]] -> (common AND (OR ...))
    """

    # Unwrap redundant single nesting
    while isinstance(dnf_list, list) and len(dnf_list) == 1 and isinstance(dnf_list[0], list):
        dnf_list = dnf_list[0]

    # --- Case: Factored form [common_clause, [or_clauses]] ---
    if (
        isinstance(dnf_list, list)
        and len(dnf_list) == 2
        and isinstance(dnf_list[0], list)
        and isinstance(dnf_list[1], list)
        and all(isinstance(c, list) for c in dnf_list[1])
    ):
        common_clause, or_clauses = dnf_list
        left = build_expression_tree(relation, common_clause)
        right = build_expression_tree(relation, or_clauses)
        return Node(node_type=NodeType.AND, left=left, right=right)

    # --- Case: flat clause (AND of tuples) ---
    if all(isinstance(x, tuple) for x in dnf_list):
        and_node = None
        for identifier, operator, value in dnf_list:
            if identifier is True or identifier is False:
                left_node = build_literal_node(identifier)
            else:
                left_node = LogicalColumn(
                    NodeType.IDENTIFIER, source_column=identifier, source=relation
                )
            comparison_node = Node(
                node_type=NodeType.COMPARISON_OPERATOR,
                value=operator,
                left=left_node,
                right=build_literal_node(value),
            )
            if operator.startswith("AnyOp"):
                comparison_node.left, comparison_node.right = (
                    comparison_node.right,
                    comparison_node.left,
                )
            and_node = (
                comparison_node
                if and_node is None
                else Node(node_type=NodeType.AND, left=and_node, right=comparison_node)
            )
        return and_node

    # --- Case: OR group (list of clauses, each clause = list of tuples) ---
    if all(isinstance(x, list) and all(isinstance(p, tuple) for p in x) for x in dnf_list):
        or_node = None
        for clause in dnf_list:
            clause_node = build_expression_tree(relation, clause)
            or_node = (
                clause_node
                if or_node is None
                else Node(node_type=NodeType.OR, left=or_node, right=clause_node)
            )
        return or_node

    # --- Case: Mixed: some flat predicates and some nested groups ---
    if any(isinstance(x, tuple) for x in dnf_list) and any(isinstance(x, list) for x in dnf_list):
        flat_preds = [x for x in dnf_list if isinstance(x, tuple)]
        subgroups = [x for x in dnf_list if isinstance(x, list)]
        left = build_expression_tree(relation, flat_preds)
        right = build_expression_tree(relation, subgroups)
        return Node(node_type=NodeType.AND, left=left, right=right)

    # --- Case: fallback, treat as OR of subgroups ---
    if isinstance(dnf_list, list):
        or_node = None
        for subgroup in dnf_list:
            subgroup_node = build_expression_tree(relation, subgroup)
            or_node = (
                subgroup_node
                if or_node is None
                else Node(node_type=NodeType.OR, left=or_node, right=subgroup_node)
            )
        return or_node

    raise ValueError(f"Unsupported DNF structure: {dnf_list}")


def plan_comment(statement, **kwargs):
    """
    Create a logical plan for COMMENT ON VIEW/TABLE/EXTENSION statement.

    COMMENT [ IF EXISTS ] ON EXTENSION object_name IS 'comment_text'

    Note: The SQL rewriter converts TABLE and VIEW to EXTENSION so the parser
    can accept them.
    """
    root_node = "Comment"
    plan = LogicalPlan()

    comment_node = LogicalPlanNode(node_type=LogicalPlanStepType.Comment)

    # Extract object name (e.g., workspace.collection.view)
    object_name_parts = statement[root_node]["object_name"]
    object_name = extract_variable(object_name_parts)
    if isinstance(object_name, list):
        object_name = ".".join(object_name)
    comment_node.object_name = object_name

    # Extract object type (should be Extension after rewrite)
    comment_node.object_type = statement[root_node].get("object_type", "Extension")

    # Extract the comment text
    comment_node.comment = statement[root_node].get("comment", "")

    # Extract IF EXISTS flag
    comment_node.if_exists = statement[root_node].get("if_exists", False)

    # Add the Comment node
    plan.add_node(random_string(), comment_node)

    return plan


QUERY_BUILDERS = {
    "Analyze": plan_analyze_query,
    "Comment": plan_comment,
    "Explain": plan_explain,
    "Query": plan_query,
    "Set": plan_set_variable,
    "ShowColumns": plan_show_columns,
    "ShowCreate": plan_show_create_query,
    # "ShowFunctions": show_functions_query,
    # "ShowVariable": plan_show_variable,  # generic SHOW handler
    # "ShowVariables": plan_show_variables,
    # "Use": plan_use
    "CreateView": plan_create_view,
    "AlterView": plan_alter_view,
    "Drop": plan_drop,  # handles DROP VIEW and DROP TABLE
    "CreateTable": plan_create_table,
    "Truncate": plan_truncate,
    "Insert": plan_insert,
}


def apply_visibility_filters(
    logical_plan: LogicalPlan, visibility_filters: dict, telemetry
) -> LogicalPlan:
    for nid, node in list(logical_plan.nodes(True)):
        if node.node_type == LogicalPlanStepType.Scan:
            filter_dnf = visibility_filters.get(node.relation)
            if filter_dnf == []:
                # TODO: This is a hack to make sure that an empty list of filters
                # means that the relation should not be visible
                expression_tree = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    left=build_literal_node(True),
                    right=build_literal_node(False),
                )

                # If the filter is an empty list, it means that the relation should not be visible
                filter_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter,
                    condition=expression_tree,  # Use the built expression tree
                    all_relations={node.relation, node.alias},
                )
                logical_plan.insert_node_after(random_string(), filter_node, nid)
                telemetry.visibility_filters_blank_condition_added += 1
            if filter_dnf:
                # Do some basic simplification early, less binding etc to do if we can
                # eliminate some elements from the tree now
                start = time.monotonic_ns()
                filter_dnf = dnf.simplify_dnf(filter_dnf)
                telemetry.time_rewriting_visibility_filters += time.monotonic_ns() - start
                # Apply the transformation from DNF to an expression tree
                expression_tree = build_expression_tree(node.alias, filter_dnf)

                filter_node = LogicalPlanNode(
                    node_type=LogicalPlanStepType.Filter,
                    condition=expression_tree,  # Use the built expression tree
                    all_relations={node.relation, node.alias},
                )

                logical_plan.insert_node_after(random_string(), filter_node, nid)
                telemetry.visibility_filters_condition_added += 1
    return logical_plan


def do_logical_planning_phase(parsed_statement: dict) -> tuple:
    # The sqlparser ast is an array of asts

    statement_type = next(iter(parsed_statement))
    if statement_type not in QUERY_BUILDERS:
        from opteryx.exceptions import UnsupportedSyntaxError
        from opteryx.utils.sql import convert_camel_to_sql_case

        raise UnsupportedSyntaxError(
            f"Opteryx does not support '{convert_camel_to_sql_case(statement_type)}' type queries."
        )
    # CTEs are Common Table Expressions, they're variations of subqueries
    ctes = extract_ctes(parsed_statement, inner_query_planner)
    return QUERY_BUILDERS[statement_type](parsed_statement), parsed_statement, ctes
