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
from opteryx.types import logical_type as _plt
from opteryx.types.logical_type import ColumnType, LogicalCategory
from opteryx.types.vectors.vector_types import (
    get_vector_source_identifier,
    node_is_vector_query_expression,
)
from opteryx.utils import dnf, random_string


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
    ShowManifest = auto()  # SHOW MANIFEST FOR <table>
    Set = auto()  # set a variable
    Limit = auto()  # limit and offset
    Order = auto()  # order by
    Distinct = auto()
    Exit = auto()
    HeapSort = auto()

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
    AlterRelation = auto()
    RenameRelation = auto()
    Insert = auto()

    CreateCollection = auto()
    DropCollection = auto()
    AlterWorkspace = auto()


class LogicalPlan(Graph):
    pass


class LogicalPlanNode(Node):
    def copy(self, memo=None) -> "Node":
        if memo is None:
            memo = {}
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        parent_copy = super().copy(memo)
        new_node = LogicalPlanNode(**parent_copy.properties)
        new_node.uuid = parent_copy.uuid
        # super().copy() registered id(self) -> the plain-Node intermediate;
        # replace it with the correctly-typed LogicalPlanNode so any other
        # reference to this same node (via the shared memo) lands here too.
        memo[id(self)] = new_node
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


def _query_body(branch):
    """Find the Query a statement runs, unwrapping the statements that wrap one.

    A statement that wraps a query carries the WITH clause on the INNER query, not on
    itself. Looking for `with` on the outer node found nothing, so the CTEs were silently
    dropped and the query failed with "dataset '<cte name>' could not be found":

        EXPLAIN WITH c AS (...) SELECT * FROM c     -- query is under Explain.statement
        INSERT INTO t WITH c AS (...) SELECT * FROM c   -- query is under Insert.source
    """
    if "Explain" in branch:
        branch = branch["Explain"]["statement"]
    elif "Insert" in branch:
        branch = branch["Insert"]["source"]
    return branch.get("Query", branch)


def extract_ctes(branch, planner):
    ctes = {}
    with_clause = _query_body(branch).get("with")
    if with_clause:
        if with_clause.get("recursive"):
            # A recursive CTE needs a fixpoint operator in the execution engine — the
            # plan graph must stay acyclic, so the loop has to live inside an operator.
            # That does not exist yet. Until it does, say so: previously the self-
            # reference was re-expanded forever and hung the planner.
            raise UnsupportedSyntaxError(
                "WITH RECURSIVE is not supported. Rewrite the query without recursion."
            )
        for _ast in with_clause["cte_tables"]:
            alias = _ast.get("alias")["name"]["value"]
            logical_plan = planner(_ast["query"]["body"])
            # CTEs don't have an exit node
            plan_head = logical_plan.get_exit_points()[0]
            logical_plan.remove_node(plan_head, True)

            # `WITH t(a, b) AS (...)` renames the CTE's output columns. The names were
            # previously parsed and dropped, so the rename silently did nothing and the
            # body's own column names leaked out. Carry them on the plan head; the
            # Relation Resolver applies them when it splices the CTE in.
            column_aliases = [
                col["name"]["value"] for col in (_ast.get("alias").get("columns") or [])
            ]
            if column_aliases:
                head_nid = logical_plan.get_exit_points()[0]
                head = logical_plan[head_nid]
                head.column_aliases = column_aliases
                logical_plan[head_nid] = head

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
        right = Node(NodeType.LITERAL, type=_plt.VARCHAR, value=filters["Like"])
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


def _validate_where_clause_expression(
    node: Node, clause_label: str = "WHERE clause", example_prefix: str = "WHERE "
) -> None:
    """Validate that a WHERE clause (or JOIN ON condition) contains a valid boolean
    expression.

    WHERE/ON clauses must contain explicit comparisons or boolean operators, not bare
    literals or identifiers. This prevents ambiguity and silent incorrect results — a
    bare-literal conjunct (e.g. `ON a.x = b.y AND FALSE`) has no column to key a join
    on and no column for a Filter step to reference, so it cannot be routed through
    the optimizer's predicate-pushdown machinery and would otherwise be silently
    dropped between planning and execution instead of failing loud.

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
        _validate_where_clause_expression(node.left, clause_label, example_prefix)
        _validate_where_clause_expression(node.right, clause_label, example_prefix)
        return

    # NOT is allowed if applied to a valid boolean expression
    if node_type == NodeType.NOT:
        _validate_where_clause_expression(node.centre, clause_label, example_prefix)
        return

    # Allowed: nested expressions
    if node_type == NodeType.NESTED:
        _validate_where_clause_expression(node.centre, clause_label, example_prefix)
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
            f"{clause_label} cannot be a bare literal ({node.value!r}). "
            f"Use a comparison (e.g., '{example_prefix}column = {{value}}') or IS operator "
            f"(e.g., '{example_prefix}column IS TRUE')."
        )

    # Disallowed: bare identifiers (column names without comparison)
    if node_type == NodeType.IDENTIFIER:
        raise UnsupportedSyntaxError(
            f"{clause_label} cannot be a bare column name ({node.value!r}). "
            f"Use a comparison (e.g., '{example_prefix}{node.value} = value') or IS operator "
            f"(e.g., '{example_prefix}{node.value} IS TRUE')."
        )

    # Any other node type in WHERE/ON is unsupported
    raise UnsupportedSyntaxError(
        f"{clause_label} contains unsupported expression type: {node_type}. "
        f"{clause_label} requires a boolean comparison or function."
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
                inner_plan.add_edge(step_id, join_step_id, "left")
                inner_plan.add_edge(right_step_id, join_step_id, "right")

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
    _RANKING_FUNCTIONS = ("ROW_NUMBER", "RANK", "DENSE_RANK")
    _window_specs: list = []  # aggregate windows: (index, agg_node, partition_by_nodes)
    # ranking windows: (kind, partition_by_nodes, order_by_pairs, win_alias)
    _ranking_specs: list = []
    for _i, proj_col in enumerate(_projection):
        _over = getattr(proj_col, "over", None)
        _is_ranking = (
            proj_col.node_type == NodeType.AGGREGATOR and proj_col.value in _RANKING_FUNCTIONS
        )
        if _is_ranking:
            if _over is None:
                raise UnsupportedSyntaxError(
                    f"{proj_col.value}() is a window function and requires an OVER (...) clause."
                )
            if not _over.get("order_by"):
                raise UnsupportedSyntaxError(
                    f"{proj_col.value}() requires an ORDER BY in its OVER (...) clause."
                )
            if _over.get("window_frame") is not None:
                raise UnsupportedSyntaxError(
                    "Window functions with frame specifications (ROWS/RANGE BETWEEN) are not supported."
                )
            _partition_by = [
                logical_planner_builders.build(pb) for pb in _over.get("partition_by", [])
            ]
            # The WINDOW's own ORDER BY (inside OVER(...)), which is a different thing
            # from the statement-level ORDER BY built below — named apart so the two
            # cannot be confused or clobber one another.
            _window_order_by = [
                (
                    logical_planner_builders.build(item["expr"]),
                    True if item["options"]["asc"] is None else item["options"]["asc"],
                )
                for item in _over.get("order_by", [])
            ]
            _win_alias = proj_col.alias or f"$win_{random_string(6)}"
            _ranking_specs.append((proj_col.value, _partition_by, _window_order_by, _win_alias))
            _ref = LogicalColumn(
                node_type=NodeType.IDENTIFIER, source_column=_win_alias, alias=_win_alias
            )
            _ref.query_column = _win_alias
            _projection[_i] = _ref
            continue
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

    # ORDER BY is BUILT here, ahead of the aggregate step below, for the same reason
    # HAVING is (see the note on the next block): `GROUP BY k ORDER BY COUNT(*) DESC`
    # sorts on an aggregate that is never selected, so that aggregate has to reach
    # `_aggregates` before the aggregate step is constructed or nothing computes it.
    # Only the BUILD moves up — the "which ORDER BY columns aren't in the projection"
    # reconciliation still runs further down, next to the Project it feeds, and it
    # already hoists an unselected aggregate as a pass-through column once the value
    # actually exists.
    _order_by = ast_branch.get("order_by")
    _order_by_columns_not_in_projection: list = []
    _order_by_columns: list = []
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
                _expr_cat = expr.type.category if isinstance(expr.type, ColumnType) else expr.type
                if _expr_cat != LogicalCategory.INTEGER:
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

    # An aggregate in ORDER BY that the SELECT list does not name is still an
    # aggregate the aggregate step must produce (`GROUP BY k ORDER BY MIN(x)`).
    # The pass-through hoist further down already carries the VALUE up to the Order
    # node; without this it would carry a value nothing ever computed, and the
    # binder would fail on the pruned operand column instead.
    for _order_column in _order_by_columns:
        for _aggregate in get_all_nodes_of_type(
            _order_column, select_nodes=(NodeType.AGGREGATOR,)
        ):
            _aggregates.append(_aggregate)

    # HAVING is BUILT here, not at its position in the plan further down, because a
    # HAVING clause may reference aggregates and group keys that never appear in the
    # SELECT list — `GROUP BY k HAVING SUM(x) > 1` with neither SUM(x) nor k selected
    # is valid SQL-92 and is exactly what canonical TPC-H Q18 does. Two things follow:
    #
    #   1. those aggregates must join `_aggregates` so the aggregate step below
    #      actually COMPUTES them (otherwise nothing produces the value, and the
    #      binder prunes the operand column out of the schema entirely), and
    #   2. the expressions must ride through the Project as pass-through columns,
    #      because the HAVING Filter sits ABOVE the Project. The Exit node prunes back
    #      to the SELECT list, so they never reach the output row.
    #
    # Collected AFTER decompose_aggregates: decomposition rewrites the projection list
    # only, so a decomposed HAVING aggregate would leave the condition tree referencing
    # an expression that nothing computes. Undecomposed aggregates over expressions are
    # supported by the aggregate operator directly.
    _having = logical_planner_builders.build(ast_branch["Select"].get("having"))
    _having_passthrough: list = []
    if _having:
        _projection_aliases = {p.alias.lower() for p in _projection if p.alias}
        _seen_expressions = {format_expression(p).lower() for p in _projection}
        _seen_expressions.update(
            p.qualified_name.lower() for p in _projection if p.qualified_name
        )

        _having_aggregates = get_all_nodes_of_type(
            _having, select_nodes=(NodeType.AGGREGATOR,)
        )

        # Identifiers INSIDE an aggregate are pre-aggregation operands (SUM(x) consumes
        # raw x) — hoisting them past the aggregate step is meaningless and they must be
        # skipped. Only bare identifiers (group keys) are pass-through candidates.
        _aggregate_operands = {
            id(identifier)
            for aggregate in _having_aggregates
            for identifier in get_all_nodes_of_type(
                aggregate, select_nodes=(NodeType.IDENTIFIER,)
            )
        }

        for _aggregate in _having_aggregates:
            # The binder dedups the aggregate list by schema_column.identity, so an
            # aggregate already named in the SELECT is not computed twice.
            _aggregates.append(_aggregate)
            _key = format_expression(_aggregate).lower()
            if _key not in _seen_expressions:
                _seen_expressions.add(_key)
                _having_passthrough.append(_aggregate)

        for _identifier in get_all_nodes_of_type(_having, select_nodes=(NodeType.IDENTIFIER,)):
            if id(_identifier) in _aggregate_operands:
                continue
            # A bare identifier naming a SELECT alias (`SUM(q) AS x ... HAVING x > 300`)
            # resolves against the Project's own output — the Project creates it, so it
            # must not be hoisted from below it.
            if (_identifier.source_column or "").lower() in _projection_aliases:
                continue
            _key = format_expression(_identifier).lower()
            if _key not in _seen_expressions:
                _seen_expressions.add(_key)
                _having_passthrough.append(_identifier)

    _groups = logical_planner_builders.build(ast_branch["Select"].get("group_by"))[0]

    # Resolve positional and aliased GROUP BY into the actual projection
    # expression, mirroring the ORDER BY resolution below:
    #
    #  * a bare positive integer is a 1-based position into the SELECT list
    #    (`GROUP BY 1` = the 1st output column). This is the de-facto convention
    #    across the analytical-engine cohort — Dremio, Trino, DuckDB, ClickHouse
    #    (ClickHouse flipped its default TO positional in 22.7). It is not in
    #    SQL-92 (ordinals there are an ORDER BY feature), but the whole peer set
    #    adopted it, so Opteryx matches. A position that lands on an aggregate is
    #    rejected, same as those engines ("aggregates are not allowed in GROUP BY").
    #  * a bare identifier matching an output alias refers to that projection
    #    expression (Postgres/DuckDB precedence — the output alias wins over a
    #    same-named source column).
    #
    # The substitution must happen HERE, before the binder's schema-pruning
    # (opteryx/planner/binder/aggregate.py) runs — otherwise a computed
    # expression's (e.g. CASE) inner columns get pruned from the schema as unused
    # and a later ColumnNotFoundError follows.
    if isinstance(_groups, list) and _groups:
        _rewritten_groups = []
        for _group_expr in _groups:
            if _group_expr.node_type == NodeType.LITERAL:
                _expr_cat = (
                    _group_expr.type.category
                    if isinstance(_group_expr.type, ColumnType)
                    else _group_expr.type
                )
                if _expr_cat == LogicalCategory.INTEGER:
                    _position = int(_group_expr.value)
                    if _position < 1 or _position > len(_projection):
                        raise UnsupportedSyntaxError(
                            f"GROUP BY position {_position} is out of range — SELECT has {len(_projection)} column(s)."
                        )
                    _target = _projection[_position - 1]
                    if get_all_nodes_of_type(_target, select_nodes=(NodeType.AGGREGATOR,)):
                        raise UnsupportedSyntaxError(
                            f"GROUP BY position {_position} refers to an aggregate in the SELECT "
                            "list — aggregates cannot appear in GROUP BY."
                        )
                    _group_expr = _target
            elif _group_expr.node_type == NodeType.IDENTIFIER:
                _alias_match = next(
                    (
                        p
                        for p in _projection
                        if p.alias and p.alias.lower() == (_group_expr.source_column or "").lower()
                    ),
                    None,
                )
                if _alias_match is not None:
                    _group_expr = _alias_match
            _rewritten_groups.append(_group_expr)
        _groups = _rewritten_groups

    if _window_specs:
        if _groups is not None and _groups != []:
            raise UnsupportedSyntaxError("Window functions cannot be combined with GROUP BY.")
        _source_scan = _find_base_scan(inner_plan)
        # Group by distinct partition spec; same partition → one Window node (shared CTE).
        _by_partition: dict = {}
        for _i, _agg_node, _partition_by in _window_specs:
            _key = tuple(
                getattr(pb, "source_column", None)
                or getattr(pb, "value", None)
                or format_expression(pb)
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

    if _ranking_specs:
        if _groups is not None and _groups != []:
            raise UnsupportedSyntaxError("Window functions cannot be combined with GROUP BY.")
        from opteryx.types.schema import SchemaColumn, mint_column_identity

        # Group ranking functions that share the same PARTITION BY + ORDER BY into a
        # single Window node (one sort serves all of them).
        # `_window_order_by` here is the WINDOW's ORDER BY, deliberately NOT named
        # `_order_by`: the statement-level ORDER BY is built above this point, and a
        # loop variable of that name would silently overwrite it for everything
        # downstream (it did — `RANK() OVER(ORDER BY id)` lost the statement's ORDER BY).
        _by_spec: dict = {}
        for _kind, _partition_by, _window_order_by, _win_alias in _ranking_specs:
            _pkey = tuple(format_expression(pb) for pb in _partition_by)
            _okey = tuple((format_expression(c), bool(a)) for c, a in _window_order_by)
            _spec_key = (_pkey, _okey)
            if _spec_key not in _by_spec:
                _by_spec[_spec_key] = (_partition_by, _window_order_by, [])
            _by_spec[_spec_key][2].append((_kind, _win_alias))
        for _spec_key, (_partition_by, _window_order_by, _outs) in _by_spec.items():
            _win_rel = f"$window-{random_string(6)}"
            _outputs = [
                (
                    _kind,
                    SchemaColumn(
                        name=_win_alias,
                        column_type=_plt.INT64,
                        identity=mint_column_identity(_win_rel, _win_alias),
                    ),
                )
                for _kind, _win_alias in _outs
            ]
            _win_step = LogicalPlanNode(node_type=LogicalPlanStepType.Window)
            _win_step.partition_by = _partition_by
            _win_step.order_by = _window_order_by
            _win_step.outputs = _outputs
            _win_step.output_relation = _win_rel
            _win_step.columns = []
            previous_step_id, step_id = step_id, random_string()
            inner_plan.add_node(step_id, _win_step)
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

    # projection
    project_step = None
    if not (
        len(_projection) == 1
        and _projection[0].node_type == NodeType.WILDCARD
        and _projection[0].except_columns is None
        and _projection[0].value is None
    ):
        for column in _projection:
            if (
                column.node_type == NodeType.LITERAL
                # A typed NULL is never a parenthesised value list — `(a, b)` always
                # carries values. `CAST(NULL AS ARRAY<E>)` folds to a NULL literal that
                # must keep ARRAY<E> (nothing downstream could recover the element type
                # otherwise), which gives it category ARRAY and would trip this refusal.
                # Exempted narrowly by absence of a value, NOT by widening the category
                # test: an array literal that HAS values still cannot be projected (its
                # materialization is broken three separate ways), and this guard is what
                # keeps that a clean error instead of a TypeError or an empty array.
                and column.value is not None
                and column.type is not None
                and column.type.category
                in (
                    LogicalCategory.ARRAY,
                    LogicalCategory.VECTOR,
                )
            ):
                if ast_branch["Select"].get("distinct"):
                    raise UnsupportedSyntaxError(
                        "A value list cannot be projected in the SELECT clause — did you mean DISTINCT ON(cols) cols FROM ?"
                    )
                # Names BOTH spellings: the message used to say "parenthesised"
                # at a caller who had typed `SELECT ['a','b']`, sending them to
                # look for parentheses they had not used.
                raise UnsupportedSyntaxError(
                    "A literal list cannot be projected in the SELECT clause, in either "
                    "`['a', 'b']` or `('a', 'b')` form. Use `UNNEST(('a', 'b'))` in the "
                    "FROM clause to build a relation from literals."
                )

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
        project_step.passthrough_columns = _order_by_columns_not_in_projection
        project_step.except_columns = _projection[0].except_columns
        previous_step_id, step_id = step_id, random_string()
        inner_plan.add_node(step_id, project_step)
        if previous_step_id is not None:
            inner_plan.add_edge(previous_step_id, step_id)

    # EXCEPT with ORDER BY creates complex situations
    if project_step and project_step.except_columns and _order_by_columns_not_in_projection:
        if any(
            col.source_column in {c.source_column for c in project_step.except_columns}
            for col in project_step.passthrough_columns
        ):
            raise UnsupportedSyntaxError(
                "Cannot ORDER BY columns excluded by the EXCEPT clause in the projection."
            )
        project_step.passthrough_columns = []

    # HAVING expressions absent from the SELECT list ride through the Project so the
    # Filter above it can read them. Appended AFTER the EXCEPT/ORDER BY reconciliation
    # above — that block clears the ORDER BY pass-throughs, which must not take the
    # HAVING ones with it (the query is unexecutable without them).
    #
    # Deduped against the ORDER BY pass-throughs already present: `HAVING SUM(x) > 1
    # ORDER BY SUM(x)` hoists the same expression from both clauses, and emitting it
    # twice trips the binder's duplicate-output check (AmbiguousIdentifierError).
    if project_step is not None and _having_passthrough:
        _existing = list(project_step.passthrough_columns or [])
        _existing_keys = {format_expression(c).lower() for c in _existing}
        for _column in _having_passthrough:
            if format_expression(_column).lower() not in _existing_keys:
                _existing_keys.add(format_expression(_column).lower())
                _existing.append(_column)
        project_step.passthrough_columns = _existing

    # having
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
        elif project_step is not None and project_step.passthrough_columns:
            # the ORDER BY value is ambiguous once rows collapse into a DISTINCT
            # group - the column must appear in the SELECT list so the ordering
            # value is well-defined per output row.
            raise UnsupportedSyntaxError(
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
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
            # A conjunct with no column reference at all (a bare literal like
            # `AND FALSE`, anywhere in the AND-tree) has no join key to extract
            # and no column for a Filter step to carry it on — the optimizer's
            # predicate-pushdown machinery has nowhere to route it and silently
            # drops it between planning and execution. Reject at plan time
            # instead, mirroring the same rule already enforced for WHERE.
            _validate_where_clause_expression(
                join_on, clause_label="JOIN condition", example_prefix="ON "
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
            join_step.using = [logical_planner_builders.build(i[0]) for i in constraint["Using"]]
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
        # If we have args, we're a function dataset (like UNNEST)
        function = relation["relation"]["Table"]
        function_name = relation_name.upper()

        # READ_JSONL/READ_PARQUET/READ_CSV derive their column names from the file's
        # schema at bind time (not yet implemented), so unlike UNNEST/VALUES/
        # GENERATE_SERIES they don't require an AS alias(columns) clause to name
        # their output columns.
        requires_column_alias = function_name not in ("READ_JSONL", "READ_PARQUET", "READ_CSV")

        if function["alias"] is None and requires_column_alias:
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
        elif function["alias"] is not None:
            function_step.alias = function["alias"]["name"]["value"]

        args = []
        named_args = {}
        for arg in function["args"]["args"]:
            if "Named" in arg:
                named = arg["Named"]
                named_args[named["name"]["value"]] = logical_planner_builders.build(named["arg"])
            else:
                args.append(logical_planner_builders.build(arg))
        function_step.args = args
        function_step.named_args = named_args
        if function["alias"] is not None:
            function_step.columns = tuple(
                col["name"]["value"] for col in function["alias"]["columns"]
            )

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
    # GRAPHVIZ is the carrier token, not a request: the parser has no MERMAID
    # keyword, so sql_rewriter.rewrite_explain sends MERMAID through as GRAPHVIZ
    # (and rejects a genuine FORMAT GRAPHVIZ before parsing). This maps it back.
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
        from opteryx.planner.relation_resolver import rename_relations

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
        # A BARE wildcard: `value` MUST be None. A non-None `value` marks a QUALIFIED
        # wildcard (`rel.*`) and binder.visit_exit then expands only columns whose
        # origin matches `value[0]` — `(None,)` matches no relation, so the EXIT bound
        # to zero columns and the set operation failed with a misleading "UNION leg
        # narrower than the union schema". Reached whenever the left leg has no
        # Project node, i.e. a bare `SELECT *` leg.
        columns = [LogicalPlanNode(NodeType.WILDCARD)]
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


def _plan_virtual_dataset_scan(relation: str, internal_relation: bool) -> LogicalPlan:
    """`SELECT * FROM <virtual dataset>`, built by the planner rather than typed.

    Shared by the `SHOW` forms that are just a wildcard read of a virtual dataset,
    so `SHOW VARIABLES` and `SHOW USER` cannot drift into producing different plan
    shapes for the same job.
    """
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = relation
    from_step.alias = relation
    # For a relation in INTERNAL_ONLY_DATASETS, binder.visit_scan rejects the scan
    # unless this flag marks it as planner-built rather than user-typed.
    from_step.internal_relation = internal_relation
    step_id = random_string()
    plan.add_node(step_id, from_step)

    exit_node = LogicalPlanNode(node_type=LogicalPlanStepType.Exit)
    # A BARE wildcard: `value` must be None. A non-None `value` marks a QUALIFIED
    # wildcard (`rel.*`) and binder.visit_exit then expands only columns whose
    # origin matches `value[0]` — so `(None,)` silently expands to nothing.
    exit_node.columns = [LogicalPlanNode(NodeType.WILDCARD)]
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, exit_node)
    plan.add_edge(previous_step_id, step_id)

    return plan


def _plan_show_manifest(table_name: str) -> LogicalPlan:
    """`SHOW MANIFEST FOR <table>` — Scan (bound for permission/manifest loading
    only, never read) -> ShowManifest (materializes the already-bound Manifest).

    No Exit node: this mirrors plan_show_columns exactly, not
    _plan_virtual_dataset_scan — ShowManifest is a non-pipeline special op
    answered directly from binder-attached metadata (serial_engine.py calls
    `head_node(None)`), not a normal Scan of real row data run through the
    native pipeline. An Exit node here would make Exit the plan's head, which
    is not a special op, and misroute the whole query into the native
    compiler, which has no operator for ShowManifestNode.

    `SHOW` has no WHERE/column-list grammar in the first place, so there is
    no filter/projection this builder needs to guard against.
    """
    plan = LogicalPlan()

    from_step = LogicalPlanNode(node_type=LogicalPlanStepType.Scan)
    from_step.relation = table_name
    from_step.alias = table_name
    from_step.hints = []
    # binder.visit_scan reads this to (a) additionally require the owner-only
    # MANIFEST permission beside the normal READ gate and (b) never compile a
    # real file scan for this Scan — the bound Manifest IS the answer.
    from_step.for_manifest_only = True
    step_id = random_string()
    plan.add_node(step_id, from_step)

    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.ShowManifest)
    show_step.relation = table_name
    previous_step_id, step_id = step_id, random_string()
    plan.add_node(step_id, show_step)
    plan.add_edge(previous_step_id, step_id)

    return plan


def plan_show_variables(statement, **kwargs):
    """SHOW VARIABLES, SHOW USER, SHOW GRANTS, SHOW MANIFEST FOR — planned from
    the parser's generic `ShowVariable` catch-all.

    The parser folds every bare `SHOW <words>` form it does not recognise as
    its own statement into a single `ShowVariable` node carrying the trailing
    words: `SHOW VARIABLES` gives an empty list, `SHOW USER` gives ["USER"],
    `SHOW TIME ZONE` gives ["TIME", "ZONE"], `SHOW MANIFEST FOR a.b.c` gives
    ["MANIFEST", "FOR", "a", "b", "c"] (the parser's `parse_identifiers` drops
    the `.` tokens between identifiers, not just whitespace, so a dotted name
    arrives as separate words to rejoin — see sqlparser's `parse_identifiers`).
    So this one builder is the front door for every form the parser did not
    recognise, and it has to name the ones that are ours.

    `SHOW VARIABLES LIKE '<pattern>'` parses to ["LIKE"] with the pattern
    DISCARDED by the parser, so it cannot be honoured here — it is rejected
    rather than silently answered with the unfiltered list, which would be a
    wrong answer wearing the shape of a right one.
    """
    root_node = "ShowVariable"

    parts = statement[root_node]["variable"]
    words = [part["value"].upper() for part in parts]
    if not words:
        # `$variables` is INTERNAL_ONLY_DATASETS: SHOW VARIABLES is its only surface.
        return _plan_virtual_dataset_scan("$variables", internal_relation=True)
    if words == ["USER"]:
        # `$user` is INTERNAL_ONLY_DATASETS on the same rule as `$variables`:
        # SHOW USER is its only surface.
        return _plan_virtual_dataset_scan("$user", internal_relation=True)
    if words == ["GRANTS"]:
        # `$grants` is INTERNAL_ONLY_DATASETS on the same rule: SHOW GRANTS is
        # its only surface. It reports the session's own policies and confers
        # nothing — Opteryx has no GRANT/REVOKE.
        return _plan_virtual_dataset_scan("$grants", internal_relation=True)
    if words == ["LIKE"]:
        # The parser discards the pattern, so we cannot apply it.
        raise UnsupportedSyntaxError(
            "Opteryx does not support `SHOW VARIABLES LIKE`; use `SHOW VARIABLES`."
        )
    if words[0] == "MANIFEST":
        if len(words) < 3 or words[1] != "FOR":
            raise UnsupportedSyntaxError(
                "`SHOW MANIFEST FOR <table>` requires a table name, e.g. "
                "`SHOW MANIFEST FOR opteryx.test.pypi`."
            )
        # Original case preserved for the table's identifier segments -- only
        # the MANIFEST/FOR control words above were matched via the uppercased
        # `words` list; catalog/schema/table names are case-sensitive.
        table_name = ".".join(part["value"] for part in parts[2:])
        return _plan_show_manifest(table_name)
    raise UnsupportedSyntaxError(
        f"Opteryx does not support 'SHOW {' '.join(words)}'; "
        "supported forms are `SHOW VARIABLES`, `SHOW USER`, `SHOW GRANTS`, and "
        "`SHOW MANIFEST FOR <table>`."
    )


def plan_show_create_query(statement, **kwargs):
    root_node = "ShowCreate"
    plan = LogicalPlan()
    show_step = LogicalPlanNode(node_type=LogicalPlanStepType.Show)
    show_step.object_type = statement[root_node]["obj_type"].upper()
    if show_step.object_type != "VIEW":
        # Rejected here, by name, rather than at execution time: a table has no
        # stored CREATE statement to show (its schema is the catalog's, not a
        # statement we kept), so no amount of planning makes this answerable.
        raise UnsupportedSyntaxError(
            f"Opteryx does not support 'SHOW CREATE {show_step.object_type}'; "
            "only `SHOW CREATE VIEW <view>` is supported."
        )
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

    # Opteryx has no materialization: the view body is stored as text and
    # planned afresh on every reference. Accepting MATERIALIZED and creating an
    # ordinary view would hand back something that reads as precomputed and is
    # not - and DROP MATERIALIZED VIEW is rejected, so it could not even be
    # dropped by the name it was created with.
    if statement[root_node].get("materialized", False):
        raise UnsupportedSyntaxError(
            "Opteryx does not support materialized views; "
            "`CREATE VIEW` stores the query and plans it on each reference."
        )

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


def plan_alter_table(statement, **kwargs):
    """
    Create a logical plan for ALTER TABLE statement.

    ALTER TABLE [IF EXISTS] table_name CLUSTER BY (column [, column ...])
    ALTER TABLE [IF EXISTS] table_name RENAME TO new_table_name

    These are the only ALTER TABLE operations Opteryx supports; any other
    operation (ADD COLUMN, DROP COLUMN, ...) is rejected.
    """
    root_node = "AlterTable"
    plan = LogicalPlan()

    alter_statement = statement[root_node]

    # Extract table name
    relation_name_parts = alter_statement["name"]
    relation_name = extract_variable(relation_name_parts)
    if isinstance(relation_name, list):
        relation_name = ".".join(relation_name)

    if_exists = alter_statement.get("if_exists", False)

    operations = alter_statement.get("operations") or []
    if len(operations) != 1:
        raise UnsupportedSyntaxError(
            "Opteryx only supports a single ALTER TABLE operation per statement."
        )
    operation = operations[0]

    if "ClusterBy" in operation:
        cluster_columns = []
        for expr in operation["ClusterBy"]["exprs"]:
            if "Identifier" not in expr:
                raise UnsupportedSyntaxError(
                    "CLUSTER BY only supports column names, not expressions."
                )
            cluster_columns.append(expr["Identifier"]["value"])

        alter_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterRelation)
        alter_relation_node.relation_name = relation_name
        alter_relation_node.cluster_columns = cluster_columns
        alter_relation_node.if_exists = if_exists

        plan.add_node(random_string(), alter_relation_node)
        return plan

    if "RenameTable" in operation:
        new_name = extract_variable(operation["RenameTable"]["table_name"]["To"])
        if isinstance(new_name, list):
            new_name = ".".join(new_name)

        # A rename may move the relation between collections but never between
        # workspaces: the two would live in different catalogs, and moving data
        # across them is a copy, not a rename. Rejected here rather than left
        # for a connector to discover half way through.
        if relation_name.split(".", 1)[0] != new_name.split(".", 1)[0]:
            raise UnsupportedSyntaxError(
                f"RENAME TO cannot move a relation between workspaces "
                f"({relation_name} -> {new_name}); the workspace must be unchanged."
            )
        if relation_name == new_name:
            raise UnsupportedSyntaxError(
                f"RENAME TO target is the same as the source ({relation_name})."
            )

        rename_relation_node = LogicalPlanNode(node_type=LogicalPlanStepType.RenameRelation)
        rename_relation_node.relation_name = relation_name
        rename_relation_node.new_relation_name = new_name
        rename_relation_node.if_exists = if_exists

        plan.add_node(random_string(), rename_relation_node)
        return plan

    raise UnsupportedSyntaxError(
        "Opteryx only supports 'ALTER TABLE ... CLUSTER BY (...)' and "
        "'ALTER TABLE ... RENAME TO ...'."
    )


def _parse_boolean_workspace_property(name: str, value):
    """ON/OFF/TRUE/FALSE -> bool, for a boolean-typed workspace property."""
    if isinstance(value, dict) and "Identifier" in value:
        token = value["Identifier"]["value"].upper()
        if token == "ON":
            return True
        if token == "OFF":
            return False
    if isinstance(value, dict) and "Value" in value:
        literal = value["Value"]["value"]
        if isinstance(literal, dict) and "Boolean" in literal:
            return literal["Boolean"]

    raise UnsupportedSyntaxError(
        f"Workspace property '{name}' is a boolean; use ON, OFF, TRUE or FALSE."
    )


# The workspace properties Opteryx will set, each with the parser that turns its
# AST value into the stored value. A property absent from this map is rejected at
# plan time - an unrecognised name must not be written through to the catalog,
# where a typo would silently become a new, meaningless property.
WORKSPACE_PROPERTIES = {
    "delete_protection": _parse_boolean_workspace_property,
}


def plan_alter_workspace(statement, **kwargs):
    """
    Create a logical plan for ALTER WORKSPACE statement.

    ALTER WORKSPACE workspace SET property TO value

    The parser has no WORKSPACE object type, so the SQL rewriter
    (sql_rewriter.rewrite_alter_workspace) turns this into ALTER FUNCTION,
    which parses to the same `<name> SET <property> TO <value>` shape.
    """
    root_node = "AlterFunction"
    plan = LogicalPlan()

    alter_statement = statement[root_node]

    workspace_name = extract_variable(alter_statement["function"]["name"])
    if isinstance(workspace_name, list):
        workspace_name = ".".join(workspace_name)
    if "." in workspace_name:
        raise UnsupportedSyntaxError(
            f"ALTER WORKSPACE names a workspace, not a relation within one (got '{workspace_name}')."
        )

    operation = alter_statement.get("operation") or {}
    actions = (operation.get("Actions") or {}).get("actions") or []
    if len(actions) != 1 or "Set" not in actions[0]:
        raise UnsupportedSyntaxError(
            "Opteryx only supports 'ALTER WORKSPACE <workspace> SET <property> TO <value>'."
        )

    action = actions[0]["Set"]

    property_name = extract_variable(action["name"])
    if isinstance(property_name, list):
        property_name = ".".join(property_name)
    property_name = property_name.lower()

    parser = WORKSPACE_PROPERTIES.get(property_name)
    if parser is None:
        supported = ", ".join(sorted(WORKSPACE_PROPERTIES))
        raise UnsupportedSyntaxError(
            f"'{property_name}' is not a settable workspace property. Supported: {supported}."
        )

    values = action["value"].get("Values") or []
    if len(values) != 1:
        raise UnsupportedSyntaxError(
            f"Workspace property '{property_name}' takes a single value."
        )

    alter_workspace_node = LogicalPlanNode(node_type=LogicalPlanStepType.AlterWorkspace)
    alter_workspace_node.workspace_name = workspace_name
    alter_workspace_node.property_name = property_name
    alter_workspace_node.property_value = parser(property_name, values[0])

    plan.add_node(random_string(), alter_workspace_node)

    return plan


def plan_create_collection(statement, **kwargs):
    """
    Create a logical plan for CREATE COLLECTION statement.

    CREATE COLLECTION [IF NOT EXISTS] [workspace].[collection]

    Rewritten to CREATE SCHEMA by the SQL rewriter
    (sql_rewriter.rewrite_create_collection) since the parser has no COLLECTION
    object type of its own. `CREATE SCHEMA` spelled directly lands here too -
    the same aliasing DROP SCHEMA already has for DROP COLLECTION.
    """
    root_node = "CreateSchema"
    plan = LogicalPlan()

    create_statement = statement[root_node]

    schema_name = create_statement["schema_name"]
    # The parser wraps the name; `Simple` is the only form a collection name can
    # take (an unqualified or dotted identifier).
    if not isinstance(schema_name, dict) or "Simple" not in schema_name:
        raise UnsupportedSyntaxError("CREATE COLLECTION expects a collection name.")

    collection_name = extract_variable(schema_name["Simple"])
    if isinstance(collection_name, list):
        collection_name = ".".join(collection_name)

    # A collection lives inside exactly one workspace, so its name is always
    # `workspace.collection`. Rejected here rather than left for a connector to
    # discover, where a bare name would resolve to some default workspace and
    # silently create the collection somewhere the caller did not name.
    if collection_name.count(".") != 1:
        raise UnsupportedSyntaxError(
            f"CREATE COLLECTION names a collection as '<workspace>.<collection>' "
            f"(got '{collection_name}')."
        )

    create_collection_node = LogicalPlanNode(node_type=LogicalPlanStepType.CreateCollection)
    create_collection_node.collection_name = collection_name
    create_collection_node.if_not_exists = create_statement.get("if_not_exists", False)

    plan.add_node(random_string(), create_collection_node)

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

    # The parser accepts these modifiers for every DROP form; Opteryx implements
    # none of them. Accepting them silently is worse than rejecting them: a
    # `DROP COLLECTION ... CASCADE` that quietly did not cascade reads as a
    # successful recursive drop, and `RESTRICT`/`PURGE` promise a guarantee about
    # dependants and storage reclamation that nothing here honours.
    for modifier in ("cascade", "restrict", "purge", "temporary"):
        if drop_statement.get(modifier, False):
            raise UnsupportedSyntaxError(
                f"Opteryx does not support `{modifier.upper()}` on DROP statements."
            )

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

    elif object_type == "Schema":
        # DROP COLLECTION path — rewritten to DROP SCHEMA by the SQL rewriter
        # (sql_rewriter.rewrite_drop_collection) since the parser has no
        # COLLECTION object type of its own.
        drop_collection_node = LogicalPlanNode(node_type=LogicalPlanStepType.DropCollection)

        names = drop_statement["names"]
        collection_names = []
        for name_parts in names:
            collection_name = extract_variable(name_parts)
            if isinstance(collection_name, list):
                collection_name = ".".join(collection_name)
            collection_names.append(collection_name)

        drop_collection_node.collection_names = collection_names
        drop_collection_node.if_exists = drop_statement.get("if_exists", False)

        plan.add_node(random_string(), drop_collection_node)
        return plan

    else:
        raise UnsupportedSyntaxError(f"DROP {object_type} is not supported")


def _plan_ctas(relation_name, if_not_exists, query_ast, or_replace=False):
    """Plan CREATE TABLE ... AS SELECT.

    Builds: SELECT subtree (Exit-headed, kept - not stripped) → InsertNode
    (create_target=True). Mirrors plan_insert's SELECT-source branch: keeping
    the Exit node is what makes serial_engine.py route this onto execute_native
    (the native engine) instead of the legacy push-pipeline, which cannot drive
    an InsertNode sink attached anywhere else. Target schema is derived at bind
    time from the SELECT's exit columns.
    """
    plan = LogicalPlan()

    source_plan = plan_query(query_ast)
    exit_node_id = source_plan.get_exit_points()[0]
    plan += source_plan
    source_tail_id = exit_node_id

    insert_step = LogicalPlanNode(node_type=LogicalPlanStepType.Insert)
    insert_step.relation_name = relation_name
    insert_step.values_feeder = None
    insert_step.source_tail_id = source_tail_id
    insert_step.explicit_columns = None
    insert_step.create_target = True
    insert_step.if_not_exists = if_not_exists
    insert_step.or_replace = or_replace

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
    from opteryx.types.schema import RelationSchema, SchemaColumn

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

    # CTAS path
    query_ast = statement[root_node].get("query")
    if query_ast is not None:
        # Check for unsupported options (or_replace is handled below, CTAS-only)
        for option in ["external", "temporary", "transient", "volatile", "iceberg"]:
            if statement[root_node].get(option):
                raise UnsupportedSyntaxError(f"CREATE TABLE option not supported: {option}")
        column_defs = statement[root_node].get("columns", [])
        if column_defs:
            raise UnsupportedSyntaxError("CREATE TABLE AS SELECT cannot specify column definitions")
        return _plan_ctas(
            relation_name=create_table_node.relation_name,
            if_not_exists=create_table_node.if_not_exists,
            query_ast=query_ast,
            or_replace=statement[root_node].get("or_replace", False),
        )

    # Check for unsupported options (plain CREATE TABLE form — or_replace not supported here)
    for option in ["or_replace", "external", "temporary", "transient", "volatile", "iceberg"]:
        if statement[root_node].get(option):
            raise UnsupportedSyntaxError(f"CREATE TABLE option not supported: {option}")

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
            raise UnsupportedSyntaxError(f"unsupported column type in CREATE TABLE: {type_key}")

        # Map sqloxide type key to canonical SQL name, then resolve via the
        # single authoritative SQL-name-to-ColumnType table (logical_type.py
        # §14 vocabulary — LogicalCategory has no alias members for DOUBLE/BLOB).
        sql_type_str = type_mapping[type_key]
        from opteryx.types.logical_type import parse_column_type

        sql_type_ct = parse_column_type(sql_type_str)

        # Check for NOT NULL constraint
        col_nullable = True
        col_options = col_def.get("options", [])
        if col_options:
            for opt in col_options:
                if isinstance(opt, dict) and opt.get("option") == "NotNull":
                    col_nullable = False
                    break

        # Create SchemaColumn
        from opteryx.types.schema import mint_column_identity

        flat_col = SchemaColumn(
            name=col_name,
            column_type=sql_type_ct,
            nullable=col_nullable,
            identity=mint_column_identity("$create", col_name),
        )
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
    relation_name = ".".join(logical_planner_builders.build(p).value for p in table_name_parts)

    # Explicit column list (may be empty/None). sqloxide represents each column
    # reference as a compound-identifier part list; a plain (non-dotted) column
    # name is a single-element list wrapping an {"Identifier": {"value": ...}}.
    explicit_columns = []
    for col in insert_stmt.get("columns") or []:
        if (
            isinstance(col, list)
            and len(col) == 1
            and isinstance(col[0], dict)
            and "Identifier" in col[0]
        ):
            explicit_columns.append(col[0]["Identifier"]["value"])
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
        # SELECT source — plan the sub-query and keep its Exit node (it already
        # carries the correct final_columns/final_names from schema binding),
        # attaching the Insert sink onto it rather than stripping it. This
        # mirrors plan_explain's pattern and keeps the SELECT subplan genuinely
        # Exit-headed, which execute_native requires to run it on the native
        # engine instead of the legacy push-pipeline.
        source_plan = plan_query(insert_stmt["source"])
        exit_node_id = source_plan.get_exit_points()[0]

        plan += source_plan
        source_tail_id = exit_node_id

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
    analyze_node.action = "analyze_table"
    analyze_node.table_name = ".".join(
        part["Identifier"]["value"] for part in statement[root]["table_name"]
    )
    # FOR COLUMNS <list> scopes the analysis; empty list = whole table.
    # NB: not `.columns` — that attribute is reserved by the binder for
    # projection columns (post_bind expects bound column objects there).
    analyze_node.analyze_columns = [c["value"] for c in statement[root].get("columns") or []]

    analyze_id = random_string()
    plan.add_node(analyze_id, analyze_node)

    return plan


def plan_drop_statistics(statement, **kwargs) -> LogicalPlan:
    """DROP STATISTICS ON t [FOR COLUMNS …] — synthesized by the planner's
    pre-parse interception (no native sqlparser grammar). Reuses the Analyze
    logical node / Table Management physical node, dispatching on `action`."""
    root = "DropStatistics"
    plan = LogicalPlan()
    node = LogicalPlanNode(node_type=LogicalPlanStepType.Analyze)
    node.action = "drop_statistics"
    node.table_name = statement[root]["table_name"]
    node.analyze_columns = list(statement[root].get("columns") or [])

    node_id = random_string()
    plan.add_node(node_id, node)

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

    # The SQL rewriter turns TABLE and VIEW into EXTENSION, which is the only
    # object type the parser accepts for a dotted name. Anything else is a form
    # the rewriter did not produce - COMMENT ON COLUMN in particular parses
    # cleanly and reached the operator, where it failed as a missing *dataset*
    # named `ws.collection.table.column`.
    object_type = statement[root_node].get("object_type", "Extension")
    if object_type != "Extension":
        raise UnsupportedSyntaxError(
            f"Opteryx does not support 'COMMENT ON {object_type.upper()}'; "
            "comments can be set on a TABLE or a VIEW."
        )
    comment_node.object_type = object_type

    # Extract the comment text
    comment_node.comment = statement[root_node].get("comment", "")

    # Extract IF EXISTS flag
    comment_node.if_exists = statement[root_node].get("if_exists", False)

    # Add the Comment node
    plan.add_node(random_string(), comment_node)

    return plan


QUERY_BUILDERS = {
    "Analyze": plan_analyze_query,
    "DropStatistics": plan_drop_statistics,
    "Comment": plan_comment,
    "Explain": plan_explain,
    "Query": plan_query,
    "Set": plan_set_variable,
    "ShowColumns": plan_show_columns,
    "ShowCreate": plan_show_create_query,
    # "ShowFunctions": show_functions_query,
    "ShowVariable": plan_show_variables,  # generic SHOW handler; only SHOW VARIABLES is supported
    # "Use": plan_use
    "CreateSchema": plan_create_collection,  # CREATE COLLECTION, rewritten by the SQL rewriter
    "CreateView": plan_create_view,
    "AlterView": plan_alter_view,
    "AlterTable": plan_alter_table,
    "AlterFunction": plan_alter_workspace,  # ALTER WORKSPACE, rewritten by the SQL rewriter
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
