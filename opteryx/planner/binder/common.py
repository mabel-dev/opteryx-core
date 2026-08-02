# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Common utilities and dispatcher for the BinderVisitor pattern."""

import re
from functools import lru_cache
from typing import Tuple

from opteryx.models import Node
from opteryx.planner.binder.aggregate import visit_aggregate_and_group, visit_distinct
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.dataset import visit_function_dataset, visit_scan
from opteryx.planner.binder.filter import visit_filter
from opteryx.planner.binder.join import visit_join
from opteryx.planner.binder.order import visit_order
from opteryx.planner.binder.project import visit_exit, visit_project
from opteryx.planner.binder.set_ops import visit_except, visit_intersect, visit_set, visit_union, visit_unnest
from opteryx.planner.binder.window import visit_window
from opteryx.planner.binder.subquery import visit_comment, visit_subquery
from opteryx.planner.binder.traversal import post_bind, traverse
from opteryx.planner.binder.view import (
    visit_alter_view,
    visit_create_view,
    visit_drop_view,
    visit_show_columns,
    visit_show_manifest,
)
from opteryx.planner.binder.relation import (
    visit_alter_relation,
    visit_create_relation,
    visit_drop_relation,
    visit_truncate_relation,
    visit_insert,
)
from opteryx.planner.logical_planner import LogicalPlan

# Re-exported for backward compatibility with external callers (e.g. predicate_pushdown.py)
from opteryx.planner.binder.join_helpers import (  # noqa: F401
    convert_using_to_on,
    extract_join_fields,
    get_mismatched_condition_column_types,
)

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


@lru_cache(maxsize=128)
def node_type_to_method_name(node_type: str) -> str:
    return f"visit_{CAMEL_TO_SNAKE.sub('_', node_type).lower()}"


class BinderVisitor:
    """
    The BinderVisitor visits each node in the query plan and adds catalogue information
    to each node. This includes:

    - identifiers, bound from the schemas
    - functions and aggregators, bound from the function catalogue
    - variables, bound from the variables collection

    """

    def visit_node(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        """
        Visits a given node and returns a new node and context after binding catalog information.

        Parameters:
            node: Node
                The query plan node to visit.
            context: BindingContext
                The current binding context.

        Returns:
            Tuple[Node, BindingContext]
            The node and context after binding.
        """
        node_type = node.node_type.name  # type: ignore
        visit_method_name = node_type_to_method_name(node_type)
        visit_method = getattr(self, visit_method_name, None)
        if visit_method is None:
            return node, context
        return visit_method(node, context)

    def visit_aggregate_and_group(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_aggregate_and_group(self, node, context)

    # Aggregate nodes without grouping delegate to the group handler
    visit_aggregate = visit_aggregate_and_group

    def visit_distinct(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_distinct(self, node, context)

    def visit_filter(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_filter(self, node, context)

    def visit_order(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_order(self, node, context)

    def visit_join(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_join(self, node, context)

    def visit_exit(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_exit(self, node, context)

    def visit_project(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_project(self, node, context)

    def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_scan(self, node, context)

    def visit_function_dataset(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_function_dataset(self, node, context)

    def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_set(self, node, context)

    def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_union(self, node, context)

    def visit_intersect(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_intersect(self, node, context)

    def visit_except(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_except(self, node, context)

    def visit_unnest(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_unnest(self, node, context)

    def visit_window(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
        return visit_window(self, node, context)

    def visit_show_columns(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_show_columns(self, node, context)

    def visit_show_manifest(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_show_manifest(self, node, context)

    def visit_create_view(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_create_view(self, node, context)

    def visit_alter_view(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_alter_view(self, node, context)

    def visit_drop_view(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_drop_view(self, node, context)

    def visit_create_relation(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_create_relation(self, node, context)

    def visit_drop_relation(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_drop_relation(self, node, context)

    def visit_truncate_relation(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_truncate_relation(self, node, context)

    def visit_alter_relation(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_alter_relation(self, node, context)

    def visit_insert(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_insert(self, node, context)

    def visit_comment(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_comment(self, node, context)

    def visit_subquery(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        return visit_subquery(self, node, context)

    def visit_dependent_join(
        self, node: Node, context: BindingContext
    ) -> Tuple[Node, BindingContext]:
        from opteryx.exceptions import UnsupportedSyntaxError
        raise UnsupportedSyntaxError(
            "DependentJoin reached the Binder — correlated subquery was not decorrelated. "
            "This is a bug in the Plan Rewriter."
        )

    def post_bind(self, node: Node) -> Node:
        return post_bind(self, node)

    def traverse(
        self, graph: LogicalPlan, node: Node, context: BindingContext
    ) -> Tuple[LogicalPlan, BindingContext]:
        return traverse(self, graph, node, context)
