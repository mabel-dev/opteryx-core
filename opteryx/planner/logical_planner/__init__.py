# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Logical Planner converts a rewritten AST into a LogicalPlan — a directed graph of
LogicalPlanNode instances connected by data-flow edges.

Input:  a single parsed and AST-rewritten statement (dict produced by sqloxide)
Output: a tuple of (LogicalPlan, ast dict, CTEs dict)

Each node in the plan carries a LogicalPlanStepType (Scan, Filter, Join, Project, etc.)
and a property bag populated from the AST. Nodes are connected source → consumer, so
graph traversal from the exit node toward the scans mirrors execution order.

The planner handles all statement types: SELECT (including set operations, subqueries,
CTEs, joins), CREATE/ALTER/DROP VIEW, EXPLAIN, SHOW, SET, ANALYZE, and COMMENT.

The plan produced here is unbound — nodes hold raw identifiers and AST fragments.
Column types, schema references, and resolved identities are added by the Binder.
"""

from opteryx.planner.logical_planner.logical_planner import (
    LogicalPlan,
    LogicalPlanNode,
    LogicalPlanStepType,
    apply_visibility_filters,
    do_logical_planning_phase,
)
from opteryx.planner.logical_planner.logical_planner_builders import build

__all__ = (
    "apply_visibility_filters",
    "LogicalPlan",
    "LogicalPlanNode",
    "LogicalPlanStepType",
    "do_logical_planning_phase",
    "build",
)
