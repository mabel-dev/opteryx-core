# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode


def get_nodes_of_type_from_logical_plan(plan: LogicalPlan, types: tuple):
    matches = []
    for node in plan.nodes(True):
        if node[1].node_type in types:
            matches.append(node)
    return matches


class PlanRewriteContext:
    def __init__(self, plan: LogicalPlan, ctes: dict):
        self.pre_rewrite_tree: LogicalPlan = plan
        self.rewritten_plan: LogicalPlan = LogicalPlan()
        self.ctes: dict = ctes
        self.node_id: str | None = None
        self.parent_nid: str | None = None
        self.bag: dict = {}


class PlanRewriteStrategy:
    def __init__(self, telemetry):
        self.telemetry = telemetry

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return True

    def visit(self, node: LogicalPlanNode, context: PlanRewriteContext) -> PlanRewriteContext:
        raise NotImplementedError(
            "visit() must be implemented in PlanRewriteStrategy subclasses."
        )

    def complete(self, plan: LogicalPlan, context: PlanRewriteContext) -> LogicalPlan:
        raise NotImplementedError(
            "complete() must be implemented in PlanRewriteStrategy subclasses."
        )
