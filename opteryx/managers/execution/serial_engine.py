# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This module provides the execution engine for processing physical plans in a serial manner.
"""

from typing import Any
from typing import Generator
from typing import Tuple

import pyarrow
from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import PhysicalPlan
from opteryx.models import QueryTelemetry

from opteryx import EOS


def execute(
    plan: PhysicalPlan, head_node: str = None, telemetry: QueryTelemetry = None
) -> Tuple[Generator[pyarrow.Table, Any, Any], ResultType]:
    from opteryx.operators.explain_node import ExplainNode
    from opteryx.operators.set_variable_node import SetVariableNode
    from opteryx.operators.show_create_node import ShowCreateNode
    from opteryx.operators.show_value_node import ShowValueNode
    from opteryx.operators.table_management_node import TableManagementNode
    from opteryx.operators.view_management_node import ViewManagementNode

    # Retrieve the tail of the query plan, which should ideally be a single head node
    head_nodes = list(set(plan.get_exit_points()))

    if len(head_nodes) != 1:
        raise InvalidInternalStateError(
            f"Query plan has {len(head_nodes)} heads, expected exactly 1."
        )

    if head_node is None:
        head_node = plan[head_nodes[0]]

    # Special case handling for 'Explain' queries
    if isinstance(head_node, ExplainNode):
        return (
            explain(plan, analyze=head_node.analyze, _format=head_node.format),
            ResultType.TABULAR,
        )

    # Special case handling
    if isinstance(head_node, SetVariableNode):
        # Set the variables and return a non-tabular result
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, (ViewManagementNode, TableManagementNode)):
        # Metadata DDL (CREATE/ALTER/DROP VIEW) - return non-tabular result
        return head_node(None), ResultType.NON_TABULAR
    if isinstance(head_node, (ShowValueNode, ShowCreateNode)):
        # There's no execution plan to execute, just return the result
        return head_node(None), ResultType.TABULAR

    def inner_execute(plan: PhysicalPlan) -> Generator:
        pump_nodes = [(nid, node) for nid, node in plan.depth_first_search_flat() if node.is_scan]
        for pump_nid, pump_instance in pump_nodes:
            for morsel in pump_instance(None):
                if morsel is not None:
                    yield from process_node(plan, pump_nid, morsel)
            yield from process_node(plan, pump_nid, EOS)

    return inner_execute(plan), ResultType.TABULAR


def explain(
    plan: PhysicalPlan, analyze: bool, _format: str
) -> Generator[pyarrow.Table, None, None]:
    from opteryx.operators.base_plan_node import BasePlanNode
    from opteryx.operators.exit_node import ExitNode
    from opteryx.operators.explain_node import ExplainNode

    def _inner_explain(node, depth):
        incoming_operators = plan.ingoing_edges(node)
        for operator_name in incoming_operators:
            operator = plan[operator_name[0]]
            if isinstance(operator, (ExitNode, ExplainNode)):  # Skip ExitNode
                yield from _inner_explain(operator_name[0], depth)
                continue
            elif isinstance(operator, BasePlanNode):
                record = {
                    "identity": operator.identity,
                    "tree": depth,
                    "operator": operator.name,
                    "config": operator.config,
                }
                if analyze:
                    record["time_ms"] = operator.execution_time / 1e6
                    record["records_in"] = operator.records_in
                    record["records_out"] = operator.records_out
                    record["bytes_in"] = operator.bytes_in
                    record["bytes_out"] = operator.bytes_out
                    record["calls"] = operator.calls
                yield record
                yield from _inner_explain(operator_name[0], depth + 1)

    head = list(dict.fromkeys(plan.get_exit_points()))
    if len(head) != 1:  # pragma: no cover
        raise InvalidInternalStateError(f"Problem with the plan - it has {len(head)} heads.")

    # for EXPLAIN ANALYZE, we execute the query and report telemetry
    if analyze:
        # we don't want the results, just the details from the plan
        temp = None
        head_node = plan.get_exit_points()[0]
        query_head, _, _ = plan.ingoing_edges(head_node)[0]
        results, result_type = execute(plan, query_head)
        if results is not None:
            results_generator, _ = next(results, ([], None))
            for temp in results_generator:
                pass
        del temp

    explained = list(_inner_explain(head[0], 1))

    if _format == "TEXT":
        table = pyarrow.Table.from_pylist(explained).select(
            col for col in explained[0] if col not in ["identity", "bytes_in", "bytes_out"]
        )
    else:
        from opteryx.utils import mermaid

        mermaid_plan = mermaid.plan_to_mermaid(plan, explained)
        # DEBUG: print(mermaid_plan)
        table = pyarrow.Table.from_pylist([{"plan": mermaid_plan}])

    yield table


def process_node(plan: PhysicalPlan, nid: str, morsel: pyarrow.Table) -> Generator:
    node = plan[nid]

    if node.is_scan:
        for _, child, _ in plan.outgoing_edges(nid):
            results = process_node(plan, child, morsel)
            yield from (result for result in results if result is not None)
    else:
        results = node(morsel)

        if results is None:
            yield None
            # If input was EOS, propagate it downstream even if node produced nothing
            if morsel is EOS:
                for _, child, _ in plan.outgoing_edges(nid):
                    yield from process_node(plan, child, EOS)
            return

        for result in (result for result in results if result is not None):
            children = plan.outgoing_edges(nid)
            if len(children) == 0 and result != EOS:
                yield result
            for _, child, _ in children:
                yield from process_node(plan, child, result)

        # After all results processed, if input was EOS, propagate EOS downstream
        if morsel is EOS:
            for _, child, _ in plan.outgoing_edges(nid):
                yield from process_node(plan, child, EOS)
