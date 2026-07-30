# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Bind-time capture of the ``like_selectivity_decay`` session variable
(opteryx/planner/binder/binder.py) onto a LIKE/ILIKE COMPARISON_OPERATOR
node, before ``predicate_rewriter`` mutates that SAME node's ``.value`` from
Like/ILike to InStr/IInStr (opteryx/planner/predicate_rewriter.py's
INSTR_REWRITES) — see selectivity.py's ``_selectivity_instr``, which reads
``node.like_selectivity_decay`` via plain attribute access.

This mirrors ``match_threshold``'s existing bind-time-capture contract: a
compiled plan must keep answering the selectivity question it was compiled
for, so a later ``SET`` must not retroactively change an already-bound plan's
estimate.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

import opteryx
from opteryx.expression import NodeType


def _bind(sql, ctx, run_optimizer=True):
    """Bind (and, by default, optimize) `sql`. predicate_rewriter's Like ->
    InStr mutation is an OPTIMIZER pass (see EXPLAIN's "predicate rewriter
    replace like with in string" step), not part of do_bind_phase itself —
    so a caller wanting the post-rewrite node needs run_optimizer=True."""
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.binder import do_bind_phase
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.optimizer import do_optimizer
    from opteryx.planner.plan_rewriter import do_plan_rewrite
    from opteryx.planner.relation_resolver import do_resolve_relations
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide
    from opteryx.models import QueryTelemetry

    telemetry = QueryTelemetry()
    clean = do_sql_rewrite(sql)
    parsed = sqloxide.parse_sql(clean, _dialect="opteryx")
    ast = do_ast_rewriter(parsed, parameters=[])[0]
    plan, _, ctes = do_logical_planning_phase(ast)
    plan = do_resolve_relations(plan, ctes, telemetry)
    plan = do_plan_rewrite(plan, telemetry)
    bound = do_bind_phase(
        plan, execution_context=ctx, query_id=str(uuid.uuid4()), telemetry=telemetry
    )
    if run_optimizer:
        bound = do_optimizer(bound, telemetry)
    return bound


def _find_instr_node(plan):
    from opteryx.planner.logical_planner import LogicalPlanStepType

    for _nid, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Filter:
            condition = getattr(node, "condition", None)
            if condition is not None and getattr(condition, "value", None) in (
                "InStr",
                "IInStr",
                "NotInStr",
                "NotIInStr",
            ):
                return condition
        predicates = getattr(node, "predicates", None)
        if predicates:
            for p in predicates:
                if getattr(p, "value", None) in ("InStr", "IInStr", "NotInStr", "NotIInStr"):
                    return p
    return None


def _session_with_decay(decay):
    # Real SET-statement execution — SystemVariablesContainer.__setitem__
    # expects a typed literal Node (via the SET grammar), not a raw Python
    # scalar, so setting the variable directly (bypassing SQL) is not a
    # faithful substitute here.
    session = opteryx.session()
    for _ in session.execute_to_morsels(f"SET like_selectivity_decay TO {decay};"):
        pass
    return session


def test_decay_is_captured_onto_the_rewritten_instr_node():
    session = _session_with_decay(0.42)
    bound = _bind("SELECT * FROM testdata.satellites WHERE name LIKE '%o%'", session.context)
    node = _find_instr_node(bound)
    assert node is not None, "expected an InStr/IInStr predicate after predicate_rewriter"
    assert node.like_selectivity_decay == 0.42


def test_decay_survives_the_like_to_instr_rewrite():
    # The captured value must be on the SAME node object predicate_rewriter
    # mutates in place -- not lost when .value changes from Like to InStr.
    session = _session_with_decay(0.55)
    bound = _bind("SELECT * FROM testdata.satellites WHERE name ILIKE '%a%'", session.context)
    node = _find_instr_node(bound)
    assert node is not None
    assert node.value in ("IInStr", "NotIInStr")
    assert node.like_selectivity_decay == 0.55


def test_set_after_bind_does_not_retroactively_change_an_already_bound_plan():
    session = _session_with_decay(0.7)
    bound = _bind("SELECT * FROM testdata.satellites WHERE name LIKE '%o%'", session.context)
    node = _find_instr_node(bound)
    captured = node.like_selectivity_decay
    assert captured == 0.7

    # A later SET on the SAME session must not reach back into the already-bound node.
    for _ in session.execute_to_morsels("SET like_selectivity_decay TO 0.1;"):
        pass
    assert session.context.variables["like_selectivity_decay"] == 0.1
    assert node.like_selectivity_decay == captured == 0.7


def test_unset_decay_is_none_not_a_default_guess():
    # Node.__getattr__ returns None for any never-set attribute -- confirms
    # the estimator's fallback trigger (decay is None -> flat constant) is
    # reachable, not just theoretical.
    from opteryx.models import Node

    fresh_node = Node(NodeType.COMPARISON_OPERATOR, value="InStr")
    assert fresh_node.like_selectivity_decay is None


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
