"""Every logical step kind fits the native plan graph and is found by kind.

The plan graph counts its steps per kind in a fixed number of native slots
(`kStepKindSlots`, src/cpp/planner/_plan_graph.hpp), and `steps_of` selects by a
bit mask over those slots. When LogicalPlanStepType grew past 64 kinds, every
statement planning to one of the newer kinds (CLONE, GRANT, CALL, ...) failed with
"plan step kind outside the plan graph's kind slots", and nothing in the required
suites planned one.

What is held here, for EVERY kind rather than a hand-picked few, so a kind added
later is covered without editing this file: a step of the kind can be put into a
LogicalPlan, and `has_step` / `count_steps` / `steps_of` find exactly that step -
in a plan holding one of every kind, so a kind in the mask's second word that
aliased onto a first-word kind would be seen twice.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.planner.plan_graph import STEP_KIND_SLOTS
from opteryx.compiled.structures import plan_steps
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext

KINDS = list(LogicalPlanStepType)


def _step_of(kind):
    """A default step of `kind`: every kind has a step class named `<Kind>Step`."""
    return vars(plan_steps)[f"{kind.name}Step"]()


def _plan_of_every_kind():
    """A plan holding one step of every kind, and `kind -> (node id, step)`."""
    plan = LogicalPlan(PlanContext())
    placed = {}
    for kind in KINDS:
        step = _step_of(kind)
        placed[kind] = (plan.add_node(step), step)
    return plan, placed


def test_the_kinds_reach_past_the_first_mask_word():
    # Without a kind of 64 or more this file would not exercise the mask's
    # second word, which is where the original failure was.
    assert max(kind.value for kind in KINDS) >= 64


def test_every_kind_is_inside_the_plan_graphs_slots():
    assert max(kind.value for kind in KINDS) < STEP_KIND_SLOTS


def test_every_kind_has_a_step_class_that_carries_it():
    for kind in KINDS:
        assert _step_of(kind).node_type is kind, kind.name


def test_each_kind_is_found_exactly_once_among_all_kinds():
    plan, placed = _plan_of_every_kind()

    for kind in KINDS:
        nid, step = placed[kind]
        assert plan.has_step(kind), kind.name
        assert plan.count_steps(kind) == 1, kind.name
        found = plan.steps_of((kind,))
        assert len(found) == 1, kind.name
        assert found[0][0] == nid, kind.name
        assert found[0][1] is step, kind.name


def test_steps_of_many_kinds_returns_them_in_insertion_order():
    plan, placed = _plan_of_every_kind()

    found = plan.steps_of(tuple(KINDS))

    assert [nid for nid, _ in found] == [placed[kind][0] for kind in KINDS]


def test_an_empty_plan_holds_no_kind():
    plan = LogicalPlan(PlanContext())

    for kind in KINDS:
        assert not plan.has_step(kind), kind.name
        assert plan.count_steps(kind) == 0, kind.name
        assert plan.steps_of((kind,)) == [], kind.name
