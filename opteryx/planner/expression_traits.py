# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Expression-shape traits shared across planner phases.

Predicates that read an expression tree and answer a question about its SHAPE.
They touch no plan graph, no schema and no catalogue, which is what lets the
Binder and the Optimizer share one definition instead of each carrying its own.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type

# Functions whose value is not a function of their arguments alone. Any rewrite
# that RELOCATES an expression — hoisting a join key into a Project, folding a
# group key away — changes how many times it is evaluated, and for these that
# changes the answer.
#
# `group_key_reduction` shares this set: dropping a group key relocates the
# expression into a Project above the aggregate, which is the same question.
# It carried its own narrower copy until the two drifted and NORMAL() was
# reduced away, silently collapsing groups that must stay separate.
#
# `constant_folding` keeps its own narrower inline tuple and must not be folded
# in here: it asks a different question — whether a zero-argument call can be
# evaluated at PLAN time — and folding NOW()/CURRENT_DATE to one timestamp per
# query is the DESIRED behaviour there. Only the RANDOM family is excluded.
VOLATILE_FUNCTIONS = frozenset(
    {
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "GEN_RANDOM_UUID",
        "NEWID",
        "NORMAL",
        "NOW",
        "RAND",
        "RANDOM",
        "RANDOM_STRING",
        "UUID",
    }
)


def has_volatile_function(expression) -> bool:
    """True if `expression` contains a call whose value can change between
    evaluations of the same row."""
    return any(
        node.value is not None and str(node.value).upper() in VOLATILE_FUNCTIONS
        for node in get_all_nodes_of_type(expression, (NodeType.FUNCTION,))
    )
