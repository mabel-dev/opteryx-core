# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Length-Only Column Strategy
===========================

Goal — stop decoding string bytes that no operation in the query ever reads.

A string column referenced *only* through length-answerable operations
(``col <> ''``, ``LENGTH(col)``) needs its per-value **length**, never its
**bytes**. Parquet already carries each value's length as a 4-byte prefix
(PLAIN) or a per-dictionary-entry length (dict), so such a column can be
decoded to lengths alone and the payload copy skipped entirely.

The prize is real: on ClickBench Q28 (``WHERE URL <> '' ... AVG(length(URL))``)
``URL`` averages ~90 bytes over ~99M rows and is never projected — ~9GB of
payload, copied twice on the current path (once into a temporary, again into
the arena), to produce values that are collapsed to a length and discarded.

What this strategy does
-----------------------
It proves eligibility and annotates the Scan; it does **not** change decoding.
The scan-side consumer of ``length_only_columns`` is a separate change, so a
wrong proof here is inert rather than corrupting.

Correctness — fail-safe by construction
---------------------------------------
* Eligibility requires that *every* reference to the column, anywhere in the
  plan, is a length-answerable operation. References are enumerated through the
  authoritative :func:`expression_roots` accessor, which cannot under-count — a
  column used raw anywhere (projected, grouped, joined, passed to any other
  function, compared to a non-empty literal) is disqualified.
* A missed *eligible* reference only forgoes the optimisation. A missed *raw*
  reference would be a correctness bug, which is why field enumeration is
  delegated to ``expression_roots`` rather than hand-picked per node type.
* Restricted to VARCHAR/VARBINARY. NVARCHAR is excluded because ``LENGTH`` on
  NVARCHAR is a *codepoint* count — ``draken_length`` scans the UTF-8 bytes for
  that type (function_kernels.cpp), so it is emphatically not length-only.
  ``IsEmpty``/``IsNotEmpty`` are length-only for every string type, but the
  rewriter that produces them already fires for VARCHAR/VARBINARY only, so the
  narrower restriction costs nothing and keeps one rule for the whole strategy.
* The Scan's own ``columns`` list (what it emits) is not a *use* — a column
  emitted but never read downstream is dead, not raw. Every genuine read shows
  up as an expression on some node and is classified there.

Length-answerable operations
----------------------------
Verified against the kernels, not assumed:

* ``IsEmpty`` / ``IsNotEmpty`` — ``draken_string_empty`` reads ``str_length``
  only, for every string type.
* ``LENGTH`` / ``CHAR_LENGTH`` / ``CHARACTER_LENGTH`` — ``draken_length``
  returns the stored length field directly for non-NVARCHAR types.
* ``OCTET_LENGTH`` — ``draken_octet_length`` returns the stored length field
  for every string type (it is a byte count by definition).

``col = ''`` / ``col <> ''`` and ``LENGTH(col) <op> 0|1`` do not need to be
listed: :func:`rewrite_string_empty_compare` has already normalised them into
``IsEmpty``/``IsNotEmpty`` by the time this strategy runs.

Extending to prefix-K
---------------------
The requirement is tracked per identity as a **byte count** (``0`` = length
only) rather than a boolean, so the natural follow-on — ``LEFT(col, n)``,
``STARTS_WITH(col, lit)``, prefix-only ``LIKE 'lit%'``, all of which need only
the first K bytes — slots in by contributing ``K`` instead of ``0`` and taking
the max across uses. Only the ``0`` bucket is emitted today; a non-zero bucket
becomes a second annotation when the decode side can honour it.
"""

from draken.draken_native import DrakenType

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner.node_expressions import expression_roots

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# Unary operators answerable from the stored length field alone.
_LENGTH_ONLY_UNARY_OPS = frozenset({"IsEmpty", "IsNotEmpty"})

# Functions answerable from the stored length field alone, for the physical
# types in _ELIGIBLE_PHYSICAL. Deliberately excludes NVARCHAR (codepoint scan).
_LENGTH_ONLY_FUNCTIONS = frozenset(
    {"LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH"}
)

# String types whose stored length is the byte length the operations above use.
_ELIGIBLE_PHYSICAL = frozenset({DrakenType.VARCHAR, DrakenType.VARBINARY})

# Bytes of payload a length-answerable use needs. Named rather than inlined so
# the prefix-K follow-on reads as a different value, not a magic literal.
_NEEDS_LENGTH_ONLY = 0


def _eligible_column(node):
    """Return ``node``'s schema column if it is a length-eligible column reference."""
    if node is None or node.node_type != NodeType.IDENTIFIER:
        return None
    column = node.schema_column
    if column is None or column.column_type is None:
        return None
    if column.column_type.physical not in _ELIGIBLE_PHYSICAL:
        return None
    return column


def _classify(expr, needs: dict, raw: set) -> None:
    """Walk one expression tree, classifying every column reference.

    A reference consumed by a length-answerable operation records its byte
    requirement in ``needs`` (identity -> max bytes needed); every other column
    reference is a disqualifying raw use recorded in ``raw``.
    """
    if expr is None:
        return

    if expr.node_type == NodeType.UNARY_OPERATOR and expr.value in _LENGTH_ONLY_UNARY_OPS:
        column = _eligible_column(expr.centre)
        if column is not None:
            needs[column.identity] = max(
                needs.get(column.identity, _NEEDS_LENGTH_ONLY), _NEEDS_LENGTH_ONLY
            )
            # The operand is fully accounted for — descending would re-count it
            # as a raw use.
            return

    if expr.node_type == NodeType.FUNCTION and expr.value in _LENGTH_ONLY_FUNCTIONS:
        parameters = expr.parameters or []
        if len(parameters) == 1:
            column = _eligible_column(parameters[0])
            if column is not None:
                needs[column.identity] = max(
                    needs.get(column.identity, _NEEDS_LENGTH_ONLY), _NEEDS_LENGTH_ONLY
                )
                return

    if expr.node_type == NodeType.IDENTIFIER and expr.schema_column is not None:
        raw.add(expr.schema_column.identity)
        return

    _classify(expr.left, needs, raw)
    _classify(expr.centre, needs, raw)
    _classify(expr.right, needs, raw)
    for parameter in expr.parameters or []:
        _classify(parameter, needs, raw)


class LengthOnlyColumnStrategy(OptimizationStrategy):
    # Runs last: every strategy that can add, remove or rewrite a column
    # reference must have had its say before uses are enumerated.
    requires = ("projection-pushed",)
    provides = ("length-only-analysed",)

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node is None:
            return context
        context.optimized_plan.add_node(context.node_id, LogicalPlanNode(**node.properties))
        if context.parent_nid:
            # Re-adding the edge must preserve its relationship: a join leg label
            # records which side of the parent join this branch feeds.
            context.optimized_plan.add_edge(
                context.node_id,
                context.parent_nid,
                context.pre_optimized_tree.relationship(context.node_id, context.parent_nid),
            )
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        needs: dict = {}
        raw: set = set()

        for _, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.Scan:
                # A Scan's `columns` is its emit list, not a read — a column
                # emitted and never consumed is dead, not raw. Its pushed
                # `predicates` (and any other expression it carries, e.g. a
                # pushed top-N spec) are real uses and must be classified, so
                # they are reached through expression_roots rather than by
                # naming predicates directly.
                exclude = ("columns",)
            elif node.node_type in (
                LogicalPlanStepType.AggregateAndGroup,
                LogicalPlanStepType.Aggregate,
            ):
                # An aggregate's `columns` is a derived input-column list
                # (redundant with groups/aggregates); it lists the column as a
                # bare identifier, which is read bookkeeping, not a raw use.
                exclude = ("columns",)
            else:
                exclude = ()
            for root in expression_roots(node, exclude=exclude):
                _classify(root, needs, raw)

        eligible = {
            identity
            for identity, byte_need in needs.items()
            if identity not in raw and byte_need == _NEEDS_LENGTH_ONLY
        }
        if not eligible:
            return plan

        # Annotate each Scan with the eligible columns it actually emits.
        for _, node in plan.nodes(True):
            if node.node_type != LogicalPlanStepType.Scan or node.schema is None:
                continue
            owned = {
                column.identity
                for column in (node.schema.columns or [])
                if column.identity in eligible
            }
            if owned:
                node.length_only_columns = owned
                self.telemetry.optimization_length_only_columns += len(owned)

        return plan
