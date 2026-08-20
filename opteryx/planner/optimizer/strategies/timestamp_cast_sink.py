# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Timestamp Cast Sink Strategy
============================

Goal — eliminate a redundant per-row buffer pass for the common shape

    <int64 column>::TIMESTAMP[unit]

When an int64-stored column is referenced *only* as ``CAST(col AS TIMESTAMP[unit])``
with a single consistent unit, the cast is a pure logical retag: INT64 and
TIMESTAMP64 share the same 8-byte payload, and ``[s]``/``[ms]``/``[us]``/``[ns]``
keep the integer verbatim (only the unit *tag* differs — verified against the
``draken_cast_int64_to_timestamp`` kernel, which copies verbatim for these
units). So instead of decoding the column as int64 and then running a cast pass,
we retype the *scan output* to TIMESTAMP64 with the cast's unit. The now-
``TIMESTAMP64`` operand makes the ``CAST`` resolve to identity (``resolve_cast``
returns the identity kernel for a TIMESTAMP64 source), so no per-row cast runs.

Retyping the scan output only works if the reader that serves the scan honours a
scan-declared type it does not find in the file. That is a *reader capability*,
declared as ``BaseTable.supports_int64_timestamp_retag`` — parquet and skene
implement it, and each does so as a strict allowlist of this one verbatim retag.
A reader that decodes exactly what its footer declares does not, and its columns
are not retyped here: the cast stays an ordinary cast. Retyping under a reader
that cannot retag does not produce a slow query, it produces a plan and a reader
that disagree about a column's type — which is precisely what the skene scan's
schema guard exists to catch, and did.

Correctness — this is fail-safe by construction:

* Eligibility requires that the column is emitted by a Scan whose connector
  declares the retag capability. A column with no Scan producer at all — a
  projection alias, a CTE, a function dataset — is therefore never eligible:
  nothing downstream would perform the retag the retyping assumes.
* Eligibility requires that *every* reference to the column is the same
  pure-retag cast. References are enumerated through the authoritative
  :func:`expression_roots` accessor (it cannot under-count), so a column used
  raw anywhere — including passed through a projection — is disqualified.
* A column carried in a pushed-down scan *predicate* is disqualified outright:
  predicates are normalised against the int64 representation, and retyping under
  one is out of scope here.
* The Scan node's own ``columns`` list (what it emits) is *not* a use — it is
  the projection list we are retyping — so it is excluded from the raw-use scan.

If a reference is missed by the analysis the only outcome is that the cast stays
an ordinary cast (unoptimised) — never a mistyped column.
"""

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import LogicalType
from draken.draken_native import TimestampUnit

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner.node_expressions import expression_roots
from opteryx.types.logical_type import ColumnType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

# CAST target-type strings (node.value) that are pure, verbatim INT64→TIMESTAMP64
# retags, mapped to the timestamp unit they tag with. ``_TIMESTAMP_DAYS`` is
# deliberately absent: it scales (days→µs) and so is NOT a verbatim retag.
_RETAG_TIMESTAMP_UNITS = {
    "_TIMESTAMP_S": TimestampUnit.SECONDS,
    "_TIMESTAMP_MS": TimestampUnit.MILLISECONDS,
    "_TIMESTAMP_US": TimestampUnit.MICROSECONDS,
    "_TIMESTAMP_NS": TimestampUnit.NANOSECONDS,
}


def _classify(expr, casted: dict, raw: set) -> None:
    """Walk one expression tree, classifying every column reference.

    A reference that is the direct operand of a pure-retag INT64→TIMESTAMP cast
    is recorded in ``casted`` (identity -> set of units seen); any other column
    reference is recorded in ``raw`` (a disqualifying raw use).
    """
    if expr is None:
        return

    if expr.node_type == NodeType.CAST:
        unit = _RETAG_TIMESTAMP_UNITS.get(expr.value)
        operand = expr.left
        if (
            unit is not None
            and operand is not None
            and operand.node_type == NodeType.IDENTIFIER
            and operand.schema_column is not None
            and operand.schema_column.column_type is not None
            and operand.schema_column.column_type.physical == DrakenType.INT64
        ):
            casted.setdefault(operand.schema_column.identity, set()).add(unit)
            # The operand is fully accounted for by this cast — do not descend
            # into it, or it would also be counted as a raw use.
            return

    if expr.node_type == NodeType.IDENTIFIER and expr.schema_column is not None:
        raw.add(expr.schema_column.identity)
        return

    _classify(expr.left, casted, raw)
    _classify(expr.centre, casted, raw)
    _classify(expr.right, casted, raw)
    for parameter in expr.parameters or []:
        _classify(parameter, casted, raw)


class TimestampCastSinkStrategy(OptimizationStrategy):
    # Run after projection/predicate pushdown so scan.columns and scan.predicates
    # are settled — eligibility reads the pushed predicates.
    requires = ("projection-pushed",)

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
        casted: dict = {}
        raw: set = set()
        # identity -> every emitting Scan's connector declares the retag. Absent
        # means no Scan emits it at all, which is a hard decline (see below).
        retag_capable: dict = {}

        for _, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.Scan:
                # Whether this scan's reader will honour a declared TIMESTAMP64
                # on an int64-stored column. A scan with no connector (or one
                # that does not declare the capability) disqualifies every
                # column it emits — including a column another, capable scan
                # also emits, which is why this ANDs rather than overwrites.
                capable = bool(
                    node.connector is not None
                    and node.connector.supports_int64_timestamp_retag
                )
                if node.schema is not None:
                    for col in node.schema.columns or []:
                        retag_capable[col.identity] = (
                            retag_capable.get(col.identity, True) and capable
                        )
                # The emit-column list is not a use; pushed predicates are (and
                # disqualify the column from retyping).
                for predicate in node.predicates or []:
                    for ident in get_all_nodes_of_type(predicate, (NodeType.IDENTIFIER,)):
                        if ident.schema_column is not None:
                            raw.add(ident.schema_column.identity)
            else:
                # An aggregate's ``columns`` is a derived input-column list
                # (redundant with groups/aggregates); it lists the column as a
                # bare identifier, which is read bookkeeping, not a raw use.
                exclude = (
                    ("columns",)
                    if node.node_type
                    in (LogicalPlanStepType.AggregateAndGroup, LogicalPlanStepType.Aggregate)
                    else ()
                )
                for root in expression_roots(node, exclude=exclude):
                    _classify(root, casted, raw)

        # Eligible: emitted by a retag-capable Scan, cast-only use, single
        # consistent unit, never used raw.
        eligible: dict = {}
        for identity, units in casted.items():
            # Default False: an identity no Scan emits has no reader to perform
            # the retag, so retyping it would mistype it.
            if not retag_capable.get(identity, False):
                continue
            if identity in raw:
                continue
            if len(units) != 1:
                continue
            eligible[identity] = ColumnType(
                physical=DrakenType.TIMESTAMP64,
                logical=LogicalType(
                    kind=LogicalKind.TIMESTAMP,
                    unit=next(iter(units)),
                    offset_minutes=0,
                ),
            )

        if not eligible:
            return plan

        # Retype every schema-column object carrying an eligible identity — the
        # scan's emitted column (so the reader retags it) and every cast operand
        # (so the cast resolves to identity). These are usually the same shared
        # object, but updating all is robust to any copy that broke sharing.
        for _, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.Scan and node.schema is not None:
                for col in node.schema.columns or []:
                    if col.identity in eligible:
                        col.column_type = eligible[col.identity]
            for root in expression_roots(node):
                if root.node_type == NodeType.IDENTIFIER and root.schema_column is not None:
                    if root.schema_column.identity in eligible:
                        root.schema_column.column_type = eligible[root.schema_column.identity]
                else:
                    for ident in get_all_nodes_of_type(root, (NodeType.IDENTIFIER,)):
                        if ident.schema_column is not None and ident.schema_column.identity in eligible:
                            ident.schema_column.column_type = eligible[ident.schema_column.identity]

        self.telemetry.optimization_timestamp_cast_sink = (
            getattr(self.telemetry, "optimization_timestamp_cast_sink", 0) + len(eligible)
        )
        return plan
